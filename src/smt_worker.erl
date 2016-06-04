-module(smt_worker).

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
        server_name,
        interval,
        conn_pid,
        expected_addr,
        real_addr,
        params,
        mstate = dict:new(),
        retry_times,
        interval_ref}).

-compile(export_all).


retry_policy() -> [1000, 1000, 1000, 1000, 1000, 1000, 5000, 5000, 5000, 60000].


%%%===================================================================
%%% API
%%%===================================================================

start_link(Pool, Addr, Params, Interval) ->
    gen_server:start_link(?MODULE, [Pool, Addr, Params, Interval], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Addr, Params, Interval]) ->
    erlang:send_after(10, self(), try_to_connect),
    {ok, #state{interval=Interval, server_name=Pool, params=Params, expected_addr=Addr, retry_times=0}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({set_interval, NewInterval}, State=#state{interval_ref=undefined}) ->
    error_logger:info_msg("issue=set_interval, new_interval=~p", [NewInterval]),
    {noreply, State#state{interval=NewInterval}};
handle_cast({set_interval, NewInterval}, State=#state{interval_ref=IntervalRef}) ->
    error_logger:info_msg("issue=set_interval, new_interval=~p", [NewInterval]),
    {ok, cancel} = timer:cancel(IntervalRef),
    {ok, NewIntervalRef} = timer:send_interval(NewInterval, self(), flush),
    {noreply, State#state{interval=NewInterval, interval_ref=NewIntervalRef}};
handle_cast(_Msg, State) ->
    {noreply, State}.

retry_interval(Retries) when is_integer(Retries) ->
    try
        lists:nth(Retries+1, retry_policy())
    catch _:_ ->
          lists:last(retry_policy())
    end.

handle_info(try_to_connect, #state{interval=Interval, params=Params, expected_addr=Addr, retry_times=Retries}=State) ->
    case try_to_connect_to(Params, Addr) of
        {ok, ConnPid, RealAddr} ->
            {ok, _} = timer:send_interval(1000, self(), keep_alive),
            {ok, IntervalRef} = timer:send_interval(Interval, self(), flush),
            mysql:prepare(ConnPid, show_status, show_status_query()),
            {noreply, State#state{conn_pid=ConnPid, interval_ref=IntervalRef, real_addr=RealAddr}};
        {error, Reason} ->
            erlang:send_after(retry_interval(Retries), self(), try_to_connect),
            error_logger:info_msg("issue=retry_to_connect, expected_addr=~p, reason=~p", [Addr, Reason]),
            {noreply, State#state{retry_times=Retries+1}}
    end;
handle_info(keep_alive, #state{conn_pid=ConnPid}=State) ->
    mysql:query(ConnPid, <<"SELECT 1">>, []),
    {noreply, State#state{}};
handle_info(flush, #state{server_name=Pool, real_addr=Addr, mstate=MState, conn_pid=ConnPid}=State) ->
    receive_all_flushes(),
    MState2 = flush_graphite(Pool, Addr, ConnPid, MState),
    {noreply, State#state{mstate=MState2}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
flush_graphite(Pool, Addr, ConnPid, MState) ->
    Timestamp = now_to_milliseconds(now()),
    Res = mysql:execute(ConnPid, show_status, []),
    RawMetrics = result_packet_to_proplist(Res),
    MState2 = update_interval(MState),
    {Metrics, MState3} = calculate_values(RawMetrics, [], MState2),
    Format = format(Pool, Addr),
    TimestampSeconds = Timestamp div 1000,
    Output = [graphite_metric(graphite_metric_name(Format, Name), Value, TimestampSeconds)
              || {Name, Value} <- Metrics],
    {ok, GraphiteHost} = application:get_env(smt, graphite_host),
    {ok, GraphitePort} = application:get_env(smt, graphite_port),
    {ok, Socket} = gen_tcp:connect(GraphiteHost, GraphitePort, []),
    gen_tcp:send(Socket, Output),
    gen_tcp:close(Socket),
    error_logger:info_msg("issue=flushed_metrics, metrics_flushed=~p, graphite_format=~p, graphite_host=~p:~p",
                          [length(Metrics), Format, GraphiteHost, GraphitePort]),
    maybe_spawn_retaliation_finder(Format, Metrics, MState3).

format(Pool, Addr) ->
    {ok, Format} = application:get_env(smt, graphite_format),
    format(Format, Pool, Addr).

format(Format, Pool, Addr) ->
   Format1 = re:replace(Format, "\\$POOL", atom_to_list(Pool), [global, {return, list}]),
   re:replace(Format1, "\\$ADDR", binary_to_list(Addr), [global, {return, list}]).

graphite_metric_name(Format, Name) ->
   re:replace(Format, "\\$NAME", Name, [global, {return, list}]).

graphite_metric(Name, Value, TimestampSeconds) ->
    io_lib:format("~s ~s ~B~n", [Name, Value, TimestampSeconds]).

now_to_milliseconds({Mega,Seconds,MicroSeconds}) ->
    Milliseconds = MicroSeconds div 1000,
    FullSeconds = 1000000*Mega+Seconds,
    FullSeconds * 1000 + Milliseconds.

result_packet_to_proplist({ok, _ColumnNames, Rows}) ->
    merge(variables(), Rows).


%% Convert a list of rows into a proplist.
%% Filter unknown rows.
merge([], _) ->
    [];
merge(_, []) ->
    [];
merge([Name|Names], [[Name, Value]|Rows]) ->
    [{Name, Value}|merge(Names, Rows)];
merge([Name|_]=Names, [[RowName|_]|Rows]) when Name > RowName ->
    %% Skip unrelated to export row
    merge(Names, Rows);
merge([_|Names], Rows) ->
    %% Skip unsupported by server metric
    merge(Names, Rows).

variables() ->
    %% Should be sorted.
    [<<"Aborted_clients">>,
     <<"Aborted_connects">>,
     <<"Bytes_received">>,
     <<"Bytes_sent">>,
     <<"Connections">>,
     <<"Innodb_buffer_pool_pages_data">>,
     <<"Innodb_buffer_pool_pages_dirty">>,
     <<"Innodb_buffer_pool_pages_flushed">>,
     <<"Innodb_buffer_pool_pages_free">>,
     <<"Innodb_buffer_pool_pages_misc">>,
     <<"Innodb_buffer_pool_pages_total">>,
     <<"Innodb_buffer_pool_read_ahead">>,
     <<"Innodb_buffer_pool_read_ahead_evicted">>,
     <<"Innodb_buffer_pool_read_ahead_rnd">>,
     <<"Innodb_buffer_pool_read_requests">>,
     <<"Innodb_buffer_pool_reads">>,
     <<"Innodb_buffer_pool_wait_free">>,
     <<"Innodb_buffer_pool_write_requests">>,
     <<"Innodb_checkpoint_age">>,
     <<"Innodb_checkpoint_max_age">>,
     <<"Innodb_data_fsyncs">>,
     <<"Innodb_data_pending_fsyncs">>,
     <<"Innodb_data_pending_reads">>,
     <<"Innodb_data_pending_writes">>,
     <<"Innodb_data_read">>,
     <<"Innodb_data_reads">>,
     <<"Innodb_data_writes">>,
     <<"Innodb_data_written">>,
     <<"Innodb_dblwr_pages_written">>,
     <<"Innodb_dblwr_writes">>,
     <<"Innodb_log_waits">>,
     <<"Innodb_log_write_requests">>,
     <<"Innodb_log_writes">>,
     <<"Innodb_os_log_fsyncs">>,
     <<"Innodb_os_log_pending_fsyncs">>,
     <<"Innodb_os_log_pending_writes">>,
     <<"Innodb_os_log_written">>,
     <<"Innodb_pages_created">>,
     <<"Innodb_pages_read">>,
     <<"Innodb_pages_written">>,
     <<"Innodb_rows_deleted">>,
     <<"Innodb_rows_inserted">>,
     <<"Innodb_rows_read">>,
     <<"Innodb_rows_updated">>,
     <<"Memory_used">>,
     <<"Queries">>,
     <<"Select_range">>,
     <<"Select_range_check">>,
     <<"Select_scan">>,
     <<"Slow_queries">>,
     <<"Sort_range">>,
     <<"Sort_rows">>,
     <<"Threads_connected">>].

calculate_values([{Name, RawValue}|RawMetrics], Metrics, MState) ->
    {Metrics2, MState2} = calculate_value(Name, RawValue, MState),
    calculate_values(RawMetrics, Metrics ++ Metrics2, MState2);
calculate_values([], Metrics, MState) ->
    {Metrics, MState}.
    
calculate_value(Name, RawValue, MState) ->
    case Name of
        <<"Aborted_clients">> ->
            events_per_second(Name, RawValue, MState);
        <<"Aborted_connects">> ->
            events_per_second(Name, RawValue, MState);
        <<"Bytes_received">> ->
            bytes_per_second(Name, RawValue, MState);
        <<"Bytes_sent">> ->
            bytes_per_second(Name, RawValue, MState);
         <<"Innodb_buffer_pool_read", _/binary>> -> 
            events_per_second(Name, RawValue, MState);
         <<"Innodb_buffer_pool_write_requests">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_buffer_pool_pages_flushed">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_data_fsyncs">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_data_read">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_data_reads">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_data_writes">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_data_written">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_dblwr_pages_written">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_dblwr_writes">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_log_write_requests">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_log_writes">> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_os_log_", _/binary>> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_pages_", _/binary>> ->
            events_per_second(Name, RawValue, MState);
        <<"Innodb_rows_", _/binary>> ->
            events_per_second(Name, RawValue, MState);
        <<"Queries">> ->
            events_per_second(Name, RawValue, MState);
        <<"Select_", _/binary>> ->
            events_per_second(Name, RawValue, MState);
        <<"Slow_queries">> ->
            events_per_second(Name, RawValue, MState);
        <<"Sort_range">> ->
            events_per_second(Name, RawValue, MState);
        <<"Sort_rows">> ->
            events_per_second(Name, RawValue, MState);
        _ ->
            {[{Name, RawValue}], MState}
    end.

events_per_second(Name, RawValue, MState) ->
    calc_difference(Name, RawValue, MState).

bytes_per_second(Name, RawValue, MState) ->
    calc_difference(Name, RawValue, MState).

calc_difference(Name, RawValue, MState) ->
    NewValue = bin_to_int(RawValue),
    case dict:find(Name, MState) of
        error ->
            %% Save initial value
            {[], dict:store(Name, NewValue, MState)};
        {ok, OldValue} when OldValue > NewValue ->
            %% Skip negative diff
            {[], dict:store(Name, NewValue, MState)};
        {ok, OldValue} ->
            Interval = get_interval(MState),
            {[{Name, calc_difference2(Name, NewValue, OldValue, Interval)}],
             dict:store(Name, NewValue, MState)}
    end.

calc_difference2(_Name, NewValue, OldValue, Interval) ->
    print_float(((NewValue - OldValue) / Interval) * 1000).

print_int(Value) when is_integer(Value) ->
    list_to_binary(integer_to_list(Value)).

print_float(Value) when is_float(Value) ->
    print_int(round(Value)).

bin_to_int(Value) when is_binary(Value) ->
    list_to_integer(binary_to_list(Value)).

update_interval(MState) ->
    NewQueryTime = now_to_milliseconds(now()),
    case dict:find(query_time, MState) of
        error ->
            dict:store(query_time, NewQueryTime, MState);
        {ok, OldQueryTime} ->
            dict:store(query_time, NewQueryTime,
                dict:store(interval_ms, NewQueryTime - OldQueryTime, MState))
    end.

%% Interval in milliseconds
get_interval(MState) ->
    dict:fetch(interval_ms, MState).

receive_all_flushes() ->
    receive
        flush -> receive_all_flushes()
    after 0 ->
          ok
    end.


try_to_connect_to(Params, Addr) ->
    error_logger:info_msg("issue=mysql:start_link, module=smt_worker", []),
    Res = mysql:start_link(Params),
    try_to_connect_to2(Res, Addr).

try_to_connect_to2({error, Reason}, _Addr) ->
    {error, Reason};
try_to_connect_to2({ok, ConnPid}, undefined) ->
    %% Do not validate addr because expected_addr=undefined
    Addr = my_address(ConnPid),
    {ok, ConnPid, Addr};
try_to_connect_to2({ok, ConnPid}, Addr) ->
    case my_address(ConnPid) of
        Addr ->
            {ok, ConnPid, Addr};
        OtherAddr ->
            error_logger:info_msg("issue=try_to_connect_to:bad_addr, expected_addr=~p, conn_addr=~p",
                                  [Addr, OtherAddr]),
            shutdown_process(ConnPid),
            {error, badaddr}
    end.
            

shutdown_process(Pid) ->
    MonRef = erlang:monitor(process, Pid),
    erlang:unlink(Pid),
    erlang:exit(Pid, shutdown),
    receive
        {'DOWN', MonRef, process, Pid, _} ->
            ok
    after 5000 ->
        erlang:exit(Pid, kill)
    end.

my_address(ConnPid) ->
    {ok, _ColumnNames, Rows} = mysql:query(ConnPid, <<"SHOW VARIABLES WHERE Variable_name = 'hostname' OR Variable_name = 'port'">>, []),
    [[<<"hostname">>, Host], [<<"port">>, Port]] = lists:sort(Rows),
    <<Host/binary, ":", Port/binary>>.


maybe_spawn_retaliation_finder(_Format, [], MState) ->
    MState;
maybe_spawn_retaliation_finder(Format, [{Name,_}|_], MState) ->
    MetricName = erlang:iolist_to_binary(graphite_metric_name(Format, Name)),
    case dict:find(retaliation_finder_spawned, MState) of
        error ->
            Worker = self(),
            Pid = spawn(fun() -> find_retaliation(Worker, MetricName) end),
            dict:store(retaliation_finder_spawned, Pid, MState);
        {ok, _} ->
            MState
    end.

find_retaliation(Worker, MetricName) ->
    try
        {ok, GraphiteAddr} = application:get_env(smt, graphite_url),
        MetricName2 = http_uri:encode(binary_to_list(MetricName)),
        httpc:request(GraphiteAddr ++ "/render/?from=-10minutes&target=" ++ MetricName2 ++ "&format=json")
    of
        {ok,{{_,200,_},_,"[]"}} ->
            error_logger:info_msg("issue=find_retaliation:not_found, metric_name=~p", [MetricName]),
            timer:sleep(5000),
            find_retaliation(Worker, MetricName);
        {ok,{{_,200,_},_,Body2}} ->
            [Data] = jsx:decode(list_to_binary(Body2)),
            Datapoints = proplists:get_value(<<"datapoints">>, Data),
            Timestamps = lists:usort([Timestamp || [_Value, Timestamp] <- Datapoints]),
            [A,B|_] = Timestamps,
            Interval = B - A, %% seconds
            gen_server:cast(Worker, {set_interval, Interval*1000})
    catch ErrorClass:ErrorReason ->
        error_logger:error_msg("issue=find_retaliation:failed, reason=~p:~p", [ErrorClass, ErrorReason])
    end.


show_status_query() ->
    [VarsH|VarsT] = variables(),
    Where = erlang:iolist_to_binary(["\"", VarsH, "\"", [[", \"", X, "\" "] || X <- VarsT]]), 
    <<"SHOW GLOBAL STATUS WHERE Variable_name IN (", Where/binary, ")">>.

