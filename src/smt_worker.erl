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
        params,
        mstate = dict:new()}).

-compile(export_all).


%%%===================================================================
%%% API
%%%===================================================================

start_link(Pool, Addr, Params, Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Pool, Addr, Params, Interval], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Addr, Params, Interval]) ->
    erlang:send_after(10, self(), try_to_connect),
    {ok, #state{interval=Interval, server_name=Pool, params=Params, expected_addr=Addr}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(try_to_connect, #state{interval=Interval, params=Params, expected_addr=Addr}=State) ->
    case try_to_connect_to(Params, Addr) of
        {ok, ConnPid} ->
            timer:send_interval(Interval, self(), flush),
            {noreply, State#state{conn_pid=ConnPid}};
        {error, Reason} ->
            erlang:send_after(1000, self(), try_to_connect),
            error_logger:info_msg("issue=retry_to_connect, expected_addr=~p, reason=~p", [Addr, Reason]),
            {noreply, State#state{}}
    end;
handle_info(flush, #state{server_name=Pool, expected_addr=Addr, mstate=MState, conn_pid=ConnPid}=State) ->
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
    Timestamp = now_to_seconds(now()),
    Res = mysql:query(ConnPid, <<"SHOW /*!50002 GLOBAL */ STATUS">>, []),
    RawMetrics = result_packet_to_proplist(Res),
    MState2 = update_interval(MState),
    {Metrics, MState3} = calculate_values(RawMetrics, [], MState2),
    Prefix = "smt." ++ atom_to_list(Pool) ++ "." ++ binary_to_list(Addr) ++ ".",
    Output = [graphite_metric(Prefix, Name, Value, Timestamp)
              || {Name, Value} <- Metrics],
    {ok, GraphiteHost} = application:get_env(smt, graphite_host),
    {ok, GraphitePort} = application:get_env(smt, graphite_port),
    {ok, Socket} = gen_tcp:connect(GraphiteHost, GraphitePort, []),
    gen_tcp:send(Socket, Output),
    gen_tcp:close(Socket),
    error_logger:info_msg("Flushed ~B metrics.", [length(Metrics)]),
    MState3.

graphite_metric(Prefix, Name, Value, Timestamp) ->
    io_lib:format("~s~s ~s ~B~n", [Prefix, Name, Value, Timestamp]).

now_to_seconds({Mega,Seconds,_}) ->
    1000000*Mega+Seconds.

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
     <<"Queries">>,
     <<"Select_range">>,
     <<"Select_range_check">>,
     <<"Select_scan">>,
     <<"Slow_queries">>].

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
    print_float((NewValue - OldValue) / Interval).

print_int(Value) when is_integer(Value) ->
    list_to_binary(integer_to_list(Value)).

print_float(Value) when is_float(Value) ->
    print_int(round(Value)).

bin_to_int(Value) when is_binary(Value) ->
    list_to_integer(binary_to_list(Value)).

update_interval(MState) ->
    NewQueryTime = now_to_seconds(now()),
    case dict:find(query_time, MState) of
        error ->
            dict:store(query_time, NewQueryTime, MState);
        {ok, OldQueryTime} ->
            dict:store(query_time, NewQueryTime,
                dict:store(interval, NewQueryTime - OldQueryTime, MState))
    end.

%% Interval in seconds
get_interval(MState) ->
    dict:fetch(interval, MState).

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
try_to_connect_to2({ok, ConnPid}, Addr) ->
    case my_address(ConnPid) of
        Addr ->
            {ok, ConnPid};
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
