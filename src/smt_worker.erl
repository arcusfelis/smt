-module(smt_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
        host,
        interval,
        mstate = dict:new()}).

-include_lib("emysql/include/emysql.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link(Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Interval], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Interval]) ->
    {ok, Host} = inet:gethostname(),
    erlang:send_after(Interval, self(), flush),
    emysql:prepare(sdt_show_global_status, <<"SHOW /*!50002 GLOBAL */ STATUS">>),
    emysql:prepare(sdt_show_innodb_status, <<"SHOW /*!50000 ENGINE*/ INNODB STATUS">>),
    {ok, #state{interval=Interval, host=Host}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush, #state{interval=Interval, host=Host, mstate=MState}=State) ->
    MState2 = flush_graphite(Host, MState),
    erlang:send_after(Interval, self(), flush),
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
flush_graphite(Host, MState) ->
    Timestamp = now_to_seconds(now()),
    GlobPacket = emysql:execute(sdt_mysql_pool, sdt_show_global_status, []),
    InnoPacket = emysql:execute(sdt_mysql_pool, sdt_show_innodb_status, []),
    RawMetrics = result_packet_to_proplist(GlobPacket)
              ++ innodb_result_packet_to_proplist(InnoPacket),
    MState2 = update_interval(MState),
    {Metrics, MState3} = calculate_values(RawMetrics, [], MState2),
    Prefix = "smt." ++ Host ++ ".",
    Output = [graphite_metric(Prefix, Name, Value, Timestamp)
              || {Name, Value} <- Metrics],
    {ok, Socket} = gen_tcp:connect("10.100.0.70", 2003, []),
    gen_tcp:send(Socket, Output),
    gen_tcp:close(Socket),
    error_logger:info_msg("Flushed ~B metrics.", [length(Metrics)]),
    MState3.

graphite_metric(Prefix, Name, Value, Timestamp) ->
    io_lib:format("~s~s ~s ~B~n", [Prefix, Name, Value, Timestamp]).

now_to_seconds({Mega,Seconds,_}) ->
    1000000*Mega+Seconds.

result_packet_to_proplist(#result_packet{rows=Rows}) ->
    merge(variables(), Rows).

innodb_result_packet_to_proplist(#error_packet{msg=Msg}) ->
    error_logger:error_msg("~p", [Msg]), 
    [];
innodb_result_packet_to_proplist(#result_packet{rows=Rows}) ->
    Status = innodb_rows_to_status(Rows),
    [{<<"Innodb_lsn">>, innodb_lsn(Status)},
     {<<"Innodb_log_flushed">>, innodb_log_flushed(Status)},
     {<<"Innodb_last_checkpoint">>, innodb_checkpoint(Status)}].

innodb_rows_to_status([[_Type, _Name, Status]]) ->
    Status.

innodb_lsn(Status) ->
    {match, [_, LSN]} =
        re:run(Status,
            <<"Log sequence number\s+(\\d+) (\\d+)">>,
            [{capture, all_but_first, binary}]),
    LSN.

innodb_log_flushed(Status) ->
    {match, [_, LSN]} =
        re:run(Status,
            <<"Log flushed up to\s+(\\d+) (\\d+)">>,
            [{capture, all_but_first, binary}]),
    LSN.

innodb_checkpoint(Status) ->
    {match, [_, LSN]} =
        re:run(Status,
            <<"Last checkpoint at\s+(\\d+) (\\d+)">>,
            [{capture, all_but_first, binary}]),
    LSN.

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
    [<<"Bytes_received">>,
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
            {[{Name, print_float((NewValue - OldValue) / get_interval(MState))}],
             dict:store(Name, NewValue, MState)}
    end.

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
