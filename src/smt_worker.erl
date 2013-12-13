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
        interval = 15000}).

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
    emysql:prepare(sdt_show_status, <<"SHOW /*!50002 GLOBAL */ STATUS">>),
    {ok, #state{interval=Interval, host=Host}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush, #state{interval=Interval, host=Host}=State) ->
    flush_graphite(Host),
    erlang:send_after(Interval, self(), flush),
    {noreply, State#state{}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
flush_graphite(Host) ->
    Timestamp = now_to_seconds(now()),
    Packet = emysql:execute(sdt_mysql_pool, sdt_show_status, []),
    Metrics = result_packet_to_proplist(Packet),
    Prefix = "smt." ++ Host ++ ".",
    Output = [graphite_metric(Prefix, Name, Value, Timestamp)
              || {Name, Value} <- Metrics],
    {ok, Socket} = gen_tcp:connect("10.100.0.70", 2003, []),
    gen_tcp:send(Socket, Output),
    gen_tcp:close(Socket),
    error_logger:info_msg("Flushed ~B metrics.", [length(Metrics)]),
    ok.

graphite_metric(Prefix, Name, Value, Timestamp) ->
    io_lib:format("~s~s ~s ~B~n", [Prefix, Name, Value, Timestamp]).

now_to_seconds({Mega,Seconds,_}) ->
    1000000*Mega+Seconds.

result_packet_to_proplist(#result_packet{rows=Rows}) ->
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
     <<"Innodb_row_lock_current_waits">>,
     <<"Innodb_row_lock_time">>,
     <<"Innodb_row_lock_time_avg">>,
     <<"Innodb_row_lock_time_max">>,
     <<"Innodb_row_lock_waits">>,
     <<"Innodb_rows_deleted">>,
     <<"Innodb_rows_inserted">>,
     <<"Innodb_rows_read">>,
     <<"Innodb_rows_updated">>,
     <<"Queries">>,
     <<"Select_range">>,
     <<"Select_range_check">>,
     <<"Select_scan">>,
     <<"Slow_queries">>].

