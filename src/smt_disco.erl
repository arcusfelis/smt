-module(smt_disco).

-behaviour(gen_server).

%% API
-export([start_link/3]).

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
        params,
        mstate = dict:new()}).

-compile(export_all).


%%%===================================================================
%%% API
%%%===================================================================

start_link(Pool, Params, Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Pool, Params, Interval], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Params, Interval]) ->
    erlang:send_after(10, self(), try_to_connect),
    {ok, #state{interval=Interval, server_name=Pool, params=Params}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(try_to_connect, #state{interval=Interval, params=Params}=State) ->
    {ok, ConnPid} = mysql:start_link(Params),
    timer:send_interval(Interval, self(), flush),
    {noreply, State#state{conn_pid=ConnPid}};
handle_info(flush, #state{server_name=Pool, params=Params, mstate=MState, conn_pid=ConnPid}=State) ->
    receive_all_flushes(),
    MState2 = discover(Pool, ConnPid, Params, MState),
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
discover(Pool, ConnPid, Params, MState) ->
    Res = mysql:query(ConnPid, <<"show global status like 'wsrep_incoming_addresses'">>, []),
    Servers = result_packet_to_proplist(ConnPid, Res),
    start_stop_workers(Pool, Servers, Params),
    MState.

start_stop_workers(Pool, Servers, Params) ->
    Started = [Addr
               || { {smt_worker, Pool1, Addr}, _Pid, _, _}
                  <- supervisor:which_children(smt_sup),
                  Pool =:= Pool1],
    ToStop = [Addr || Addr <- Started, not lists:member(Addr, Servers)],
    ToStart = [Addr || Addr <- Servers, not lists:member(Addr, Started)],
    [stop_worker(Pool, Addr) || Addr <- ToStop],
    [start_worker(Pool, Addr, Params) || Addr <- ToStart],
    ok.

stop_worker(Pool, Addr) ->
    supervisor:terminate_child(smt_sup, {smt_worker, Pool, Addr}),
    supervisor:delete_child(smt_sup, {smt_worker, Pool, Addr}),
    ok.

start_worker(Pool, Addr, Params) ->
    supervisor:start_child(smt_sup, smt_worker(Pool, Addr, Params)).

smt_worker(Pool, Addr, Params) ->
    {{smt_worker, Pool},
     {smt_worker, start_link, [Pool, Addr, Params, 10000]},
        permanent, 5000, worker, [smt_worker]}.


result_packet_to_proplist(ConnPid, {ok, _ColumnNames, []}) ->
    [my_address(ConnPid)];
result_packet_to_proplist(_, {ok, _ColumnNames, [[<<"wsrep_incoming_addresses">>, Addrs]]}) ->
    binary:split(Addrs, <<",">>, [global]).

my_address(ConnPid) ->
    {ok, _ColumnNames, Rows} = mysql:query(ConnPid, <<"SHOW VARIABLES WHERE Variable_name = 'hostname' OR Variable_name = 'port'">>, []),
    [[<<"hostname">>, Host], [<<"port">>, Port]] = lists:sort(Rows),
    <<Host/binary, ":", Port/binary>>.


%% Interval in seconds
get_interval(MState) ->
    dict:fetch(interval, MState).

receive_all_flushes() ->
    receive
        flush -> receive_all_flushes()
    after 0 ->
          ok
    end.
