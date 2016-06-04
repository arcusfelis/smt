
-module(smt_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, DiscoHosts} = application:get_env(smt, mysql_disco_hosts),
    {ok, Hosts} = application:get_env(smt, mysql_hosts),
    Specs1 = [ smt_worker(PoolName, Params) || {PoolName, Params} <- Hosts ],
    Specs2 = [ smt_disco(PoolName, Params) || {PoolName, Params} <- DiscoHosts ],
    {ok, { {one_for_one, 5, 10}, Specs1 ++ Specs2} }.

smt_worker(PoolName, Params) ->
    {{smt_worker, PoolName},
     {smt_worker, start_link, [PoolName, undefined, Params, 10000]},
        permanent, 5000, worker, [smt_worker]}.

smt_disco(PoolName, Params) ->
    {{smt_disco, PoolName},
     {smt_disco, start_link, [PoolName, Params, 10000]},
        permanent, 5000, worker, [smt_disco]}.
