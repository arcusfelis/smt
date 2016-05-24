
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
    {ok, Hosts} = application:get_env(smt, mysql_hosts),
    Specs = [ smt_worker(PoolName, Params) || {PoolName, Params} <- Hosts ],
    {ok, { {one_for_one, 5, 10}, Specs} }.

smt_worker(PoolName, Params) ->
    {smt_worker, {smt_worker, start_link, [PoolName, Params, 10000]},
        permanent, 5000, worker, [smt_worker]}.

