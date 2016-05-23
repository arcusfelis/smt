-module(smt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    emysql:add_pool(smt_mysql_pool, 1,
        "arc_ejabberd", "arc_ejabberd", "10.100.0.78", 3306,
        "arc_ejabberd", utf8),
    smt_sup:start_link().

stop(_State) ->
    ok.
