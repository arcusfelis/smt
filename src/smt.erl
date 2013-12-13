-module(smt).
-export([start/0]).

start() ->
    application:start(sasl),
    application:start(crypto),
    application:start(emysql),
    application:start(smt).
