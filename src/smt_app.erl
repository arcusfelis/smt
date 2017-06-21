-module(smt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    import_os_env_variables(),
    smt_sup:start_link().

stop(_State) ->
    ok.


import_os_env_variables() ->
	set_env("GRAPHITE_HOST", graphite_host, string),
	set_env("GRAPHITE_PORT", graphite_port, integer),
	set_env("GRAPHITE_URL",  graphite_url,  string),
	set_env("GRAPHITE_FORMAT", graphite_format,  string),
    import_mysql_hosts(),
    ok.

import_mysql_hosts() ->
    case os:getenv("MYSQL_POOLS") of
        false ->
            ok;
        MySQL_pools_raw ->
            Pools = string:tokens(MySQL_pools_raw, " "),
            Hosts = [import_mysql_host(Pool) || Pool <- Pools],
			application:set_env(smt, mysql_hosts, Hosts)
    end.

import_mysql_host(Pool) ->
    Prefix = "MYSQL_" ++ string:to_upper(Pool) ++ "_",
    PoolAtom = list_to_atom(Pool),
    Opts = [
			parse_opt(Prefix, "HOST", host, string),
			parse_opt(Prefix, "USER", user, string),
			parse_opt(Prefix, "PASSWORD", password, string),
			parse_opt(Prefix, "DATABASE", database, string, ""),
			parse_opt(Prefix, "ENCODING", encoding, atom, utf8)
           ],
    {PoolAtom, lists:append(Opts)}.


parse_opt(Prefix, Suffix, AtomId, EnvType, Default) ->
    case os:getenv(Prefix ++ Suffix) of
        false ->
            [{AtomId, Default}];
        StringValue ->
            [{AtomId, convert(EnvType, StringValue)}]
   end.

parse_opt(Prefix, Suffix, AtomId, EnvType) ->
    case os:getenv(Prefix ++ Suffix) of
        false ->
            [];
        StringValue ->
            [{AtomId, convert(EnvType, StringValue)}]
   end.

set_env(OsEnvName, EnvKey, EnvType) ->
    case os:getenv(OsEnvName) of
		false ->
			ok;
		StringValue ->
            Value = convert(EnvType, StringValue),
            error_logger:info_msg("issue=setting_value ~p=~p", [EnvKey, Value]),
			application:set_env(smt, EnvKey, Value)
    end.

convert(string, StringValue) ->
    StringValue;
convert(integer, StringValue) ->
    list_to_integer(StringValue);
convert(atom, StringValue) ->
    list_to_atom(StringValue).
