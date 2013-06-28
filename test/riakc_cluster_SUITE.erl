%%% 
%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%% 
%%% vim: set ts=4 sts=4 sw=4 et:

-module(riakc_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile([export_all]).

-define(RIAK_NODE, 'riak@localhost').
-define(RIAK_HOST, "localhost").
-define(RIAK_PORT, 8087).

-include("riakc_cluster.hrl").

-record(riakc_obj, {
    bucket,
    key,
    vclock,
    contents,
    updatemetadata,
    updatevalue
}).

all() -> [
    basic_test,
    host_unreachable_test,
    host_unreachable2_test,
    host_fail_test,
    more_fails_test,
    timeouts_test,
    say_down_test
].

init_per_testcase(_, Config) ->
    ets:new(test_table, [set, named_table, public]),
    mock_riakc_pb_socket(),
    Config.

end_per_testcase(_, Config) ->
    ets:delete(test_table),
    meck:unload(),
    Config.

basic_test(_) ->
    mock_application([
        {peers, [{?RIAK_NODE, {?RIAK_HOST, ?RIAK_PORT}}]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),
    [] = gen_server:call(Pid, get_nodes_down),
    [{?RIAK_NODE, Pool}] = gen_server:call(Pid, get_nodes_up),
    [_] = gen_fsm:sync_send_all_state_event(Pool, get_avail_workers),

    riakc_cluster:delete(<<"table1">>, <<"key1">>),
    riakc_cluster:delete(<<"table1">>, <<"key2">>),

    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    % delete doesn't return notfound:
    ok = riakc_cluster:delete(<<"table1">>, <<"key1">>),

    ok = riakc_cluster:put(<<"table1">>, <<"key1">>, "value1"),
    {ok, "value1"} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    ok = riakc_cluster:put(<<"table1">>, <<"key2">>, "value2"),
    {ok, "value2"} = riakc_cluster:get(<<"table1">>, <<"key2">>),

    {ok, Keys} = riakc_cluster:list_keys(<<"table1">>),
    true = (lists:sort(Keys) == [<<"key1">>, <<"key2">>]),

    ok = riakc_cluster:delete(<<"table1">>, <<"key1">>),
    ok = riakc_cluster:delete(<<"table1">>, <<"key2">>),
    ok = riakc_cluster:delete(<<"table1">>, <<"key3">>),

    ok = riakc_cluster:stop().

host_unreachable_test(_) ->
    mock_application([
        {peers, [{?RIAK_NODE, {"undefined", ?RIAK_PORT}}]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),

    [] = gen_server:call(Pid, get_nodes_up),
    [{?RIAK_NODE, down}] = gen_server:call(Pid, get_nodes_down),

    {error, no_available_nodes} = riakc_cluster:get(<<"table1">>, <<"key1">>),

    ok = riakc_cluster:stop().

host_unreachable2_test(_) ->
    mock_application([
        {peers, [
            {?RIAK_NODE, {?RIAK_HOST, ?RIAK_PORT}},
            {undefined,  {"undefined", ?RIAK_PORT}}
        ]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),

    [{?RIAK_NODE, _Pool}] = gen_server:call(Pid, get_nodes_up),
    [{undefined, down}] = gen_server:call(Pid, get_nodes_down),

    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),

    ok = riakc_cluster:stop().

host_fail_test(_) ->
    mock_application([
        {peers, [
            {?RIAK_NODE,   {?RIAK_HOST, ?RIAK_PORT}},
            {'riak@riak0', {"riak0", ?RIAK_PORT}},
            {'riak@riak1', {"riak1", ?RIAK_PORT}}
        ]},
        {options, [
            {min_reconnect_timeout, 1000},
            {max_reconnect_timeout, 10000},
            {concurrency_level, 9}
        ]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),

    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    
    3 = length(gen_server:call(Pid, get_nodes_up)),
    [{_, Pool1} | _] = lists:sort(gen_server:call(Pid, get_nodes_up)),
    exit(Pool1, kill),
    timer:sleep(100),

    2 = length(gen_server:call(Pid, get_nodes_up)),
    [{'riak@localhost', down}] = gen_server:call(Pid, get_nodes_down),
    timer:sleep(1000),

    3 = length(gen_server:call(Pid, get_nodes_up)),
    [] = gen_server:call(Pid, get_nodes_down),

    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key2">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key3">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key4">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key5">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key6">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key7">>),

    ok = riakc_cluster:stop().

more_fails_test(_) ->
    mock_application([
        {peers, [
            {?RIAK_NODE,   {?RIAK_HOST, ?RIAK_PORT}},
            {'riak@riak0', {"riak0", ?RIAK_PORT}},
            {'riak@riak1', {"riak1", ?RIAK_PORT}}
        ]},
        {options, [
            {min_reconnect_timeout, 1000},
            {max_reconnect_timeout, 10000},
            {concurrency_level, 9}
        ]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),

    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    
    3 = length(gen_server:call(Pid, get_nodes_up)),
    [{_, Pool1} | _] = lists:sort(gen_server:call(Pid, get_nodes_up)),
    exit(Pool1, kill),
    timer:sleep(100),

    State1 = riakc_cluster:get_state(),
    [{'riak@localhost',{1000,undefined}}] = dict:to_list(State1#state.restart_timeouts),
    2 = length(gen_server:call(Pid, get_nodes_up)),
    timer:sleep(1000),
    
    State2 = riakc_cluster:get_state(),
    [{'riak@localhost',{2000, Timer2}}] = dict:to_list(State2#state.restart_timeouts),
    true = Timer2 =/= undefined,
    3 = length(gen_server:call(Pid, get_nodes_up)),
    [{_, Pool2} | _] = lists:sort(gen_server:call(Pid, get_nodes_up)),
    exit(Pool2, kill),
    timer:sleep(100),

    2 = length(gen_server:call(Pid, get_nodes_up)),
    timer:sleep(1000),
    % one pool is still down since it crached right after the last start:
    2 = length(gen_server:call(Pid, get_nodes_up)),
    timer:sleep(1000),
    % min_reconnect_timeout * 2 msecs have passes, up:
    3 = length(gen_server:call(Pid, get_nodes_up)),

    State3 = riakc_cluster:get_state(),
    [{'riak@localhost',{4000,Timer3}}] = dict:to_list(State3#state.restart_timeouts),
    true = Timer3 =/= undefined,
    timer:sleep(10100),
    % after max_reconnect_timeout has passed, we consider the node
    % fully alive and reset the restart strategy
    State4 = riakc_cluster:get_state(),
    [] = dict:to_list(State4#state.restart_timeouts),

    ok = riakc_cluster:stop().

timeouts_test(_) ->
    mock_application([
        {peers, [
            {?RIAK_NODE, {?RIAK_HOST, ?RIAK_PORT}}
        ]},
        {options, [
            {min_reconnect_timeout, 3000},
            {max_reconnect_timeout, 10000}
        ]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),

    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, notfound} = riakc_cluster:get(<<"table1">>, <<"key1">>),
    {error, timeout} = riakc_cluster:get(<<"table1">>, <<"timeout">>),
    {error, timeout} = riakc_cluster:get(<<"table1">>, <<"timeout">>),

    timer:sleep(2000),
    [{_, {SM1, TM1}}] = gen_server:call(Pid, get_counters),
    4 = jn_mavg:getEventsPer(SM1, ?TIMEOUT_RATE_PERIOD),
    2 = jn_mavg:getEventsPer(TM1, ?TIMEOUT_RATE_PERIOD),

    [_] = gen_server:call(Pid, get_nodes_up),
    [] = gen_server:call(Pid, get_nodes_down),

    % this should trigger node slow
    {error, timeout} = riakc_cluster:get(<<"table1">>, <<"timeout">>),

    timer:sleep(2000),
    [{_, {SM2, TM2}}] = gen_server:call(Pid, get_counters),
    4 = jn_mavg:getEventsPer(SM2, ?TIMEOUT_RATE_PERIOD),
    3 = jn_mavg:getEventsPer(TM2, ?TIMEOUT_RATE_PERIOD),

    [] = gen_server:call(Pid, get_nodes_up),
    [{?RIAK_NODE, slow}] = gen_server:call(Pid, get_nodes_down),

    timer:sleep(1100),

    % pool has been restarted
    [_] = gen_server:call(Pid, get_nodes_up),
    [] = gen_server:call(Pid, get_nodes_down),

    ok = riakc_cluster:stop().

say_down_test(_) ->
    mock_application([
        {peers, [{?RIAK_NODE, {?RIAK_HOST, ?RIAK_PORT}}]}
    ]),
    {ok, Pid} = riakc_cluster:start_link(),
    [] = gen_server:call(Pid, get_nodes_down),
    [{?RIAK_NODE, Pool}] = gen_server:call(Pid, get_nodes_up),

    riakc_cluster:say_down(?RIAK_NODE),
    [{?RIAK_NODE, force}] = gen_server:call(Pid, get_nodes_down),

    ok = riakc_cluster:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internals
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tput(K, V) ->
    ets:insert(test_table, {K, V}),
    ok.

tget(K) ->
    try
        [{_, V}] = ets:lookup(test_table, K),
        {ok, V}
    catch _:_ -> {error, notfound}
    end.

tdelete(K) ->
    case ets:lookup(test_table, K) of
        [Obj] -> ets:delete_object(test_table, Obj);
        _ -> ok
    end,
    ok.

pb_start_link("undefined", _Port, _Opts) ->
    {error,{tcp,nxdomain}};
pb_start_link(_Host, _Port, _Opts) ->
    Pid = spawn_link(fun() ->
        receive
            stop -> ok
        after
            20000 -> ok
        end
    end),
    {ok, Pid}.

mock_application(Config) ->
    meck:new(application, [unstick]),
    meck:expect(application, get_env, fun(riak_clusters) ->
        {ok, [{riakc_cluster, Config}]}
    end).

mock_riakc_pb_socket() ->
    meck:new(riakc_pb_socket),
    meck:expect(riakc_pb_socket, start_link, fun pb_start_link/3),
    meck:expect(riakc_pb_socket, list_keys,
        fun(_Pid, Table, _Timeout) ->
            case tget(Table) of
                {error, notfound} -> {ok, []};
                {ok, L} -> {ok, L}
            end
        end),
    meck:expect(riakc_pb_socket, delete,
        fun(_Pid, Table, Key, _Opts, _Timeout) ->
            {ok, Keys} = riakc_pb_socket:list_keys(1, Table, 1),
            tput(Table, lists:filter(fun(K) -> K =/= Key end, Keys)),
            tdelete({Table, Key})
        end),
    meck:expect(riakc_pb_socket, get,
        fun
            (_Pid, _Table, <<"timeout">>, _Opts, _Timeout) ->
                {error, timeout};
            (_Pid, Table, Key, _Opts, _Timeout) ->
                tget({Table, Key})
        end),
    meck:expect(riakc_pb_socket, put,
        fun(_Pid, Obj, _Opts, _Timeout) ->
            Table = riakc_obj:bucket(Obj),
            Key = riakc_obj:key(Obj),
            NewObj = Obj#riakc_obj{
                contents = [{Obj#riakc_obj.updatemetadata,
                    Obj#riakc_obj.updatevalue}]},
            {ok, Keys} = riakc_pb_socket:list_keys(1, Table, 1),
            tput(Table, [Key | Keys]),
            tput({Table, Key}, NewObj)
        end),
    ok.
