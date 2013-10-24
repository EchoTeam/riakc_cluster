%%% vim: set ts=4 sts=4 sw=4 et:

-module(riakc_cluster_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("riakc_cluster.hrl").

-define('CNAME', test).

cluster_setup() ->
    meck:new(riakc_pb_worker),
    meck:expect(riakc_pb_worker, start_link, fun(_Args) ->
        Pid = spawn_link(fun() ->
            receive undefined -> ok end
        end),
        {ok, Pid}
    end),
    Peers = [{node(), {"localhost", 1234}}],
    {ok, _Pid} = riakc_cluster:start_link(?CNAME, [{peers, Peers}]),
    ok.

cluster_cleanup(_) ->
    riakc_cluster:stop(?CNAME),
    meck:unload().

no_nodes_test_() ->
    {setup,
        fun cluster_setup/0,
        fun cluster_cleanup/1,
        [fun test_no_nodes/0]
    }.

test_no_nodes() ->
    Nodes = get_nodes(up),
    ?assertEqual(true, length(Nodes) > 0),
    [riakc_cluster:say_down(?CNAME, Node) || Node <- Nodes],
    timer:sleep(100),
    ?assertEqual([], get_nodes(up)),
    ?assertEqual({error, no_available_nodes}, riakc_cluster:get(?CNAME, <<"table">>, <<"key">>)).

get_test_() ->
    {setup,
        fun get_setup/0,
        fun cluster_cleanup/1,
        test_get_generator()
    }.

get_setup() ->
    cluster_setup(),

    meck:new(riakc_pb_socket),
    meck:expect(riakc_pb_socket, get, fun(_Pid, _Bucket, Key, _Options, _Timeout) ->
        case Key of
            {ok, Value} ->
                {ok, {obj, term_to_binary(Value)}};
            {error, _Reason} = Error ->
                Error
        end
    end),
    meck:new(riakc_obj),
    meck:expect(riakc_obj, get_value, fun({obj, Value}) ->
        Value
    end).

test_get_generator() ->
    [{Title, fun() ->
        ?assertEqual(Expectation, riakc_cluster:get(?CNAME, <<"table">>, Key, []))
    end} || {Title, {Expectation, Key}} <- [
        {"error",
            {{error, disconnected}, {error, disconnected}}},
        {"ok",
            {{ok, value}, {ok, value}}}
    ]].

put_test_() ->
    {setup,
        fun put_setup/0,
        fun cluster_cleanup/1,
        test_put_generator()
    }.

put_setup() ->
    get_setup(),

    meck:expect(riakc_pb_socket, put, fun(_Pid, {obj, Value}, _Options, _Timeout) ->
        case Value of
            {error, _Reason} = Error ->
                Error;
            ok ->
                ok
        end
    end),
    meck:expect(riakc_obj, new, fun(_Bucket, _Key, Value, _ContentType) ->
        {obj, binary_to_term(Value)}
    end),
    meck:expect(riakc_obj, update_value, fun({obj, _OldValue}, NewValue, _ContentType) ->
        {obj, binary_to_term(NewValue)}
    end).

test_put_generator() ->
    [{Title, fun() ->
        ?assertEqual(Expectation, riakc_cluster:put(?CNAME, <<"table">>, Key, Value, []))
    end} || {Title, {Expectation, {Key, Value}}} <- [
        {"get error",
            {{error, disconnected}, {{error, disconnected}, undefined}}},
        {"get: not found; put: error",
            {{error, disconnected}, {{error, notfound}, {error, disconnected}}}},
        {"get: not found; put: ok",
            {ok, {{error, notfound}, ok}}},
        {"get: found; put: error",
            {{error, disconnected}, {{ok, value}, {error, disconnected}}}},
        {"get: found; put: ok",
            {ok, {{ok, value}, ok}}}
    ]].

server_exception_test_() ->
    {setup,
        fun server_exception_setup/0,
        fun cluster_cleanup/1,
        [{timeout, ?TIMEOUT_EXTERNAL + 1000, fun test_server_exception/0}]
    }.

server_exception_setup() ->
    cluster_setup(),

    meck:new(riakc_pb_socket),
    meck:expect(riakc_pb_socket, get, fun(_Pid, _Bucket, _Key, _Options, _Timeout) ->
        erlang:error(someerror)
    end),
    meck:new(riakc_obj),
    meck:expect(riakc_obj, get_value, fun(Value) ->
        Value
    end).

test_server_exception() ->
    ?assertMatch({exit, {timeout, {gen_server, call, _}}},
                    try riakc_cluster:get(?CNAME, <<"table">>, undefined, []) catch C:R -> {C, R} end).

server_timeout_test_() ->
    {setup,
        fun server_timeout_setup/0,
        fun cluster_cleanup/1,
        [{timeout, ?TIMEOUT_EXTERNAL + 1000, fun test_server_timeout/0}]
    }.

server_timeout_setup() ->
    get_setup(),

    meck:expect(riakc_pb_socket, get, fun(_Pid, _Bucket, _Key, _Options, _Timeout) ->
        timer:sleep(?TIMEOUT_EXTERNAL + 100)
    end).

test_server_timeout() ->
    ?assertMatch({exit, {timeout, {gen_server, call, _}}},
                    try riakc_cluster:get(?CNAME, <<"table">>, undefined, []) catch C:R -> {C, R} end).

down_nodes_test_() ->
    {setup,
        fun down_nodes_setup/0,
        fun cluster_cleanup/1,
        [fun test_down_nodes/0]
    }.

down_nodes_setup() ->
    meck:new(poolboy),
    meck:expect(poolboy, start, fun(_PoolArgs, WorkerArgs) ->
        case proplists:get_value(port, WorkerArgs) of
            1 ->
                Pid = spawn_link(fun() ->
                    receive undefined -> ok end
                end),
                {ok, Pid};
            2 ->
                {error, somererror}
        end
    end),
    Peers = [{node1, {"localhost", 1}}, {node2, {"localhost", 2}}],
    {ok, _Pid} = riakc_cluster:start_link(?CNAME, [{peers, Peers}]),
    ok.

test_down_nodes() ->
    ?assertEqual([node1], get_nodes(up)),
    ?assertEqual([node2], get_nodes(down)).

% private functions

get_nodes(Type) when Type =:= up; Type =:= down ->
    [Node || {Node, _} <- gen_server:call(?CNAME, list_to_atom("get_nodes_" ++ atom_to_list(Type)))].

-endif.
