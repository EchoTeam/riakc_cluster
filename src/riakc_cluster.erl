%%% 
%%% Copyright (c) 2012 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%% 
%%% vim: set ts=4 sts=4 sw=4 et:

-module(riakc_cluster).
-behaviour(gen_server).

% public API
-export([
    get/2,
    get/3,
    get/4,
    put/3,
    put/4,
    put/5,
    delete/2,
    delete/3,
    delete/4,
    list_keys/1,
    list_keys/2,
    list_keys/3,
    list_buckets/0,
    list_buckets/1,
    list_buckets/2,

    say_up/1,
    say_up/2,
    say_down/1,
    say_down/2,

    start_link/0,
    start_link/1,
    start_link/2,
    stop/0,
    stop/1,
    get_state/0,
    get_state/1
]).

% gen_server callbacks
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-include("riakc_cluster.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    start_link(?MODULE).

start_link(ClusterName) ->
    start_link(ClusterName, riak_clusters).

start_link(ClusterName, ConfigKey) ->
    {ok, Config} = application:get_env(ConfigKey),
    ClusterConfig = proplists:get_value(ClusterName, Config),
    case proplists:get_value(peers, ClusterConfig, []) of
        [] -> erlang:error(no_peers);
        Peers ->
            Options = proplists:get_value(options, ClusterConfig, []),
            gen_server:start_link({local, ClusterName}, ?MODULE,
                [ClusterName, Peers, Options], [])
    end.

stop() ->
    stop(?MODULE).
stop(ClusterName) ->
    do(ClusterName, stop).

put(Table, Key, Value) ->
    put(?MODULE, Table, Key, Value, []).
put(Table, Key, Value, Options) when is_binary(Table) ->
    put(?MODULE, Table, Key, Value, Options);
put(ClusterName, Table, Key, Value) ->
    put(ClusterName, Table, Key, Value, []).
put(ClusterName, Table, Key, Value, Options) ->
    do(ClusterName, {put, {Table, Key, Value, Options}}).

get(Table, Key) ->
    get(?MODULE, Table, Key, []).
get(Table, Key, Options) when is_binary(Table) ->
    get(?MODULE, Table, Key, Options);
get(ClusterName, Table, Key) ->
    get(ClusterName, Table, Key, []).
get(ClusterName, Table, Key, Options) ->
    do(ClusterName, {get, {Table, Key, Options}}).  

delete(Table, Key) ->
    delete(?MODULE, Table, Key, []).
delete(Table, Key, Options) when is_binary(Table) ->
    delete(?MODULE, Table, Key, Options);
delete(ClusterName, Table, Key) ->
    delete(ClusterName, Table, Key, []).
delete(ClusterName, Table, Key, Options) ->
    do(ClusterName, {delete, {Table, Key, Options}}).  

list_keys(Table) ->
    list_keys(?MODULE, Table, ?TIMEOUT_INTERNAL).
list_keys(Table, Timeout) when is_binary(Table) ->
    list_keys(?MODULE, Table, Timeout);
list_keys(ClusterName, Table) ->
    list_keys(ClusterName, Table, ?TIMEOUT_INTERNAL).
list_keys(ClusterName, Table, Timeout) ->
    do(ClusterName, {list_keys, {Table, Timeout}}).

list_buckets() ->
    list_buckets(?MODULE, ?TIMEOUT_INTERNAL).
list_buckets(ClusterName) ->
    list_buckets(ClusterName, ?TIMEOUT_INTERNAL).
list_buckets(ClusterName, Timeout) ->
    do(ClusterName, {list_buckets, Timeout}).

get_state() ->
    get_state(?MODULE).
get_state(ClusterName) ->
    do(ClusterName, get_state).

say_up(Node) ->
    say_up(?MODULE, Node).
say_up(ClusterName, Node) ->
    ClusterName ! {node_mon, Node, up}.

say_down(Node) ->
    say_down(?MODULE, Node).
say_down(ClusterName, Node) ->
    ClusterName ! {node_mon, Node, down}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ClusterName, Peers, Options]) ->
    process_flag(trap_exit, true),

    ClusterSize = length(Peers),
    ConcurrencyLevel = proplists:get_value(concurrency_level,
        Options, ClusterSize),
    PoolsNumber = min(ConcurrencyLevel, ClusterSize),
    PoolSize = ceiling(ConcurrencyLevel/PoolsNumber),
    MinReconnectTimeout = proplists:get_value(min_reconnect_timeout, Options,
        ?DEFAULT_MIN_RECONNECT_TIMEOUT),
    MaxReconnectTimeout = proplists:get_value(max_reconnect_timeout, Options,
        ?DEFAULT_MAX_RECONNECT_TIMEOUT),
    PoolOptions = [
        {size, PoolSize},
        {max_overflow, proplists:get_value(max_overflow, Options, 0)},
        {min_restart_timeout, MinReconnectTimeout},
        {max_restart_timeout, MaxReconnectTimeout},
        {worker_module, riakc_pb_worker}
    ],

    PeersUsed = dict:from_list(lists:sublist(shuffle_list(Peers),
        PoolsNumber)),

    State = #state{ name = ClusterName, pool_opts = PoolOptions,
        peers = PeersUsed },

    Pools = start_pools(State),
    DownPools = [{N, down} || {down, N} <- Pools],
    UpPools   = [{N, Pool} || {up, N, Pool} <- Pools],
    [true = link(P) || {_, P} <- UpPools],

    {ok, State#state{up = dict:from_list(UpPools),
        down = dict:from_list(DownPools)}}.

terminate(_Reason, #state{up = Up}) ->
    %% stop poolboys
    dict:map(fun(_, Pool) ->
        catch poolboy:stop(Pool)
    end, Up),
    ok.

handle_call({put, {Table, Key, Value, Options}}, From, State) ->
    riak_operation(State, From,
        fun(Pid) ->
            W = proplists:get_value(w, Options, 2),
            % TODO: consider removing get from here or make a seprate method without it
            case riakc_pb_socket:get(Pid, Table, Key, [{r, W}], ?TIMEOUT_INTERNAL) of
                {ok, Obj} ->
                    riakc_pb_socket:put(Pid, riakc_obj:update_value(Obj, term_to_binary(Value), <<"application/x-erlang-term">>), Options, ?TIMEOUT_INTERNAL);
                {error, notfound} ->
                    riakc_pb_socket:put(Pid, riakc_obj:new(Table, Key, term_to_binary(Value), <<"application/x-erlang-term">>), Options, ?TIMEOUT_INTERNAL)
            end
        end);

handle_call({get, {Table, Key, Options}}, From, State) ->
    riak_operation(State, From,
        fun(Pid) ->
            case riakc_pb_socket:get(Pid, Table, Key,
                    Options, ?TIMEOUT_INTERNAL) of
                {ok, Obj} ->
                    {ok, binary_to_term(riakc_obj:get_value(Obj))};
                Error -> Error
            end
        end),
    {noreply, State};

handle_call({delete, {Table, Key, Options}}, From, State) ->
    riak_operation(State, From,
        fun(Pid)  ->
            riakc_pb_socket:delete(Pid, Table, Key, Options, ?TIMEOUT_INTERNAL)
        end);

handle_call({list_keys, {Table, Timeout}}, From, State) ->
    riak_operation(State, From,
        fun(Pid) ->
            riakc_pb_socket:list_keys(Pid, Table, Timeout)
        end);

handle_call({list_buckets, Timeout}, From, State) ->
    riak_operation(State, From,
        fun(Pid) ->
            riakc_pb_socket:list_buckets(Pid, Timeout)
        end);

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(get_nodes_up, _From, #state{up = Up} = State) ->
    {reply, dict:to_list(Up), State};

handle_call(get_nodes_down, _From, #state{down = Down} = State) ->
    {reply, dict:to_list(Down), State};

handle_call(get_counters, _From,
        #state{counters = Counters} = State) ->
    {reply, dict:to_list(Counters), State};

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({add, Nodes}, State) ->
    {noreply, add_nodes(State, Nodes)};

handle_info({node_mon, Node, down}, State) ->
    {noreply, maybe_stop_pool(State, Node, force, false)};

handle_info({node_mon, Node, up}, State) ->
    maybe_start_pool(State, Node, true),
    {noreply, State};

handle_info({try_start_pool_for, Node},
        #state{pool_opts = PoolOptions} = State) ->
    maybe_start_pool(State, Node, false),
    MaxTimeout = proplists:get_value(max_restart_timeout, PoolOptions),
    {CurTimeout, _} = get_restart_timeout(State, Node),
    NewState = change_restart_timeout(State, Node,
        new_restart_timeout(CurTimeout, MaxTimeout)),
    {noreply, NewState};

handle_info({reset_restart_timeout, Node},
        #state{restart_timeouts = Timeouts} = State) ->
    NewState = State#state{
        restart_timeouts = dict:erase(Node, Timeouts)},
    {noreply, NewState};

handle_info({'EXIT', Pid, _Reason}, #state{up = Up} = State) ->
    UpList = dict:to_list(Up),
    NewState = case lists:keyfind(Pid, 2, UpList) of
        {Node, _} -> maybe_stop_pool(State, Node, down, true);
        _ -> State
    end,
    {noreply, NewState};

handle_info({bump_timeout_counter, Node},
        #state{counters = Counters} = State) ->
    NewMavg = jn_mavg:new_mavg(?TIMEOUT_RATE_PERIOD),
    NewCounters = dict:update(Node, fun({SuccessMavg, TimeoutMavg}) ->
        {SuccessMavg, jn_mavg:bump_mavg(TimeoutMavg, 1)}
    end, {NewMavg, jn_mavg:bump_mavg(NewMavg, 1)}, Counters),
    NewState = State#state{counters = NewCounters},
    {noreply, maybe_slow_node(NewState, Node)};

handle_info({bump_success_counter, Node},
        #state{counters = Counters} = State) ->
    NewMavg = jn_mavg:new_mavg(?TIMEOUT_RATE_PERIOD),
    NewCounters = dict:update(Node, fun({SuccessMavg, TimeoutMavg}) ->
        {jn_mavg:bump_mavg(SuccessMavg, 1), TimeoutMavg}
    end, {jn_mavg:bump_mavg(NewMavg, 1), NewMavg}, Counters),
    {noreply, State#state{counters = NewCounters}};

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

shuffle_list(List) ->
    D = [{random:uniform(), A} || A <- List],
    {_, D1} = lists:unzip(lists:keysort(1, D)), 
    D1.

ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true  -> T;
        false -> T + 1
    end.

% Invoke a given function with a random element of a [small] list.
with_random(Alternatives, Fun) ->
    N = length(Alternatives),
    R = element(3, now()),
    Element = lists:nth((R rem N) + 1, Alternatives),
    Fun(Element).

do(ClusterName, Request) ->
    gen_server:call(ClusterName, Request, ?TIMEOUT_EXTERNAL).

start_pools(#state{peers = Peers} = State) ->
    PeersList = dict:to_list(Peers),
    [start_pool(State, {Node, Host, Port})
        || {Node, {Host, Port}} <- PeersList].
start_pool(#state{peers = Peers} = State, Node) when is_atom(Node) ->
    {ok, {Host, Port}} = dict:find(Node, Peers),
    start_pool(State, {Node, Host, Port});
start_pool(#state{pool_opts = PoolArgs} = State, {Node, Host, Port}) ->
    case poolboy:start(PoolArgs, [{host, Host}, {port, Port}]) of
        {ok, Pool} ->
            {up, Node, Pool};
        _ ->
            try_restart_pool_later(State, Node),
            {down, Node}
    end.

change_restart_timeout(#state{restart_timeouts = Timeouts} = State,
        Node, Timeout) ->
    NewTimeouts = dict:update(Node, fun({_, Timer}) ->
        {Timeout, Timer}
    end, {Timeout, undefined}, Timeouts),
    State#state{restart_timeouts = NewTimeouts}.

new_restart_timeout(OldTimeout, MaxTimeout) ->
    lists:min([OldTimeout * 2, MaxTimeout]).

get_restart_timeout(#state{restart_timeouts = Timeouts,
        pool_opts = PoolOptions}, Node) ->
    case dict:find(Node, Timeouts) of
        error ->
            MinTimeout = proplists:get_value(min_restart_timeout, PoolOptions),
            {MinTimeout, undefined};
        {ok, V} -> V
    end.

try_restart_pool_later(#state{name = ClusterName} = State, Node) ->
    {Timeout, _} = get_restart_timeout(State, Node),
    timer:send_after(Timeout, ClusterName, {try_start_pool_for, Node}).

maybe_start_pool(#state{name = ClusterName, down = Down} = State, Node, IgnoreForceCheck) ->
    case dict:find(Node, Down) of
        {ok, force} when IgnoreForceCheck == false -> nop;
        {ok, _} ->
            spawn_link(fun() ->
                Pool = start_pool(State, Node),
                erlang:send(ClusterName, {add, [Pool]})
            end);
        _ -> nop
    end.

maybe_stop_pool(#state{up = Up} = State, Node, Reason, NeedRestart) ->
    case dict:find(Node, Up) of
        {ok, Pool} ->
            catch poolboy:stop(Pool),
            maybe_restart_pool(State, Node, NeedRestart),
            node_down(State, Node, Reason);
        _ ->
            node_down(State, Node, Reason)
    end.

maybe_slow_node(#state{counters = Counters} = State, Node) ->
    {ok, {SM, TM}} = dict:find(Node, Counters), 
    SC = jn_mavg:getEventsPer(SM, ?TIMEOUT_RATE_PERIOD),
    TC = jn_mavg:getEventsPer(TM, ?TIMEOUT_RATE_PERIOD),
    Total = SC + TC,
    case Total == 0 orelse TC/Total < ?TIMEOUT_RATE_THRESHOLD of
        true -> State;
        false -> maybe_stop_pool(State, Node, slow, true)
    end.

maybe_restart_pool(State, Node, true) ->
    try_restart_pool_later(State, Node);
maybe_restart_pool(_, _, _) -> nop.

add_nodes(State, Nodes) ->
    NewUpNodes   = [{N, C} || {up, N, C} <- Nodes],
    NewDownNodes = [{N, down} || {down, N} <- Nodes],

    [true = link(P) || {_, P} <- NewUpNodes],

    NewState1 = lists:foldl(fun({Node, Pool}, Acc) ->
        node_up(Acc, Node, Pool)
    end, State, NewUpNodes),
    NewState2 = lists:foldl(fun({Node, Reason}, Acc) ->
        node_down(Acc, Node, Reason)
    end, NewState1, NewDownNodes),
    NewState2.

node_up(#state{up = Up, down = Down, 
        restart_timeouts = RestartTimeouts} = State,
        Node, Pool) ->
    {ok, Timer, CurTimeout} = set_reset_restart_timer(State, Node),
    State#state{
        up = dict:store(Node, Pool, Up),
        down = dict:erase(Node, Down),
        restart_timeouts = dict:store(Node, {CurTimeout, Timer},
            RestartTimeouts)}.

node_down(#state{up = Up, down = Down,
        restart_timeouts = RestartTimeouts} = State,
        Node, Reason) ->
    {ok, CurTimeout} = cancel_reset_restart_timer(State, Node),
    State#state{
        up = dict:erase(Node, Up),
        down = dict:store(Node, Reason, Down),
        restart_timeouts = dict:store(Node, {CurTimeout, undefined},
            RestartTimeouts)}.

set_reset_restart_timer(#state{pool_opts = PoolOptions,
        name = ClusterName} = State, Node) ->
    MaxTimeout = proplists:get_value(max_restart_timeout, PoolOptions),
    {ok, CurTimeout} = cancel_reset_restart_timer(State, Node),
    {ok, Timer} = timer:send_after(MaxTimeout, ClusterName,
        {reset_restart_timeout, Node}),
    {ok, Timer, CurTimeout}.

cancel_reset_restart_timer(State, Node) ->
    {CurTimeout, CurTimer} = get_restart_timeout(State, Node),
    case CurTimer of
        undefined -> nop;
        _ -> timer:cancel(CurTimer)
    end,
    {ok, CurTimeout}.

riak_operation(#state{up = Up} = State, From, Fun) ->
    UpList = dict:to_list(Up),
    riak_operation_ll(State, From, Fun, UpList),
    {noreply, State}.

riak_operation_ll(_State, From, _Fun, []) ->
    gen_server:reply(From, {error, no_available_nodes});
riak_operation_ll(State, From, Fun, Nodes) ->
    spawn(fun() ->
        {Node, Reply} = with_random(Nodes,
            fun({Node, Pool}) ->
                poolboy:transaction(Pool, fun(Worker) ->
                    {Node, Fun(Worker)}
                end)
            end),
        gen_server:reply(From, Reply),
        bump_counters(State, Node, Reply)
    end).

bump_counters(State, Node, {error, timeout} = _Reply) ->
    whereis(State#state.name) ! {bump_timeout_counter, Node};
bump_counters(State, Node, _) ->
    whereis(State#state.name) ! {bump_success_counter, Node}.
