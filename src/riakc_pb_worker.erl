%%% 
%%% Copyright (c) 2012 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%% 
%%% vim: set ts=4 sts=4 sw=4 et:

-module(riakc_pb_worker).
-behaviour(poolboy_worker).

-export([start_link/1]).

start_link(Args) ->
    Host = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    DefaultOptions = [
        {auto_reconnect, false}
    ],
    Options = proplists:get_value(options, Args, []),
    NewOptions = lists:foldl(fun({K, V}, Acc) ->
        lists:keystore(K, 1, Acc, {K, proplists:get_value(K, Acc, V)})
    end, Options, DefaultOptions),
    riakc_pb_socket:start_link(Host, Port, NewOptions).
