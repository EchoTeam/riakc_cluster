%%% vim: set ts=4 sts=4 sw=4 et:

-type errors() :: 'timeout' | 'disconnected' | 'timeout_external' | 'no_available_nodes' | term().
-type error() :: {'error', 'notfound' | errors()}.
-type cluster_name() :: atom().
-type table() :: binary().
-type key() :: binary().
-type value() :: term().
-type options() :: [proplists:property()].
