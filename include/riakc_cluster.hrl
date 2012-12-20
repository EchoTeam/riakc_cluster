
-define(TIMEOUT_INTERNAL, 5000). % in msec
-define(TIMEOUT_EXTERNAL, 5100). % in msec

-define(TIMEOUT_RATE_PERIOD, 30). % in seconds
-define(TIMEOUT_RATE_THRESHOLD, 0.3). % rate per TIMEOUT_RATE_PERIOD

-define(DEFAULT_MIN_RECONNECT_TIMEOUT, 60000). % in msec
-define(DEFAULT_MAX_RECONNECT_TIMEOUT, 10*60000). % in msec

-record(state, {
    name,
    pool_opts = [],
    peers = dict:new(),
    up = dict:new(),
    down = dict:new(),
    restart_timeouts = dict:new(),
    counters = dict:new()
}).
