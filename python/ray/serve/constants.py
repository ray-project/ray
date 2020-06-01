#: Actor name used to register master actor
SERVE_MASTER_NAME = "SERVE_MASTER_ACTOR"

#: Actor name used to register router actor
SERVE_ROUTER_NAME = "SERVE_ROUTER_ACTOR"

#: Actor name used to register HTTP proxy actor
SERVE_PROXY_NAME = "SERVE_PROXY_ACTOR"

#: Actor name used to register metric monitor actor
SERVE_METRIC_SINK_NAME = "SERVE_METRIC_SINK_ACTOR"

#: HTTP Address
DEFAULT_HTTP_ADDRESS = "http://127.0.0.1:8000"

#: HTTP Host
DEFAULT_HTTP_HOST = "127.0.0.1"

#: HTTP Port
DEFAULT_HTTP_PORT = 8000

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

#: Default latency SLO
DEFAULT_LATENCY_SLO_MS = 1e9

#: Interval for metric client to push metrics to exporters
METRIC_PUSH_INTERVAL_S = 2

#: Time to wait for HTTP proxy in `serve.init()`
HTTP_PROXY_TIMEOUT = 60
