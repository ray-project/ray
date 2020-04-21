#: Actor name used to register master actor
SERVE_MASTER_NAME = "SERVE_MASTER_ACTOR"

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

#: Key for storing no http route services
NO_ROUTE_KEY = "NO_ROUTE"

#: Interval in sections for pushing metrics to the metric sink
METRIC_PUSH_INTERVAL_S = 5
