#: The interval which http server refreshes its routing table
HTTP_ROUTER_CHECKER_INTERVAL_S = 2

#: Actor name used to register actor nursery
SERVE_NURSERY_NAME = "SERVE_ACTOR_NURSERY"

#: KVStore connector key in bootstrap config
BOOTSTRAP_KV_STORE_CONN_KEY = "kv_store_connector"

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
