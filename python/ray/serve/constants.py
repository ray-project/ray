from enum import auto, Enum

#: Actor name used to register controller
SERVE_CONTROLLER_NAME = "SERVE_CONTROLLER_ACTOR"

#: Actor name used to register HTTP proxy actor
SERVE_PROXY_NAME = "SERVE_PROXY_ACTOR"

#: HTTP Address
DEFAULT_HTTP_ADDRESS = "http://127.0.0.1:8000"

#: HTTP Host
DEFAULT_HTTP_HOST = "127.0.0.1"

#: HTTP Port
DEFAULT_HTTP_PORT = 8000

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

#: Max time to wait for HTTP proxy in `serve.start()`.
HTTP_PROXY_TIMEOUT = 60

#: Default histogram buckets for latency tracker.
DEFAULT_LATENCY_BUCKET_MS = [
    1,
    2,
    5,
    10,
    20,
    50,
    100,
    200,
    500,
    1000,
    2000,
    5000,
]

#: Name of backend reconfiguration method implemented by user.
BACKEND_RECONFIGURE_METHOD = "reconfigure"


class LongPollKey(Enum):
    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    REPLICA_HANDLES = auto()
    TRAFFIC_POLICIES = auto()
    BACKEND_CONFIGS = auto()
    ROUTE_TABLE = auto()


#: Backend worker waits for this duration after no work to do then shutdown.
DEFAULT_GRACEFUL_SHUTDOWN_LOOP_WAIT_S = 2

#: Controller waits for this duration to forcefully kill the replica for
#  shutdown.
DEFAULT_GRACEFUL_SHUTDOWN_CONTROLLER_WAIT_S = 10
