from enum import Enum

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

#: Controller checkpoint path
DEFAULT_CHECKPOINT_PATH = "ray://"

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

# How often to call the control loop on the controller.
CONTROL_LOOP_PERIOD_S = 0.1

#: Max time to wait for HTTP proxy in `serve.start()`.
HTTP_PROXY_TIMEOUT = 60

#: Max retry count for allowing failures in replica constructor.
#: If no replicas at target version is running by the time we're at
#: max construtor retry count, deploy() is considered failed.
#: By default we set threshold as min(num_replicas * 3, this value)
MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT = 100

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

#: Name of deployment health check method implemented by user.
HEALTH_CHECK_METHOD = "check_health"

#: Name of deployment reconfiguration method implemented by user.
RECONFIGURE_METHOD = "reconfigure"

SERVE_ROOT_URL_ENV_KEY = "RAY_SERVE_ROOT_URL"

#: Number of historically deleted deployments to store in the checkpoint.
MAX_NUM_DELETED_DEPLOYMENTS = 1000

#: Limit the number of cached handles because each handle has long poll
#: overhead. See https://github.com/ray-project/ray/issues/18980
MAX_CACHED_HANDLES = 100

#: Because ServeController will accept one long poll request per handle, its
#: concurrency needs to scale as O(num_handles)
CONTROLLER_MAX_CONCURRENCY = 15000

DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S = 20
DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S = 2
DEFAULT_HEALTH_CHECK_PERIOD_S = 10
DEFAULT_HEALTH_CHECK_TIMEOUT_S = 30

#: Number of times in a row that a replica must fail the health check before
#: being marked unhealthy.
REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 3

# Key used to idenfity given json represents a serialized RayServeHandle
SERVE_HANDLE_JSON_KEY = "__SerializedServeHandle__"


class ServeHandleType(str, Enum):
    SYNC = "SYNC"
    ASYNC = "ASYNC"
