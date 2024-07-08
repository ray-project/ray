import os

#: Used for debugging to turn on DEBUG-level logs
DEBUG_LOG_ENV_VAR = "SERVE_DEBUG_LOG"

#: Logger used by serve components
SERVE_LOGGER_NAME = "ray.serve"

#: Actor name used to register controller
SERVE_CONTROLLER_NAME = "SERVE_CONTROLLER_ACTOR"

#: Actor name used to register HTTP proxy actor
SERVE_PROXY_NAME = "SERVE_PROXY_ACTOR"

#: Ray namespace used for all Serve actors
SERVE_NAMESPACE = "serve"

#: HTTP Address
DEFAULT_HTTP_ADDRESS = "http://127.0.0.1:8000"

#: HTTP Host
DEFAULT_HTTP_HOST = "127.0.0.1"

#: HTTP Port
DEFAULT_HTTP_PORT = 8000

#: Uvicorn timeout_keep_alive Config
DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S = 5

#: gRPC Port
DEFAULT_GRPC_PORT = 9000

#: Default Serve application name
SERVE_DEFAULT_APP_NAME = "default"

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

# How long to sleep between control loop cycles on the controller.
CONTROL_LOOP_INTERVAL_S = float(os.getenv("RAY_SERVE_CONTROL_LOOP_INTERVAL_S", 0.1))
assert CONTROL_LOOP_INTERVAL_S >= 0, (
    f"Got unexpected value {CONTROL_LOOP_INTERVAL_S} for "
    "RAY_SERVE_CONTROL_LOOP_INTERVAL_S environment variable. "
    "RAY_SERVE_CONTROL_LOOP_INTERVAL_S cannot be negative."
)

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
    300,
    400,
    500,
    1000,
    2000,
    # 5 seconds
    5000,
    # 10 seconds
    10000,
    # 60 seconds
    60000,
    # 2min
    120000,
    # 5 min
    300000,
    # 10 min
    600000,
]

#: Name of deployment health check method implemented by user.
HEALTH_CHECK_METHOD = "check_health"

#: Name of deployment reconfiguration method implemented by user.
RECONFIGURE_METHOD = "reconfigure"

SERVE_ROOT_URL_ENV_KEY = "RAY_SERVE_ROOT_URL"

#: Limit the number of cached handles because each handle has long poll
#: overhead. See https://github.com/ray-project/ray/issues/18980
MAX_CACHED_HANDLES = int(os.getenv("MAX_CACHED_HANDLES", 100))
assert MAX_CACHED_HANDLES > 0, (
    f"Got unexpected value {MAX_CACHED_HANDLES} for "
    "MAX_CACHED_HANDLES environment variable. "
    "MAX_CACHED_HANDLES must be positive."
)

#: Because ServeController will accept one long poll request per handle, its
#: concurrency needs to scale as O(num_handles)
CONTROLLER_MAX_CONCURRENCY = int(os.getenv("CONTROLLER_MAX_CONCURRENCY", 15_000))
assert CONTROLLER_MAX_CONCURRENCY > 0, (
    f"Got unexpected value {CONTROLLER_MAX_CONCURRENCY} for "
    "CONTROLLER_MAX_CONCURRENCY environment variable. "
    "CONTROLLER_MAX_CONCURRENCY must be positive."
)

DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S = 20
DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S = 2
DEFAULT_HEALTH_CHECK_PERIOD_S = 10
DEFAULT_HEALTH_CHECK_TIMEOUT_S = 30
DEFAULT_MAX_ONGOING_REQUESTS = 5
DEFAULT_TARGET_ONGOING_REQUESTS = 2

# HTTP Proxy health check configs
PROXY_HEALTH_CHECK_TIMEOUT_S = (
    float(os.environ.get("RAY_SERVE_PROXY_HEALTH_CHECK_TIMEOUT_S", "10")) or 10
)
PROXY_HEALTH_CHECK_PERIOD_S = (
    float(os.environ.get("RAY_SERVE_PROXY_HEALTH_CHECK_PERIOD_S", "10")) or 10
)
PROXY_READY_CHECK_TIMEOUT_S = (
    float(os.environ.get("RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S", "5")) or 5
)

# Number of times in a row that a HTTP proxy must fail the health check before
# being marked unhealthy.
PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 3

# The minimum drain period for a HTTP proxy.
PROXY_MIN_DRAINING_PERIOD_S = (
    float(os.environ.get("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "30")) or 30
)
# The time in seconds that the http proxy state waits before
# rechecking whether the proxy actor is drained or not.
PROXY_DRAIN_CHECK_PERIOD_S = 5

#: Number of times in a row that a replica must fail the health check before
#: being marked unhealthy.
REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 3

# The time in seconds that the Serve client waits before rechecking deployment state
CLIENT_POLLING_INTERVAL_S: float = 1

# The time in seconds that the Serve client waits before checking if
# deployment has been created
CLIENT_CHECK_CREATION_POLLING_INTERVAL_S: float = 0.1

# Handle metric push interval. (This interval will affect the cold start time period)
HANDLE_METRIC_PUSH_INTERVAL_S = float(
    os.environ.get("RAY_SERVE_HANDLE_METRIC_PUSH_INTERVAL_S", "10")
)

# Timeout for GCS internal KV service
RAY_SERVE_KV_TIMEOUT_S = float(os.environ.get("RAY_SERVE_KV_TIMEOUT_S", "0")) or None

# Timeout for GCS RPC request
RAY_GCS_RPC_TIMEOUT_S = 3.0

# Maximum duration to wait until broadcasting a long poll update if there are
# still replicas in the RECOVERING state.
RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S = 10.0

# Minimum duration to wait until broadcasting model IDs.
PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S = 0.1


# Deprecation message for V1 migrations.
MIGRATION_MESSAGE = (
    "See https://docs.ray.io/en/latest/serve/index.html for more information."
)

DAG_DEPRECATION_MESSAGE = (
    "The DAG API is deprecated. Please use the recommended model composition pattern "
    "instead (see https://docs.ray.io/en/latest/serve/model_composition.html)."
)

# Environment variable name for to specify the encoding of the log messages
RAY_SERVE_LOG_ENCODING = os.environ.get("RAY_SERVE_LOG_ENCODING", "TEXT")

# Jsonify the log messages. This constant is deprecated and will be removed in the
# future. Use RAY_SERVE_LOG_ENCODING or 'LoggingConfig' to enable json format.
RAY_SERVE_ENABLE_JSON_LOGGING = os.environ.get("RAY_SERVE_ENABLE_JSON_LOGGING") == "1"

# Setting RAY_SERVE_LOG_TO_STDERR=0 will disable logging to the stdout and stderr.
# Also, redirect them to serve's log files.
RAY_SERVE_LOG_TO_STDERR = os.environ.get("RAY_SERVE_LOG_TO_STDERR", "1") == "1"

# Logging format attributes
SERVE_LOG_REQUEST_ID = "request_id"
SERVE_LOG_ROUTE = "route"
SERVE_LOG_APPLICATION = "application"
SERVE_LOG_DEPLOYMENT = "deployment"
SERVE_LOG_REPLICA = "replica"
SERVE_LOG_COMPONENT = "component_name"
SERVE_LOG_COMPONENT_ID = "component_id"
SERVE_LOG_MESSAGE = "message"
SERVE_LOG_ACTOR_ID = "actor_id"
SERVE_LOG_WORKER_ID = "worker_id"
# This is a reserved for python logging module attribute, it should not be changed.
SERVE_LOG_LEVEL_NAME = "levelname"
SERVE_LOG_TIME = "asctime"

# Logging format with record key to format string dict
SERVE_LOG_RECORD_FORMAT = {
    SERVE_LOG_REQUEST_ID: "%(request_id)s",
    SERVE_LOG_ROUTE: "%(route)s",
    SERVE_LOG_APPLICATION: "%(application)s",
    SERVE_LOG_MESSAGE: "%(filename)s:%(lineno)d - %(message)s",
    SERVE_LOG_LEVEL_NAME: "%(levelname)s",
    SERVE_LOG_TIME: "%(asctime)s",
}

# There are some attributes that we only use internally or don't provide values to the
# users. Adding to this set will remove them from structured logs.
SERVE_LOG_UNWANTED_ATTRS = {
    "serve_access_log",
    "task_id",
    "job_id",
}

SERVE_LOG_EXTRA_FIELDS = "ray_serve_extra_fields"

# Serve HTTP request header key for routing requests.
SERVE_MULTIPLEXED_MODEL_ID = "serve_multiplexed_model_id"

# Feature flag to turn on node locality routing for proxies. On by default.
RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING = (
    os.environ.get("RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING", "1") == "1"
)

# Feature flag to turn on AZ locality routing for proxies. On by default.
RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING = (
    os.environ.get("RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING", "1") == "1"
)

# Serve HTTP proxy callback import path.
RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH = os.environ.get(
    "RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH", None
)
# Serve controller callback import path.
RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH = os.environ.get(
    "RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH", None
)

# How often autoscaling metrics are recorded on Serve replicas.
RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S = 0.5

# How often autoscaling metrics are recorded on Serve handles.
RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_PERIOD_S = 0.5

# Serve multiplexed matching timeout.
# This is the timeout for the matching process of multiplexed requests. To avoid
# thundering herd problem, the timeout value will be randomed between this value
# and this value * 2. The unit is second.
# If the matching process takes longer than the timeout, the request will be
# fallen to the default routing strategy.
RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S = float(
    os.environ.get("RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S", "1")
)

# Enable memray in all Serve actors.
RAY_SERVE_ENABLE_MEMORY_PROFILING = (
    os.environ.get("RAY_SERVE_ENABLE_MEMORY_PROFILING", "0") == "1"
)

# Enable cProfile in all Serve actors.
RAY_SERVE_ENABLE_CPU_PROFILING = (
    os.environ.get("RAY_SERVE_ENABLE_CPU_PROFILING", "0") == "1"
)

# Max value allowed for max_replicas_per_node option.
# TODO(jjyao) the <= 100 limitation is an artificial one
# and is due to the fact that Ray core only supports resource
# precision up to 0.0001.
# This limitation should be lifted in the long term.
MAX_REPLICAS_PER_NODE_MAX_VALUE = 100

# Argument name for passing in the gRPC context into a replica.
GRPC_CONTEXT_ARG_NAME = "grpc_context"

# Whether or not to forcefully kill replicas that fail health checks.
RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS = (
    os.environ.get("RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS", "0") == "1"
)

# Initial deadline for queue length responses in the router.
RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S = float(
    os.environ.get("RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S", 0.1)
)

# Maximum deadline for queue length responses in the router (in backoff).
RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S = float(
    os.environ.get("RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S", 1.0)
)

# Feature flag for caching queue lengths for faster routing in each handle.
RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE = (
    os.environ.get("RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE", "1") == "1"
)

# Feature flag for strictly enforcing max_ongoing_requests (replicas will reject
# requests).
RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS = (
    os.environ.get("RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS", "0") == "1"
    # Strict enforcement path must be enabled for the queue length cache.
    or RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE
)

# Length of time to respect entries in the queue length cache when scheduling requests.
RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S = float(
    os.environ.get("RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S", 10.0)
)

# The default autoscaling policy to use if none is specified.
DEFAULT_AUTOSCALING_POLICY = "ray.serve.autoscaling_policy:default_autoscaling_policy"

# Feature flag to enable collecting all queued and ongoing request
# metrics at handles instead of replicas. ON by default.
RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE = (
    os.environ.get("RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE", "1") == "1"
)

RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S = float(
    os.environ.get("RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S", 10.0)
)

# Feature flag to always run a proxy on the head node even if it has no replicas.
RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE = (
    os.environ.get("RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE", "1") == "1"
)


# Default is 2GiB, the max for a signed int.
RAY_SERVE_GRPC_MAX_MESSAGE_SIZE = int(
    os.environ.get("RAY_SERVE_GRPC_MAX_MESSAGE_SIZE", (2 * 1024 * 1024 * 1024) - 1)
)

# Serve's gRPC server options.
SERVE_GRPC_OPTIONS = [
    ("grpc.max_send_message_length", RAY_SERVE_GRPC_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length", RAY_SERVE_GRPC_MAX_MESSAGE_SIZE),
]

# Feature flag to eagerly start replacement replicas. This means new
# replicas will start before waiting for old replicas to fully stop.
RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS = (
    os.environ.get("RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS", "1") == "1"
)

# Timeout for gracefully shutting down metrics pusher, e.g. in routers or replicas
METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S = 10

# Feature flag to set `enable_task_events=True` on Serve-managed actors.
RAY_SERVE_ENABLE_TASK_EVENTS = (
    os.environ.get("RAY_SERVE_ENABLE_TASK_EVENTS", "0") == "1"
)

# Use compact instead of spread scheduling strategy
RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY = (
    os.environ.get("RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY", "0") == "1"
)
