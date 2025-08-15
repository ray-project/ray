from typing import List

from ray.serve._private.constants_utils import (
    get_env_bool,
    get_env_float,
    get_env_float_non_negative,
    get_env_float_non_zero_with_warning,
    get_env_int,
    get_env_int_positive,
    get_env_str,
    parse_latency_buckets,
    str_to_list,
)

#: Logger used by serve components
SERVE_LOGGER_NAME = "ray.serve"

#: Actor name used to register controller
SERVE_CONTROLLER_NAME = "SERVE_CONTROLLER_ACTOR"

#: Actor name used to register HTTP proxy actor
SERVE_PROXY_NAME = "SERVE_PROXY_ACTOR"

#: Ray namespace used for all Serve actors
SERVE_NAMESPACE = "serve"

#: HTTP Host
DEFAULT_HTTP_HOST = get_env_str("RAY_SERVE_DEFAULT_HTTP_HOST", "127.0.0.1")

#: HTTP Port
DEFAULT_HTTP_PORT = get_env_int("RAY_SERVE_DEFAULT_HTTP_PORT", 8000)

#: Uvicorn timeout_keep_alive Config
DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S = 90

#: gRPC Port
DEFAULT_GRPC_PORT = get_env_int("RAY_SERVE_DEFAULT_GRPC_PORT", 9000)

#: Default Serve application name
SERVE_DEFAULT_APP_NAME = "default"

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

# How long to sleep between control loop cycles on the controller.
CONTROL_LOOP_INTERVAL_S = get_env_float_non_negative(
    "RAY_SERVE_CONTROL_LOOP_INTERVAL_S", 0.1
)

#: Max time to wait for HTTP proxy in `serve.start()`.
HTTP_PROXY_TIMEOUT = 60

#: Max retry count for allowing failures in replica constructor.
#: If no replicas at target version is running by the time we're at
#: max construtor retry count, deploy() is considered failed.
#: By default we set threshold as min(num_replicas * 3, this value)
MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT = get_env_int(
    "MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT", 20
)

# Max retry on deployment constructor is
# min(num_replicas * MAX_PER_REPLICA_RETRY_COUNT, MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT)
MAX_PER_REPLICA_RETRY_COUNT = get_env_int("MAX_PER_REPLICA_RETRY_COUNT", 3)

# If you are wondering why we are using histogram buckets, please refer to
# https://prometheus.io/docs/practices/histograms/
# short answer is that its cheaper to calculate percentiles on the histogram
# than to calculate them on raw data, both in terms of time and space.

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

# Example usage:
# RAY_SERVE_REQUEST_LATENCY_BUCKET_MS="1,2,3,4"
# RAY_SERVE_MODEL_LOAD_LATENCY_BUCKET_MS="1,2,3,4"
#: Histogram buckets for request latency.
REQUEST_LATENCY_BUCKETS_MS = parse_latency_buckets(
    get_env_str("REQUEST_LATENCY_BUCKETS_MS", ""), DEFAULT_LATENCY_BUCKET_MS
)
#: Histogram buckets for model load/unload latency.
MODEL_LOAD_LATENCY_BUCKETS_MS = parse_latency_buckets(
    get_env_str("MODEL_LOAD_LATENCY_BUCKETS_MS", ""), DEFAULT_LATENCY_BUCKET_MS
)

#: Name of deployment health check method implemented by user.
HEALTH_CHECK_METHOD = "check_health"

#: Name of deployment reconfiguration method implemented by user.
RECONFIGURE_METHOD = "reconfigure"

SERVE_ROOT_URL_ENV_KEY = "RAY_SERVE_ROOT_URL"

#: Limit the number of cached handles because each handle has long poll
#: overhead. See https://github.com/ray-project/ray/issues/18980
MAX_CACHED_HANDLES = get_env_int_positive("MAX_CACHED_HANDLES", 100)

#: Because ServeController will accept one long poll request per handle, its
#: concurrency needs to scale as O(num_handles)
CONTROLLER_MAX_CONCURRENCY = get_env_int_positive("CONTROLLER_MAX_CONCURRENCY", 15_000)

DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S = 20
DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S = 2
DEFAULT_HEALTH_CHECK_PERIOD_S = 10
DEFAULT_HEALTH_CHECK_TIMEOUT_S = 30
DEFAULT_MAX_ONGOING_REQUESTS = 5
DEFAULT_TARGET_ONGOING_REQUESTS = 2

# HTTP Proxy health check configs
PROXY_HEALTH_CHECK_TIMEOUT_S = get_env_float_non_zero_with_warning(
    "RAY_SERVE_PROXY_HEALTH_CHECK_TIMEOUT_S", 10.0
)

PROXY_HEALTH_CHECK_PERIOD_S = get_env_float_non_zero_with_warning(
    "RAY_SERVE_PROXY_HEALTH_CHECK_PERIOD_S", 10.0
)
PROXY_READY_CHECK_TIMEOUT_S = get_env_float_non_zero_with_warning(
    "RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S", 5.0
)

# Number of times in a row that a HTTP proxy must fail the health check before
# being marked unhealthy.
PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 3

# The minimum drain period for a HTTP proxy.
PROXY_MIN_DRAINING_PERIOD_S = get_env_float_non_zero_with_warning(
    "RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", 30.0
)
# The time in seconds that the http proxy state waits before
# rechecking whether the proxy actor is drained or not.
PROXY_DRAIN_CHECK_PERIOD_S = 5

#: Number of times in a row that a replica must fail the health check before
#: being marked unhealthy.
REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 3

# The time in seconds that the Serve client waits before rechecking deployment state
CLIENT_POLLING_INTERVAL_S = 1.0

# The time in seconds that the Serve client waits before checking if
# deployment has been created
CLIENT_CHECK_CREATION_POLLING_INTERVAL_S = 0.1

# Timeout for GCS internal KV service
RAY_SERVE_KV_TIMEOUT_S = get_env_float_non_zero_with_warning(
    "RAY_SERVE_KV_TIMEOUT_S", None
)

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

# Environment variable name for to specify the encoding of the log messages
RAY_SERVE_LOG_ENCODING = get_env_str("RAY_SERVE_LOG_ENCODING", "TEXT")

# Jsonify the log messages. This constant is deprecated and will be removed in the
# future. Use RAY_SERVE_LOG_ENCODING or 'LoggingConfig' to enable json format.
RAY_SERVE_ENABLE_JSON_LOGGING = get_env_bool("RAY_SERVE_ENABLE_JSON_LOGGING", "0")

# Setting RAY_SERVE_LOG_TO_STDERR=0 will disable logging to the stdout and stderr.
# Also, redirect them to serve's log files.
RAY_SERVE_LOG_TO_STDERR = get_env_bool("RAY_SERVE_LOG_TO_STDERR", "1")

# Logging format attributes
SERVE_LOG_REQUEST_ID = "request_id"
SERVE_LOG_ROUTE = "route"
SERVE_LOG_APPLICATION = "application"
SERVE_LOG_DEPLOYMENT = "deployment"
SERVE_LOG_REPLICA = "replica"
SERVE_LOG_COMPONENT = "component_name"
SERVE_LOG_COMPONENT_ID = "component_id"
SERVE_LOG_MESSAGE = "message"
# This is a reserved for python logging module attribute, it should not be changed.
SERVE_LOG_LEVEL_NAME = "levelname"
SERVE_LOG_TIME = "asctime"

# Logging format with record key to format string dict
SERVE_LOG_RECORD_FORMAT = {
    SERVE_LOG_REQUEST_ID: "%(request_id)s",
    SERVE_LOG_APPLICATION: "%(application)s",
    SERVE_LOG_MESSAGE: "-- %(message)s",
    SERVE_LOG_LEVEL_NAME: "%(levelname)s",
    SERVE_LOG_TIME: "%(asctime)s",
}

# There are some attributes that we only use internally or don't provide values to the
# users. Adding to this set will remove them from structured logs.
SERVE_LOG_UNWANTED_ATTRS = {
    "serve_access_log",
    "task_id",
    "job_id",
    "skip_context_filter",
}

RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S = get_env_int(
    "RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S", 0
)

RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S = (
    get_env_float("RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S", 0.0)
    or get_env_float("SERVE_REQUEST_PROCESSING_TIMEOUT_S", 0.0)
    or None
)

SERVE_LOG_EXTRA_FIELDS = "ray_serve_extra_fields"

# Serve HTTP request header key for routing requests.
SERVE_MULTIPLEXED_MODEL_ID = "serve_multiplexed_model_id"

# HTTP request ID
SERVE_HTTP_REQUEST_ID_HEADER = "x-request-id"

# Feature flag to turn on node locality routing for proxies. On by default.
RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING = get_env_bool(
    "RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING", "1"
)

# Feature flag to turn on AZ locality routing for proxies. On by default.
RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING = get_env_bool(
    "RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING", "1"
)

# Serve HTTP proxy callback import path.
RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH = get_env_str(
    "RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH", None
)
# Serve controller callback import path.
RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH = get_env_str(
    "RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH", None
)

# How often autoscaling metrics are recorded on Serve replicas.
RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_INTERVAL_S = get_env_float(
    "RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_INTERVAL_S", 0.5
)

# Replica autoscaling metrics push interval.
RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S = get_env_float(
    "RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S", 10.0
)

# How often autoscaling metrics are recorded on Serve handles.
RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_INTERVAL_S = get_env_float(
    "RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_INTERVAL_S", 0.5
)

# Handle autoscaling metrics push interval. (This interval will affect the cold start time period)
RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S = get_env_float(
    "RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S",
    # Legacy env var for RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S
    get_env_float("RAY_SERVE_HANDLE_METRIC_PUSH_INTERVAL_S", 10.0),
)

# Serve multiplexed matching timeout.
# This is the timeout for the matching process of multiplexed requests. To avoid
# thundering herd problem, the timeout value will be randomed between this value
# and this value * 2. The unit is second.
# If the matching process takes longer than the timeout, the request will be
# fallen to the default routing strategy.
RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S = get_env_float(
    "RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S", 1.0
)

# Enable memray in all Serve actors.
RAY_SERVE_ENABLE_MEMORY_PROFILING = get_env_bool(
    "RAY_SERVE_ENABLE_MEMORY_PROFILING", "0"
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
RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS = get_env_bool(
    "RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS", "0"
)

# Initial deadline for queue length responses in the router.
RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S = get_env_float(
    "RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S", 0.1
)

# Maximum deadline for queue length responses in the router (in backoff).
RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S = get_env_float(
    "RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S", 1.0
)

# Length of time to respect entries in the queue length cache when routing requests.
RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S = get_env_float(
    "RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S", 10.0
)

# Backoff seconds when choosing router failed, backoff time is calculated as
# initial_backoff_s * backoff_multiplier ** attempt.
# The default backoff time is [0, 0.025, 0.05, 0.1, 0.2, 0.4, 0.5, 0.5 ... ].
RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S = get_env_float(
    "RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S", 0.025
)
RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER = get_env_int(
    "RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER", 2
)
RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S = get_env_float(
    "RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S", 0.5
)

# The default autoscaling policy to use if none is specified.
DEFAULT_AUTOSCALING_POLICY_NAME = (
    "ray.serve.autoscaling_policy:default_autoscaling_policy"
)

# Feature flag to enable collecting all queued and ongoing request
# metrics at handles instead of replicas. ON by default.
RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE = get_env_bool(
    "RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE", "1"
)

RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S = get_env_float(
    "RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S", 10.0
)

# Feature flag to always run a proxy on the head node even if it has no replicas.
RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE = get_env_bool(
    "RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE", "1"
)

# Default is 2GiB, the max for a signed int.
RAY_SERVE_GRPC_MAX_MESSAGE_SIZE = get_env_int(
    "RAY_SERVE_GRPC_MAX_MESSAGE_SIZE", (2 * 1024 * 1024 * 1024) - 1
)

# Default options passed when constructing gRPC servers.
DEFAULT_GRPC_SERVER_OPTIONS = [
    ("grpc.max_send_message_length", RAY_SERVE_GRPC_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length", RAY_SERVE_GRPC_MAX_MESSAGE_SIZE),
]

# Timeout for gracefully shutting down metrics pusher, e.g. in routers or replicas
METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S = 10

# Feature flag to set `enable_task_events=True` on Serve-managed actors.
RAY_SERVE_ENABLE_TASK_EVENTS = get_env_bool("RAY_SERVE_ENABLE_TASK_EVENTS", "0")

# Use compact instead of spread scheduling strategy
RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY = get_env_bool(
    "RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY", "0"
)

# Comma-separated list of custom resources prioritized in scheduling. Sorted from highest to lowest priority.
# Example: "customx,customy"
RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES: List[str] = str_to_list(
    get_env_str("RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES", "")
)

# Feature flag to always override local_testing_mode to True in serve.run.
# This is used for internal testing to avoid passing the flag to every invocation.
RAY_SERVE_FORCE_LOCAL_TESTING_MODE = get_env_bool(
    "RAY_SERVE_FORCE_LOCAL_TESTING_MODE", "0"
)

# Run sync methods defined in the replica in a thread pool by default.
RAY_SERVE_RUN_SYNC_IN_THREADPOOL = get_env_bool("RAY_SERVE_RUN_SYNC_IN_THREADPOOL", "0")

RAY_SERVE_RUN_SYNC_IN_THREADPOOL_WARNING = (
    "Calling sync method '{method_name}' directly on the "
    "asyncio loop. In a future version, sync methods will be run in a "
    "threadpool by default. Ensure your sync methods are thread safe "
    "or keep the existing behavior by making them `async def`. Opt "
    "into the new behavior by setting "
    "RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1."
)

# Feature flag to turn off GC optimizations in the proxy (in case there is a
# memory leak or negative performance impact).
RAY_SERVE_ENABLE_PROXY_GC_OPTIMIZATIONS = get_env_bool(
    "RAY_SERVE_ENABLE_PROXY_GC_OPTIMIZATIONS", "1"
)

# Used for gc.set_threshold() when proxy GC optimizations are enabled.
RAY_SERVE_PROXY_GC_THRESHOLD = get_env_int("RAY_SERVE_PROXY_GC_THRESHOLD", 10_000)

# Interval at which cached metrics will be exported using the Ray metric API.
# Set to `0` to disable caching entirely.
RAY_SERVE_METRICS_EXPORT_INTERVAL_MS = get_env_int(
    "RAY_SERVE_METRICS_EXPORT_INTERVAL_MS", 100
)

# The default request router class to use if none is specified.
DEFAULT_REQUEST_ROUTER_PATH = (
    "ray.serve._private.request_router:PowerOfTwoChoicesRequestRouter"
)

# The default request routing period to use if none is specified.
DEFAULT_REQUEST_ROUTING_STATS_PERIOD_S = 10

# The default request routing timeout to use if none is specified.
DEFAULT_REQUEST_ROUTING_STATS_TIMEOUT_S = 30

# Name of deployment request routing stats method implemented by user.
REQUEST_ROUTING_STATS_METHOD = "record_routing_stats"

# By default, we run user code in a separate event loop.
# This flag can be set to 0 to run user code in the same event loop as the
# replica's main event loop.
RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD = get_env_bool(
    "RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD", "1"
)

# By default, we run the router in a separate event loop.
# This flag can be set to 0 to run the router in the same event loop as the
# replica's main event loop.
RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP = get_env_bool(
    "RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP", "1"
)

# The default buffer size for request path logs. Setting to 1 will ensure
# logs are flushed to file handler immediately, otherwise it will be buffered
# and flushed to file handler when the buffer is full or when there is a log
# line with level ERROR.
RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE = get_env_int(
    "RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE", 1
)

# The message to return when the replica is healthy.
HEALTHY_MESSAGE = "success"
