"""Ray constants used in the Python code."""

import json
import logging
import os
import sys

logger = logging.getLogger(__name__)


def env_integer(key, default):
    if key in os.environ:
        value = os.environ[key]
        if value.isdigit():
            return int(os.environ[key])

        logger.debug(
            f"Found {key} in environment, but value must "
            f"be an integer. Got: {value}. Returning "
            f"provided default {default}."
        )
        return default
    return default


def env_float(key, default):
    if key in os.environ:
        value = os.environ[key]
        try:
            return float(value)
        except ValueError:
            logger.debug(
                f"Found {key} in environment, but value must "
                f"be a float. Got: {value}. Returning "
                f"provided default {default}."
            )
            return default
    return default


def env_bool(key, default):
    if key in os.environ:
        return (
            True
            if os.environ[key].lower() == "true" or os.environ[key] == "1"
            else False
        )
    return default


def env_set_by_user(key):
    return key in os.environ


# Whether event logging to driver is enabled. Set to 0 to disable.
AUTOSCALER_EVENTS = env_integer("RAY_SCHEDULER_EVENTS", 1)

# Whether to disable the C++ failure signal handler that provides stack traces
# on crashes. Disabling this is necessary when using Java libraries
# because Ray's signal handler conflicts with the JVM's signal handling.
RAY_DISABLE_FAILURE_SIGNAL_HANDLER = env_bool(
    "RAY_DISABLE_FAILURE_SIGNAL_HANDLER", False
)

RAY_LOG_TO_DRIVER = env_bool("RAY_LOG_TO_DRIVER", True)

# Filter level under which events will be filtered out, i.e. not printing to driver
RAY_LOG_TO_DRIVER_EVENT_LEVEL = os.environ.get("RAY_LOG_TO_DRIVER_EVENT_LEVEL", "INFO")

# Internal kv keys for storing monitor debug status.
DEBUG_AUTOSCALING_ERROR = "__autoscaling_error"
DEBUG_AUTOSCALING_STATUS = "__autoscaling_status"
DEBUG_AUTOSCALING_STATUS_LEGACY = "__autoscaling_status_legacy"

ID_SIZE = 28

# The following constants are used to create default values for
# resource isolation when it is enabled.
# TODO(54703): Link to OSS documentation about the feature once it's available.
DEFAULT_CGROUP_PATH = "/sys/fs/cgroup"
# The default proportion of cpu cores to reserve for ray system processes.
DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION = env_float(
    "RAY_DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION", 0.05
)
# The default minimum number of cpu cores to reserve for ray system processes.
# This value is used if the available_cores * DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION < this value.
DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES = env_float(
    "RAY_DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES", 1.0
)
# The default maximum number of cpu cores to reserve for ray system processes.
# This value is used if the available_cores * DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION > this value.
DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES = env_float(
    "RAY_DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES", 3.0
)
# The values for SYSTEM_RESERVED_MEMORY do not include the memory reserveed
# for the object store.
# The default proportion available memory to reserve for ray system processes.
DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION = env_float(
    "RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION", 0.10
)
# The default minimum number of bytes to reserve for ray system processes.
# This value is used if the available_memory * DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION < this value.
DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES = env_integer(
    "RAY_DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES", (500) * (1024**2)
)
# The default maximum number of bytes to reserve for ray system processes.
# This value is used if the available_memory * DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION > this value.
DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES = env_integer(
    "RAY_DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES", (10) * (1024**3)
)

# The default maximum number of bytes to allocate to the object store unless
# overridden by the user.
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = env_integer(
    "RAY_DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES", (200) * (10**9)  # 200 GB
)
# The default proportion of available memory allocated to the object store
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = env_float(
    "RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION",
    0.3,
)
# The smallest cap on the memory used by the object store that we allow.
# This must be greater than MEMORY_RESOURCE_UNIT_BYTES
OBJECT_STORE_MINIMUM_MEMORY_BYTES = 75 * 1024 * 1024
# Each ObjectRef currently uses about 3KB of caller memory.
CALLER_MEMORY_USAGE_PER_OBJECT_REF = 3000
# Above this number of bytes, raise an error by default unless the user sets
# RAY_ALLOW_SLOW_STORAGE=1. This avoids swapping with large object stores.
REQUIRE_SHM_SIZE_THRESHOLD = 10**10
# Mac with 16GB memory has degraded performance when the object store size is
# greater than 2GB.
# (see https://github.com/ray-project/ray/issues/20388 for details)
# The workaround here is to limit capacity to 2GB for Mac by default,
# and raise error if the capacity is overwritten by user.
MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT = (2) * (2**30)
# If a user does not specify a port for the primary Ray service,
# we attempt to start the service running at this port.
DEFAULT_PORT = 6379

RAY_ADDRESS_ENVIRONMENT_VARIABLE = "RAY_ADDRESS"
RAY_API_SERVER_ADDRESS_ENVIRONMENT_VARIABLE = "RAY_API_SERVER_ADDRESS"
RAY_NAMESPACE_ENVIRONMENT_VARIABLE = "RAY_NAMESPACE"
RAY_RUNTIME_ENV_ENVIRONMENT_VARIABLE = "RAY_RUNTIME_ENV"
RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR = (
    "RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S"
)
# Ray populates this env var to the working dir in the creation of a runtime env.
# For example, `pip` and `conda` users can use this environment variable to locate the
# `requirements.txt` file.
RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR = "RAY_RUNTIME_ENV_CREATE_WORKING_DIR"
# Defaults to 10 minutes. This should be longer than the total time it takes for
# the local working_dir and py_modules to be uploaded, or these files might get
# garbage collected before the job starts.
RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT = 10 * 60
# If set to 1, then `.gitignore` files will not be parsed and loaded into "excludes"
# when using a local working_dir or py_modules.
RAY_RUNTIME_ENV_IGNORE_GITIGNORE = "RAY_RUNTIME_ENV_IGNORE_GITIGNORE"
# Hook for running a user-specified runtime-env hook. This hook will be called
# unconditionally given the runtime_env dict passed for ray.init. It must return
# a rewritten runtime_env dict. Example: "your.module.runtime_env_hook".
RAY_RUNTIME_ENV_HOOK = "RAY_RUNTIME_ENV_HOOK"
# Hook that is invoked on `ray start`. It will be given the cluster parameters and
# whether we are the head node as arguments. The function can modify the params class,
# but otherwise returns void. Example: "your.module.ray_start_hook".
RAY_START_HOOK = "RAY_START_HOOK"
# Hook that is invoked on `ray job submit`. It will be given all the same args as the
# job.cli.submit() function gets, passed as kwargs to this function.
RAY_JOB_SUBMIT_HOOK = "RAY_JOB_SUBMIT_HOOK"
# Headers to pass when using the Job CLI. It will be given to
# instantiate a Job SubmissionClient.
RAY_JOB_HEADERS = "RAY_JOB_HEADERS"

# Timeout waiting for the dashboard to come alive during node startup.
RAY_DASHBOARD_STARTUP_TIMEOUT_S = env_integer("RAY_DASHBOARD_STARTUP_TIMEOUT_S", 60)

DEFAULT_DASHBOARD_IP = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 8265
DASHBOARD_ADDRESS = "dashboard"
DASHBOARD_CLIENT_MAX_SIZE = 100 * 1024**2
PROMETHEUS_SERVICE_DISCOVERY_FILE = "prom_metrics_service_discovery.json"
DEFAULT_DASHBOARD_AGENT_LISTEN_PORT = 52365
# Default resource requirements for actors when no resource requirements are
# specified.
DEFAULT_ACTOR_METHOD_CPU_SIMPLE = 1
DEFAULT_ACTOR_CREATION_CPU_SIMPLE = 0
# Default resource requirements for actors when some resource requirements are
# specified in .
DEFAULT_ACTOR_METHOD_CPU_SPECIFIED = 0
DEFAULT_ACTOR_CREATION_CPU_SPECIFIED = 1
# Default number of return values for each actor method.
DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS = 1

# Wait 30 seconds for client to reconnect after unexpected disconnection
DEFAULT_CLIENT_RECONNECT_GRACE_PERIOD = 30

# If a remote function or actor (or some other export) has serialized size
# greater than this quantity, print an warning.
FUNCTION_SIZE_WARN_THRESHOLD = 10**7
FUNCTION_SIZE_ERROR_THRESHOLD = env_integer("FUNCTION_SIZE_ERROR_THRESHOLD", (10**8))

# If remote functions with the same source are imported this many times, then
# print a warning.
DUPLICATE_REMOTE_FUNCTION_THRESHOLD = 100

# The maximum resource quantity that is allowed. TODO(rkn): This could be
# relaxed, but the current implementation of the node manager will be slower
# for large resource quantities due to bookkeeping of specific resource IDs.
MAX_RESOURCE_QUANTITY = 100e12

# Number of units 1 resource can be subdivided into.
MIN_RESOURCE_GRANULARITY = 0.0001

# Set this environment variable to populate the dashboard URL with
# an external hosted Ray dashboard URL (e.g. because the
# dashboard is behind a proxy or load balancer). This only overrides
# the dashboard URL when returning or printing to a user through a public
# API, but not in the internal KV store.
RAY_OVERRIDE_DASHBOARD_URL = "RAY_OVERRIDE_DASHBOARD_URL"


# Different types of Ray errors that can be pushed to the driver.
# TODO(rkn): These should be defined in flatbuffers and must be synced with
# the existing C++ definitions.
PICKLING_LARGE_OBJECT_PUSH_ERROR = "pickling_large_object"
WAIT_FOR_FUNCTION_PUSH_ERROR = "wait_for_function"
VERSION_MISMATCH_PUSH_ERROR = "version_mismatch"
WORKER_CRASH_PUSH_ERROR = "worker_crash"
WORKER_DIED_PUSH_ERROR = "worker_died"
WORKER_POOL_LARGE_ERROR = "worker_pool_large"
PUT_RECONSTRUCTION_PUSH_ERROR = "put_reconstruction"
RESOURCE_DEADLOCK_ERROR = "resource_deadlock"
REMOVED_NODE_ERROR = "node_removed"
MONITOR_DIED_ERROR = "monitor_died"
LOG_MONITOR_DIED_ERROR = "log_monitor_died"
DASHBOARD_AGENT_DIED_ERROR = "dashboard_agent_died"
DASHBOARD_DIED_ERROR = "dashboard_died"
RAYLET_DIED_ERROR = "raylet_died"
DETACHED_ACTOR_ANONYMOUS_NAMESPACE_ERROR = "detached_actor_anonymous_namespace"
EXCESS_QUEUEING_WARNING = "excess_queueing_warning"

# Used by autoscaler to set the node custom resources and labels
# from cluster.yaml.
RESOURCES_ENVIRONMENT_VARIABLE = "RAY_OVERRIDE_RESOURCES"
LABELS_ENVIRONMENT_VARIABLE = "RAY_OVERRIDE_LABELS"

# Temporary flag to disable log processing in the dashboard.  This is useful
# if the dashboard is overloaded by logs and failing to process other
# dashboard API requests (e.g. Job Submission).
DISABLE_DASHBOARD_LOG_INFO = env_integer("RAY_DISABLE_DASHBOARD_LOG_INFO", 0)

LOGGER_FORMAT = "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
LOGGER_FORMAT_ESCAPE = json.dumps(LOGGER_FORMAT.replace("%", "%%"))
LOGGER_FORMAT_HELP = f"The logging format. default={LOGGER_FORMAT_ESCAPE}"
# Configure the default logging levels for various Ray components.
# TODO (kevin85421): Currently, I don't encourage Ray users to configure
# `RAY_LOGGER_LEVEL` until its scope and expected behavior are clear and
# easy to understand. Now, only Ray developers should use it.
LOGGER_LEVEL = os.environ.get("RAY_LOGGER_LEVEL", "info")
LOGGER_LEVEL_CHOICES = ["debug", "info", "warning", "error", "critical"]
LOGGER_LEVEL_HELP = (
    "The logging level threshold, choices=['debug', 'info',"
    " 'warning', 'error', 'critical'], default='info'"
)

LOGGING_REDIRECT_STDERR_ENVIRONMENT_VARIABLE = "RAY_LOG_TO_STDERR"
# Logging format when logging stderr. This should be formatted with the
# component before setting the formatter, e.g. via
#   format = LOGGER_FORMAT_STDERR.format(component="dashboard")
#   handler.setFormatter(logging.Formatter(format))
LOGGER_FORMAT_STDERR = (
    "%(asctime)s\t%(levelname)s ({component}) %(filename)s:%(lineno)s -- %(message)s"
)

# Constants used to define the different process types.
PROCESS_TYPE_REAPER = "reaper"
PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_RAY_CLIENT_SERVER = "ray_client_server"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_DASHBOARD = "dashboard"
PROCESS_TYPE_DASHBOARD_AGENT = "dashboard_agent"
PROCESS_TYPE_RUNTIME_ENV_AGENT = "runtime_env_agent"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_GCS_SERVER = "gcs_server"
PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER = "python-core-driver"
PROCESS_TYPE_PYTHON_CORE_WORKER = "python-core-worker"

# Log file names
MONITOR_LOG_FILE_NAME = f"{PROCESS_TYPE_MONITOR}.log"
LOG_MONITOR_LOG_FILE_NAME = f"{PROCESS_TYPE_LOG_MONITOR}.log"

# Enable log deduplication.
RAY_DEDUP_LOGS = env_bool("RAY_DEDUP_LOGS", True)

# How many seconds of messages to buffer for log deduplication.
RAY_DEDUP_LOGS_AGG_WINDOW_S = env_integer("RAY_DEDUP_LOGS_AGG_WINDOW_S", 5)

# Regex for log messages to never deduplicate, or None. This takes precedence over
# the skip regex below. A default pattern is set for testing.
TESTING_NEVER_DEDUP_TOKEN = "__ray_testing_never_deduplicate__"
RAY_DEDUP_LOGS_ALLOW_REGEX = os.environ.get(
    "RAY_DEDUP_LOGS_ALLOW_REGEX", TESTING_NEVER_DEDUP_TOKEN
)

# Regex for log messages to always skip / suppress, or None.
RAY_DEDUP_LOGS_SKIP_REGEX = os.environ.get("RAY_DEDUP_LOGS_SKIP_REGEX")

WORKER_PROCESS_TYPE_IDLE_WORKER = "ray::IDLE"
WORKER_PROCESS_TYPE_SPILL_WORKER_NAME = "SpillWorker"
WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME = "RestoreWorker"
WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE = (
    f"ray::IDLE_{WORKER_PROCESS_TYPE_SPILL_WORKER_NAME}"
)
WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE = (
    f"ray::IDLE_{WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME}"
)
WORKER_PROCESS_TYPE_SPILL_WORKER = f"ray::SPILL_{WORKER_PROCESS_TYPE_SPILL_WORKER_NAME}"
WORKER_PROCESS_TYPE_RESTORE_WORKER = (
    f"ray::RESTORE_{WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME}"
)
WORKER_PROCESS_TYPE_SPILL_WORKER_DELETE = (
    f"ray::DELETE_{WORKER_PROCESS_TYPE_SPILL_WORKER_NAME}"
)
WORKER_PROCESS_TYPE_RESTORE_WORKER_DELETE = (
    f"ray::DELETE_{WORKER_PROCESS_TYPE_RESTORE_WORKER_NAME}"
)

# The number of files the log monitor will open. If more files exist, they will
# be ignored.
LOG_MONITOR_MAX_OPEN_FILES = int(
    os.environ.get("RAY_LOG_MONITOR_MAX_OPEN_FILES", "200")
)

# The maximum batch of lines to be read in a single iteration. We _always_ try
# to read this number of lines even if there aren't any new lines.
LOG_MONITOR_NUM_LINES_TO_READ = int(
    os.environ.get("RAY_LOG_MONITOR_NUM_LINES_TO_READ", "1000")
)

# Autoscaler events are denoted by the ":event_summary:" magic token.
LOG_PREFIX_EVENT_SUMMARY = ":event_summary:"
# Cluster-level info events are denoted by the ":info_message:" magic token. These may
# be emitted in the stderr of Ray components.
LOG_PREFIX_INFO_MESSAGE = ":info_message:"
# Actor names are recorded in the logs with this magic token as a prefix.
LOG_PREFIX_ACTOR_NAME = ":actor_name:"
# Task names are recorded in the logs with this magic token as a prefix.
LOG_PREFIX_TASK_NAME = ":task_name:"
# Job ids are recorded in the logs with this magic token as a prefix.
LOG_PREFIX_JOB_ID = ":job_id:"

# The object metadata field uses the following format: It is a comma
# separated list of fields. The first field is mandatory and is the
# type of the object (see types below) or an integer, which is interpreted
# as an error value. The second part is optional and if present has the
# form DEBUG:<breakpoint_id>, it is used for implementing the debugger.

# A constant used as object metadata to indicate the object is cross language.
OBJECT_METADATA_TYPE_CROSS_LANGUAGE = b"XLANG"
# A constant used as object metadata to indicate the object is python specific.
OBJECT_METADATA_TYPE_PYTHON = b"PYTHON"
# A constant used as object metadata to indicate the object is raw bytes.
OBJECT_METADATA_TYPE_RAW = b"RAW"

# A constant used as object metadata to indicate the object is an actor handle.
# This value should be synchronized with the Java definition in
# ObjectSerializer.java
# TODO(fyrestone): Serialize the ActorHandle via the custom type feature
# of XLANG.
OBJECT_METADATA_TYPE_ACTOR_HANDLE = b"ACTOR_HANDLE"

# A constant indicating the debugging part of the metadata (see above).
OBJECT_METADATA_DEBUG_PREFIX = b"DEBUG:"

AUTOSCALER_RESOURCE_REQUEST_CHANNEL = b"autoscaler_resource_request"

REDIS_DEFAULT_USERNAME = ""

REDIS_DEFAULT_PASSWORD = ""

# The default ip address to bind to.
NODE_DEFAULT_IP = "127.0.0.1"

# The Mach kernel page size in bytes.
MACH_PAGE_SIZE_BYTES = 4096

# The max number of bytes for task execution error message.
MAX_APPLICATION_ERROR_LEN = 500

# Max 64 bit integer value, which is needed to ensure against overflow
# in C++ when passing integer values cross-language.
MAX_INT64_VALUE = 9223372036854775807

# Object Spilling related constants
DEFAULT_OBJECT_PREFIX = "ray_spilled_objects"

GCS_PORT_ENVIRONMENT_VARIABLE = "RAY_GCS_SERVER_PORT"

HEALTHCHECK_EXPIRATION_S = os.environ.get("RAY_HEALTHCHECK_EXPIRATION_S", 10)

# Filename of "shim process" that sets up Python worker environment.
# Should be kept in sync with kSetupWorkerFilename in
# src/ray/common/constants.h.
SETUP_WORKER_FILENAME = "setup_worker.py"

# Directory name where runtime_env resources will be created & cached.
DEFAULT_RUNTIME_ENV_DIR_NAME = "runtime_resources"

# The timeout seconds for the creation of runtime env,
# dafault timeout is 10 minutes
DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS = 600

# Used to separate lines when formatting the call stack where an ObjectRef was
# created.
CALL_STACK_LINE_DELIMITER = " | "

# The default gRPC max message size is 4 MiB, we use a larger number of 512 MiB
# NOTE: This is equal to the C++ limit of (RAY_CONFIG::max_grpc_message_size)
GRPC_CPP_MAX_MESSAGE_SIZE = 512 * 1024 * 1024

# The gRPC send & receive max length for "dashboard agent" server.
# NOTE: This is equal to the C++ limit of RayConfig::max_grpc_message_size
#       and HAVE TO STAY IN SYNC with it (ie, meaning that both of these values
#       have to be set at the same time)
AGENT_GRPC_MAX_MESSAGE_LENGTH = env_integer(
    "AGENT_GRPC_MAX_MESSAGE_LENGTH", 20 * 1024 * 1024  # 20MB
)


# GRPC options
GRPC_ENABLE_HTTP_PROXY = (
    1
    if os.environ.get("RAY_grpc_enable_http_proxy", "0").lower() in ("1", "true")
    else 0
)
GLOBAL_GRPC_OPTIONS = (("grpc.enable_http_proxy", GRPC_ENABLE_HTTP_PROXY),)

# Internal kv namespaces
KV_NAMESPACE_DASHBOARD = b"dashboard"
KV_NAMESPACE_SESSION = b"session"
KV_NAMESPACE_TRACING = b"tracing"
KV_NAMESPACE_PDB = b"ray_pdb"
KV_NAMESPACE_HEALTHCHECK = b"healthcheck"
KV_NAMESPACE_JOB = b"job"
KV_NAMESPACE_CLUSTER = b"cluster"
KV_HEAD_NODE_ID_KEY = b"head_node_id"
# TODO: Set package for runtime env
# We need to update ray client for this since runtime env use ray client
# This might introduce some compatibility issues so leave it here for now.
KV_NAMESPACE_PACKAGE = None
KV_NAMESPACE_FUNCTION_TABLE = b"fun"

LANGUAGE_WORKER_TYPES = ["python", "java", "cpp"]

NEURON_CORES = "neuron_cores"
GPU = "GPU"
TPU = "TPU"
NPU = "NPU"
HPU = "HPU"


RAY_WORKER_NICENESS = "RAY_worker_niceness"

# Default max_retries option in @ray.remote for non-actor
# tasks.
DEFAULT_TASK_MAX_RETRIES = 3

# Default max_concurrency option in @ray.remote for threaded actors.
DEFAULT_MAX_CONCURRENCY_THREADED = 1

# Ray internal flags. These flags should not be set by users, and we strip them on job
# submission.
# This should be consistent with src/ray/common/ray_internal_flag_def.h
RAY_INTERNAL_FLAGS = [
    "RAY_JOB_ID",
    "RAY_RAYLET_PID",
    "RAY_OVERRIDE_NODE_ID_FOR_TESTING",
]


def gcs_actor_scheduling_enabled():
    return os.environ.get("RAY_gcs_actor_scheduling_enabled") == "true"


DEFAULT_RESOURCES = {"CPU", "GPU", "memory", "object_store_memory"}

# Supported Python versions for runtime env's "conda" field. Ray downloads
# Ray wheels into the conda environment, so the Ray wheels for these Python
# versions must be available online.
RUNTIME_ENV_CONDA_PY_VERSIONS = [(3, 9), (3, 10), (3, 11), (3, 12)]

# Whether to enable Ray clusters (in addition to local Ray).
# Ray clusters are not explicitly supported for Windows and OSX.
IS_WINDOWS_OR_OSX = sys.platform == "darwin" or sys.platform == "win32"
ENABLE_RAY_CLUSTERS_ENV_VAR = "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER"
ENABLE_RAY_CLUSTER = env_bool(
    ENABLE_RAY_CLUSTERS_ENV_VAR,
    not IS_WINDOWS_OR_OSX,
)

SESSION_LATEST = "session_latest"
NUM_PORT_RETRIES = 40
NUM_REDIS_GET_RETRIES = int(os.environ.get("RAY_NUM_REDIS_GET_RETRIES", "20"))

# The allowed cached ports in Ray. Refer to Port configuration for more details:
# https://docs.ray.io/en/latest/ray-core/configure.html#ports-configurations
RAY_ALLOWED_CACHED_PORTS = {
    "metrics_agent_port",
    "metrics_export_port",
    "dashboard_agent_listen_port",
    "runtime_env_agent_port",
    "gcs_server_port",  # the `port` option for gcs port.
}

# Turn this on if actor task log's offsets are expected to be recorded.
# With this enabled, actor tasks' log could be queried with task id.
RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING = env_bool(
    "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING", False
)

# RuntimeEnv env var to indicate it exports a function
WORKER_PROCESS_SETUP_HOOK_ENV_VAR = "__RAY_WORKER_PROCESS_SETUP_HOOK_ENV_VAR"
RAY_WORKER_PROCESS_SETUP_HOOK_LOAD_TIMEOUT_ENV_VAR = (
    "RAY_WORKER_PROCESS_SETUP_HOOK_LOAD_TIMEOUT"  # noqa
)

RAY_DEFAULT_LABEL_KEYS_PREFIX = "ray.io/"

RAY_TPU_MAX_CONCURRENT_CONNECTIONS_ENV_VAR = "RAY_TPU_MAX_CONCURRENT_ACTIVE_CONNECTIONS"

RAY_NODE_IP_FILENAME = "node_ip_address.json"

RAY_LOGGING_CONFIG_ENCODING = os.environ.get("RAY_LOGGING_CONFIG_ENCODING")

RAY_BACKEND_LOG_JSON_ENV_VAR = "RAY_BACKEND_LOG_JSON"

# Write export API event of all resource types to file if enabled.
# RAY_enable_export_api_write_config will not be considered if
# this is enabled.
RAY_ENABLE_EXPORT_API_WRITE = env_bool("RAY_enable_export_api_write", False)

# Comma separated string containing individual resource
# to write export API events for. This configuration is only used if
# RAY_enable_export_api_write is not enabled. Full list of valid
# resource types in ExportEvent.SourceType enum in
# src/ray/protobuf/export_api/export_event.proto
# Example config:
# `export RAY_enable_export_api_write_config='EXPORT_SUBMISSION_JOB,EXPORT_ACTOR'`
RAY_ENABLE_EXPORT_API_WRITE_CONFIG_STR = os.environ.get(
    "RAY_enable_export_api_write_config", ""
)
RAY_ENABLE_EXPORT_API_WRITE_CONFIG = RAY_ENABLE_EXPORT_API_WRITE_CONFIG_STR.split(",")

RAY_EXPORT_EVENT_MAX_FILE_SIZE_BYTES = env_bool(
    "RAY_EXPORT_EVENT_MAX_FILE_SIZE_BYTES", 100 * 1e6
)

RAY_EXPORT_EVENT_MAX_BACKUP_COUNT = env_bool("RAY_EXPORT_EVENT_MAX_BACKUP_COUNT", 20)

# If this flag is set and you run the driver with `uv run`, Ray propagates the `uv run`
# environment to all workers. Ray does this by setting the `py_executable` to the
# `uv run`` command line and by propagating the working directory
# via the `working_dir` plugin so uv finds the pyproject.toml.
# If you enable RAY_ENABLE_UV_RUN_RUNTIME_ENV AND you run the driver
# with `uv run`, Ray deactivates the regular RAY_RUNTIME_ENV_HOOK
# because in most cases the hooks wouldn't work unless you specifically make the code
# for the runtime env hook available in your uv environment and make sure your hook
# is compatible with your uv runtime environment. If you want to combine a custom
# RAY_RUNTIME_ENV_HOOK with `uv run`, you should flag off RAY_ENABLE_UV_RUN_RUNTIME_ENV
# and call ray._private.runtime_env.uv_runtime_env_hook.hook manually in your hook or
# manually set the py_executable in your runtime environment hook.
RAY_ENABLE_UV_RUN_RUNTIME_ENV = env_bool("RAY_ENABLE_UV_RUN_RUNTIME_ENV", True)

# Prometheus metric cardinality level setting, either "legacy" or "recommended".
#
# Legacy: report all metrics to prometheus with the set of labels that are reported by
#   the component, including WorkerId, (task or actor) Name, etc. This is the default.
# Recommended: report only the node level metrics to prometheus. This means that the
#   WorkerId will be removed from all metrics.
# Low: Same as recommended, but also drop the Name label for tasks and actors.
RAY_METRIC_CARDINALITY_LEVEL = os.environ.get(
    "RAY_metric_cardinality_level", "recommended"
)

# Whether enable OpenTelemetry as the metrics collection backend. The default is
# using OpenCensus.
RAY_ENABLE_OPEN_TELEMETRY = env_bool("RAY_enable_open_telemetry", True)

# How long to wait for a fetch for an RDT object to complete during ray.get before timing out and raising an exception to the user.
#
# NOTE: This is a tenth of `RayConfig::fetch_fail_timeout_milliseconds` by default as RDT transfers are expected to be much faster.
RDT_FETCH_FAIL_TIMEOUT_SECONDS = (
    env_integer("RAY_rdt_fetch_fail_timeout_milliseconds", 60000) / 1000
)

# Whether to enable zero-copy serialization for PyTorch tensors.
# When enabled, Ray serializes PyTorch tensors by converting them to NumPy arrays
# and leveraging pickle5's zero-copy buffer sharing. This avoids copying the
# underlying tensor data, which can improve performance when passing large tensors
# across tasks or actors. Note that this is experimental and should be used with caution
# as we won't copy and allow a write to shared memory. One process changing a tensor
# after ray.get could be reflected in another process.
#
# This feature is experimental and works best under the following conditions:
# - The tensor has `requires_grad=False` (i.e., is detached from the autograd graph).
# - The tensor is contiguous in memory
# - Performance benefits from this are larger if the tensor resides in CPU memory
# - You are not using Ray Direct Transport
#
# Tensors on GPU or non-contiguous tensors are still supported: Ray will
# automatically move them to CPU and/or make them contiguous as needed.
# While this incurs an initial copy, subsequent serialization may still benefit
# from reduced overhead compared to the default path.
#
# Use with caution and ensure tensors meet the above criteria before enabling.
# Default: False.
RAY_ENABLE_ZERO_COPY_TORCH_TENSORS = env_bool(
    "RAY_ENABLE_ZERO_COPY_TORCH_TENSORS", False
)
