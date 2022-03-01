"""Ray constants used in the Python code."""

import logging
import math
import os

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


def env_bool(key, default):
    if key in os.environ:
        return True if os.environ[key].lower() == "true" else False
    return default


# Whether event logging to driver is enabled. Set to 0 to disable.
AUTOSCALER_EVENTS = env_integer("RAY_SCHEDULER_EVENTS", 1)

# Internal kv keys for storing monitor debug status.
DEBUG_AUTOSCALING_ERROR = "__autoscaling_error"
DEBUG_AUTOSCALING_STATUS = "__autoscaling_status"
DEBUG_AUTOSCALING_STATUS_LEGACY = "__autoscaling_status_legacy"

ID_SIZE = 28

# The default maximum number of bytes to allocate to the object store unless
# overridden by the user.
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200 * 10 ** 9
# The default proportion of available memory allocated to the object store
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.3
# The smallest cap on the memory used by the object store that we allow.
# This must be greater than MEMORY_RESOURCE_UNIT_BYTES
OBJECT_STORE_MINIMUM_MEMORY_BYTES = 75 * 1024 * 1024
# The default maximum number of bytes that the non-primary Redis shards are
# allowed to use unless overridden by the user.
DEFAULT_REDIS_MAX_MEMORY_BYTES = 10 ** 10
# The smallest cap on the memory used by Redis that we allow.
REDIS_MINIMUM_MEMORY_BYTES = 10 ** 7
# Above this number of bytes, raise an error by default unless the user sets
# RAY_ALLOW_SLOW_STORAGE=1. This avoids swapping with large object stores.
REQUIRE_SHM_SIZE_THRESHOLD = 10 ** 10
# Mac with 16GB memory has degraded performance when the object store size is
# greater than 2GB.
# (see https://github.com/ray-project/ray/issues/20388 for details)
# The workaround here is to limit capacity to 2GB for Mac by default,
# and raise error if the capacity is overwritten by user.
MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT = 2 * 2 ** 30
# If a user does not specify a port for the primary Ray service,
# we attempt to start the service running at this port.
DEFAULT_PORT = 6379

RAY_ADDRESS_ENVIRONMENT_VARIABLE = "RAY_ADDRESS"
RAY_NAMESPACE_ENVIRONMENT_VARIABLE = "RAY_NAMESPACE"
RAY_RUNTIME_ENV_ENVIRONMENT_VARIABLE = "RAY_RUNTIME_ENV"

DEFAULT_DASHBOARD_IP = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 8265
DASHBOARD_ADDRESS = "dashboard"
PROMETHEUS_SERVICE_DISCOVERY_FILE = "prom_metrics_service_discovery.json"
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
FUNCTION_SIZE_WARN_THRESHOLD = 10 ** 7
FUNCTION_SIZE_ERROR_THRESHOLD = env_integer("FUNCTION_SIZE_ERROR_THRESHOLD", (10 ** 8))

# If remote functions with the same source are imported this many times, then
# print a warning.
DUPLICATE_REMOTE_FUNCTION_THRESHOLD = 100

# The maximum resource quantity that is allowed. TODO(rkn): This could be
# relaxed, but the current implementation of the node manager will be slower
# for large resource quantities due to bookkeeping of specific resource IDs.
MAX_RESOURCE_QUANTITY = 100e12

# Each memory "resource" counts as this many bytes of memory.
MEMORY_RESOURCE_UNIT_BYTES = 1

# Number of units 1 resource can be subdivided into.
MIN_RESOURCE_GRANULARITY = 0.0001


def round_to_memory_units(memory_bytes, round_up):
    """Round bytes to the nearest memory unit."""
    return from_memory_units(to_memory_units(memory_bytes, round_up))


def from_memory_units(memory_units):
    """Convert from memory units -> bytes."""
    return memory_units * MEMORY_RESOURCE_UNIT_BYTES


def to_memory_units(memory_bytes, round_up):
    """Convert from bytes -> memory units."""
    value = memory_bytes / MEMORY_RESOURCE_UNIT_BYTES
    if value < 1:
        raise ValueError(
            "The minimum amount of memory that can be requested is {} bytes, "
            "however {} bytes was asked.".format(
                MEMORY_RESOURCE_UNIT_BYTES, memory_bytes
            )
        )
    if isinstance(value, float) and not value.is_integer():
        # TODO(ekl) Ray currently does not support fractional resources when
        # the quantity is greater than one. We should fix memory resources to
        # be allocated in units of bytes and not 100MB.
        if round_up:
            value = int(math.ceil(value))
        else:
            value = int(math.floor(value))
    return int(value)


# Different types of Ray errors that can be pushed to the driver.
# TODO(rkn): These should be defined in flatbuffers and must be synced with
# the existing C++ definitions.
WAIT_FOR_CLASS_PUSH_ERROR = "wait_for_class"
PICKLING_LARGE_OBJECT_PUSH_ERROR = "pickling_large_object"
WAIT_FOR_FUNCTION_PUSH_ERROR = "wait_for_function"
TASK_PUSH_ERROR = "task"
REGISTER_REMOTE_FUNCTION_PUSH_ERROR = "register_remote_function"
FUNCTION_TO_RUN_PUSH_ERROR = "function_to_run"
VERSION_MISMATCH_PUSH_ERROR = "version_mismatch"
CHECKPOINT_PUSH_ERROR = "checkpoint"
WORKER_CRASH_PUSH_ERROR = "worker_crash"
WORKER_DIED_PUSH_ERROR = "worker_died"
WORKER_POOL_LARGE_ERROR = "worker_pool_large"
PUT_RECONSTRUCTION_PUSH_ERROR = "put_reconstruction"
INFEASIBLE_TASK_ERROR = "infeasible_task"
RESOURCE_DEADLOCK_ERROR = "resource_deadlock"
REMOVED_NODE_ERROR = "node_removed"
MONITOR_DIED_ERROR = "monitor_died"
LOG_MONITOR_DIED_ERROR = "log_monitor_died"
REPORTER_DIED_ERROR = "reporter_died"
DASHBOARD_AGENT_DIED_ERROR = "dashboard_agent_died"
DASHBOARD_DIED_ERROR = "dashboard_died"
RAYLET_CONNECTION_ERROR = "raylet_connection_error"
DETACHED_ACTOR_ANONYMOUS_NAMESPACE_ERROR = "detached_actor_anonymous_namespace"
EXCESS_QUEUEING_WARNING = "excess_queueing_warning"

# Used in gpu detection
RESOURCE_CONSTRAINT_PREFIX = "accelerator_type:"

RESOURCES_ENVIRONMENT_VARIABLE = "RAY_OVERRIDE_RESOURCES"

# The reporter will report its statistics this often (milliseconds).
REPORTER_UPDATE_INTERVAL_MS = env_integer("REPORTER_UPDATE_INTERVAL_MS", 2500)

# Number of attempts to ping the Redis server. See
# `services.py::wait_for_redis_to_start()` and
# `services.py::create_redis_client()`
START_REDIS_WAIT_RETRIES = env_integer("RAY_START_REDIS_WAIT_RETRIES", 16)

LOGGER_FORMAT = "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
LOGGER_FORMAT_HELP = f"The logging format. default='{LOGGER_FORMAT}'"
LOGGER_LEVEL = "info"
LOGGER_LEVEL_CHOICES = ["debug", "info", "warning", "error", "critical"]
LOGGER_LEVEL_HELP = (
    "The logging level threshold, choices=['debug', 'info',"
    " 'warning', 'error', 'critical'], default='info'"
)

LOGGING_ROTATE_BYTES = 512 * 1024 * 1024  # 512MB.
LOGGING_ROTATE_BACKUP_COUNT = 5  # 5 Backup files at max.

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
# TODO(sang): Delete it.
PROCESS_TYPE_REPORTER = "reporter"
PROCESS_TYPE_DASHBOARD = "dashboard"
PROCESS_TYPE_DASHBOARD_AGENT = "dashboard_agent"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"
PROCESS_TYPE_GCS_SERVER = "gcs_server"
PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER = "python-core-driver"
PROCESS_TYPE_PYTHON_CORE_WORKER = "python-core-worker"

# Log file names
MONITOR_LOG_FILE_NAME = f"{PROCESS_TYPE_MONITOR}.log"
LOG_MONITOR_LOG_FILE_NAME = f"{PROCESS_TYPE_LOG_MONITOR}.log"

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

LOG_MONITOR_MAX_OPEN_FILES = 200

# Autoscaler events are denoted by the ":event_summary:" magic token.
LOG_PREFIX_EVENT_SUMMARY = ":event_summary:"
# Actor names are recorded in the logs with this magic token as a prefix.
LOG_PREFIX_ACTOR_NAME = ":actor_name:"
# Task names are recorded in the logs with this magic token as a prefix.
LOG_PREFIX_TASK_NAME = ":task_name:"

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

# The default password to prevent redis port scanning attack.
# Hex for ray.
REDIS_DEFAULT_PASSWORD = "5241590000000000"

# The default ip address to bind to.
NODE_DEFAULT_IP = "127.0.0.1"

# The Mach kernel page size in bytes.
MACH_PAGE_SIZE_BYTES = 4096

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

# Used to separate lines when formatting the call stack where an ObjectRef was
# created.
CALL_STACK_LINE_DELIMITER = " | "

# The default gRPC max message size is 4 MiB, we use a larger number of 100 MiB
# NOTE: This is equal to the C++ limit of (RAY_CONFIG::max_grpc_message_size)
GRPC_CPP_MAX_MESSAGE_SIZE = 100 * 1024 * 1024

# Internal kv namespaces
KV_NAMESPACE_DASHBOARD = b"dashboard"
KV_NAMESPACE_SESSION = b"session"
KV_NAMESPACE_TRACING = b"tracing"
KV_NAMESPACE_PDB = b"ray_pdb"
KV_NAMESPACE_HEALTHCHECK = b"healthcheck"
KV_NAMESPACE_JOB = b"job"
KV_NAMESPACE_CLUSTER = b"cluster"
# TODO: Set package for runtime env
# We need to update ray client for this since runtime env use ray client
# This might introduce some compatibility issues so leave it here for now.
KV_NAMESPACE_PACKAGE = None
KV_NAMESPACE_SERVE = b"serve"
KV_NAMESPACE_FUNCTION_TABLE = b"fun"
