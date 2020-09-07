"""Ray constants used in the Python code."""

import logging
import math
import os

logger = logging.getLogger(__name__)


def env_integer(key, default):
    if key in os.environ:
        return int(os.environ[key])
    return default


def env_bool(key, default):
    if key in os.environ:
        return True if os.environ[key].lower() == "true" else False
    return default


ID_SIZE = 20

# The default maximum number of bytes to allocate to the object store unless
# overridden by the user.
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200 * 10**9
# The smallest cap on the memory used by the object store that we allow.
# This must be greater than MEMORY_RESOURCE_UNIT_BYTES * 0.7
OBJECT_STORE_MINIMUM_MEMORY_BYTES = 75 * 1024 * 1024
# The default maximum number of bytes that the non-primary Redis shards are
# allowed to use unless overridden by the user.
DEFAULT_REDIS_MAX_MEMORY_BYTES = 10**10
# The smallest cap on the memory used by Redis that we allow.
REDIS_MINIMUM_MEMORY_BYTES = 10**7
# If a user does not specify a port for the primary Ray service,
# we attempt to start the service running at this port.
DEFAULT_PORT = 6379

DEFAULT_DASHBOARD_IP = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 8265
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

# If a remote function or actor (or some other export) has serialized size
# greater than this quantity, print an warning.
PICKLE_OBJECT_WARNING_SIZE = 10**7

# If remote functions with the same source are imported this many times, then
# print a warning.
DUPLICATE_REMOTE_FUNCTION_THRESHOLD = 100

# The maximum resource quantity that is allowed. TODO(rkn): This could be
# relaxed, but the current implementation of the node manager will be slower
# for large resource quantities due to bookkeeping of specific resource IDs.
MAX_RESOURCE_QUANTITY = 100000

# Each memory "resource" counts as this many bytes of memory.
MEMORY_RESOURCE_UNIT_BYTES = 50 * 1024 * 1024

# Number of units 1 resource can be subdivided into.
MIN_RESOURCE_GRANULARITY = 0.0001

# Fraction of plasma memory that can be reserved. It is actually 70% but this
# is set to 69% to leave some headroom.
PLASMA_RESERVABLE_MEMORY_FRACTION = 0.69


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
            "however {} bytes was asked.".format(MEMORY_RESOURCE_UNIT_BYTES,
                                                 memory_bytes))
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
REGISTER_ACTOR_PUSH_ERROR = "register_actor"
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

# Used in gpu detection
RESOURCE_CONSTRAINT_PREFIX = "accelerator_type:"

RESOURCES_ENVIRONMENT_VARIABLE = "RAY_OVERRIDE_RESOURCES"

# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
AUTOSCALER_MAX_NUM_FAILURES = env_integer("AUTOSCALER_MAX_NUM_FAILURES", 5)

# The maximum number of nodes to launch in a single request.
# Multiple requests may be made for this batch size, up to
# the limit of AUTOSCALER_MAX_CONCURRENT_LAUNCHES.
AUTOSCALER_MAX_LAUNCH_BATCH = env_integer("AUTOSCALER_MAX_LAUNCH_BATCH", 5)

# Max number of nodes to launch at a time.
AUTOSCALER_MAX_CONCURRENT_LAUNCHES = env_integer(
    "AUTOSCALER_MAX_CONCURRENT_LAUNCHES", 10)

# Interval at which to perform autoscaling updates.
AUTOSCALER_UPDATE_INTERVAL_S = env_integer("AUTOSCALER_UPDATE_INTERVAL_S", 5)

# The autoscaler will attempt to restart Ray on nodes it hasn't heard from
# in more than this interval.
AUTOSCALER_HEARTBEAT_TIMEOUT_S = env_integer("AUTOSCALER_HEARTBEAT_TIMEOUT_S",
                                             30)

# The reporter will report its statistics this often (milliseconds).
REPORTER_UPDATE_INTERVAL_MS = env_integer("REPORTER_UPDATE_INTERVAL_MS", 2500)

# Max number of retries to AWS (default is 5, time increases exponentially)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 12)
# Max number of retries to create an EC2 node (retry different subnet)
BOTO_CREATE_MAX_RETRIES = env_integer("BOTO_CREATE_MAX_RETRIES", 5)

LOGGER_FORMAT = (
    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s")
LOGGER_FORMAT_HELP = f"The logging format. default='{LOGGER_FORMAT}'"
LOGGER_LEVEL = "info"
LOGGER_LEVEL_CHOICES = ["debug", "info", "warning", "error", "critical"]
LOGGER_LEVEL_HELP = ("The logging level threshold, choices=['debug', 'info',"
                     " 'warning', 'error', 'critical'], default='info'")

# Constants used to define the different process types.
PROCESS_TYPE_REAPER = "reaper"
PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_REPORTER = "reporter"
PROCESS_TYPE_DASHBOARD = "dashboard"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_RAYLET = "raylet"
PROCESS_TYPE_PLASMA_STORE = "plasma_store"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"
PROCESS_TYPE_GCS_SERVER = "gcs_server"

LOG_MONITOR_MAX_OPEN_FILES = 200

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
