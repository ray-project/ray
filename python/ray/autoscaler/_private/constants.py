import os
import sys

from ray.ray_constants import (  # noqa F401
    AUTOSCALER_RESOURCE_REQUEST_CHANNEL, DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES,
    DEFAULT_OBJECT_STORE_MEMORY_PROPORTION, LOGGER_FORMAT,
    MEMORY_RESOURCE_UNIT_BYTES, RESOURCES_ENVIRONMENT_VARIABLE)


def env_integer(key, default):
    if key in os.environ:
        val = os.environ[key]
        if val == "inf":
            return sys.maxsize
        else:
            return int(val)
    return default


# Whether to avoid launching GPU nodes for CPU only tasks.
AUTOSCALER_CONSERVE_GPU_NODES = env_integer("AUTOSCALER_CONSERVE_GPU_NODES", 1)

# How long to wait for a node to start, in seconds.
AUTOSCALER_NODE_START_WAIT_S = env_integer("AUTOSCALER_NODE_START_WAIT_S", 900)

# Interval at which to check if node SSH became available.
AUTOSCALER_NODE_SSH_INTERVAL_S = env_integer("AUTOSCALER_NODE_SSH_INTERVAL_S",
                                             5)

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
# The maximum number of nodes (including failed nodes) that the autoscaler will
# track for logging purposes.
AUTOSCALER_MAX_NODES_TRACKED = 1500

AUTOSCALER_MAX_FAILURES_DISPLAYED = 20

# The maximum allowed resource demand vector size to guarantee the resource
# demand scheduler bin packing algorithm takes a reasonable amount of time
# to run.
AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE = 1000

# Port that autoscaler prometheus metrics will be exported to
AUTOSCALER_METRIC_PORT = env_integer("AUTOSCALER_METRIC_PORT", 44217)

# Max number of retries to AWS (default is 5, time increases exponentially)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 12)
# Max number of retries to create an EC2 node (retry different subnet)
BOTO_CREATE_MAX_RETRIES = env_integer("BOTO_CREATE_MAX_RETRIES", 5)

# ray home path in the container image
RAY_HOME = "/home/ray"

RAY_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name
    # (only the first 15 charactors of the executable name on Linux);
    # if False, is to filter ps results by command with all its arguments.
    # See STANDARD FORMAT SPECIFIERS section of
    # http://man7.org/linux/man-pages/man1/ps.1.html
    # about comm and args. This can help avoid killing non-ray processes.
    # Format:
    # Keyword to filter, filter by command (True)/filter by args (False)
    ["raylet", True],
    ["plasma_store", True],
    ["gcs_server", True],
    ["monitor.py", False],
    ["ray.util.client.server", False],
    ["redis-server", False],
    ["default_worker.py", False],  # Python worker.
    ["setup_worker.py", False],  # Python environment setup worker.
    ["ray::", True],  # Python worker. TODO(mehrdadn): Fix for Windows
    ["io.ray.runtime.runner.worker.DefaultWorker", False],  # Java worker.
    ["log_monitor.py", False],
    ["reporter.py", False],
    [os.path.join("dashboard", "dashboard.py"), False],
    [os.path.join("dashboard", "agent.py"), False],
    ["ray_process_reaper.py", False],
]

# Max Concurrent SSH Calls to stop Docker
MAX_PARALLEL_SHUTDOWN_WORKERS = env_integer("MAX_PARALLEL_SHUTDOWN_WORKERS",
                                            50)
