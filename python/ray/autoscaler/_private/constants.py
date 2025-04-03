import os
import sys

from ray._private.ray_constants import (  # noqa F401
    AUTOSCALER_RESOURCE_REQUEST_CHANNEL,
    DEFAULT_OBJECT_STORE_MEMORY_PROPORTION,
    LABELS_ENVIRONMENT_VARIABLE,
    LOGGER_FORMAT,
    RESOURCES_ENVIRONMENT_VARIABLE,
)


def env_integer(key, default):
    if key in os.environ:
        val = os.environ[key]
        if val == "inf":
            return sys.maxsize
        else:
            return int(val)
    return default


# Whether autoscaler cluster status logging is enabled. Set to 0 disable.
AUTOSCALER_STATUS_LOG = env_integer("RAY_ENABLE_CLUSTER_STATUS_LOG", 1)

# The name of the environment variable for plugging in a utilization scorer.
AUTOSCALER_UTILIZATION_SCORER_KEY = "RAY_AUTOSCALER_UTILIZATION_SCORER"

# Whether to avoid launching GPU nodes for CPU only tasks.
AUTOSCALER_CONSERVE_GPU_NODES = env_integer("AUTOSCALER_CONSERVE_GPU_NODES", 1)

# How long to wait for a node to start and terminate, in seconds.
AUTOSCALER_NODE_START_WAIT_S = env_integer("AUTOSCALER_NODE_START_WAIT_S", 900)
AUTOSCALER_NODE_TERMINATE_WAIT_S = env_integer("AUTOSCALER_NODE_TERMINATE_WAIT_S", 900)

# Interval at which to check if node SSH became available.
AUTOSCALER_NODE_SSH_INTERVAL_S = env_integer("AUTOSCALER_NODE_SSH_INTERVAL_S", 5)

# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
AUTOSCALER_MAX_NUM_FAILURES = env_integer("AUTOSCALER_MAX_NUM_FAILURES", 5)

# The maximum number of nodes to launch in a single request.
# Multiple requests may be made for this batch size, up to
# the limit of AUTOSCALER_MAX_CONCURRENT_LAUNCHES.
AUTOSCALER_MAX_LAUNCH_BATCH = env_integer("AUTOSCALER_MAX_LAUNCH_BATCH", 5)

# Max number of nodes to launch at a time.
AUTOSCALER_MAX_CONCURRENT_LAUNCHES = env_integer(
    "AUTOSCALER_MAX_CONCURRENT_LAUNCHES", 10
)

# Default upscaling speed for the autoscaler. This specifies how many nodes
# to request at a time, where the desired number to upscale is
#   min(1, upscaling_speed * current_num_nodes)
# e.g. 1.0 means to request enough nodes to double
# the cluster size in each round of requests.
# When the upscaling speed is 0.0, the autoscaler will request 1 node.
DEFAULT_UPSCALING_SPEED = 0.0

# Interval at which to perform autoscaling updates.
AUTOSCALER_UPDATE_INTERVAL_S = env_integer("AUTOSCALER_UPDATE_INTERVAL_S", 5)

# The autoscaler will attempt to restart Ray on nodes it hasn't heard from
# in more than this interval.
AUTOSCALER_HEARTBEAT_TIMEOUT_S = env_integer("AUTOSCALER_HEARTBEAT_TIMEOUT_S", 30)
# The maximum number of nodes (including failed nodes) that the autoscaler will
# track for logging purposes.
AUTOSCALER_MAX_NODES_TRACKED = 1500

AUTOSCALER_MAX_FAILURES_DISPLAYED = 20

AUTOSCALER_NODE_AVAILABILITY_MAX_STALENESS_S = env_integer(
    "AUTOSCALER_NODE_AVAILABILITY_MAX_STALENESS_S", 30 * 60
)

AUTOSCALER_REPORT_PER_NODE_STATUS = (
    env_integer("AUTOSCALER_REPORT_PER_NODE_STATUS", 1) == 1
)

# The maximum allowed resource demand vector size to guarantee the resource
# demand scheduler bin packing algorithm takes a reasonable amount of time
# to run.
AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE = env_integer(
    "AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE", 1000
)

# Port that autoscaler prometheus metrics will be exported to
AUTOSCALER_METRIC_PORT = env_integer("AUTOSCALER_METRIC_PORT", 44217)

# Max number of retries to AWS (default is 5, time increases exponentially)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 12)
# Max number of retries to create an EC2 node (retry different subnet)
BOTO_CREATE_MAX_RETRIES = env_integer("BOTO_CREATE_MAX_RETRIES", 5)

# ray home path in the container image
RAY_HOME = "/home/ray"

# The order of this list matters! `scripts.py` kills the ray processes in order of this
# list. Think twice when you add to this list.
# Invariants:
# RAYLET must be the first in the list.
# GCS SERVER must be the last in the list.
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
    ["monitor.py", False],
    ["ray.util.client.server", False],
    ["default_worker.py", False],  # Python worker.
    ["setup_worker.py", False],  # Python environment setup worker.
    # For mac osx, setproctitle doesn't change the process name returned
    # by psutil but only cmdline.
    [
        "ray::",
        sys.platform != "darwin",
    ],  # Python worker. TODO(mehrdadn): Fix for Windows
    ["io.ray.runtime.runner.worker.DefaultWorker", False],  # Java worker.
    ["log_monitor.py", False],
    ["reporter.py", False],
    [os.path.join("dashboard", "agent.py"), False],
    [os.path.join("dashboard", "dashboard.py"), False],
    [os.path.join("runtime_env", "agent", "main.py"), False],
    ["ray_process_reaper.py", False],
    ["gcs_server", True],
]

# Max Concurrent SSH Calls to stop Docker
MAX_PARALLEL_SHUTDOWN_WORKERS = env_integer("MAX_PARALLEL_SHUTDOWN_WORKERS", 50)

DISABLE_NODE_UPDATERS_KEY = "disable_node_updaters"
DISABLE_LAUNCH_CONFIG_CHECK_KEY = "disable_launch_config_check"
FOREGROUND_NODE_LAUNCH_KEY = "foreground_node_launch"
WORKER_LIVENESS_CHECK_KEY = "worker_liveness_check"

KUBERAY_OPERATOR_DEFAULT_ACCESS_KEY = "KubeRayOperator"
KUBERAY_OPERATOR_DEFAULT_ACCESS_SECRET = "S3ViZVJheU9wZXJhdG9y"
