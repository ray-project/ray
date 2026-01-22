import os

from ray._private.ray_constants import env_bool, env_float, env_integer

DASHBOARD_LOG_FILENAME = "dashboard.log"
DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX = "DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:"
DASHBOARD_AGENT_ADDR_IP_PREFIX = "DASHBOARD_AGENT_ADDR_IP_PREFIX:"
DASHBOARD_AGENT_LOG_FILENAME = "dashboard_agent.log"
DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S_ENV_NAME = (
    "RAY_DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S"  # noqa
)
DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S = env_integer(
    DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S_ENV_NAME, 0.4
)
# The maximum time that parent can be considered
# as dead before agent kills itself.
_PARENT_DEATH_THREASHOLD = 5
RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME = "RAY_STATE_SERVER_MAX_HTTP_REQUEST"
# Default number of in-progress requests to the state api server.
RAY_STATE_SERVER_MAX_HTTP_REQUEST = env_integer(
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME, 100
)
# Max allowed number of in-progress requests could be configured.
RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED = 1000

RAY_DASHBOARD_STATS_PURGING_INTERVAL = env_integer(
    "RAY_DASHBOARD_STATS_PURGING_INTERVAL", 60 * 10
)
RAY_DASHBOARD_STATS_UPDATING_INTERVAL = env_integer(
    "RAY_DASHBOARD_STATS_UPDATING_INTERVAL", 15
)
GCS_SERVER_ADDRESS = "GcsServerAddress"
# GCS check alive
GCS_CHECK_ALIVE_INTERVAL_SECONDS = env_integer("GCS_CHECK_ALIVE_INTERVAL_SECONDS", 5)
GCS_RPC_TIMEOUT_SECONDS = env_integer("RAY_DASHBOARD_GCS_RPC_TIMEOUT_SECONDS", 60)
# aiohttp_cache
AIOHTTP_CACHE_TTL_SECONDS = 2
AIOHTTP_CACHE_MAX_SIZE = 128
AIOHTTP_CACHE_DISABLE_ENVIRONMENT_KEY = "RAY_DASHBOARD_NO_CACHE"
# Default value for datacenter (the default value in protobuf)
DEFAULT_LANGUAGE = "PYTHON"
DEFAULT_JOB_ID = "ffff"
# Hook that is invoked on the dashboard `/api/component_activities` endpoint.
# Environment variable stored here should be a callable that does not
# take any arguments and should return a dictionary mapping
# activity component type (str) to
# ray.dashboard.modules.api.api_head.RayActivityResponse.
# Example: "your.module.ray_cluster_activity_hook".
RAY_CLUSTER_ACTIVITY_HOOK = "RAY_CLUSTER_ACTIVITY_HOOK"

# The number of candidate agents
CANDIDATE_AGENT_NUMBER = max(env_integer("CANDIDATE_AGENT_NUMBER", 1), 1)
# when head receive JobSubmitRequest, maybe not any agent is available,
# we need to wait for agents in other node start
WAIT_AVAILABLE_AGENT_TIMEOUT = 10
TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS = 0.5
RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR = "RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES"
RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR = (
    "RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG"
)

# The max time to wait for the JobSupervisor to start before failing the job.
DEFAULT_JOB_START_TIMEOUT_SECONDS = 60 * 15
RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR = "RAY_JOB_START_TIMEOUT_SECONDS"
# Port that dashboard prometheus metrics will be exported to
DASHBOARD_METRIC_PORT = env_integer("DASHBOARD_METRIC_PORT", 44227)

# We use RayNodeType to mark head/worker nodes. IsHeadNode is retained
# for backward compatibility for user-customized dashboards that might rely on it
NODE_TAG_KEYS = ["ip", "Version", "SessionName", "IsHeadNode", "RayNodeType"]
GPU_TAG_KEYS = NODE_TAG_KEYS + ["GpuDeviceName", "GpuIndex"]

# TpuDeviceName and TpuIndex are expected to be equal to the number of TPU
# chips in the cluster. TpuType and TpuTopology are proportional to the number
# of node pools.
TPU_TAG_KEYS = NODE_TAG_KEYS + ["TpuDeviceName", "TpuIndex", "TpuType", "TpuTopology"]
CLUSTER_TAG_KEYS = ["node_type", "Version", "SessionName"]
COMPONENT_METRICS_TAG_KEYS = ["ip", "pid", "Version", "Component", "SessionName"]
COMPONENT_GPU_TAG_KEYS = GPU_TAG_KEYS + COMPONENT_METRICS_TAG_KEYS

# Dashboard metrics are tracked separately at the dashboard. TODO(sang): Support GCS.
# Note that for dashboard subprocess module, the component name is "dashboard_[module_name]".
AVAILABLE_COMPONENT_NAMES_FOR_METRICS = {
    "workers",
    "raylet",
    "agent",
    "dashboard",
    "gcs",
}
METRICS_INPUT_ROOT = os.path.join(
    os.path.dirname(__file__), "modules", "metrics", "export"
)
METRICS_RECORD_INTERVAL_S = env_integer("METRICS_RECORD_INTERVAL_S", 5)
PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    METRICS_INPUT_ROOT, "prometheus", "prometheus.yml"
)
PARENT_HEALTH_CHECK_BY_PIPE = env_bool(
    "RAY_enable_pipe_based_agent_to_parent_health_check", False
)

# Maximum time to wait for the subprocess module to be ready.
SUBPROCESS_MODULE_WAIT_READY_TIMEOUT = env_float(
    "RAY_DASHBOARD_SUBPROCESS_MODULE_WAIT_READY_TIMEOUT", 30.0
)
