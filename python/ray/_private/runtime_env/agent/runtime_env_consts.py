import os
import ray._private.ray_constants as ray_constants

RUNTIME_ENV_RETRY_TIMES = ray_constants.env_integer("RUNTIME_ENV_RETRY_TIMES", 3)

RUNTIME_ENV_RETRY_INTERVAL_MS = ray_constants.env_integer(
    "RUNTIME_ENV_RETRY_INTERVAL_MS", 1000
)

# Cache TTL for bad runtime env. After this time, delete the cache and retry to create
# runtime env if needed.
BAD_RUNTIME_ENV_CACHE_TTL_SECONDS = ray_constants.env_integer(
    "BAD_RUNTIME_ENV_CACHE_TTL_SECONDS", 60 * 10
)

# Disable working dir GC or not
DISABLE_WORKING_DIR_GC = ray_constants.env_bool(
    "RAY_RUNTIME_ENV_DISABLE_WORKING_DIR_GC", False
)

# Disable job dir GC or not
DISABLE_JOB_DIR_GC = ray_constants.env_bool("RAY_RUNTIME_ENV_DISABLE_JOB_DIR_GC", False)

RUNTIME_ENV_LOG_FILENAME = "runtime_env.log"
RUNTIME_ENV_AGENT_PORT_PREFIX = "RUNTIME_ENV_AGENT_PORT_PREFIX:"
RUNTIME_ENV_AGENT_LOG_FILENAME = "runtime_env_agent.log"
RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S_ENV_NAME = (
    "RAY_RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S"  # noqa
)

# Make the `RAY_WORKING_DIR` configurable
RAY_WORKING_DIR = os.environ.get("RAY_WORKING_DIR", "RAY_WORKING_DIR")

RAY_JOB_DIR = os.environ.get("RAY_JOB_DIR", "RAY_JOB_DIR")

# ZDFS Config
ZDFS_LOG_PATH = "/tmp/log_zdfs.LOG"
ZDFS_CONF_PATH = "/tmp/zdfs_client_conf.json"
ZDFS_THREAD_NUM = 4
ZDFS_BUFFER_LEN = 20971520  # 20MB
