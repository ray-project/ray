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

RUNTIME_ENV_LOG_FILENAME = "runtime_env.log"
RUNTIME_ENV_AGENT_PORT_PREFIX = "RUNTIME_ENV_AGENT_PORT_PREFIX:"
RUNTIME_ENV_AGENT_LOG_FILENAME = "runtime_env_agent.log"
RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S_ENV_NAME = (
    "RAY_RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S"  # noqa
)
