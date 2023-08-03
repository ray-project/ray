import ray._private.ray_constants as ray_constants

RUNTIME_ENV_RETRY_TIMES = ray_constants.env_integer("RUNTIME_ENV_RETRY_TIMES", 3)

RUNTIME_ENV_RETRY_INTERVAL_MS = ray_constants.env_integer(
    "RUNTIME_ENV_RETRY_INTERVAL_MS", 1000
)
