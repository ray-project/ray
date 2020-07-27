import ray.ray_constants as ray_constants

REPORTER_PREFIX = "RAY_REPORTER:"
# The reporter will report its statistics this often (milliseconds).
REPORTER_UPDATE_INTERVAL_MS = ray_constants.env_integer(
    "REPORTER_UPDATE_INTERVAL_MS", 2500)
