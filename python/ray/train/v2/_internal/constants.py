# Unsupported configs can use this value to detect if the user has set it.
_UNSUPPORTED = "UNSUPPORTED"

# Polling interval for the Train controller.
# This determines how many seconds the controller will wait between
# polling the worker group for its status.
HEALTH_CHECK_INTERVAL_S_ENV_VAR = "RAY_TRAIN_HEALTH_CHECK_INTERVAL_S"
DEFAULT_HEALTH_CHECK_INTERVAL_S = 1.0

# The number of consecutive health check that a worker must miss
# before the controller marks the worker as dead and handles the failure.
# A health check "miss" is when the worker's status polling task doesn't finish
# within the polling interval.
MAX_CONSECUTIVE_HEALTH_CHECK_MISSES_ENV_VAR = (
    "RAY_TRAIN_MAX_CONSECUTIVE_HEALTH_CHECK_MISSES"
)
DEFAULT_MAX_CONSECUTIVE_HEALTH_CHECK_MISSES = 5
