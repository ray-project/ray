import os
from typing import Dict

from ray._private.ray_constants import env_bool, env_set_by_user

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

# V2 feature flag.
V2_ENABLED_ENV_VAR = "RAY_TRAIN_V2_ENABLED"
V2_ENABLED = env_bool(V2_ENABLED_ENV_VAR, False)


ENV_VARS_TO_PROPAGATE = (V2_ENABLED_ENV_VAR,)


def get_env_vars_to_propagate() -> Dict[str, str]:
    """Returns a dictionary of environment variables that should be propagated
    from the driver to each training worker.

    This way, users only need to set environment variables in one place
    when launching the script instead of needing to manually set a runtime environment.
    """
    env_vars = {}
    for env_var in ENV_VARS_TO_PROPAGATE:
        if env_set_by_user(env_var):
            env_vars[env_var] = os.environ[env_var]
    return env_vars
