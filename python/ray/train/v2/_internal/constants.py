import os
from typing import Dict

from ray._private.ray_constants import env_bool, env_set_by_user

# Unsupported configs can use this value to detect if the user has set it.
_UNSUPPORTED = "UNSUPPORTED"

# Polling interval for the Train controller.
# This determines how many seconds the controller will wait between
# polling the worker group for its status.
HEALTH_CHECK_INTERVAL_S_ENV_VAR = "RAY_TRAIN_HEALTH_CHECK_INTERVAL_S"
DEFAULT_HEALTH_CHECK_INTERVAL_S: float = 2.0

# The time in seconds a worker health check must be hanging for
# before the controller marks the worker as dead and handles the failure.
WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR = "RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S"
DEFAULT_WORKER_HEALTH_CHECK_TIMEOUT_S: float = 10 * 60

# Timeout in seconds for the worker group to start.
WORKER_GROUP_START_TIMEOUT_S_ENV_VAR = "RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S"
DEFAULT_WORKER_GROUP_START_TIMEOUT_S: float = 30.0

# The name of the file that is used to validate the storage.
VALIDATE_STORAGE_MARKER_FILENAME = ".validate_storage_marker"
# The name of the file that is used to store the checkpoint manager snapshot.
CHECKPOINT_MANAGER_SNAPSHOT_FILENAME = "checkpoint_manager_snapshot.json"
# The coding scheme used to encode the checkpoint manager snapshot.
CHECKPOINT_MANAGER_SNAPSHOT_CODING_SCHEME = "utf-8"

# V2 feature flag.
V2_ENABLED_ENV_VAR = "RAY_TRAIN_V2_ENABLED"


def is_v2_enabled() -> bool:
    return env_bool(V2_ENABLED_ENV_VAR, False)


ENV_VARS_TO_PROPAGATE = (
    V2_ENABLED_ENV_VAR,
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
    WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
)


def get_env_vars_to_propagate() -> Dict[str, str]:
    """Returns a dictionary of environment variables that should be propagated
    from the driver to the controller, and then from the controller
    to each training worker.

    This way, users only need to set environment variables in one place
    when launching the script instead of needing to manually set a runtime environment.
    """
    env_vars = {}
    for env_var in ENV_VARS_TO_PROPAGATE:
        if env_set_by_user(env_var):
            env_vars[env_var] = os.environ[env_var]
    return env_vars
