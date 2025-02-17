import os
from typing import Dict

from ray._private.ray_constants import env_bool, env_set_by_user

# Unsupported configs can use this value to detect if the user has set it.
_UNSUPPORTED = "UNSUPPORTED"
_DEPRECATED = "DEPRECATED"

# The name of the file that is used to validate the storage.
VALIDATE_STORAGE_MARKER_FILENAME = ".validate_storage_marker"
# The name of the file that is used to store the checkpoint manager snapshot.
CHECKPOINT_MANAGER_SNAPSHOT_FILENAME = "checkpoint_manager_snapshot.json"


# =====================
# Environment Variables
# =====================

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

# Timeout in seconds for `ray.train.report` to block on synchronization barriers,
# after which a timeout error will be raised.
REPORT_BARRIER_TIMEOUT_S_ENV_VAR = "RAY_TRAIN_REPORT_BARRIER_TIMEOUT_S"
DEFAULT_REPORT_BARRIER_TIMEOUT_S: float = 60 * 30
# Time in seconds for `ray.train.report` to log a warning if it is waiting for sync
# actor notification of releasing.
REPORT_BARRIER_WARN_INTERVAL_S_ENV_VAR = "RAY_TRAIN_REPORT_BARRIER_WARN_INTERVAL_S"
DEFAULT_REPORT_BARRIER_WARN_INTERVAL_S: float = 60

# The environment variable to enable the Ray Train Metrics.
METRICS_ENABLED_ENV_VAR = "RAY_TRAIN_METRICS_ENABLED"

# Environment variable to enable the print function patching.
ENABLE_PRINT_PATCH_ENV_VAR = "RAY_TRAIN_ENABLE_PRINT_PATCH"
DEFAULT_ENABLE_PRINT_PATCH = "1"

# Whether or not to run the controller as an actor.
RUN_CONTROLLER_AS_ACTOR_ENV_VAR = "RAY_TRAIN_RUN_CONTROLLER_AS_ACTOR"
DEFAULT_RUN_CONTROLLER_AS_ACTOR = "1"

# V2 feature flag.
V2_ENABLED_ENV_VAR = "RAY_TRAIN_V2_ENABLED"


def is_v2_enabled() -> bool:
    return env_bool(V2_ENABLED_ENV_VAR, False)


ENV_VARS_TO_PROPAGATE = {
    V2_ENABLED_ENV_VAR,
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
    WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
    ENABLE_PRINT_PATCH_ENV_VAR,
}


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
