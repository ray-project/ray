import warnings

from ray._private.ray_constants import env_bool
from ray.util.annotations import RayDeprecationWarning


# Set this to 1 to enable deprecation warnings for V2 migration.
ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR = "RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS"

V2_MIGRATION_GUIDE_MESSAGE = (
    "See this issue for more context: https://github.com/ray-project/ray/issues/49454"
)


def _v2_migration_warnings_enabled() -> bool:
    return env_bool(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, False)


def _log_deprecation_warning(message: str):
    warnings.warn(
        message,
        RayDeprecationWarning,
        stacklevel=2,
    )
