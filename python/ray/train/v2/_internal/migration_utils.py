from ray._private.ray_constants import env_bool


# Set this to 1 to enable deprecation warnings for V2 migration.
ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR = "RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS"

V2_MIGRATION_GUIDE_MESSAGE = (
    "See this issue for more context: https://github.com/ray-project/ray/issues/49454"
)

FAIL_FAST_DEPRECATION_MESSAGE = (
    "`ray.train.FailureConfig(fail_fast)` is deprecated since it is "
    "only relevant in the context of Ray Tune. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

TRAINER_RESOURCES_DEPRECATION_MESSAGE = (
    "`ScalingConfig(trainer_resources)` is deprecated. "
    "This parameter was an advanced configuration that specified "
    "resources for the Ray Train driver actor, which doesn't "
    "need to reserve logical resources because it doesn't perform "
    "any heavy computation. "
    "Only the `resources_per_worker` parameter is useful "
    "to specify resources for the training workers. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)


def _v2_migration_warnings_enabled() -> bool:
    return env_bool(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, False)
