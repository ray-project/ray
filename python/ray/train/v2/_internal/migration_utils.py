from ray._private.ray_constants import env_bool


# Set this to 1 to enable deprecation warnings for V2 migration.
ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR = "RAY_TRAIN_ENABLE_V2_MIGRATION_WARNINGS"

V2_MIGRATION_GUIDE_MESSAGE = (
    "See this issue for more context and migration options: "
    "https://github.com/ray-project/ray/issues/49454"
)

FAIL_FAST_DEPRECATION_MESSAGE = (
    "`ray.train.FailureConfig(fail_fast)` is deprecated since it is "
    "only relevant in the context of multiple trials running in Ray Tune. "
    "This parameter is still available in `ray.tune.FailureConfig`. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

TRAINER_RESOURCES_DEPRECATION_MESSAGE = (
    "`ray.train.ScalingConfig(trainer_resources)` is deprecated. "
    "This parameter was an advanced configuration that specified "
    "resources for the Ray Train driver actor, which doesn't "
    "need to reserve logical resources because it doesn't perform "
    "any heavy computation. "
    "Only the `resources_per_worker` parameter should be used "
    "to specify resources for the training workers. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

VERBOSE_DEPRECATION_MESSAGE = (
    "`ray.train.RunConfig(verbose)` is deprecated. "
    "This parameter controls Ray Tune logging verbosity, "
    "and is only relevant when using Ray Tune. "
    "This parameter is still available in `ray.tune.RunConfig`. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

LOG_TO_FILE_DEPRECATION_MESSAGE = (
    "`ray.train.RunConfig(log_to_file)` is deprecated. "
    "The Ray Train driver actor and the training worker actors "
    "already log stdout/stderr as part of Ray's logging system. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

STOP_DEPRECATION_MESSAGE = (
    "`ray.train.RunConfig(stop)` is deprecated. "
    "This parameter is only relevant when using Ray Tune "
    "and is still available in `ray.tune.RunConfig`. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

CALLBACKS_DEPRECATION_MESSAGE = (
    "`ray.train.RunConfig(callbacks: List[ray.tune.Callback])` is deprecated. "
    "Ray Train no longer accepts Ray Tune callbacks, since the Ray Train "
    "execution backend is being separated from Ray Tune. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

PROGRESS_REPORTER_DEPRECATION_MESSAGE = (
    "`ray.train.RunConfig(progress_reporter)` is deprecated. "
    "This parameter controls the Ray Tune console output reporter, "
    "and is only relevant when using Ray Tune. "
    "This parameter is still available in `ray.tune.RunConfig`. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

SYNC_CONFIG_DEPRECATION_MESSAGE = (
    "`ray.train.RunConfig(sync_config)` is deprecated. "
    "This configuration controls advanced syncing behavior, "
    "which Ray Train is dropping support for. "
    "This parameter is still available in `ray.tune.RunConfig`, "
    "and this class is moved to `ray.tune.SyncConfig`. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)


def _v2_migration_warnings_enabled() -> bool:
    return env_bool(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, False)
