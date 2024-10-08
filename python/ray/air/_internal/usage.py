import collections
import json
import os
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train._internal.storage import StorageContext
    from ray.train.trainer import BaseTrainer
    from ray.tune import Callback
    from ray.tune.schedulers import TrialScheduler
    from ray.tune.search import BasicVariantGenerator, Searcher


AIR_TRAINERS = {
    "HorovodTrainer",
    "LightGBMTrainer",
    "TensorflowTrainer",
    "TorchTrainer",
    "XGBoostTrainer",
}

# searchers implemented by Ray Tune.
TUNE_SEARCHERS = {
    "AxSearch",
    "BayesOptSearch",
    "TuneBOHB",
    "HEBOSearch",
    "HyperOptSearch",
    "NevergradSearch",
    "OptunaSearch",
    "ZOOptSearch",
}

# These are just wrappers around real searchers.
# We don't want to double tag in this case, otherwise, the real tag
# will be overwritten.
TUNE_SEARCHER_WRAPPERS = {
    "ConcurrencyLimiter",
    "Repeater",
}

TUNE_SCHEDULERS = {
    "FIFOScheduler",
    "AsyncHyperBandScheduler",
    "MedianStoppingRule",
    "HyperBandScheduler",
    "HyperBandForBOHB",
    "PopulationBasedTraining",
    "PopulationBasedTrainingReplay",
    "PB2",
    "ResourceChangingScheduler",
}


class AirEntrypoint(Enum):
    TUNER = "Tuner.fit"
    TRAINER = "Trainer.fit"
    TUNE_RUN = "tune.run"
    TUNE_RUN_EXPERIMENTS = "tune.run_experiments"


def _find_class_name(obj, allowed_module_path_prefix: str, whitelist: Set[str]):
    """Find the class name of the object. If the object is not
    under `allowed_module_path_prefix` or if its class is not in the whitelist,
    return "Custom".

    Args:
        obj: The object under inspection.
        allowed_module_path_prefix: If the `obj`'s class is not under
            the `allowed_module_path_prefix`, its class name will be anonymized.
        whitelist: If the `obj`'s class is not in the `whitelist`,
            it will be anonymized.
    Returns:
        The class name to be tagged with telemetry.
    """
    module_path = obj.__module__
    cls_name = obj.__class__.__name__
    if module_path.startswith(allowed_module_path_prefix) and cls_name in whitelist:
        return cls_name
    else:
        return "Custom"


def tag_air_trainer(trainer: "BaseTrainer"):
    from ray.train.trainer import BaseTrainer

    assert isinstance(trainer, BaseTrainer)
    trainer_name = _find_class_name(trainer, "ray.train", AIR_TRAINERS)
    record_extra_usage_tag(TagKey.AIR_TRAINER, trainer_name)


def tag_searcher(searcher: Union["BasicVariantGenerator", "Searcher"]):
    from ray.tune.search import BasicVariantGenerator, Searcher

    if isinstance(searcher, BasicVariantGenerator):
        # Note this could be highly inflated as all train flows are treated
        # as using BasicVariantGenerator.
        record_extra_usage_tag(TagKey.TUNE_SEARCHER, "BasicVariantGenerator")
    elif isinstance(searcher, Searcher):
        searcher_name = _find_class_name(
            searcher, "ray.tune.search", TUNE_SEARCHERS.union(TUNE_SEARCHER_WRAPPERS)
        )
        if searcher_name in TUNE_SEARCHER_WRAPPERS:
            # ignore to avoid double tagging with wrapper name.
            return
        record_extra_usage_tag(TagKey.TUNE_SEARCHER, searcher_name)
    else:
        assert False, (
            "Not expecting a non-BasicVariantGenerator, "
            "non-Searcher type passed in for `tag_searcher`."
        )


def tag_scheduler(scheduler: "TrialScheduler"):
    from ray.tune.schedulers import TrialScheduler

    assert isinstance(scheduler, TrialScheduler)
    scheduler_name = _find_class_name(scheduler, "ray.tune.schedulers", TUNE_SCHEDULERS)
    record_extra_usage_tag(TagKey.TUNE_SCHEDULER, scheduler_name)


def tag_setup_wandb():
    record_extra_usage_tag(TagKey.AIR_SETUP_WANDB_INTEGRATION_USED, "1")


def tag_setup_mlflow():
    record_extra_usage_tag(TagKey.AIR_SETUP_MLFLOW_INTEGRATION_USED, "1")


def _count_callbacks(callbacks: Optional[List["Callback"]]) -> Dict[str, int]:
    """Creates a map of callback class name -> count given a list of callbacks."""
    from ray.air.integrations.comet import CometLoggerCallback
    from ray.air.integrations.mlflow import MLflowLoggerCallback
    from ray.air.integrations.wandb import WandbLoggerCallback
    from ray.tune import Callback
    from ray.tune.logger import LoggerCallback
    from ray.tune.logger.aim import AimLoggerCallback
    from ray.tune.utils.callback import DEFAULT_CALLBACK_CLASSES

    built_in_callbacks = (
        WandbLoggerCallback,
        MLflowLoggerCallback,
        CometLoggerCallback,
        AimLoggerCallback,
    ) + DEFAULT_CALLBACK_CLASSES

    callback_names = [callback_cls.__name__ for callback_cls in built_in_callbacks]
    callback_counts = collections.defaultdict(int)

    callbacks = callbacks or []
    for callback in callbacks:
        if not isinstance(callback, Callback):
            # This will error later, but don't include this as custom usage.
            continue

        callback_name = callback.__class__.__name__

        if callback_name in callback_names:
            callback_counts[callback_name] += 1
        elif isinstance(callback, LoggerCallback):
            callback_counts["CustomLoggerCallback"] += 1
        else:
            callback_counts["CustomCallback"] += 1

    return callback_counts


def tag_callbacks(callbacks: Optional[List["Callback"]]) -> bool:
    """Records built-in callback usage via a JSON str representing a
    dictionary mapping callback class name -> counts.

    User-defined callbacks will increment the count under the `CustomLoggerCallback`
    or `CustomCallback` key depending on which of the provided interfaces they subclass.
    NOTE: This will NOT track the name of the user-defined callback,
    nor its implementation.

    This will NOT report telemetry if no callbacks are provided by the user.

    Returns:
        bool: True if usage was recorded, False otherwise.
    """
    if not callbacks:
        # User didn't pass in any callbacks -> no usage recorded.
        return False

    callback_counts = _count_callbacks(callbacks)

    if callback_counts:
        callback_counts_str = json.dumps(callback_counts)
        record_extra_usage_tag(TagKey.AIR_CALLBACKS, callback_counts_str)


def tag_storage_type(storage: "StorageContext"):
    """Records the storage configuration of an experiment.

    The storage configuration is set by `RunConfig(storage_path, storage_filesystem)`.

    The possible storage types (defined by `pyarrow.fs.FileSystem.type_name`) are:
    - 'local' = pyarrow.fs.LocalFileSystem. This includes NFS usage.
    - 'mock' = pyarrow.fs._MockFileSystem. This is used for testing.
    - ('s3', 'gcs', 'abfs', 'hdfs'): Various remote storage schemes
        with default implementations in pyarrow.
    - 'custom' = All other storage schemes, which includes ALL cases where a
        custom `storage_filesystem` is provided.
    - 'other' = catches any other cases not explicitly handled above.
    """
    whitelist = {"local", "mock", "s3", "gcs", "abfs", "hdfs"}

    if storage.custom_fs_provided:
        storage_config_tag = "custom"
    elif storage.storage_filesystem.type_name in whitelist:
        storage_config_tag = storage.storage_filesystem.type_name
    else:
        storage_config_tag = "other"

    record_extra_usage_tag(TagKey.AIR_STORAGE_CONFIGURATION, storage_config_tag)


def tag_ray_air_env_vars() -> bool:
    """Records usage of environment variables exposed by the Ray AIR libraries.

    NOTE: This does not track the values of the environment variables, nor
    does this track environment variables not explicitly included in the
    `all_ray_air_env_vars` allow-list.

    Returns:
        bool: True if at least one environment var is supplied by the user.
    """
    from ray.air.constants import AIR_ENV_VARS
    from ray.train.constants import TRAIN_ENV_VARS
    from ray.tune.constants import TUNE_ENV_VARS

    all_ray_air_env_vars = sorted(
        set().union(AIR_ENV_VARS, TUNE_ENV_VARS, TRAIN_ENV_VARS)
    )

    user_supplied_env_vars = []

    for env_var in all_ray_air_env_vars:
        if env_var in os.environ:
            user_supplied_env_vars.append(env_var)

    if user_supplied_env_vars:
        env_vars_str = json.dumps(user_supplied_env_vars)
        record_extra_usage_tag(TagKey.AIR_ENV_VARS, env_vars_str)
        return True

    return False


def tag_air_entrypoint(entrypoint: AirEntrypoint) -> None:
    """Records the entrypoint to an AIR training run."""
    assert entrypoint in AirEntrypoint
    record_extra_usage_tag(TagKey.AIR_ENTRYPOINT, entrypoint.value)
