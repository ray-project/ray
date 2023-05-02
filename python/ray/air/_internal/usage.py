import json
import os
from typing import TYPE_CHECKING, Set, Union

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer
    from ray.tune.schedulers import TrialScheduler
    from ray.tune.search import BasicVariantGenerator, Searcher

AIR_TRAINERS = {
    "AccelerateTrainer",
    "HorovodTrainer",
    "HuggingFaceTrainer",
    "LightGBMTrainer",
    "LightningTrainer",
    "MosaicTrainer",
    "RLTrainer",
    "SklearnTrainer",
    "TensorflowTrainer",
    "TorchTrainer",
    "XGBoostTrainer",
}

# searchers implemented by Ray Tune.
TUNE_SEARCHERS = {
    "AxSearch",
    "BayesOptSearch",
    "TuneBOHB",
    "DragonflySearch",
    "HEBOSearch",
    "HyperOptSearch",
    "NevergradSearch",
    "OptunaSearch",
    "SkOptSearch",
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
    "AsyncHyperBandScheduler",
    "MedianStoppingRule",
    "HyperBandScheduler",
    "HyperBandForBOHB",
    "PopulationBasedTraining",
    "PopulationBasedTrainingReplay",
    "PB2",
    "ResourceChangingScheduler",
}

AIR_ENV_VARS = {
    "RAY_AIR_LOCAL_CACHE_DIR",
    "RAY_AIR_FULL_TRACEBACKS",
}

TUNE_ENV_VARS = {
    "TUNE_DISABLE_AUTO_CALLBACK_LOGGERS",
    "TUNE_DISABLE_AUTO_CALLBACK_SYNCER",
    "TUNE_DISABLE_AUTO_INIT",
    "TUNE_DISABLE_DATED_SUBDIR",
    "TUNE_DISABLE_STRICT_METRIC_CHECKING",
    "TUNE_DISABLE_SIGINT_HANDLER",
    "TUNE_FALLBACK_TO_LATEST_CHECKPOINT",
    "TUNE_FORCE_TRIAL_CLEANUP_S",
    "TUNE_GET_EXECUTOR_EVENT_WAIT_S",
    "TUNE_FUNCTION_THREAD_TIMEOUT_S",
    "TUNE_GLOBAL_CHECKPOINT_S",
    "TUNE_MAX_LEN_IDENTIFIER",
    "TUNE_MAX_PENDING_TRIALS_PG",
    "TUNE_NODE_SYNCING_MIN_ITER_THRESHOLD",
    "TUNE_NODE_SYNCING_MIN_TIME_S_THRESHOLD",
    "TUNE_PLACEMENT_GROUP_PREFIX",
    "TUNE_PLACEMENT_GROUP_RECON_INTERVAL",
    "TUNE_PRINT_ALL_TRIAL_ERRORS",
    "TUNE_RESULT_DIR",
    "TUNE_RESULT_BUFFER_LENGTH",
    "TUNE_RESULT_DELIM",
    "TUNE_RESULT_BUFFER_MAX_TIME_S",
    "TUNE_RESULT_BUFFER_MIN_TIME_S",
    "TUNE_WARN_THRESHOLD_S",
    "TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S",
    "TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S_AUTOSCALER",
    "TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S",
    "TUNE_STATE_REFRESH_PERIOD",
    "TUNE_RESTORE_RETRY_NUM",
    "TUNE_CHECKPOINT_CLOUD_RETRY_NUM",
    "TUNE_CHECKPOINT_CLOUD_RETRY_WAIT_TIME_S",
}


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


def tag_env_vars() -> bool:
    """Records environment variable usage.

    Returns:
        bool: True if at least one environment var is supplied by the user.
    """
    from ray.train.constants import TRAIN_ENV_VARS

    all_env_vars = sorted(set().union(AIR_ENV_VARS, TUNE_ENV_VARS, TRAIN_ENV_VARS))

    user_supplied_env_vars = []

    for env_var in all_env_vars:
        if env_var in os.environ:
            user_supplied_env_vars.append(env_var)

    if user_supplied_env_vars:
        env_vars_str = json.dumps(user_supplied_env_vars)
        record_extra_usage_tag(TagKey.AIR_ENV_VARS, env_vars_str)
        return True

    return False
