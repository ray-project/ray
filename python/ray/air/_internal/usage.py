import collections
import json
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer
    from ray.tune.schedulers import TrialScheduler
    from ray.tune.search import BasicVariantGenerator, Searcher
    from ray.tune import Callback


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


def _count_callbacks(callbacks: Optional[List["Callback"]]) -> Dict[str, int]:
    """Creates a map of callback class name -> count given a list of callbacks."""
    from ray.tune import Callback
    from ray.tune.logger import LoggerCallback
    from ray.tune.utils.callback import DEFAULT_CALLBACK_CLASSES

    from ray.air.integrations.wandb import WandbLoggerCallback
    from ray.air.integrations.mlflow import MLflowLoggerCallback
    from ray.air.integrations.comet import CometLoggerCallback
    from ray.tune.logger.aim import AimLoggerCallback

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
        return True

    return False
