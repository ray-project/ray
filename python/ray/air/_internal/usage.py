import json
import os
from typing import TYPE_CHECKING, Optional, Set, Union
import urllib.parse

from ray.air._internal.remote_storage import _is_network_mount
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer
    from ray.tune.schedulers import TrialScheduler
    from ray.tune.search import BasicVariantGenerator, Searcher
    from ray.tune import SyncConfig

AIR_TRAINERS = {
    "AccelerateTrainer",
    "HorovodTrainer",
    "TransformersTrainer",
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


def _get_tag_for_remote_path(remote_path: str) -> str:
    scheme = urllib.parse.urlparse(remote_path).scheme
    if scheme == "file":
        # NOTE: We treat a file:// storage_path as a "remote" path, so this case
        # differs from the local path only case.
        # In particular, default syncing to head node is not enabled here.
        tag = "local_uri"
    elif scheme == "memory":
        # NOTE: This is used in tests and does not make sense to actually use.
        # This condition filters the tag out of the `custom` catch-all.
        tag = "memory"
    elif scheme == "hdfs":
        tag = "hdfs"
    elif scheme in {"s3", "s3a"}:
        tag = "s3"
    elif scheme in {"gs", "gcs"}:
        tag = "gs"
    else:
        tag = "custom_remote_storage"
    return tag


def tag_ray_air_storage_config(
    local_path: str, remote_path: Optional[str], sync_config: "SyncConfig"
) -> None:
    """Records the storage storage configuration of an experiment.

    The storage configuration is set by `RunConfig(storage_path, sync_config)`.

    The possible configurations are:
    - 'driver' = Default syncing to Tune driver node if no remote path is specified.
    - 'local' = No synchronization at all.
    - 'nfs' = Using a mounted shared network filesystem.
    - ('s3', 'gs', 'hdfs', 'custom_remote_storage'): Various remote storage schemes.
    - ('local_uri', 'memory'): Mostly used by internal testing by setting `storage_path`
        to `file://` or `memory://`.
    """
    if remote_path:
        # HDFS or cloud storage
        storage_config_tag = _get_tag_for_remote_path(remote_path)
    elif _is_network_mount(local_path):
        # NFS
        storage_config_tag = "nfs"
    elif sync_config.syncer is None:
        # Syncing is disabled - results are only available on node-local storage
        storage_config_tag = "local"
    else:
        # The driver node's local storage is the synchronization point.
        storage_config_tag = "driver"

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
    from ray.tune.constants import TUNE_ENV_VARS
    from ray.train.constants import TRAIN_ENV_VARS

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
