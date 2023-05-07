from typing import TYPE_CHECKING, Optional, Set, Union
import urllib.parse

from ray.air._internal.remote_storage import is_mounted
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer
    from ray.tune.schedulers import TrialScheduler
    from ray.tune.search import BasicVariantGenerator, Searcher
    from ray.tune import SyncConfig

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


def _get_tag_for_remote_path(remote_path: str) -> str:
    scheme = urllib.parse.urlparse(remote_path).scheme
    if scheme == "file":
        # NOTE: We treat a file:// storage_path as a "remote" path, so this case
        # differs from the local path only case.
        # In particular, default syncing to head node is not enabled here.
        tag = "local_uri"
    elif scheme == "memory":
        # NOTE: This is used in tests and does not make sense to use in
        # real usage. This condition filters the tag out of the `custom` catch-all.
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
    - 'local+sync' = Default head node syncing if no remote path is specified
    - 'local+no_sync' = No synchronization at all.
    - 'nfs' = Using a mounted shared network filesystem.
        NOTE: This currently detects *any* mount, not necessarily
        a mounted network filesystem.
    - ('s3', 'gs', 'hdfs', 'custom_remote_storage'): Various remote storage schemes.
    - ('local_uri', 'memory'): Mostly used by internal testing by setting `storage_path`
        to `file://` or `memory://`.
    """
    if remote_path:
        # HDFS or cloud storage
        storage_config_tag = _get_tag_for_remote_path(remote_path)
    elif is_mounted(local_path):
        # NFS
        storage_config_tag = "nfs"
    else:
        # Local
        storage_config_tag = (
            "local+no_sync" if sync_config.syncer is None else "local+sync"
        )

    record_extra_usage_tag(TagKey.AIR_STORAGE_CONFIGURATION, storage_config_tag)
