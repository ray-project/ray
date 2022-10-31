from ray._private.usage import usage_lib
from ray.train.backend import BackendConfig
from ray.train.checkpoint import CheckpointStrategy
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.train_loop_utils import (
    get_dataset_shard,
    load_checkpoint,
    local_rank,
    report,
    save_checkpoint,
    world_rank,
    world_size,
)
from ray.train.trainer import Trainer, TrainingIterator


from ray.util.annotations import Deprecated

_deprecation_msg = (
    "`ray.train.callbacks` and the `ray.train.Trainer` API are deprecated in Ray "
    "2.0, and are replaced by Ray AI Runtime (Ray AIR). Ray AIR "
    "(https://docs.ray.io/en/latest/ray-air/getting-started.html) "
    "provides greater functionality and a unified API "
    "compared to the current Ray Train API. "
    "This module will be removed in the future."
)


@Deprecated(message=_deprecation_msg)
class TrainingCallback:
    """Abstract Train callback class."""

    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type):
        raise DeprecationWarning(_deprecation_msg)


usage_lib.record_library_usage("train")

__all__ = [
    "BackendConfig",
    "get_dataset_shard",
    "load_checkpoint",
    "local_rank",
    "report",
    "save_checkpoint",
    "TrainingIterator",
    "TrainingCallback",
    "Trainer",
    "world_rank",
    "world_size",
    "TRAIN_DATASET_KEY",
    "CheckpointStrategy",
]
