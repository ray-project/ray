from ray._private.usage import usage_lib
from ray.train.backend import BackendConfig
from ray.train.callbacks import TrainingCallback
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
from ray.air.config import CheckpointConfig

# Deprecated. Alias of CheckpointConfig for backwards compat
deprecation_message = (
    "`CheckpointStrategy` is deprecated and will be removed in "
    "the future. Please use `ray.air.config.CheckpointStrategy` "
    "instead."
)

@Deprecated(message=deprecation_message)
@dataclass
class CheckpointStrategy(CheckpointConfig):
    def __post_init__(self):
        warnings.warn(deprecation_message)
        super().__post_init__()

usage_lib.record_library_usage("train")

__all__ = [
    "BackendConfig",
    "CheckpointConfig",
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
