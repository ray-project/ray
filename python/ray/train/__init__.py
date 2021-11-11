from ray.train.backend import BackendConfig
from ray.train.tensorflow import TensorflowConfig
from ray.train.horovod import HorovodConfig
from ray.train.callbacks import TrainingCallback
from ray.train.checkpoint import CheckpointStrategy
from ray.train.session import (get_dataset_shard, local_rank, load_checkpoint,
                               report, save_checkpoint, world_rank, world_size)
from ray.train.trainer import Trainer, TrainingIterator


class _ImportFailedConfig:
    framework_str: str

    def __init__(self):
        raise ValueError(f"`{self.framework_str}` is not installed. "
                         f"Please install {self.framework_str} to use "
                         f"this "
                         "backend.")


try:
    from ray.train.torch import TorchConfig
except ImportError:
    TorchConfig = _ImportFailedConfig
    TorchConfig.framework_str = "torch"

__all__ = [
    "BackendConfig", "CheckpointStrategy", "get_dataset_shard",
    "HorovodConfig", "load_checkpoint", "local_rank", "report",
    "save_checkpoint", "TrainingIterator", "TensorflowConfig",
    "TrainingCallback", "TorchConfig", "Trainer", "world_rank", "world_size"
]
