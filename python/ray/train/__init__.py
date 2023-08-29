# Try import ray[train] core requirements (defined in setup.py)
try:
    import pandas  # noqa: F401
    import requests  # noqa: F401
    import pyarrow  # noqa: F401
    import fsspec  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Can't import ray.train as some dependencies are missing. "
        'Run `pip install "ray[train]"` to fix.'
    ) from exc


from ray._private.usage import usage_lib

from ray.train._internal.storage import _use_storage_context

# Import this first so it can be used in other modules
if _use_storage_context():
    from ray.train._checkpoint import Checkpoint
else:
    from ray.air import Checkpoint

from ray.train._internal.data_config import DataConfig
from ray.train._internal.session import get_checkpoint, get_dataset_shard, report
from ray.train._internal.syncer import SyncConfig
from ray.train.backend import BackendConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.context import get_context
from ray.train.trainer import TrainingIterator

from ray.air.config import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.air.result import Result

usage_lib.record_library_usage("train")


__all__ = [
    "get_checkpoint",
    "get_context",
    "get_dataset_shard",
    "report",
    "BackendConfig",
    "Checkpoint",
    "CheckpointConfig",
    "DataConfig",
    "FailureConfig",
    "Result",
    "RunConfig",
    "ScalingConfig",
    "SyncConfig",
    "TrainingIterator",
    "TRAIN_DATASET_KEY",
]
