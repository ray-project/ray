# Try import ray[train] core requirements (defined in setup.py)
# isort: off
try:
    import fsspec  # noqa: F401
    import pandas  # noqa: F401
    import pyarrow  # noqa: F401
    import requests  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Can't import ray.train as some dependencies are missing. "
        'Run `pip install "ray[train]"` to fix.'
    ) from exc
# isort: on


from ray.air.config import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.air.result import Result

# Import this first so it can be used in other modules
from ray.train._checkpoint import Checkpoint
from ray.train._internal.data_config import DataConfig
from ray.train._internal.session import get_checkpoint, get_dataset_shard, report
from ray.train._internal.syncer import SyncConfig
from ray.train.backend import BackendConfig
from ray.train.base_trainer import TrainingFailedError
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.context import TrainContext, get_context
from ray.train.v2._internal.constants import is_v2_enabled

if is_v2_enabled():
    try:
        import pydantic  # noqa: F401
    except (ImportError, ModuleNotFoundError) as exc:
        raise ImportError(
            "`ray.train.v2` requires the pydantic package, which is missing. "
            "Run the following command to fix this: `pip install pydantic`"
        ) from exc
    from ray.train.v2.api.callback import UserCallback  # noqa: F811
    from ray.train.v2.api.config import (  # noqa: F811
        CheckpointConfig,
        FailureConfig,
        RunConfig,
        ScalingConfig,
    )
    from ray.train.v2.api.context import TrainContext  # noqa: F811
    from ray.train.v2.api.exceptions import (  # noqa: F811
        ControllerError,
        TrainingFailedError,
        WorkerGroupError,
    )
    from ray.train.v2.api.report_config import (  # noqa: F811
        CheckpointConsistencyMode,
        CheckpointUploadMode,
    )
    from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint  # noqa: F811
    from ray.train.v2.api.result import Result  # noqa: F811
    from ray.train.v2.api.train_fn_utils import (  # noqa: F811
        get_all_reported_checkpoints,
        get_checkpoint,
        get_context,
        get_dataset_shard,
        report,
    )


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
    "TrainContext",
    "TrainingFailedError",
    "TRAIN_DATASET_KEY",
]

get_checkpoint.__module__ = "ray.train"
get_context.__module__ = "ray.train"
get_dataset_shard.__module__ = "ray.train"
report.__module__ = "ray.train"
BackendConfig.__module__ = "ray.train"
Checkpoint.__module__ = "ray.train"
CheckpointConfig.__module__ = "ray.train"
DataConfig.__module__ = "ray.train"
FailureConfig.__module__ = "ray.train"
Result.__module__ = "ray.train"
RunConfig.__module__ = "ray.train"
ScalingConfig.__module__ = "ray.train"
SyncConfig.__module__ = "ray.train"
TrainContext.__module__ = "ray.train"
TrainingFailedError.__module__ = "ray.train"

# TODO: consider implementing these in v1 and raising ImportError instead.
if is_v2_enabled():
    __all__.extend(
        [
            "CheckpointUploadMode",
            "CheckpointConsistencyMode",
            "ControllerError",
            "ReportedCheckpoint",
            "UserCallback",
            "WorkerGroupError",
            "get_all_reported_checkpoints",
        ]
    )

    CheckpointUploadMode.__module__ = "ray.train"
    CheckpointConsistencyMode.__module__ = "ray.train"
    ControllerError.__module__ = "ray.train"
    ReportedCheckpoint.__module__ = "ray.train"
    UserCallback.__module__ = "ray.train"
    WorkerGroupError.__module__ = "ray.train"
    get_all_reported_checkpoints.__module__ = "ray.train"


# DO NOT ADD ANYTHING AFTER THIS LINE.
