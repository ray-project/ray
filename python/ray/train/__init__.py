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
    from ray.data.checkpoint.interfaces import (  # noqa: F401, F811
        DatasetCheckpointConfig,
    )
    from ray.train.v2.api.callback import UserCallback  # noqa: F811
    from ray.train.v2.api.config import (  # noqa: F811
        CheckpointConfig,
        FailureConfig,
        LoggingConfig,
        RunConfig,
        ScalingConfig,
    )
    from ray.train.v2.api.context import TrainContext  # noqa: F811
    from ray.train.v2.api.exceptions import (  # noqa: F811
        ControllerError,
        NCCLHangError,
        PreemptionError,
        TrainingFailedError,
        WorkerGroupError,
    )
    from ray.train.v2.api.preemption import PreemptionInfo  # noqa: F811
    from ray.train.v2.api.report_config import (  # noqa: F811
        CheckpointConsistencyMode,
        CheckpointUploadMode,
    )
    from ray.train.v2.api.reported_checkpoint import (  # noqa: F811
        ReportedCheckpoint,
        ReportedCheckpointStatus,
    )
    from ray.train.v2.api.result import Result  # noqa: F811
    from ray.train.v2.api.train_fn_utils import (  # noqa: F811
        annotate,
        get_all_reported_checkpoints,
        get_checkpoint,
        get_context,
        get_dataset_shard,
        get_preemption_info,
        report,
    )
    from ray.train.v2.api.validation_config import (  # noqa: F811
        ValidationConfig,
        ValidationFn,
        ValidationTaskConfig,
    )
else:
    from ray.train._internal.syncer import SyncConfig
    from ray.train.constants import _log_v1_deprecation_warning

    _log_v1_deprecation_warning()


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
TrainContext.__module__ = "ray.train"
TrainingFailedError.__module__ = "ray.train"

# TODO: consider implementing these in v1 and raising ImportError instead.
if is_v2_enabled():
    __all__.extend(
        [
            "CheckpointUploadMode",
            "CheckpointConsistencyMode",
            "ControllerError",
            "DatasetCheckpointConfig",
            "NCCLHangError",
            "LoggingConfig",
            "PreemptionError",
            "PreemptionInfo",
            "ReportedCheckpoint",
            "ReportedCheckpointStatus",
            "UserCallback",
            "WorkerGroupError",
            "ValidationConfig",
            "ValidationFn",
            "ValidationTaskConfig",
            "annotate",
            "get_all_reported_checkpoints",
            "get_preemption_info",
        ]
    )

    annotate.__module__ = "ray.train"
    CheckpointUploadMode.__module__ = "ray.train"
    CheckpointConsistencyMode.__module__ = "ray.train"
    ControllerError.__module__ = "ray.train"
    DatasetCheckpointConfig.__module__ = "ray.train"
    NCCLHangError.__module__ = "ray.train"
    LoggingConfig.__module__ = "ray.train"
    PreemptionError.__module__ = "ray.train"
    PreemptionInfo.__module__ = "ray.train"
    ReportedCheckpoint.__module__ = "ray.train"
    ReportedCheckpointStatus.__module__ = "ray.train"
    UserCallback.__module__ = "ray.train"
    WorkerGroupError.__module__ = "ray.train"
    ValidationConfig.__module__ = "ray.train"
    ValidationFn.__module__ = "ray.train"
    ValidationTaskConfig.__module__ = "ray.train"
    get_all_reported_checkpoints.__module__ = "ray.train"
    get_preemption_info.__module__ = "ray.train"

    # With Train V2, `ray.train.SyncConfig` is a deprecated alias of the V1 class,
    # which only Ray Tune still uses. It's kept out of the module globals and
    # `__all__` so that only an explicit `ray.train.SyncConfig` warns, and its
    # `__module__` keeps pointing at `ray.train._internal.syncer` so that pickles
    # of it (e.g. a Tune `RunConfig` in experiment state, or one shipped to a Ray
    # worker) never resolve the deprecated alias.
    from ray._common.deprecation import (
        DeprecatedAlias as _DeprecatedAlias,
        make_deprecated_getattr as _make_deprecated_getattr,
    )
    from ray.train.constants import V2_MIGRATION_GUIDE_LINK_MESSAGE as _MIGRATION_NOTE

    _DEPRECATED_ALIASES = {
        "SyncConfig": _DeprecatedAlias(
            "ray.train._internal.syncer:SyncConfig",
            (("ray.tune.SyncConfig", "with Ray Tune"),),
            note=_MIGRATION_NOTE,
        ),
    }

    __getattr__ = _make_deprecated_getattr(__name__, _DEPRECATED_ALIASES, "3.0")

    def __dir__():
        return sorted(set(globals()) | set(_DEPRECATED_ALIASES))

else:
    __all__.append("SyncConfig")
    SyncConfig.__module__ = "ray.train"


# DO NOT ADD ANYTHING AFTER THIS LINE.
