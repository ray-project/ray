import json
import logging
from collections.abc import Mapping
from typing import Dict

from google.protobuf.struct_pb2 import Struct

from ray.core.generated.export_train_state_pb2 import (
    ExportTrainRunAttemptEventData as ProtoTrainRunAttempt,
    ExportTrainRunEventData as ProtoTrainRun,
)
from ray.dashboard.modules.metrics.dashboards.common import Panel
from ray.dashboard.modules.metrics.dashboards.train_dashboard_panels import (
    TRAIN_RUN_PANELS,
    TRAIN_WORKER_PANELS,
)
from ray.train.v2._internal.state.schema import (
    ActorStatus,
    BackendConfig,
    DataConfig,
    RunAttemptStatus,
    RunConfig,
    RunSettings,
    RunStatus,
    ScalingConfig,
    TrainRun,
    TrainRunAttempt,
    TrainWorker,
)
from ray.train.v2._internal.util import TrainingFramework

TRAIN_SCHEMA_VERSION = 3
RAY_TRAIN_VERSION = 2

# Status mapping dictionaries
_ACTOR_STATUS_MAP = {
    ActorStatus.ALIVE: ProtoTrainRunAttempt.ActorStatus.ALIVE,
    ActorStatus.DEAD: ProtoTrainRunAttempt.ActorStatus.DEAD,
}

_RUN_ATTEMPT_STATUS_MAP = {
    RunAttemptStatus.PENDING: ProtoTrainRunAttempt.RunAttemptStatus.PENDING,
    RunAttemptStatus.RUNNING: ProtoTrainRunAttempt.RunAttemptStatus.RUNNING,
    RunAttemptStatus.FINISHED: ProtoTrainRunAttempt.RunAttemptStatus.FINISHED,
    RunAttemptStatus.ERRORED: ProtoTrainRunAttempt.RunAttemptStatus.ERRORED,
    RunAttemptStatus.ABORTED: ProtoTrainRunAttempt.RunAttemptStatus.ABORTED,
}

_RUN_STATUS_MAP = {
    RunStatus.INITIALIZING: ProtoTrainRun.RunStatus.INITIALIZING,
    RunStatus.SCHEDULING: ProtoTrainRun.RunStatus.SCHEDULING,
    RunStatus.RUNNING: ProtoTrainRun.RunStatus.RUNNING,
    RunStatus.RESTARTING: ProtoTrainRun.RunStatus.RESTARTING,
    RunStatus.RESIZING: ProtoTrainRun.RunStatus.RESIZING,
    RunStatus.FINISHED: ProtoTrainRun.RunStatus.FINISHED,
    RunStatus.ERRORED: ProtoTrainRun.RunStatus.ERRORED,
    RunStatus.ABORTED: ProtoTrainRun.RunStatus.ABORTED,
}

_TRAINING_FRAMEWORK_MAP = {
    None: ProtoTrainRun.BackendConfig.TrainingFramework.TRAINING_FRAMEWORK_UNSPECIFIED,
    TrainingFramework.TORCH: ProtoTrainRun.BackendConfig.TrainingFramework.TORCH,
    TrainingFramework.JAX: ProtoTrainRun.BackendConfig.TrainingFramework.JAX,
    TrainingFramework.TENSORFLOW: ProtoTrainRun.BackendConfig.TrainingFramework.TENSORFLOW,
    TrainingFramework.XGBOOST: ProtoTrainRun.BackendConfig.TrainingFramework.XGBOOST,
    TrainingFramework.LIGHTGBM: ProtoTrainRun.BackendConfig.TrainingFramework.LIGHTGBM,
}

logger = logging.getLogger(__name__)

# Helper conversion functions
def _dict_to_human_readable_struct(d: Dict, *, max_depth: int = 3) -> Struct:
    """
    Convert a dict into a human-readable protobuf Struct.

    Use to convert dicts containing variable value types (custom objects,
    complex nesting, etc.) to a protobuf Struct for export serialization.
    Non-JSON-native values are converted to their string representation.

    Args:
        d: The dict to convert. Must be a dict; raises ValueError otherwise.
        max_depth: Maximum recursion depth; deeper structures are replaced with ``"..."``.

    Returns:
        A protobuf Struct populated with the converted dict.

    Raises:
        ValueError: If ``d`` is not a dict or if ``max_depth`` is <= 0.

    Example:

    Depth counts only dict nesting — lists recurse at the same depth level as their
    parent. This means a list of primitives at any dict depth is always shown in full,
    while a dict nested beyond ``max_depth`` is replaced with ``"..."``.

    ```python
    class MyConfig:
        def __init__(self, lr: float):
            self.lr = lr

        def __str__(self):
            return f"MyConfig(lr={self.lr})"

    # Input: dict with mixed value types (nested dict + custom object + list)
    cfg = {"epochs": 3, "custom": MyConfig(lr=0.001), "steps": [1, 2, 3],
           "nested": {"inner": {"deep": 99}}}

    proto_cfg = _dict_to_human_readable_struct(cfg, max_depth=2)

    # Output: `proto_cfg` is a `google.protobuf.struct_pb2.Struct`.
    # When rendered to JSON/dict (e.g., via protobuf's json_format), it looks like:
    # {
    #   "custom": "MyConfig(lr=0.001)",
    #   "epochs": 3,
    #   "steps": [1, 2, 3],      # list shown in full — lists don't consume depth
    #   "nested": {"inner": "..."}  # dict beyond max_depth is truncated
    # }
    ```
    """

    if max_depth <= 0:
        raise ValueError(
            "Error parsing object to JSON for export: max_depth must be greater than 0"
        )

    if not isinstance(d, dict):
        raise ValueError(
            "Error parsing object to JSON for export: argument must be a dictionary"
        )

    def to_human_readable(value, depth):
        # JSON-native types
        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        # Dict-like
        if isinstance(value, Mapping):
            if depth <= 0:
                return "..."
            return {str(k): to_human_readable(v, depth - 1) for k, v in value.items()}

        # List / tuple / set
        if isinstance(value, (list, tuple, set)):
            return [to_human_readable(v, depth) for v in value]

        # Fallback: string representation
        return str(value)

    try:
        human_readable_json = to_human_readable(d, max_depth)

        json_str = json.dumps(
            human_readable_json,
            sort_keys=True,
            ensure_ascii=False,
        )

        human_readable_dict = json.loads(json_str)
    except Exception as e:
        logger.warning(f"Failed to convert value to protobuf Struct for export: {e}")
        human_readable_dict = {}

    proto_struct = Struct()
    proto_struct.update(human_readable_dict)
    return proto_struct


def _to_proto_resources(resources: dict) -> ProtoTrainRunAttempt.TrainResources:
    """Convert resources dictionary to protobuf TrainResources."""
    return ProtoTrainRunAttempt.TrainResources(resources=resources)


def _to_proto_worker(worker: TrainWorker) -> ProtoTrainRunAttempt.TrainWorker:
    """Convert TrainWorker to protobuf format."""
    status = None
    if worker.status is not None:
        status = _ACTOR_STATUS_MAP[worker.status]

    return ProtoTrainRunAttempt.TrainWorker(
        world_rank=worker.world_rank,
        local_rank=worker.local_rank,
        node_rank=worker.node_rank,
        actor_id=bytes.fromhex(worker.actor_id),
        node_id=bytes.fromhex(worker.node_id),
        node_ip=worker.node_ip,
        pid=worker.pid,
        gpu_ids=worker.gpu_ids,
        status=status,
        resources=_to_proto_resources(worker.resources.resources),
        log_file_path=worker.log_file_path,
    )


# Main conversion functions
def train_run_attempt_to_proto(attempt: TrainRunAttempt) -> ProtoTrainRunAttempt:
    """Convert TrainRunAttempt to protobuf format."""
    proto_attempt = ProtoTrainRunAttempt(
        schema_version=TRAIN_SCHEMA_VERSION,
        ray_train_version=RAY_TRAIN_VERSION,
        run_id=attempt.run_id,
        attempt_id=attempt.attempt_id,
        status=_RUN_ATTEMPT_STATUS_MAP[attempt.status],
        status_detail=attempt.status_detail,
        start_time_ns=attempt.start_time_ns,
        end_time_ns=attempt.end_time_ns,
        resources=[_to_proto_resources(r.resources) for r in attempt.resources],
        workers=[_to_proto_worker(w) for w in attempt.workers],
    )

    return proto_attempt


def _to_proto_dashboard_panel(panel: Panel) -> ProtoTrainRun.DashboardPanelMetadata:
    """Convert Dashboard Panel to protobuf format."""
    proto_panel = ProtoTrainRun.DashboardPanelMetadata(
        id=str(panel.id),
        title=panel.title,
    )

    return proto_panel


def to_proto_backend_config(
    backend_config: BackendConfig,
) -> ProtoTrainRun.BackendConfig:
    """Convert BackendConfig to protobuf format."""
    proto_backend_config = ProtoTrainRun.BackendConfig(
        framework=_TRAINING_FRAMEWORK_MAP[backend_config.framework],
    )

    proto_backend_config.config.CopyFrom(
        _dict_to_human_readable_struct(backend_config.config)
    )

    return proto_backend_config


def to_proto_scaling_config(
    scaling_config: ScalingConfig,
) -> ProtoTrainRun.ScalingConfig:
    """Convert ScalingConfig to protobuf format."""
    proto_scaling_config = ProtoTrainRun.ScalingConfig(
        use_gpu=scaling_config.use_gpu,
        placement_strategy=scaling_config.placement_strategy,
        use_tpu=scaling_config.use_tpu,
    )

    if isinstance(scaling_config.num_workers, tuple):
        proto_scaling_config.num_workers_range.CopyFrom(
            ProtoTrainRun.ScalingConfig.IntRange(
                min=scaling_config.num_workers[0],
                max=scaling_config.num_workers[1],
            )
        )
    else:
        proto_scaling_config.num_workers_fixed = scaling_config.num_workers

    if scaling_config.resources_per_worker is not None:
        proto_scaling_config.resources_per_worker.values.update(
            scaling_config.resources_per_worker
        )

    if scaling_config.accelerator_type is not None:
        proto_scaling_config.accelerator_type = scaling_config.accelerator_type
    if scaling_config.topology is not None:
        proto_scaling_config.topology = scaling_config.topology

    if scaling_config.bundle_label_selector is not None:
        selectors = scaling_config.bundle_label_selector
        if isinstance(selectors, dict):
            selectors = [selectors]
        proto_scaling_config.bundle_label_selector.extend(
            [ProtoTrainRun.ScalingConfig.StringMap(values=s) for s in selectors]
        )

    return proto_scaling_config


def to_proto_data_config(data_config: DataConfig) -> ProtoTrainRun.DataConfig:
    """Convert DataConfig to protobuf format."""
    proto_data_config = ProtoTrainRun.DataConfig(
        enable_shard_locality=data_config.enable_shard_locality,
    )

    if data_config.datasets_to_split == "all":
        proto_data_config.all.SetInParent()
    else:
        proto_data_config.datasets.values.extend(data_config.datasets_to_split)

    if data_config.execution_options is not None:
        proto_data_config.execution_options.CopyFrom(
            _dict_to_human_readable_struct(data_config.execution_options)
        )

    return proto_data_config


def _to_proto_failure_config(run_config: RunConfig) -> ProtoTrainRun.FailureConfig:
    """Convert RunConfig.failure_config to protobuf format."""
    return ProtoTrainRun.FailureConfig(
        max_failures=run_config.failure_config.max_failures,
        controller_failure_limit=run_config.failure_config.controller_failure_limit,
    )


def _to_proto_worker_runtime_env(run_config: RunConfig) -> Struct:
    """Convert RunConfig.worker_runtime_env to protobuf Struct."""
    return _dict_to_human_readable_struct(run_config.worker_runtime_env)


def _to_proto_checkpoint_config(
    run_config: RunConfig,
) -> ProtoTrainRun.CheckpointConfig:
    """Convert RunConfig.checkpoint_config to protobuf format."""
    checkpoint_score_order = ProtoTrainRun.CheckpointConfig.CheckpointScoreOrder.Value(
        run_config.checkpoint_config.checkpoint_score_order.upper()
    )

    proto_checkpoint_config = ProtoTrainRun.CheckpointConfig(
        checkpoint_score_order=checkpoint_score_order
    )
    if run_config.checkpoint_config.num_to_keep is not None:
        proto_checkpoint_config.num_to_keep = run_config.checkpoint_config.num_to_keep
    if run_config.checkpoint_config.checkpoint_score_attribute is not None:
        proto_checkpoint_config.checkpoint_score_attribute = (
            run_config.checkpoint_config.checkpoint_score_attribute
        )
    return proto_checkpoint_config


def to_proto_run_config(run_config: RunConfig) -> ProtoTrainRun.RunConfig:
    """Convert RunConfig to protobuf format."""
    proto_run_config = ProtoTrainRun.RunConfig(
        name=run_config.name,
        failure_config=_to_proto_failure_config(run_config),
        worker_runtime_env=_to_proto_worker_runtime_env(run_config),
        checkpoint_config=_to_proto_checkpoint_config(run_config),
        storage_path=run_config.storage_path,
    )

    if run_config.storage_filesystem is not None:
        proto_run_config.storage_filesystem = run_config.storage_filesystem

    return proto_run_config


def _to_proto_run_settings(run_settings: RunSettings) -> ProtoTrainRun.RunSettings:
    """Convert RunSettings to protobuf format."""

    proto_run_settings = ProtoTrainRun.RunSettings(
        backend_config=to_proto_backend_config(run_settings.backend_config),
        scaling_config=to_proto_scaling_config(run_settings.scaling_config),
        datasets=run_settings.datasets,
        data_config=to_proto_data_config(run_settings.data_config),
        run_config=to_proto_run_config(run_settings.run_config),
    )

    if run_settings.train_loop_config is not None:
        proto_run_settings.train_loop_config.CopyFrom(
            _dict_to_human_readable_struct(run_settings.train_loop_config)
        )

    return proto_run_settings


def train_run_to_proto(run: TrainRun) -> ProtoTrainRun:
    """Convert TrainRun to protobuf format."""

    proto_train_run_panels = [_to_proto_dashboard_panel(p) for p in TRAIN_RUN_PANELS]
    proto_train_worker_panels = [
        _to_proto_dashboard_panel(p) for p in TRAIN_WORKER_PANELS
    ]

    proto_train_run = ProtoTrainRun(
        schema_version=TRAIN_SCHEMA_VERSION,
        ray_train_version=RAY_TRAIN_VERSION,
        id=run.id,
        name=run.name,
        job_id=bytes.fromhex(run.job_id),
        controller_actor_id=bytes.fromhex(run.controller_actor_id),
        status=_RUN_STATUS_MAP[run.status],
        status_detail=run.status_detail,
        start_time_ns=run.start_time_ns,
        end_time_ns=run.end_time_ns,
        controller_log_file_path=run.controller_log_file_path,
        train_run_panels=proto_train_run_panels,
        train_worker_panels=proto_train_worker_panels,
        framework_versions=run.framework_versions,
        run_settings=_to_proto_run_settings(run.run_settings),
    )

    return proto_train_run
