import json
import logging
from collections.abc import Mapping
from typing import Any

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
def _to_human_readable_json(obj: Any, *, max_depth: int = 10) -> str:
    """
    Convert arbitrary Python objects into a human-readable, JSON-serializable string.

    Use to convert user-defined dicts containing variable field types, custom objects,
    complex nesting to standard JSON string for protobuf serialization.

    Args:
        obj: The object to convert into a JSON-serializable form.
        max_depth: Maximum recursion depth; deeper structures are replaced with "<truncated>".

    Returns:
        A JSON string representation of the converted object.

    Example:

    ```python
    class MyConfig:
        def __init__(self, lr: float):
            self.lr = lr
            self.tags = ["a", "b"]

    # Input: mixed types (nested dict + custom object)
    cfg = {"epochs": 3, "custom": MyConfig(lr=0.001)}

    json_str = _to_human_readable_json(cfg)

    # Output: JSON string safe to export/store (example)
    # {
    #   "custom": {"__type__": "MyConfig", "attributes": {"lr": 0.001, "tags": ["a", "b"]}},
    #   "epochs": 3
    # }
    ```
    """

    def to_human_readable(value, depth):
        if depth <= 0:
            return "<truncated>"

        # JSON-native types
        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        # Dict-like
        if isinstance(value, Mapping):
            return {str(k): to_human_readable(v, depth - 1) for k, v in value.items()}

        # List / tuple / set
        if isinstance(value, (list, tuple, set)):
            return [to_human_readable(v, depth - 1) for v in value]

        # Objects with __dict__
        if hasattr(value, "__dict__"):
            return {
                "__type__": value.__class__.__name__,
                "attributes": to_human_readable(vars(value), depth - 1),
            }

        # Fallback: string representation
        return str(value)

    try:
        human_readable_json = to_human_readable(obj, max_depth)
    except Exception as e:
        logger.debug(f"Failed to convert value to JSON for export: {e}")
        human_readable_json = "N/A"

    return json.dumps(
        human_readable_json,
        sort_keys=True,
        ensure_ascii=False,
    )


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
    human_readable_config = json.loads(_to_human_readable_json(backend_config.config))
    config = Struct()
    config.update(human_readable_config)

    return ProtoTrainRun.BackendConfig(
        framework=_TRAINING_FRAMEWORK_MAP[backend_config.framework],
        config=config,
    )


def to_proto_scaling_config(
    scaling_config: ScalingConfig,
) -> ProtoTrainRun.ScalingConfig:
    """Convert ScalingConfig to protobuf format."""
    proto = ProtoTrainRun.ScalingConfig(
        num_workers=scaling_config.num_workers,
        use_gpu=scaling_config.use_gpu,
        placement_strategy=scaling_config.placement_strategy,
        use_tpu=scaling_config.use_tpu,
    )

    if scaling_config.resources_per_worker is not None:
        proto.resources_per_worker.values.update(scaling_config.resources_per_worker)

    # optional scalar fields: assign only if not None
    if scaling_config.accelerator_type is not None:
        proto.accelerator_type = scaling_config.accelerator_type
    if scaling_config.topology is not None:
        proto.topology = scaling_config.topology

    # Normalize bundle label selector to protobuf format
    if scaling_config.bundle_label_selector is not None:
        selectors = scaling_config.bundle_label_selector
        if isinstance(selectors, dict):
            selectors = [selectors]
        proto.bundle_label_selector.extend(
            [ProtoTrainRun.ScalingConfig.StringMap(values=s) for s in selectors]
        )

    return proto


def to_proto_data_config(data_config: DataConfig) -> ProtoTrainRun.DataConfig:
    """Convert DataConfig to protobuf format."""
    proto = ProtoTrainRun.DataConfig(
        enable_shard_locality=data_config.enable_shard_locality,
    )

    if data_config.datasets_to_split == "all":
        proto.all.SetInParent()
    else:
        proto.datasets.values.extend(data_config.datasets_to_split)

    if data_config.execution_options is not None:
        human_readable_execution_options = json.loads(
            _to_human_readable_json(data_config.execution_options)
        )
        proto.execution_options.update(human_readable_execution_options)

    return proto


def to_proto_run_config(
    run_config: RunConfig,
) -> ProtoTrainRun.RunConfig:
    """Convert RunConfig to protobuf format."""

    failure_config = ProtoTrainRun.FailureConfig(
        max_failures=run_config.failure_config.max_failures,
        controller_failure_limit=run_config.failure_config.controller_failure_limit,
    )

    worker_runtime_env = Struct()
    human_readable_worker_runtime_env = json.loads(
        _to_human_readable_json(run_config.worker_runtime_env)
    )
    worker_runtime_env.update(human_readable_worker_runtime_env)

    checkpoint_score_order = ProtoTrainRun.CheckpointConfig.CheckpointScoreOrder.Value(
        run_config.checkpoint_config.checkpoint_score_order.upper()
    )

    checkpoint_config = ProtoTrainRun.CheckpointConfig(
        checkpoint_score_order=checkpoint_score_order
    )
    if run_config.checkpoint_config.num_to_keep is not None:
        checkpoint_config.num_to_keep = run_config.checkpoint_config.num_to_keep
    if run_config.checkpoint_config.checkpoint_score_attribute is not None:
        checkpoint_config.checkpoint_score_attribute = (
            run_config.checkpoint_config.checkpoint_score_attribute
        )

    return ProtoTrainRun.RunConfig(
        failure_config=failure_config,
        worker_runtime_env=worker_runtime_env,
        checkpoint_config=checkpoint_config,
        storage_path=run_config.storage_path,
    )


def _to_proto_run_settings(run_settings: RunSettings) -> ProtoTrainRun.RunSettings:
    """Convert RunSettings to protobuf format."""

    proto = ProtoTrainRun.RunSettings(
        backend_config=to_proto_backend_config(run_settings.backend_config),
        scaling_config=to_proto_scaling_config(run_settings.scaling_config),
        datasets=run_settings.datasets,
        data_config=to_proto_data_config(run_settings.data_config),
        run_config=to_proto_run_config(run_settings.run_config),
    )

    if run_settings.train_loop_config is not None:
        proto.train_loop_config = _to_human_readable_json(
            run_settings.train_loop_config
        )

    return proto


def train_run_to_proto(run: TrainRun) -> ProtoTrainRun:
    """Convert TrainRun to protobuf format."""

    train_run_panels_proto = [_to_proto_dashboard_panel(p) for p in TRAIN_RUN_PANELS]
    train_worker_panels_proto = [
        _to_proto_dashboard_panel(p) for p in TRAIN_WORKER_PANELS
    ]

    proto_run = ProtoTrainRun(
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
        train_run_panels=train_run_panels_proto,
        train_worker_panels=train_worker_panels_proto,
        framework_versions=run.framework_versions,
        run_settings=_to_proto_run_settings(run.run_settings),
    )

    return proto_run
