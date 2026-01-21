import json
import logging

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
    RunSettings,
    RunStatus,
    RuntimeConfig,
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
    None: ProtoTrainRun.TrainingFramework.TRAINING_FRAMEWORK_UNSPECIFIED,
    TrainingFramework.TORCH: ProtoTrainRun.TrainingFramework.TORCH,
    TrainingFramework.JAX: ProtoTrainRun.TrainingFramework.JAX,
    TrainingFramework.TENSORFLOW: ProtoTrainRun.TrainingFramework.TENSORFLOW,
    TrainingFramework.XGBOOST: ProtoTrainRun.TrainingFramework.XGBOOST,
    TrainingFramework.LIGHTGBM: ProtoTrainRun.TrainingFramework.LIGHTGBM,
}

logger = logging.getLogger(__name__)

# Helper conversion functions
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
    return ProtoTrainRun.BackendConfig(
        framework=_TRAINING_FRAMEWORK_MAP[backend_config.framework],
        config=backend_config.config,
    )


def to_proto_scaling_config(
    scaling_config: ScalingConfig,
) -> ProtoTrainRun.ScalingConfig:
    """Convert ScalingConfig to protobuf format."""
    return ProtoTrainRun.ScalingConfig(
        num_workers=scaling_config.num_workers,
        use_gpu=scaling_config.use_gpu,
        resources_per_worker=scaling_config.resources_per_worker,
        placement_strategy=scaling_config.placement_strategy,
        accelerator_type=scaling_config.accelerator_type,
        use_tpu=scaling_config.use_tpu,
        topology=scaling_config.topology,
        bundle_label_selector=scaling_config.bundle_label_selector,
    )


def to_proto_data_config(data_config: DataConfig) -> ProtoTrainRun.DataConfig:
    """Convert DataConfig to protobuf format."""
    return ProtoTrainRun.DataConfig(
        datasets_to_split=data_config.datasets_to_split,
        execution_options=data_config.execution_options,
        enable_shard_locality=data_config.enable_shard_locality,
    )


def to_proto_runtime_config(
    runtime_config: RuntimeConfig,
) -> ProtoTrainRun.RuntimeConfig:
    """Convert RuntimeConfig to protobuf format."""

    failure_config = ProtoTrainRun.FailureConfig(
        max_failures=runtime_config.failure_config.max_failures,
        controller_failure_limit=runtime_config.failure_config.controller_failure_limit,
    )

    checkpoint_config = ProtoTrainRun.CheckpointConfig(
        num_to_keep=runtime_config.checkpoint_config.num_to_keep,
        checkpoint_score_attribute=runtime_config.checkpoint_config.checkpoint_score_attribute,
        checkpoint_score_order=runtime_config.checkpoint_config.checkpoint_score_order,
    )

    return ProtoTrainRun.RuntimeConfig(
        failure_config=failure_config,
        worker_runtime_env=runtime_config.worker_runtime_env,
        checkpoint_config=checkpoint_config,
        storage_path=runtime_config.storage_path,
    )


def _to_proto_run_settings(run_settings: RunSettings) -> ProtoTrainRun.RunContext:
    """Convert RunSettings to protobuf format."""
    try:
        loop_config_json = json.dumps(run_settings.train_loop_config)
    except (TypeError, ValueError):
        loop_config_json = json.dumps(
            {"message": "Non-JSON serializable train_loop_config"}
        )
        logger.debug("train_loop_config is not JSON serializable")

    return ProtoTrainRun.RunSettings(
        train_loop_config=loop_config_json,
        backend_config=to_proto_backend_config(run_settings.backend_config),
        scaling_config=to_proto_scaling_config(run_settings.scaling_config),
        datasets=run_settings.datasets,
        data_config=to_proto_data_config(run_settings.data_config),
        runtime_config=to_proto_runtime_config(run_settings.runtime_config),
    )


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
