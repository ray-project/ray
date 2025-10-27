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
    RunAttemptStatus,
    RunStatus,
    TrainRun,
    TrainRunAttempt,
    TrainWorker,
)

TRAIN_SCHEMA_VERSION = 2
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
    )

    return proto_run
