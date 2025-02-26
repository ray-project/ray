from ray.core.generated.export_train_state_pb2 import (
    ExportTrainRun as ProtoTrainRun,
    ExportTrainRunAttempt as ProtoTrainRunAttempt,
)
from ray.train.v2._internal.state.schema import (
    TrainRunAttempt,
    TrainRun,
    TrainWorker,
    RunAttemptStatus,
    RunStatus,
    ActorStatus,
)


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
def _ms_to_ns(time_ms: int) -> int:
    """Convert millisecond timestamp to ns."""
    return time_ms * int(1e6)


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
        actor_id=bytes.from_hex(worker.actor_id),
        node_id=bytes.from_hex(worker.node_id),
        node_ip=worker.node_ip,
        pid=worker.pid,
        gpu_ids=worker.gpu_ids,
        status=status,
        resources=_to_proto_resources(worker.resources.resources),
    )


# Main conversion functions
def train_run_attempt_to_proto(attempt: TrainRunAttempt) -> ProtoTrainRunAttempt:
    """Convert TrainRunAttempt to protobuf format."""
    proto_attempt = ProtoTrainRunAttempt(
        run_id=attempt.run_id,
        attempt_id=attempt.attempt_id,
        status=_RUN_ATTEMPT_STATUS_MAP[attempt.status],
        status_detail=attempt.status_detail,
        start_time=_ms_to_ns(attempt.start_time_ms),
        resources=[_to_proto_resources(r.resources) for r in attempt.resources],
        workers=[_to_proto_worker(w) for w in attempt.workers],
    )

    if attempt.end_time_ms is not None:
        proto_attempt.end_time.CopyFrom(_ms_to_ns(attempt.end_time_ms))

    return proto_attempt


def train_run_to_proto(run: TrainRun) -> ProtoTrainRun:
    """Convert TrainRun to protobuf format."""
    proto_run = ProtoTrainRun(
        id=run.id,
        name=run.name,
        job_id=bytes.from_hex(run.job_id),
        controller_actor_id=bytes.from_hex(run.controller_actor_id),
        status=_RUN_STATUS_MAP[run.status],
        status_detail=run.status_detail,
        start_time=_ms_to_ns(run.start_time_ms),
    )

    if run.end_time_ms is not None:
        proto_run.end_time.CopyFrom(_ms_to_ns(run.end_time_ms))

    return proto_run
