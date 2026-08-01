from typing import Optional

from ray.core.generated.export_train_state_pb2 import (
    ExportTrainRunAttemptEventData as ProtoTrainRunAttempt,
    ExportTrainRunEventData as ProtoTrainRun,
)
from ray.train._internal.state.schema import (
    ActorStatusEnum,
    RunStatusEnum,
    TrainRunInfo,
    TrainWorkerInfo,
)

TRAIN_SCHEMA_VERSION = 1
RAY_TRAIN_VERSION = 1

# Status mapping dictionaries
_ACTOR_STATUS_MAP = {
    ActorStatusEnum.ALIVE: ProtoTrainRunAttempt.ActorStatus.ALIVE,
    ActorStatusEnum.DEAD: ProtoTrainRunAttempt.ActorStatus.DEAD,
}

_RUN_ATTEMPT_STATUS_MAP = {
    RunStatusEnum.STARTED: ProtoTrainRunAttempt.RunAttemptStatus.PENDING,
    RunStatusEnum.RUNNING: ProtoTrainRunAttempt.RunAttemptStatus.RUNNING,
    RunStatusEnum.FINISHED: ProtoTrainRunAttempt.RunAttemptStatus.FINISHED,
    RunStatusEnum.ERRORED: ProtoTrainRunAttempt.RunAttemptStatus.ERRORED,
    RunStatusEnum.ABORTED: ProtoTrainRunAttempt.RunAttemptStatus.ABORTED,
}

_RUN_STATUS_MAP = {
    RunStatusEnum.STARTED: ProtoTrainRun.RunStatus.INITIALIZING,
    RunStatusEnum.RUNNING: ProtoTrainRun.RunStatus.RUNNING,
    RunStatusEnum.FINISHED: ProtoTrainRun.RunStatus.FINISHED,
    RunStatusEnum.ERRORED: ProtoTrainRun.RunStatus.ERRORED,
    RunStatusEnum.ABORTED: ProtoTrainRun.RunStatus.ABORTED,
}


def _ms_to_ns(ms: Optional[int]) -> Optional[int]:
    if ms is None:
        return None
    return ms * 1000000


# Helper conversion functions
def _to_proto_resources(resources: dict) -> ProtoTrainRunAttempt.TrainResources:
    """Convert resources dictionary to protobuf TrainResources."""
    return ProtoTrainRunAttempt.TrainResources(resources=resources)


def _to_proto_worker(worker: TrainWorkerInfo) -> ProtoTrainRunAttempt.TrainWorker:
    """Convert TrainWorker to protobuf format."""
    proto_worker = ProtoTrainRunAttempt.TrainWorker(
        world_rank=worker.world_rank,
        local_rank=worker.local_rank,
        node_rank=worker.node_rank,
        actor_id=bytes.fromhex(worker.actor_id),
        node_id=bytes.fromhex(worker.node_id),
        node_ip=worker.node_ip,
        pid=worker.pid,
        gpu_ids=worker.gpu_ids,
        status=_ACTOR_STATUS_MAP[worker.status],
        resources=_to_proto_resources(worker.resources),
    )

    return proto_worker


# Main conversion functions
def train_run_info_to_proto_run(run_info: TrainRunInfo) -> ProtoTrainRun:
    """Convert TrainRunInfo to TrainRun protobuf format."""
    proto_run = ProtoTrainRun(
        schema_version=TRAIN_SCHEMA_VERSION,
        ray_train_version=RAY_TRAIN_VERSION,
        id=run_info.id,
        name=run_info.name,
        job_id=bytes.fromhex(run_info.job_id),
        controller_actor_id=bytes.fromhex(run_info.controller_actor_id),
        status=_RUN_STATUS_MAP[run_info.run_status],
        status_detail=run_info.status_detail,
        start_time_ns=_ms_to_ns(run_info.start_time_ms),
        end_time_ns=_ms_to_ns(run_info.end_time_ms),
    )

    return proto_run


def train_run_info_to_proto_attempt(run_info: TrainRunInfo) -> ProtoTrainRunAttempt:
    """Convert TrainRunInfo to TrainRunAttempt protobuf format."""

    proto_attempt = ProtoTrainRunAttempt(
        schema_version=TRAIN_SCHEMA_VERSION,
        ray_train_version=RAY_TRAIN_VERSION,
        run_id=run_info.id,
        attempt_id=run_info.id,  # Same as run_id
        status=_RUN_ATTEMPT_STATUS_MAP[run_info.run_status],
        status_detail=run_info.status_detail,
        start_time_ns=_ms_to_ns(run_info.start_time_ms),
        end_time_ns=_ms_to_ns(run_info.end_time_ms),
        resources=[_to_proto_resources(r) for r in run_info.resources],
        workers=[_to_proto_worker(worker) for worker in run_info.workers],
    )

    return proto_attempt
