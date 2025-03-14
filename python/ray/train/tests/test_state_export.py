import uuid

import pytest
from ray.core.generated.export_train_state_pb2 import (
    ExportTrainRunEventData as ProtoTrainRun,
    ExportTrainRunAttemptEventData as ProtoTrainRunAttempt,
)
from ray.train._internal.state.schema import (
    ActorStatusEnum,
    RunStatusEnum,
    TrainRunInfo,
    TrainWorkerInfo,
)
from ray.train._internal.state.export import (
    train_run_info_to_proto_run,
    train_run_info_to_proto_attempt,
)


def create_mock_train_run_info(run_status=RunStatusEnum.RUNNING):
    """Create a minimal mock TrainRunInfo."""
    worker = TrainWorkerInfo(
        actor_id=uuid.uuid4().hex,
        world_rank=0,
        local_rank=0,
        node_rank=0,
        node_id=uuid.uuid4().hex,
        node_ip="127.0.0.1",
        pid=1234,
        gpu_ids=[0],
        status=ActorStatusEnum.ALIVE,
        resources={"CPU": 1},
        worker_log_file_path="/tmp/ray/session_xxx/logs/train/worker-0.err",
    )

    return TrainRunInfo(
        name="test_run",
        id=uuid.uuid4().hex,
        job_id=uuid.uuid4().hex,
        controller_actor_id=uuid.uuid4().hex,
        workers=[worker],
        datasets=[],
        run_status=run_status,
        status_detail="Error details" if run_status == RunStatusEnum.ERRORED else "",
        start_time_ms=1000,
        end_time_ms=2000
        if run_status in [RunStatusEnum.FINISHED, RunStatusEnum.ERRORED]
        else None,
        controller_log_file_path="/tmp/ray/session_xxx/logs/train/worker-controller.err",
        resources=[{"CPU": 1}],
    )


def test_train_run_info_to_proto_run():
    """Test that run info is correctly exported to proto run."""
    run_info = create_mock_train_run_info()
    proto_run = train_run_info_to_proto_run(run_info)

    assert proto_run.id == run_info.id
    assert proto_run.name == run_info.name
    assert proto_run.job_id == bytes.fromhex(run_info.job_id)
    assert proto_run.status == ProtoTrainRun.RunStatus.RUNNING
    assert proto_run.controller_log_file_path == run_info.controller_log_file_path


def test_train_run_info_to_proto_attempt():
    """Test that run info is correctly exported to proto attempt."""
    run_info = create_mock_train_run_info()
    proto_attempt = train_run_info_to_proto_attempt(run_info)

    assert proto_attempt.run_id == run_info.id
    assert proto_attempt.status == ProtoTrainRunAttempt.RunAttemptStatus.RUNNING

    # Verify worker fields
    assert len(proto_attempt.workers) == 1
    proto_worker = proto_attempt.workers[0]
    worker_info = run_info.workers[0]

    assert proto_worker.world_rank == worker_info.world_rank
    assert proto_worker.actor_id == bytes.fromhex(worker_info.actor_id)
    assert proto_worker.log_file_path == worker_info.worker_log_file_path


def test_status_mapping():
    """Test that status is correctly mapped between schemas."""
    status_pairs = [
        (
            RunStatusEnum.STARTED,
            ProtoTrainRun.RunStatus.INITIALIZING,
            ProtoTrainRunAttempt.RunAttemptStatus.PENDING,
        ),
        (
            RunStatusEnum.RUNNING,
            ProtoTrainRun.RunStatus.RUNNING,
            ProtoTrainRunAttempt.RunAttemptStatus.RUNNING,
        ),
        (
            RunStatusEnum.FINISHED,
            ProtoTrainRun.RunStatus.FINISHED,
            ProtoTrainRunAttempt.RunAttemptStatus.FINISHED,
        ),
        (
            RunStatusEnum.ERRORED,
            ProtoTrainRun.RunStatus.ERRORED,
            ProtoTrainRunAttempt.RunAttemptStatus.ERRORED,
        ),
        (
            RunStatusEnum.ABORTED,
            ProtoTrainRun.RunStatus.ABORTED,
            ProtoTrainRunAttempt.RunAttemptStatus.ABORTED,
        ),
    ]

    for run_status, expected_run_status, expected_attempt_status in status_pairs:
        run_info = create_mock_train_run_info(run_status=run_status)

        proto_run = train_run_info_to_proto_run(run_info)
        assert proto_run.status == expected_run_status

        proto_attempt = train_run_info_to_proto_attempt(run_info)
        assert proto_attempt.status == expected_attempt_status


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
