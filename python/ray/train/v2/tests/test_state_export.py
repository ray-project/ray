import json
import os
import time
import uuid

import pytest
import ray

from ray.train.v2._internal.state.schema import (
    ActorStatus,
    RunAttemptStatus,
    RunStatus,
    TrainResources,
    TrainRun,
    TrainRunAttempt,
    TrainWorker,
)
from ray.train.v2._internal.state.state_actor import get_or_create_state_actor


@pytest.fixture
def shutdown_only():
    yield
    ray.shutdown()


_RUN_ID = "mock_run_id"


def _create_mock_train_run(status: RunStatus = RunStatus.RUNNING):
    return TrainRun(
        schema_version=0,
        id=_RUN_ID,
        name="test_run",
        job_id=uuid.uuid4().hex,
        controller_actor_id=uuid.uuid4().hex,
        status=status,
        status_detail=None,
        start_time_ns=time.time_ns(),
        controller_log_file_path="/tmp/ray/session_xxx/logs/train/ray-train-app-controller.log",
    )


def _create_mock_train_run_attempt(
    attempt_id: str = "mock_attempt_id",
    status: RunAttemptStatus = RunAttemptStatus.RUNNING,
):
    worker = TrainWorker(
        world_rank=0,
        local_rank=0,
        node_rank=0,
        actor_id=uuid.uuid4().hex,
        node_id=uuid.uuid4().hex,
        node_ip="127.0.0.1",
        pid=1234,
        gpu_ids=[0],
        status=ActorStatus.ALIVE,
        resources=TrainResources(resources={"CPU": 1}),
        log_file_path="/tmp/ray/session_xxx/logs/train/ray-train-app-worker.log",
    )

    return TrainRunAttempt(
        schema_version=0,
        run_id=_RUN_ID,
        attempt_id=attempt_id,
        status=status,
        status_detail=None,
        start_time_ns=time.time_ns(),
        resources=[TrainResources(resources={"CPU": 1})],
        workers=[worker],
    )


def _get_export_file_path() -> str:
    return os.path.join(
        ray._private.worker._global_node.get_session_dir_path(),
        "logs",
        "export_events",
        "event_EXPORT_TRAIN_STATE.log",
    )


def _get_exported_data():
    exported_file = _get_export_file_path()
    assert os.path.isfile(exported_file)

    with open(exported_file, "r") as f:
        data = f.readlines()

    return [json.loads(line) for line in data]


@pytest.fixture
def enable_export_api_config(shutdown_only):
    """Enable export API for the EXPORT_TRAIN_RUN resource type."""
    ray.init(
        num_cpus=4,
        runtime_env={
            "env_vars": {"RAY_enable_export_api_write_config": "EXPORT_TRAIN_RUN"}
        },
    )
    yield


@pytest.fixture
def enable_export_api_write(shutdown_only):
    """Enable export API for all resource types."""
    ray.init(
        num_cpus=4,
        runtime_env={"env_vars": {"RAY_enable_export_api_write": "1"}},
    )
    yield


def test_export_disabled(ray_start_4_cpus):
    """Test that no export files are created when export API is disabled."""
    state_actor = get_or_create_state_actor()

    # Create or update train run
    ray.get(state_actor.create_or_update_train_run.remote(_create_mock_train_run()))
    ray.get(
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt()
        )
    )

    # Check that no export files were created
    assert not os.path.exists(_get_export_file_path())


def _test_train_run_export():
    """Test that train run export events are written when export API is enabled."""
    state_actor = get_or_create_state_actor()

    # Create or update train run
    ray.get(
        state_actor.create_or_update_train_run.remote(
            _create_mock_train_run(RunStatus.RUNNING)
        )
    )

    # Check that export files were created
    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["source_type"] == "EXPORT_TRAIN_RUN"
    assert data[0]["event_data"]["status"] == "RUNNING"


def test_export_train_run_enabled_by_config(enable_export_api_config):
    _test_train_run_export()


def test_export_train_run(enable_export_api_write):
    _test_train_run_export()


def test_export_train_run_attempt(enable_export_api_write):
    """Test that train run attempt export events are written when export API is enabled."""
    state_actor = get_or_create_state_actor()

    # Create or update train run attempt
    ray.get(
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt(RunAttemptStatus.RUNNING)
        )
    )

    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["source_type"] == "EXPORT_TRAIN_RUN_ATTEMPT"
    assert data[0]["event_data"]["status"] == "RUNNING"


def test_export_multiple_source_types(enable_export_api_write):
    """Test that multiple source types (Run and RunAttempt) can be written to the same file."""
    state_actor = get_or_create_state_actor()

    events = [
        state_actor.create_or_update_train_run.remote(
            _create_mock_train_run(RunStatus.RUNNING)
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt(
                attempt_id="attempt_1", status=RunAttemptStatus.RUNNING
            )
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt(
                attempt_id="attempt_2", status=RunAttemptStatus.RUNNING
            )
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt(
                attempt_id="attempt_1", status=RunAttemptStatus.FINISHED
            )
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt(
                attempt_id="attempt_2", status=RunAttemptStatus.FINISHED
            )
        ),
        state_actor.create_or_update_train_run.remote(
            _create_mock_train_run(RunStatus.FINISHED)
        ),
    ]
    ray.get(events)

    data = _get_exported_data()
    assert len(data) == len(events)

    expected_source_types = (
        ["EXPORT_TRAIN_RUN"] + ["EXPORT_TRAIN_RUN_ATTEMPT"] * 4 + ["EXPORT_TRAIN_RUN"]
    )
    expected_statuses = ["RUNNING"] * 3 + ["FINISHED"] * 3

    assert [d["source_type"] for d in data] == expected_source_types
    assert [d["event_data"]["status"] for d in data] == expected_statuses


def test_export_optional_fields(enable_export_api_write):
    """Test that optional fields are correctly exported when present and absent."""
    state_actor = get_or_create_state_actor()

    # Create run with optional fields
    run_with_optional = _create_mock_train_run(RunStatus.FINISHED)
    run_with_optional.status_detail = "Finished with details"
    run_with_optional.end_time_ns = 1000000000000000000

    # Create attempt with optional fields
    attempt_with_optional = _create_mock_train_run_attempt(
        attempt_id="attempt_with_optional",
        status=RunAttemptStatus.FINISHED,
    )
    attempt_with_optional.status_detail = "Attempt details"
    attempt_with_optional.end_time_ns = 1000000000000000000

    # Create and update states
    events = [
        state_actor.create_or_update_train_run.remote(_create_mock_train_run()),
        state_actor.create_or_update_train_run_attempt.remote(
            _create_mock_train_run_attempt()
        ),
        state_actor.create_or_update_train_run.remote(run_with_optional),
        state_actor.create_or_update_train_run_attempt.remote(attempt_with_optional),
    ]
    ray.get(events)

    data = _get_exported_data()
    assert len(data) == 4

    # Verify run without optional fields
    run_data = data[0]
    assert run_data["source_type"] == "EXPORT_TRAIN_RUN"
    assert "status_detail" not in run_data["event_data"]
    assert "end_time_ns" not in run_data["event_data"]

    # Verify attempt without optional fields
    attempt_data = data[1]
    assert attempt_data["source_type"] == "EXPORT_TRAIN_RUN_ATTEMPT"
    assert "status_detail" not in attempt_data["event_data"]
    assert "end_time_ns" not in attempt_data["event_data"]

    # Verify run with optional fields
    run_data = data[2]
    assert run_data["source_type"] == "EXPORT_TRAIN_RUN"
    assert run_data["event_data"]["status_detail"] == "Finished with details"
    assert "end_time_ns" in run_data["event_data"]

    # Verify attempt with optional fields
    attempt_data = data[3]
    assert attempt_data["source_type"] == "EXPORT_TRAIN_RUN_ATTEMPT"
    assert attempt_data["event_data"]["status_detail"] == "Attempt details"
    assert "end_time_ns" in attempt_data["event_data"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
