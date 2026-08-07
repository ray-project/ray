import sys
from unittest.mock import AsyncMock

import pytest

import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
from ray._common.ray_constants import (
    LOGGING_ROTATE_BACKUP_COUNT,
    LOGGING_ROTATE_BYTES,
)
from ray.core.generated import events_event_aggregator_service_pb2, gcs_pb2
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.dashboard.modules.task_events.task_events_head import TaskEventsHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig


def _make_config() -> SubprocessModuleConfig:
    return SubprocessModuleConfig(
        cluster_id_hex="deadbeef",
        gcs_address="127.0.0.1:6379",
        session_name="test_session",
        temp_dir="/tmp",
        session_dir="/tmp",
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir="/tmp",
        logging_filename=dashboard_consts.DASHBOARD_LOG_FILENAME,
        logging_rotate_bytes=LOGGING_ROTATE_BYTES,
        logging_rotate_backup_count=LOGGING_ROTATE_BACKUP_COUNT,
        socket_dir="/tmp",
    )


def _make_head() -> TaskEventsHead:
    return TaskEventsHead(_make_config())


def _make_add_events_request(num_events: int) -> bytes:
    events_data = events_event_aggregator_service_pb2.RayEventsData()
    for i in range(num_events):
        events_data.events.append(RayEvent(event_id=f"event_{i}".encode()))
    request = events_event_aggregator_service_pb2.AddEventsRequest(
        events_data=events_data
    )
    return request.SerializeToString()


def _fake_request(body: bytes):
    request = AsyncMock()
    request.read.return_value = body
    return request


@pytest.mark.asyncio
async def test_add_task_events_buffers_events():
    head = _make_head()
    assert head.num_events_received == 0

    body = _make_add_events_request(num_events=2)
    response = await head.add_task_events(_fake_request(body))

    assert response.status == 200
    assert head.num_events_received == 2


@pytest.mark.asyncio
async def test_add_task_events_accumulates_across_requests():
    head = _make_head()

    await head.add_task_events(_fake_request(_make_add_events_request(num_events=2)))
    await head.add_task_events(_fake_request(_make_add_events_request(num_events=3)))

    assert head.num_events_received == 5


@pytest.mark.asyncio
async def test_add_task_events_empty_payload():
    head = _make_head()

    response = await head.add_task_events(_fake_request(_make_add_events_request(0)))

    assert response.status == 200
    assert head.num_events_received == 0


@pytest.mark.asyncio
async def test_add_task_events_bad_payload_returns_error():
    head = _make_head()

    # A lone continuation byte is a truncated protobuf tag varint and reliably
    # fails to parse, unlike arbitrary ASCII which protobuf may accept as
    # unknown fields.
    response = await head.add_task_events(_fake_request(b"\xff"))

    # Malformed body must not raise; the handler returns an error response and
    # nothing is buffered.
    assert response.status != 200
    assert head.num_events_received == 0


def test_deserialize_request_roundtrip():
    head = _make_head()

    body = _make_add_events_request(num_events=1)
    request = head._deserialize_request(body)

    assert len(request.events_data.events) == 1


def test_handle_worker_delta_buffers():
    head = _make_head()
    assert head.num_dead_workers_received == 0

    head._handle_worker_delta(
        gcs_pb2.WorkerDeltaData(worker_id=b"worker_1", node_id=b"node_1")
    )

    assert head.num_dead_workers_received == 1


def test_handle_job_update_buffers_finished_job():
    head = _make_head()

    head._handle_job_update(
        gcs_pb2.JobTableData(job_id=b"job_1", is_dead=True, end_time=123)
    )

    assert head.num_finished_jobs_received == 1


def test_handle_job_update_ignores_running_job():
    head = _make_head()

    head._handle_job_update(gcs_pb2.JobTableData(job_id=b"job_1", is_dead=False))

    assert head.num_finished_jobs_received == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
