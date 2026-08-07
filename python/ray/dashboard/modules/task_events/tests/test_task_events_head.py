import asyncio
import sys
from unittest.mock import AsyncMock

import pytest

import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
from ray._common.ray_constants import (
    LOGGING_ROTATE_BACKUP_COUNT,
    LOGGING_ROTATE_BYTES,
)
from ray._common.test_utils import async_wait_for_condition
from ray._raylet import JobID
from ray.core.generated import events_event_aggregator_service_pb2, gcs_pb2
from ray.core.generated.common_pb2 import TaskType
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_task_definition_event_pb2 import TaskDefinitionEvent
from ray.dashboard.modules.task_events.task_events_head import TaskEventsHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig

_JOB = JobID.from_int(1).binary()


def _task_id(n: int) -> bytes:
    return bytes([n]) * 24


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


def _make_add_events_request(num_events: int, start: int = 0) -> bytes:
    events_data = events_event_aggregator_service_pb2.RayEventsData()
    for i in range(start, start + num_events):
        events_data.events.append(
            RayEvent(
                event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                task_definition_event=TaskDefinitionEvent(
                    task_id=_task_id(i),
                    task_attempt=0,
                    job_id=_JOB,
                    task_type=TaskType.NORMAL_TASK,
                    task_name=f"task_{i}",
                ),
            )
        )
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
    assert head.num_task_events_stored == 0

    body = _make_add_events_request(num_events=2)
    response = await head.add_task_events(_fake_request(body))

    assert response.status == 200
    assert head.num_task_events_stored == 2


@pytest.mark.asyncio
async def test_add_task_events_accumulates_across_requests():
    head = _make_head()

    await head.add_task_events(_fake_request(_make_add_events_request(2, start=0)))
    await head.add_task_events(_fake_request(_make_add_events_request(3, start=2)))

    assert head.num_task_events_stored == 5


@pytest.mark.asyncio
async def test_add_task_events_empty_payload():
    head = _make_head()

    response = await head.add_task_events(_fake_request(_make_add_events_request(0)))

    assert response.status == 200
    assert head.num_task_events_stored == 0


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
    assert head.num_task_events_stored == 0


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


class _FakeSubscriber:
    """Delivers a canned batch on the first poll, then parks so the loop settles."""

    def __init__(self, messages):
        self._messages = messages
        self._delivered = False

    async def subscribe(self):
        pass

    async def poll(self, batch_size, timeout=None):
        if self._delivered:
            await asyncio.sleep(3600)
            return []
        self._delivered = True
        return self._messages


async def _cancel(task: asyncio.Task) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_gc_job_summary_loop_trims_over_cap(monkeypatch):
    monkeypatch.setattr(
        "ray.dashboard.modules.task_events.task_events_head.GC_JOB_SUMMARY_INTERVAL_S",
        0.01,
    )
    monkeypatch.setattr(
        "ray.dashboard.modules.task_events.task_event_storage."
        "MAX_DROPPED_TASK_ATTEMPTS_PER_JOB",
        2,
    )
    head = _make_head()
    summary = head._store._summary(_JOB)
    for i in range(10):
        summary.record_task_attempt_dropped((_task_id(i), 0))
    assert len(summary._dropped_task_attempts) == 10

    task = asyncio.create_task(head._gc_job_summary_loop())
    try:
        await async_wait_for_condition(lambda: len(summary._dropped_task_attempts) < 10)
    finally:
        await _cancel(task)


@pytest.mark.asyncio
async def test_worker_death_subscription_loop_buffers(monkeypatch):
    head = _make_head()
    worker_delta = gcs_pb2.WorkerDeltaData(worker_id=b"worker_1", node_id=b"node_1")
    fake = _FakeSubscriber([(b"key", worker_delta)])
    monkeypatch.setattr(
        "ray.dashboard.modules.task_events.task_events_head."
        "GcsAioWorkerDeltaSubscriber",
        lambda address=None: fake,
    )

    task = asyncio.create_task(head._subscribe_for_worker_deaths())
    try:
        await async_wait_for_condition(lambda: head.num_dead_workers_received == 1)
    finally:
        await _cancel(task)


@pytest.mark.asyncio
async def test_job_subscription_loop_buffers_only_finished(monkeypatch):
    head = _make_head()
    running = gcs_pb2.JobTableData(job_id=b"job_1", is_dead=False)
    finished = gcs_pb2.JobTableData(job_id=b"job_2", is_dead=True)
    fake = _FakeSubscriber([(b"k1", running), (b"k2", finished)])
    monkeypatch.setattr(
        "ray.dashboard.modules.task_events.task_events_head.GcsAioJobSubscriber",
        lambda address=None: fake,
    )

    task = asyncio.create_task(head._subscribe_for_finished_jobs())
    try:
        await async_wait_for_condition(lambda: head.num_finished_jobs_received == 1)
    finally:
        await _cancel(task)

    # The running job was filtered out; only the finished one was buffered.
    assert head.num_finished_jobs_received == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
