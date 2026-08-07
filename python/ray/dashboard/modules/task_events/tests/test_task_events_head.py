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
from ray.core.generated.common_pb2 import TaskStatus, TaskType
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


_HEAD = "ray.dashboard.modules.task_events.task_events_head"
_WORKER = b"worker_1"
_WORKER_2 = b"worker_2"


def _worker_table_data(worker_id: bytes, is_alive: bool) -> gcs_pb2.WorkerTableData:
    data = gcs_pb2.WorkerTableData(is_alive=is_alive, exit_detail="boom", end_time_ms=5)
    data.worker_address.worker_id = worker_id
    return data


def _add_stored_task(head, task_id, worker=None, job=_JOB):
    event = gcs_pb2.TaskEvents(task_id=task_id, attempt_number=0, job_id=job)
    event.task_info.type = TaskType.NORMAL_TASK
    if worker is not None:
        event.state_updates.worker_id = worker
    head._store.add_or_replace_task_event(event)


def _is_failed(head, task_id) -> bool:
    task_event = head._store.get_task_event((task_id, 0))
    return (
        task_event is not None
        and TaskStatus.FAILED in task_event.state_updates.state_ts_ns
    )


@pytest.mark.asyncio
async def test_handle_worker_delta_fails_tasks(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    head = _make_head()
    _add_stored_task(head, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        assert worker_id == _WORKER
        return gcs_pb2.WorkerTableData(exit_detail="boom", end_time_ms=5)

    monkeypatch.setattr(head, "_get_worker_info", fake_get)

    head._handle_worker_delta(gcs_pb2.WorkerDeltaData(worker_id=_WORKER))

    await async_wait_for_condition(lambda: _is_failed(head, _task_id(1)))


@pytest.mark.asyncio
async def test_handle_worker_delta_missing_worker_info_still_fails(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    head = _make_head()
    _add_stored_task(head, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        return None

    monkeypatch.setattr(head, "_get_worker_info", fake_get)

    head._handle_worker_delta(gcs_pb2.WorkerDeltaData(worker_id=_WORKER))

    # Even without worker table data, the task is failed — with a fallback message.
    await async_wait_for_condition(lambda: _is_failed(head, _task_id(1)))
    error = head._store.get_task_event((_task_id(1), 0)).state_updates.error_info
    assert "could not be fetched" in error.error_message


@pytest.mark.asyncio
async def test_get_worker_info_returns_none_on_rpc_failure():
    head = _make_head()

    stub = AsyncMock()
    stub.GetWorkerInfo.side_effect = RuntimeError("gcs unavailable")
    head._worker_info_stub = stub

    # A failing fetch must be swallowed (returns None) so the spawned reconciliation task
    # doesn't die silently; the caller then fails the tasks without exit details.
    assert await head._get_worker_info(_WORKER) is None


@pytest.mark.asyncio
async def test_handle_job_update_finished_fails_tasks(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    head = _make_head()
    _add_stored_task(head, _task_id(1))

    head._handle_job_update(gcs_pb2.JobTableData(job_id=_JOB, is_dead=True, end_time=5))

    await async_wait_for_condition(lambda: _is_failed(head, _task_id(1)))


@pytest.mark.asyncio
async def test_handle_job_update_ignores_running_job():
    head = _make_head()
    _add_stored_task(head, _task_id(1))

    head._handle_job_update(gcs_pb2.JobTableData(job_id=_JOB, is_dead=False))
    await asyncio.sleep(0.05)

    assert not _is_failed(head, _task_id(1))


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
async def test_worker_death_subscription_loop_reconciles(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    head = _make_head()
    _add_stored_task(head, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        return gcs_pb2.WorkerTableData(exit_detail="boom", end_time_ms=5)

    monkeypatch.setattr(head, "_get_worker_info", fake_get)

    # Isolate the subscription path from the startup backfill (no real GCS here).
    async def no_backfill():
        return []

    monkeypatch.setattr(head, "_get_all_worker_info", no_backfill)

    fake = _FakeSubscriber([(b"key", gcs_pb2.WorkerDeltaData(worker_id=_WORKER))])
    monkeypatch.setattr(
        f"{_HEAD}.GcsAioWorkerDeltaSubscriber", lambda address=None: fake
    )

    task = asyncio.create_task(head._subscribe_for_worker_deaths())
    try:
        await async_wait_for_condition(lambda: _is_failed(head, _task_id(1)))
    finally:
        await _cancel(task)


@pytest.mark.asyncio
async def test_reconcile_dead_workers_on_startup(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    head = _make_head()
    _add_stored_task(head, _task_id(1), worker=_WORKER)
    _add_stored_task(head, _task_id(2), worker=_WORKER_2)

    async def fake_get_all():
        return [
            _worker_table_data(_WORKER, is_alive=False),
            _worker_table_data(_WORKER_2, is_alive=True),
        ]

    monkeypatch.setattr(head, "_get_all_worker_info", fake_get_all)

    await head._reconcile_dead_workers_on_startup()

    # The dead worker's task is failed; the live worker's task is left alone.
    assert _is_failed(head, _task_id(1))
    assert not _is_failed(head, _task_id(2))


@pytest.mark.asyncio
async def test_reconcile_finished_jobs_on_startup(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    head = _make_head()
    finished_job = JobID.from_int(1).binary()
    running_job = JobID.from_int(2).binary()
    _add_stored_task(head, _task_id(1), job=finished_job)
    _add_stored_task(head, _task_id(2), job=running_job)

    async def fake_get_all():
        return [
            gcs_pb2.JobTableData(job_id=finished_job, is_dead=True),
            gcs_pb2.JobTableData(job_id=running_job, is_dead=False),
        ]

    monkeypatch.setattr(head, "_get_all_job_info", fake_get_all)

    await head._reconcile_finished_jobs_on_startup()

    # The finished job's task is failed; the running job's task is left alone.
    assert _is_failed(head, _task_id(1))
    assert not _is_failed(head, _task_id(2))


@pytest.mark.asyncio
async def test_job_subscription_loop_reconciles_only_finished(monkeypatch):
    monkeypatch.setattr(f"{_HEAD}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    head = _make_head()
    running_job = JobID.from_int(1).binary()
    finished_job = JobID.from_int(2).binary()
    _add_stored_task(head, _task_id(1), job=running_job)
    _add_stored_task(head, _task_id(2), job=finished_job)

    fake = _FakeSubscriber(
        [
            (b"k1", gcs_pb2.JobTableData(job_id=running_job, is_dead=False)),
            (
                b"k2",
                gcs_pb2.JobTableData(job_id=finished_job, is_dead=True, end_time=5),
            ),
        ]
    )
    monkeypatch.setattr(f"{_HEAD}.GcsAioJobSubscriber", lambda address=None: fake)

    task = asyncio.create_task(head._subscribe_for_finished_jobs())
    try:
        await async_wait_for_condition(lambda: _is_failed(head, _task_id(2)))
    finally:
        await _cancel(task)

    # The running job was ignored; its task is not failed.
    assert not _is_failed(head, _task_id(1))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
