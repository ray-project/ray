import asyncio
import sys
from unittest.mock import AsyncMock

import pytest

from ray._common.test_utils import async_wait_for_condition
from ray._raylet import JobID
from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import TaskStatus, TaskType
from ray.dashboard.modules.task_events.task_event_manager import TaskEventManager
from ray.dashboard.modules.task_events.task_event_storage import TaskEventStorage

_MANAGER = "ray.dashboard.modules.task_events.task_event_manager"
_JOB = JobID.from_int(1).binary()
_WORKER = b"worker_1"
_WORKER_2 = b"worker_2"


def _worker_table_data(worker_id: bytes, is_alive: bool) -> gcs_pb2.WorkerTableData:
    data = gcs_pb2.WorkerTableData(is_alive=is_alive, exit_detail="boom", end_time_ms=5)
    data.worker_address.worker_id = worker_id
    return data


def _task_id(n: int) -> bytes:
    return bytes([n]) * 24


def _make_manager() -> TaskEventManager:
    return TaskEventManager(TaskEventStorage(), "127.0.0.1:6379", None)


def _add_stored_task(manager, task_id, worker=None, job=_JOB):
    event = gcs_pb2.TaskEvents(task_id=task_id, attempt_number=0, job_id=job)
    event.task_info.type = TaskType.NORMAL_TASK
    if worker is not None:
        event.state_updates.worker_id = worker
    manager._store.add_or_replace_task_event(event)


def _is_failed(manager, task_id) -> bool:
    task_event = manager._store.get_task_event((task_id, 0))
    return (
        task_event is not None
        and TaskStatus.FAILED in task_event.state_updates.state_ts_ns
    )


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
async def test_handle_worker_delta_fails_tasks(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        assert worker_id == _WORKER
        return gcs_pb2.WorkerTableData(exit_detail="boom", end_time_ms=5)

    monkeypatch.setattr(manager, "_get_worker_info", fake_get)

    manager._handle_worker_delta(gcs_pb2.WorkerDeltaData(worker_id=_WORKER))

    await async_wait_for_condition(lambda: _is_failed(manager, _task_id(1)))


@pytest.mark.asyncio
async def test_handle_worker_delta_missing_worker_info_still_fails(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        return None

    monkeypatch.setattr(manager, "_get_worker_info", fake_get)

    manager._handle_worker_delta(gcs_pb2.WorkerDeltaData(worker_id=_WORKER))

    # Even without worker table data, the task is failed — with a fallback message.
    await async_wait_for_condition(lambda: _is_failed(manager, _task_id(1)))
    error = manager._store.get_task_event((_task_id(1), 0)).state_updates.error_info
    assert "could not be fetched" in error.error_message


@pytest.mark.asyncio
async def test_get_worker_info_returns_none_on_rpc_failure():
    manager = _make_manager()

    stub = AsyncMock()
    stub.GetWorkerInfo.side_effect = RuntimeError("gcs unavailable")
    manager._worker_info_stub = stub

    # A failing fetch must be swallowed (returns None) so the spawned reconciliation task
    # doesn't die silently; the caller then fails the tasks without exit details.
    assert await manager._get_worker_info(_WORKER) is None


@pytest.mark.asyncio
async def test_handle_job_update_finished_fails_tasks(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1))

    manager._handle_job_update(
        gcs_pb2.JobTableData(job_id=_JOB, is_dead=True, end_time=5)
    )

    await async_wait_for_condition(lambda: _is_failed(manager, _task_id(1)))


@pytest.mark.asyncio
async def test_handle_job_update_ignores_running_job():
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1))

    manager._handle_job_update(gcs_pb2.JobTableData(job_id=_JOB, is_dead=False))
    await asyncio.sleep(0.05)

    assert not _is_failed(manager, _task_id(1))


@pytest.mark.asyncio
async def test_gc_job_summary_loop_trims_over_cap(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}.GC_JOB_SUMMARY_INTERVAL_S", 0.01)
    monkeypatch.setattr(
        "ray.dashboard.modules.task_events.task_event_storage."
        "MAX_DROPPED_TASK_ATTEMPTS_PER_JOB",
        2,
    )
    manager = _make_manager()
    summary = manager._store._summary(_JOB)
    for i in range(10):
        summary.record_task_attempt_dropped((_task_id(i), 0))
    assert len(summary._dropped_task_attempts) == 10

    task = asyncio.create_task(manager._gc_job_summary_loop())
    try:
        await async_wait_for_condition(lambda: len(summary._dropped_task_attempts) < 10)
    finally:
        await _cancel(task)


@pytest.mark.asyncio
async def test_worker_death_subscription_loop_reconciles(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        return gcs_pb2.WorkerTableData(exit_detail="boom", end_time_ms=5)

    monkeypatch.setattr(manager, "_get_worker_info", fake_get)

    # Isolate the subscription path from the startup backfill (no real GCS here).
    async def no_backfill():
        return []

    monkeypatch.setattr(manager, "_get_all_worker_info", no_backfill)

    fake = _FakeSubscriber([(b"key", gcs_pb2.WorkerDeltaData(worker_id=_WORKER))])
    monkeypatch.setattr(
        f"{_MANAGER}.GcsAioWorkerDeltaSubscriber", lambda address=None: fake
    )

    task = asyncio.create_task(manager._subscribe_for_worker_deaths())
    try:
        await async_wait_for_condition(lambda: _is_failed(manager, _task_id(1)))
    finally:
        await _cancel(task)


@pytest.mark.asyncio
async def test_reconcile_dead_workers_on_startup(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1), worker=_WORKER)
    _add_stored_task(manager, _task_id(2), worker=_WORKER_2)

    async def fake_get_all():
        return [
            _worker_table_data(_WORKER, is_alive=False),
            _worker_table_data(_WORKER_2, is_alive=True),
        ]

    monkeypatch.setattr(manager, "_get_all_worker_info", fake_get_all)

    await manager._reconcile_dead_workers_on_startup()

    # The dead worker's task is failed; the live worker's task is left alone.
    assert _is_failed(manager, _task_id(1))
    assert not _is_failed(manager, _task_id(2))


@pytest.mark.asyncio
async def test_reconcile_finished_jobs_on_startup(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    manager = _make_manager()
    finished_job = JobID.from_int(1).binary()
    running_job = JobID.from_int(2).binary()
    _add_stored_task(manager, _task_id(1), job=finished_job)
    _add_stored_task(manager, _task_id(2), job=running_job)

    async def fake_get_all():
        return [
            gcs_pb2.JobTableData(job_id=finished_job, is_dead=True),
            gcs_pb2.JobTableData(job_id=running_job, is_dead=False),
        ]

    monkeypatch.setattr(manager, "_get_all_job_info", fake_get_all)

    await manager._reconcile_finished_jobs_on_startup()

    # The finished job's task is failed; the running job's task is left alone.
    assert _is_failed(manager, _task_id(1))
    assert not _is_failed(manager, _task_id(2))


@pytest.mark.asyncio
async def test_job_subscription_loop_reconciles_only_finished(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    manager = _make_manager()
    running_job = JobID.from_int(1).binary()
    finished_job = JobID.from_int(2).binary()
    _add_stored_task(manager, _task_id(1), job=running_job)
    _add_stored_task(manager, _task_id(2), job=finished_job)

    fake = _FakeSubscriber(
        [
            (b"k1", gcs_pb2.JobTableData(job_id=running_job, is_dead=False)),
            (
                b"k2",
                gcs_pb2.JobTableData(job_id=finished_job, is_dead=True, end_time=5),
            ),
        ]
    )
    monkeypatch.setattr(f"{_MANAGER}.GcsAioJobSubscriber", lambda address=None: fake)

    task = asyncio.create_task(manager._subscribe_for_finished_jobs())
    try:
        await async_wait_for_condition(lambda: _is_failed(manager, _task_id(2)))
    finally:
        await _cancel(task)

    # The running job was ignored; its task is not failed.
    assert not _is_failed(manager, _task_id(1))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
