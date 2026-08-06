import asyncio
import sys

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
async def test_handle_worker_delta_missing_worker_info_is_noop(monkeypatch):
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    manager = _make_manager()
    _add_stored_task(manager, _task_id(1), worker=_WORKER)

    async def fake_get(worker_id):
        return None

    monkeypatch.setattr(manager, "_get_worker_info", fake_get)

    manager._handle_worker_delta(gcs_pb2.WorkerDeltaData(worker_id=_WORKER))
    await asyncio.sleep(0.05)

    assert not _is_failed(manager, _task_id(1))


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
