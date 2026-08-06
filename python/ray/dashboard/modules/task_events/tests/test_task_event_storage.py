import sys

import pytest

from ray._raylet import JobID, TaskID
from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import ErrorType, TaskAttempt, TaskStatus, TaskType
from ray.dashboard.modules.task_events import task_event_storage as tes

_JOB = JobID.from_int(1).binary()


def _task_id(n: int) -> bytes:
    return bytes([n]) * 24


def _event(
    task_id, attempt=0, task_type=None, finished=False, name="", worker=None
) -> gcs_pb2.TaskEvents:
    event = gcs_pb2.TaskEvents(task_id=task_id, attempt_number=attempt, job_id=_JOB)
    if task_type is not None:
        event.task_info.type = task_type
        event.task_info.name = name
    if finished:
        event.state_updates.state_ts_ns[TaskStatus.FINISHED] = 1
    if worker is not None:
        event.state_updates.worker_id = worker
    return event


def test_add_and_merge_dedups_by_attempt():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(
        _event(_task_id(1), task_type=TaskType.NORMAL_TASK, name="foo")
    )
    store.add_or_replace_task_event(_event(_task_id(1), finished=True))

    assert store.num_task_events_stored == 1
    stored = store.get_task_event((_task_id(1), 0))
    assert stored.task_info.name == "foo"
    assert TaskStatus.FINISHED in stored.state_updates.state_ts_ns
    assert store.stats[tes.STAT_TOTAL_REPORTED] == 2


def test_task_type_counters_increment_once():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(_event(_task_id(1), task_type=TaskType.NORMAL_TASK))
    store.add_or_replace_task_event(_event(_task_id(2), task_type=TaskType.ACTOR_TASK))

    assert store.stats[tes._TASK_TYPE_STAT[TaskType.NORMAL_TASK]] == 1
    assert store.stats[tes._TASK_TYPE_STAT[TaskType.ACTOR_TASK]] == 1


def test_eviction_prefers_lower_priority_tier():
    store = tes.TaskEventStorage(max_num_task_events=2)
    store.add_or_replace_task_event(_event(_task_id(1), task_type=TaskType.NORMAL_TASK))
    store.add_or_replace_task_event(_event(_task_id(2), task_type=TaskType.ACTOR_TASK))
    store.add_or_replace_task_event(_event(_task_id(3), finished=True))

    assert store.num_task_events_stored == 2
    assert store.get_task_event((_task_id(3), 0)) is None
    assert store.get_task_event((_task_id(1), 0)) is not None
    assert store.get_task_event((_task_id(2), 0)) is not None
    assert store.stats[tes.STAT_TOTAL_ATTEMPTS_DROPPED] == 1


def test_evicted_attempt_is_rejected():
    store = tes.TaskEventStorage(max_num_task_events=1)
    store.add_or_replace_task_event(_event(_task_id(1), task_type=TaskType.NORMAL_TASK))
    store.add_or_replace_task_event(_event(_task_id(2), finished=True))

    # The finished attempt was the lowest tier, so it was evicted; re-adding is rejected.
    assert store.get_task_event((_task_id(2), 0)) is None
    store.add_or_replace_task_event(_event(_task_id(2), finished=True))
    assert store.get_task_event((_task_id(2), 0)) is None


def test_profile_events_truncated_fifo(monkeypatch):
    monkeypatch.setattr(tes, "MAX_NUM_PROFILE_EVENTS_PER_TASK", 2)
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(_event(_task_id(1), task_type=TaskType.NORMAL_TASK))

    update = gcs_pb2.TaskEvents(task_id=_task_id(1), attempt_number=0, job_id=_JOB)
    for i in range(4):
        update.profile_events.events.add().event_name = f"e{i}"
    store.add_or_replace_task_event(update)

    kept = store.get_task_event((_task_id(1), 0)).profile_events.events
    assert [e.event_name for e in kept] == ["e2", "e3"]
    assert store.stats[tes.STAT_TOTAL_PROFILE_DROPPED] == 2


def test_record_data_loss_evicts_and_rejects():
    store = tes.TaskEventStorage()
    task_id = TaskID.for_fake_task(JobID.from_int(1)).binary()
    store.add_or_replace_task_event(_event(task_id, task_type=TaskType.NORMAL_TASK))

    store.record_data_loss_from_worker([TaskAttempt(task_id=task_id, attempt_number=0)])
    assert store.get_task_event((task_id, 0)) is None

    store.add_or_replace_task_event(_event(task_id, task_type=TaskType.NORMAL_TASK))
    assert store.get_task_event((task_id, 0)) is None


def test_nil_ids_skipped_but_counted_as_reported():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(gcs_pb2.TaskEvents())

    assert store.num_task_events_stored == 0
    assert store.stats[tes.STAT_TOTAL_REPORTED] == 1


def test_job_summary_on_job_ends_clears_drops():
    summary = tes.JobTaskSummary()
    summary.record_task_attempt_dropped((_task_id(1), 0))
    assert summary.should_drop_task_attempt((_task_id(1), 0))

    summary.on_job_ends()
    assert not summary.should_drop_task_attempt((_task_id(1), 0))


def test_job_summary_gc_trims_over_cap(monkeypatch):
    monkeypatch.setattr(tes, "MAX_DROPPED_TASK_ATTEMPTS_PER_JOB", 4)
    summary = tes.JobTaskSummary()
    for i in range(10):
        summary.record_task_attempt_dropped((_task_id(i), 0))

    summary.gc_old_dropped_task_attempts(_JOB)

    # 10 tracked, cap 4 -> 6 evicted; total-ever count is preserved.
    assert summary.num_task_attempts_dropped == 10


_WORKER = b"w" * 28


def test_mark_tasks_failed_on_worker_dead():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(
        _event(_task_id(1), task_type=TaskType.NORMAL_TASK, worker=_WORKER)
    )

    store.mark_tasks_failed_on_worker_dead(
        _WORKER, gcs_pb2.WorkerTableData(exit_detail="boom", end_time_ms=5)
    )

    state = store.get_task_event((_task_id(1), 0)).state_updates
    assert state.state_ts_ns[TaskStatus.FAILED] == 5 * 10**6
    assert state.error_info.error_type == ErrorType.WORKER_DIED
    assert "boom" in state.error_info.error_message


def test_mark_tasks_failed_on_worker_dead_without_worker_data():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(
        _event(_task_id(1), task_type=TaskType.NORMAL_TASK, worker=_WORKER)
    )

    # No worker table data (record evicted, or the fetch failed): the task is still failed,
    # stamped with a best-effort time and a message noting the details are unavailable.
    store.mark_tasks_failed_on_worker_dead(_WORKER, None)

    state = store.get_task_event((_task_id(1), 0)).state_updates
    assert state.state_ts_ns[TaskStatus.FAILED] > 0
    assert state.error_info.error_type == ErrorType.WORKER_DIED
    assert "could not be fetched" in state.error_info.error_message


def test_mark_tasks_failed_on_worker_dead_skips_terminated():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(
        _event(
            _task_id(1), task_type=TaskType.NORMAL_TASK, worker=_WORKER, finished=True
        )
    )

    store.mark_tasks_failed_on_worker_dead(
        _WORKER, gcs_pb2.WorkerTableData(end_time_ms=5)
    )

    state = store.get_task_event((_task_id(1), 0)).state_updates
    assert TaskStatus.FAILED not in state.state_ts_ns


def test_mark_tasks_failed_on_worker_dead_unknown_worker_is_noop():
    store = tes.TaskEventStorage()
    # No tasks indexed for this worker; must not raise.
    store.mark_tasks_failed_on_worker_dead(_WORKER, gcs_pb2.WorkerTableData())


def test_mark_tasks_failed_on_job_ends():
    store = tes.TaskEventStorage()
    store.add_or_replace_task_event(_event(_task_id(1), task_type=TaskType.NORMAL_TASK))

    store.mark_tasks_failed_on_job_ends(_JOB, 9)

    state = store.get_task_event((_task_id(1), 0)).state_updates
    assert state.state_ts_ns[TaskStatus.FAILED] == 9
    assert state.error_info.error_type == ErrorType.WORKER_DIED


def test_update_job_summary_on_job_done_clears_drops():
    store = tes.TaskEventStorage()
    summary = store._summary(_JOB)
    summary.record_task_attempt_dropped((_task_id(1), 0))
    assert summary.should_drop_task_attempt((_task_id(1), 0))

    store.update_job_summary_on_job_done(_JOB)

    assert not summary.should_drop_task_attempt((_task_id(1), 0))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
