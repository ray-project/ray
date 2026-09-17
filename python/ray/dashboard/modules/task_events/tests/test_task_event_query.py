import sys

import pytest

from ray._raylet import JobID, TaskID
from ray.core.generated import gcs_pb2, gcs_service_pb2
from ray.core.generated.common_pb2 import TaskAttempt, TaskStatus, TaskType
from ray.dashboard.modules.task_events import task_event_query as q
from ray.dashboard.modules.task_events.task_event_storage import TaskEventStorage

_EQUAL = gcs_service_pb2.FilterPredicate.EQUAL
_NOT_EQUAL = gcs_service_pb2.FilterPredicate.NOT_EQUAL

_JOB = JobID.from_int(1).binary()
_JOB_B = JobID.from_int(2).binary()


def _task_id(n: int) -> bytes:
    return bytes([n]) * 24


def _event(
    task_id,
    *,
    job_id=_JOB,
    task_type=TaskType.NORMAL_TASK,
    name="",
    actor_id=b"",
    states=None,
) -> gcs_pb2.TaskEvents:
    event = gcs_pb2.TaskEvents(task_id=task_id, attempt_number=0, job_id=job_id)
    event.task_info.type = task_type
    event.task_info.name = name
    event.task_info.job_id = job_id
    if actor_id:
        event.task_info.actor_id = actor_id
    for status, ts in (states or {}).items():
        event.state_updates.state_ts_ns[status] = ts
    return event


def _request(limit=None, exclude_driver=False) -> gcs_service_pb2.GetTaskEventsRequest:
    request = gcs_service_pb2.GetTaskEventsRequest()
    request.filters.exclude_driver = exclude_driver
    if limit is not None:
        request.limit = limit
    return request


def _add_filter(request, kind, value, predicate=_EQUAL):
    filters = request.filters
    if kind == "job":
        f = filters.job_filters.add()
        f.job_id = value
    elif kind == "task":
        f = filters.task_filters.add()
        f.task_id = value
    elif kind == "actor":
        f = filters.actor_filters.add()
        f.actor_id = value
    elif kind == "name":
        f = filters.task_name_filters.add()
        f.task_name = value
    elif kind == "state":
        f = filters.state_filters.add()
        f.state = value
    f.predicate = predicate


def _store(*events) -> TaskEventStorage:
    store = TaskEventStorage()
    for event in events:
        store.add_or_replace_task_event(event)
    return store


def _task_ids(reply):
    return {e.task_id for e in reply.events_by_task}


def test_no_filters_returns_all():
    store = _store(_event(_task_id(1)), _event(_task_id(2)), _event(_task_id(3)))

    reply = q.get_task_events(store, _request())

    assert _task_ids(reply) == {_task_id(1), _task_id(2), _task_id(3)}
    assert reply.num_total_stored == 3
    assert reply.num_truncated == 0
    assert reply.num_filtered_on_gcs == 0
    assert reply.num_profile_task_events_dropped == 0
    assert reply.num_status_task_events_dropped == 0


def test_job_filter_returns_only_that_job():
    store = _store(
        _event(_task_id(1), job_id=_JOB),
        _event(_task_id(2), job_id=_JOB),
        _event(_task_id(3), job_id=_JOB_B),
    )

    request = _request()
    _add_filter(request, "job", _JOB)
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(1), _task_id(2)}
    # Candidate set is the job's events, so total-stored is scoped to the job.
    assert reply.num_total_stored == 2


def test_task_filter_returns_only_that_task():
    store = _store(_event(_task_id(1)), _event(_task_id(2)))

    request = _request()
    _add_filter(request, "task", _task_id(1))
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(1)}
    # Single equality task filter uses the index, so total-stored is scoped to it.
    assert reply.num_total_stored == 1


def test_task_filter_not_equal_scans_and_filters():
    store = _store(_event(_task_id(1)), _event(_task_id(2)), _event(_task_id(3)))

    request = _request()
    _add_filter(request, "task", _task_id(1), predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(2), _task_id(3)}
    # A NOT_EQUAL filter can't use the index, so it scans all events.
    assert reply.num_total_stored == 3
    assert reply.num_filtered_on_gcs == 1


def test_multiple_equal_task_ids_returns_empty():
    store = _store(_event(_task_id(1)), _event(_task_id(2)))

    request = _request()
    _add_filter(request, "task", _task_id(1))
    _add_filter(request, "task", _task_id(2))
    reply = q.get_task_events(store, request)

    assert len(reply.events_by_task) == 0
    assert reply.num_total_stored == 0


def test_multiple_not_equal_task_ids_scans():
    store = _store(_event(_task_id(1)), _event(_task_id(2)), _event(_task_id(3)))

    request = _request()
    _add_filter(request, "task", _task_id(1), predicate=_NOT_EQUAL)
    _add_filter(request, "task", _task_id(2), predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(3)}
    assert reply.num_total_stored == 3
    assert reply.num_filtered_on_gcs == 2


def test_task_equal_and_not_equal_mixed():
    store = _store(_event(_task_id(1)), _event(_task_id(2)), _event(_task_id(3)))

    request = _request()
    _add_filter(request, "task", _task_id(1), predicate=_NOT_EQUAL)
    _add_filter(request, "task", _task_id(2), predicate=_EQUAL)
    reply = q.get_task_events(store, request)

    # The single EQUAL id drives index selection; the NOT_EQUAL is applied on top.
    assert _task_ids(reply) == {_task_id(2)}
    assert reply.num_total_stored == 1
    assert reply.num_filtered_on_gcs == 0


def test_job_filter_not_equal_scans_and_filters():
    store = _store(
        _event(_task_id(1), job_id=_JOB),
        _event(_task_id(2), job_id=_JOB_B),
    )

    request = _request()
    _add_filter(request, "job", _JOB, predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(2)}
    assert reply.num_total_stored == 2
    assert reply.num_filtered_on_gcs == 1


def test_multiple_equal_job_ids_returns_empty():
    store = _store(_event(_task_id(1), job_id=_JOB), _event(_task_id(2), job_id=_JOB_B))

    request = _request()
    _add_filter(request, "job", _JOB)
    _add_filter(request, "job", _JOB_B)
    reply = q.get_task_events(store, request)

    assert len(reply.events_by_task) == 0


def test_multiple_not_equal_job_ids_scans():
    _JOB_C = JobID.from_int(3).binary()
    store = _store(
        _event(_task_id(1), job_id=_JOB),
        _event(_task_id(2), job_id=_JOB_B),
        _event(_task_id(3), job_id=_JOB_C),
    )

    request = _request()
    _add_filter(request, "job", _JOB, predicate=_NOT_EQUAL)
    _add_filter(request, "job", _JOB_B, predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(3)}
    assert reply.num_total_stored == 3
    assert reply.num_filtered_on_gcs == 2


def test_job_equal_and_not_equal_mixed():
    _JOB_C = JobID.from_int(3).binary()
    store = _store(
        _event(_task_id(1), job_id=_JOB),
        _event(_task_id(2), job_id=_JOB_B),
        _event(_task_id(3), job_id=_JOB_C),
    )

    request = _request()
    _add_filter(request, "job", _JOB, predicate=_EQUAL)
    _add_filter(request, "job", _JOB_B, predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    # The single EQUAL job drives index selection; the NOT_EQUAL is applied on top.
    assert _task_ids(reply) == {_task_id(1)}
    assert reply.num_total_stored == 1
    assert reply.num_filtered_on_gcs == 0


def test_task_and_job_combined_filter():
    # Task 1 is in job A; requiring it also be in job B filters it out.
    store = _store(
        _event(_task_id(1), job_id=_JOB),
        _event(_task_id(2), job_id=_JOB_B),
    )

    request = _request()
    _add_filter(request, "task", _task_id(1))
    _add_filter(request, "job", _JOB_B)
    reply = q.get_task_events(store, request)

    assert len(reply.events_by_task) == 0
    # Candidates come from the single-task index; the job filter drops the one candidate.
    assert reply.num_total_stored == 1
    assert reply.num_filtered_on_gcs == 1


def test_task_equal_and_job_not_equal_combined():
    store = _store(
        _event(_task_id(1), job_id=_JOB),
        _event(_task_id(2), job_id=_JOB_B),
    )

    request = _request()
    _add_filter(request, "task", _task_id(1))
    _add_filter(request, "job", _JOB, predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    assert len(reply.events_by_task) == 0
    assert reply.num_total_stored == 1
    assert reply.num_filtered_on_gcs == 1


def test_exclude_driver():
    store = _store(
        _event(_task_id(1), task_type=TaskType.NORMAL_TASK),
        _event(_task_id(2), task_type=TaskType.DRIVER_TASK),
    )

    reply = q.get_task_events(store, _request(exclude_driver=True))

    assert _task_ids(reply) == {_task_id(1)}
    assert reply.num_filtered_on_gcs == 1


def test_include_driver():
    store = _store(
        _event(_task_id(1), task_type=TaskType.NORMAL_TASK),
        _event(_task_id(2), task_type=TaskType.DRIVER_TASK),
    )

    reply = q.get_task_events(store, _request(exclude_driver=False))

    assert _task_ids(reply) == {_task_id(1), _task_id(2)}
    assert reply.num_filtered_on_gcs == 0


_ACTOR_A = b"a" * 16
_ACTOR_B = b"b" * 16


def _actor_store() -> TaskEventStorage:
    return _store(
        _event(_task_id(1), task_type=TaskType.ACTOR_TASK, actor_id=_ACTOR_A),
        _event(_task_id(2), task_type=TaskType.ACTOR_TASK, actor_id=_ACTOR_B),
        _event(_task_id(3)),
    )


def test_actor_filter():
    request = _request()
    _add_filter(request, "actor", _ACTOR_A)
    reply = q.get_task_events(_actor_store(), request)

    assert _task_ids(reply) == {_task_id(1)}
    assert reply.num_filtered_on_gcs == 2


def test_actor_filter_not_equal():
    request = _request()
    _add_filter(request, "actor", _ACTOR_A, predicate=_NOT_EQUAL)
    reply = q.get_task_events(_actor_store(), request)

    # Non-actor tasks have an empty actor id, so they also satisfy != actor_a.
    assert _task_ids(reply) == {_task_id(2), _task_id(3)}
    assert reply.num_filtered_on_gcs == 1


def test_multiple_equal_actor_ids_returns_empty():
    request = _request()
    _add_filter(request, "actor", _ACTOR_A)
    _add_filter(request, "actor", _ACTOR_B)
    reply = q.get_task_events(_actor_store(), request)

    assert len(reply.events_by_task) == 0
    assert reply.num_filtered_on_gcs == 3


def _name_store() -> TaskEventStorage:
    return _store(
        _event(_task_id(1), name="Foo"),
        _event(_task_id(2), name="Bar"),
        _event(_task_id(3), name="Baz"),
    )


def test_name_filter_is_case_insensitive():
    request = _request()
    _add_filter(request, "name", "foo")
    reply = q.get_task_events(_name_store(), request)

    assert _task_ids(reply) == {_task_id(1)}
    assert reply.num_filtered_on_gcs == 2


def test_name_filter_not_equal():
    request = _request()
    _add_filter(request, "name", "FOO", predicate=_NOT_EQUAL)
    reply = q.get_task_events(_name_store(), request)

    assert _task_ids(reply) == {_task_id(2), _task_id(3)}
    assert reply.num_filtered_on_gcs == 1


def test_multiple_equal_names_returns_empty():
    request = _request()
    _add_filter(request, "name", "Foo")
    _add_filter(request, "name", "Bar")
    reply = q.get_task_events(_name_store(), request)

    assert len(reply.events_by_task) == 0
    assert reply.num_filtered_on_gcs == 3


def _state_store() -> TaskEventStorage:
    # Latest state is the highest-valued status present: RUNNING + FINISHED -> FINISHED.
    return _store(
        _event(_task_id(1), states={TaskStatus.RUNNING: 1, TaskStatus.FINISHED: 2}),
        _event(_task_id(2), states={TaskStatus.RUNNING: 1}),
        _event(_task_id(3)),  # no state updates -> latest state NIL
    )


@pytest.mark.parametrize(
    "state,expected",
    [
        ("finished", {1}),
        ("RUNNING", {2}),
        ("nil", {3}),
    ],
)
def test_state_filter_uses_latest_state(state, expected):
    request = _request()
    _add_filter(request, "state", state)
    reply = q.get_task_events(_state_store(), request)

    assert _task_ids(reply) == {_task_id(n) for n in expected}


def test_state_filter_not_equal():
    request = _request()
    _add_filter(request, "state", "RUNNING", predicate=_NOT_EQUAL)
    reply = q.get_task_events(_state_store(), request)

    assert _task_ids(reply) == {_task_id(1), _task_id(3)}
    assert reply.num_filtered_on_gcs == 1


def test_multiple_equal_states_returns_empty():
    request = _request()
    _add_filter(request, "state", "RUNNING")
    _add_filter(request, "state", "NIL")
    reply = q.get_task_events(_state_store(), request)

    assert len(reply.events_by_task) == 0
    assert reply.num_filtered_on_gcs == 3


def test_not_equal_predicate():
    store = _store(_event(_task_id(1)), _event(_task_id(2)))

    request = _request()
    _add_filter(request, "task", _task_id(1), predicate=_NOT_EQUAL)
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(2)}


def test_name_and_actor_combined_no_match():
    # task_id(1) has the name but no actor; nothing satisfies both.
    store = _store(
        _event(_task_id(1), name="foo"),
        _event(_task_id(2), task_type=TaskType.ACTOR_TASK, actor_id=_ACTOR_A),
    )

    request = _request()
    _add_filter(request, "name", "foo")
    _add_filter(request, "actor", _ACTOR_A)
    reply = q.get_task_events(store, request)

    assert len(reply.events_by_task) == 0


def test_actor_and_state_combined():
    store = _store(
        _event(
            _task_id(1),
            task_type=TaskType.ACTOR_TASK,
            actor_id=_ACTOR_A,
            states={TaskStatus.RUNNING: 1},
        ),
        _event(
            _task_id(2),
            task_type=TaskType.ACTOR_TASK,
            actor_id=_ACTOR_A,
            states={TaskStatus.FINISHED: 1},
        ),
    )

    request = _request()
    _add_filter(request, "actor", _ACTOR_A)
    _add_filter(request, "state", "running")
    reply = q.get_task_events(store, request)

    assert _task_ids(reply) == {_task_id(1)}
    assert reply.num_filtered_on_gcs == 1


def test_invalid_predicate_returns_invalid_argument_status():
    store = _store(_event(_task_id(1)))

    request = _request()
    _add_filter(request, "task", _task_id(1), predicate=100)
    reply = q.get_task_events(store, request)

    assert reply.status.code == q._INVALID_ARGUMENT_STATUS_CODE
    assert len(reply.events_by_task) == 0


def test_success_reply_has_ok_status():
    store = _store(_event(_task_id(1)))

    reply = q.get_task_events(store, _request())

    assert reply.HasField("status")
    assert reply.status.code == 0


def test_limit_truncates_and_counts_dropped():
    store = _store(
        _event(_task_id(1), states={TaskStatus.RUNNING: 1}),
        _event(_task_id(2), states={TaskStatus.RUNNING: 1}),
        _event(_task_id(3), states={TaskStatus.RUNNING: 1}),
    )

    reply = q.get_task_events(store, _request(limit=1))

    assert len(reply.events_by_task) == 1
    assert reply.num_total_stored == 3
    assert reply.num_truncated == 2
    # Truncated status events are reported as dropped.
    assert reply.num_status_task_events_dropped == 2


@pytest.mark.parametrize(
    "limit,expected_kept",
    [
        (2, 2),  # partial
        (0, 0),  # keep nothing, everything truncated
        (-1, 3),  # unlimited
    ],
)
def test_limit_boundaries(limit, expected_kept):
    store = _store(
        _event(_task_id(1), states={TaskStatus.RUNNING: 1}),
        _event(_task_id(2), states={TaskStatus.RUNNING: 1}),
        _event(_task_id(3), states={TaskStatus.RUNNING: 1}),
    )

    reply = q.get_task_events(store, _request(limit=limit))

    num_truncated = 3 - expected_kept
    assert len(reply.events_by_task) == expected_kept
    assert reply.num_total_stored == 3
    assert reply.num_truncated == num_truncated
    assert reply.num_status_task_events_dropped == num_truncated


def test_limit_keeps_most_recent():
    # Insert oldest-to-newest; a limit must keep the most recently added events.
    events = [_event(_task_id(n), states={TaskStatus.RUNNING: 1}) for n in range(1, 21)]
    store = _store(*events)

    reply = q.get_task_events(store, _request(limit=5))

    newest_five = {_task_id(n) for n in range(16, 21)}
    assert _task_ids(reply) == newest_five
    assert reply.num_truncated == 15


def test_global_dropped_counts_reported():
    store = _store(_event(_task_id(1)))
    dropped = TaskID.for_fake_task(JobID.from_int(1)).binary()
    store.record_data_loss_from_worker([TaskAttempt(task_id=dropped, attempt_number=0)])

    reply = q.get_task_events(store, _request())

    assert reply.num_status_task_events_dropped == 1


def test_per_job_dropped_counts_reported():
    store = _store(_event(_task_id(1), job_id=JobID.from_int(1).binary()))
    dropped = TaskID.for_fake_task(JobID.from_int(1)).binary()
    store.record_data_loss_from_worker([TaskAttempt(task_id=dropped, attempt_number=0)])

    request = _request()
    _add_filter(request, "job", JobID.from_int(1).binary())
    reply = q.get_task_events(store, request)

    assert reply.num_status_task_events_dropped == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
