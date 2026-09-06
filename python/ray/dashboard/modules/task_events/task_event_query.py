"""Read-path query over the task-event store — answers ``GetTaskEvents`` requests.

Candidate events are selected via the store's indices where an equality filter allows it,
then the full filter set is applied and the result truncated to the requested limit.
"""
from ray.core.generated import gcs_pb2, gcs_service_pb2
from ray.core.generated.common_pb2 import TaskStatus, TaskType
from ray.dashboard.modules.task_events.task_event_storage import TaskEventStorage

_EQUAL = gcs_service_pb2.FilterPredicate.EQUAL
_NOT_EQUAL = gcs_service_pb2.FilterPredicate.NOT_EQUAL

# GcsStatus.code for an invalid argument.
_INVALID_ARGUMENT_STATUS_CODE = 34

# TaskStatus values highest first: a task's latest state is the highest-valued status
# present in its state timestamps.
_TASK_STATUS_DESC = sorted(TaskStatus.values(), reverse=True)


def _apply_predicate(predicate: int, actual, expected) -> bool:
    if predicate == _EQUAL:
        return actual == expected
    if predicate == _NOT_EQUAL:
        return actual != expected
    raise ValueError(f"Unknown filter predicate: {predicate}")


def _apply_predicate_ignore_case(predicate: int, actual: str, expected: str) -> bool:
    return _apply_predicate(predicate, actual.lower(), expected.lower())


def _latest_state_name(task_event: gcs_pb2.TaskEvents) -> str:
    state_ts_ns = task_event.state_updates.state_ts_ns
    latest = TaskStatus.NIL
    for status in _TASK_STATUS_DESC:
        if status in state_ts_ns:
            latest = status
            break
    return TaskStatus.Name(latest)


def _passes_filters(
    task_event: gcs_pb2.TaskEvents,
    filters: gcs_service_pb2.GetTaskEventsRequest.Filters,
) -> bool:
    if not task_event.HasField("task_info"):
        return False
    task_info = task_event.task_info
    if filters.exclude_driver and task_info.type == TaskType.DRIVER_TASK:
        return False
    for task_filter in filters.task_filters:
        if not _apply_predicate(
            task_filter.predicate, task_event.task_id, task_filter.task_id
        ):
            return False
    for job_filter in filters.job_filters:
        if not _apply_predicate(
            job_filter.predicate, task_info.job_id, job_filter.job_id
        ):
            return False
    for actor_filter in filters.actor_filters:
        if not _apply_predicate(
            actor_filter.predicate, task_info.actor_id, actor_filter.actor_id
        ):
            return False
    for name_filter in filters.task_name_filters:
        if not _apply_predicate_ignore_case(
            name_filter.predicate, task_info.name, name_filter.task_name
        ):
            return False
    if len(filters.state_filters) > 0:
        state_name = _latest_state_name(task_event)
        for state_filter in filters.state_filters:
            if not _apply_predicate_ignore_case(
                state_filter.predicate, state_name, state_filter.state
            ):
                return False
    return True


def _select_candidates(
    store: TaskEventStorage,
    request: gcs_service_pb2.GetTaskEventsRequest,
    reply: gcs_service_pb2.GetTaskEventsReply,
):
    """Pick candidate events via index where an equality filter allows it, else scan all.

    A single equality filter on task id or job id uses that index; multiple equality ids
    short-circuit to an empty result; anything else scans all stored events. Populates the
    reply's dropped counts at the same scope as the candidates (per-job or global).
    """
    filters = request.filters
    if len(filters.task_filters) > 0:
        task_ids = {f.task_id for f in filters.task_filters if f.predicate == _EQUAL}
        if len(task_ids) == 1:
            return store.get_task_events_by_tasks(task_ids)
        if len(task_ids) > 1:
            return []
    elif len(filters.job_filters) > 0:
        job_ids = {f.job_id for f in filters.job_filters if f.predicate == _EQUAL}
        if len(job_ids) == 1:
            job_id = next(iter(job_ids))
            candidates = store.get_task_events_by_job(job_id)
            summary = store.job_summary(job_id)
            if summary is not None:
                reply.num_profile_task_events_dropped = (
                    summary.num_profile_events_dropped
                )
                reply.num_status_task_events_dropped = summary.num_task_attempts_dropped
            return candidates
        if len(job_ids) > 1:
            return []

    candidates = store.get_all_task_events()
    reply.num_profile_task_events_dropped = store.num_profile_events_dropped()
    reply.num_status_task_events_dropped = store.num_task_attempts_dropped()
    return candidates


def get_task_events(
    store: TaskEventStorage, request: gcs_service_pb2.GetTaskEventsRequest
) -> gcs_service_pb2.GetTaskEventsReply:
    """Answer a ``GetTaskEventsRequest``: select candidates, filter, truncate to limit."""
    reply = gcs_service_pb2.GetTaskEventsReply()
    # Every reply carries a status field (OK unless a bad predicate below).
    reply.status.SetInParent()
    candidates = _select_candidates(store, request, reply)

    limit = request.limit if request.HasField("limit") else -1
    count = 0
    num_status_truncated = 0
    num_profile_truncated = 0
    num_truncated = 0
    num_filtered = 0
    try:
        # Iterate newest-first so a limit keeps the most recent events.
        for task_event in reversed(candidates):
            if not _passes_filters(task_event, request.filters):
                num_filtered += 1
                continue
            if limit < 0 or count < limit:
                count += 1
                reply.events_by_task.add().CopyFrom(task_event)
            else:
                num_profile_truncated += len(task_event.profile_events.events)
                num_status_truncated += 1 if task_event.HasField("state_updates") else 0
                num_truncated += 1
    except ValueError as e:
        reply.Clear()
        reply.status.code = _INVALID_ARGUMENT_STATUS_CODE
        reply.status.message = str(e)
        return reply

    # Truncated events count as dropped for the caller's data-loss accounting.
    reply.num_profile_task_events_dropped += num_profile_truncated
    reply.num_status_task_events_dropped += num_status_truncated
    reply.num_total_stored = len(candidates)
    reply.num_truncated = num_truncated
    reply.num_filtered_on_gcs = num_filtered
    return reply
