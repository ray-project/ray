"""Convert aggregator ``RayEvent``s into the ``TaskEvents`` storage model.
"""
from typing import List, Tuple

from ray._raylet import TaskID
from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import FunctionDescriptor, TaskInfoEntry, TaskType
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_event_aggregator_service_pb2 import AddEventsRequest


def _short_name(name: str) -> str:
    """Return the component after the last '.' (e.g. ``module.foo`` -> ``foo``)."""
    return name[name.rfind(".") + 1 :]


def _call_string(function_descriptor: FunctionDescriptor) -> str:
    """Return the short function/class name (for ``TaskInfoEntry.func_or_class_name``)
    encoded in a ``FunctionDescriptor`` proto."""
    which = function_descriptor.WhichOneof("function_descriptor")
    if which == "python_function_descriptor":
        p = function_descriptor.python_function_descriptor
        if not p.class_name:
            return _short_name(p.function_name)
        return f"{_short_name(p.class_name)}.{_short_name(p.function_name)}"
    if which == "java_function_descriptor":
        j = function_descriptor.java_function_descriptor
        if not j.class_name:
            return j.function_name
        return f"{j.class_name}.{j.function_name}"
    if which == "cpp_function_descriptor":
        return function_descriptor.cpp_function_descriptor.function_name
    return ""


def _populate_task_runtime_and_function_info(
    task_info: TaskInfoEntry,
    serialized_runtime_env: str,
    function_descriptor: FunctionDescriptor,
    required_resources,
    language,
) -> None:
    task_info.language = language
    task_info.runtime_env_info.serialized_runtime_env = serialized_runtime_env
    task_info.func_or_class_name = _call_string(function_descriptor)
    task_info.required_resources.update(required_resources)


def _convert_task_definition_event(event) -> gcs_pb2.TaskEvents:
    task_event = gcs_pb2.TaskEvents()
    task_event.task_id = event.task_id
    task_event.attempt_number = event.task_attempt
    task_event.job_id = event.job_id

    task_info = task_event.task_info
    task_info.type = event.task_type
    task_info.name = event.task_name
    task_info.task_id = event.task_id
    task_info.job_id = event.job_id
    task_info.parent_task_id = event.parent_task_id
    if event.task_type == TaskType.ACTOR_CREATION_TASK:
        task_info.actor_id = TaskID(event.task_id).actor_id().binary()
        if event.is_detached_actor:
            task_info.is_detached_actor = True
    if event.placement_group_id:
        task_info.placement_group_id = event.placement_group_id
    if event.HasField("call_site"):
        task_info.call_site = event.call_site
    if event.label_selector:
        task_info.label_selector.update(event.label_selector)
    if event.HasField("fallback_strategy"):
        task_info.fallback_strategy.CopyFrom(event.fallback_strategy)

    _populate_task_runtime_and_function_info(
        task_info,
        event.serialized_runtime_env,
        event.task_func,
        event.required_resources,
        event.language,
    )
    return task_event


_TASK_LOG_INFO_FIELDS = (
    "stdout_file",
    "stderr_file",
    "stdout_start",
    "stdout_end",
    "stderr_start",
    "stderr_end",
)


def _convert_task_log_info(src, dst) -> None:
    """Copy only the fields the event actually set.

    The log paths and start offsets arrive when the task starts and the end offsets in a
    later event, so copying an unset field would overwrite what the earlier event
    reported once the two are merged.
    """
    for field in _TASK_LOG_INFO_FIELDS:
        if src.HasField(field):
            setattr(dst, field, getattr(src, field))


def _convert_task_lifecycle_event(event) -> gcs_pb2.TaskEvents:
    task_event = gcs_pb2.TaskEvents()
    task_event.task_id = event.task_id
    task_event.attempt_number = event.task_attempt
    task_event.job_id = event.job_id

    state_update = task_event.state_updates
    if event.node_id:
        state_update.node_id = event.node_id
    if event.worker_id:
        state_update.worker_id = event.worker_id
    # worker pid can never be 0.
    if event.worker_pid != 0:
        state_update.worker_pid = event.worker_pid
    if event.HasField("ray_error_info"):
        state_update.error_info.CopyFrom(event.ray_error_info)
    if event.HasField("is_debugger_paused"):
        state_update.is_debugger_paused = event.is_debugger_paused
    if event.HasField("actor_repr_name"):
        state_update.actor_repr_name = event.actor_repr_name
    if event.HasField("task_log_info"):
        _convert_task_log_info(event.task_log_info, state_update.task_log_info)

    for transition in event.state_transitions:
        ts = transition.timestamp
        state_update.state_ts_ns[transition.state] = ts.seconds * 10**9 + ts.nanos
    return task_event


def _convert_actor_task_definition_event(event) -> gcs_pb2.TaskEvents:
    task_event = gcs_pb2.TaskEvents()
    task_event.task_id = event.task_id
    task_event.attempt_number = event.task_attempt
    task_event.job_id = event.job_id

    task_info = task_event.task_info
    task_info.type = TaskType.ACTOR_TASK
    task_info.name = event.actor_task_name
    task_info.task_id = event.task_id
    task_info.job_id = event.job_id
    task_info.parent_task_id = event.parent_task_id
    if event.placement_group_id:
        task_info.placement_group_id = event.placement_group_id
    if event.actor_id:
        task_info.actor_id = event.actor_id
    if event.is_detached_actor:
        task_info.is_detached_actor = True
    if event.HasField("call_site"):
        task_info.call_site = event.call_site
    if event.label_selector:
        task_info.label_selector.update(event.label_selector)
    if event.HasField("fallback_strategy"):
        task_info.fallback_strategy.CopyFrom(event.fallback_strategy)

    _populate_task_runtime_and_function_info(
        task_info,
        event.serialized_runtime_env,
        event.actor_func,
        event.required_resources,
        event.language,
    )
    return task_event


def _convert_task_profile_events(event) -> gcs_pb2.TaskEvents:
    task_event = gcs_pb2.TaskEvents()
    task_event.task_id = event.task_id
    task_event.attempt_number = event.attempt_number
    task_event.job_id = event.job_id

    if event.HasField("profile_events"):
        task_event.profile_events.CopyFrom(event.profile_events)
    return task_event


def convert_to_task_events(
    request: AddEventsRequest,
) -> Tuple[List[gcs_pb2.TaskEvents], list]:
    """Convert an ``AddEventsRequest`` into ``TaskEvents`` for the store, plus the
    upstream dropped-task-attempt metadata.
    Returns ``(task_events, dropped_task_attempts)``.
    """
    events_data = request.events_data
    task_events: List[gcs_pb2.TaskEvents] = []
    for event in events_data.events:
        event_type = event.event_type
        if event_type == RayEvent.EventType.TASK_DEFINITION_EVENT:
            task_events.append(
                _convert_task_definition_event(event.task_definition_event)
            )
        elif event_type == RayEvent.EventType.TASK_LIFECYCLE_EVENT:
            task_events.append(
                _convert_task_lifecycle_event(event.task_lifecycle_event)
            )
        elif event_type == RayEvent.EventType.TASK_PROFILE_EVENT:
            task_events.append(_convert_task_profile_events(event.task_profile_events))
        elif event_type == RayEvent.EventType.ACTOR_TASK_DEFINITION_EVENT:
            task_events.append(
                _convert_actor_task_definition_event(event.actor_task_definition_event)
            )
        else:
            # the aggregator only forwards the four exposable task event types,
            # so any other type is a bug.
            raise AssertionError(
                f"Unsupported event type for task events: {event_type}"
            )
    dropped_task_attempts = list(events_data.task_events_metadata.dropped_task_attempts)
    return task_events, dropped_task_attempts
