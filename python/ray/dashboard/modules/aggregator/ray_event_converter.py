"""
Convert RayEvent objects to TaskEvents for GCS storage.

This module mirrors the C++ conversion logic from gcs_ray_event_converter.cc,
converting RayEvent protocol buffer messages to TaskEvents format for GCS storage.
"""

import logging
from typing import Dict, List, Optional

from ray.core.generated import (
    common_pb2,
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
    gcs_pb2,
    gcs_service_pb2,
)
from ray.core.generated.events_actor_task_definition_event_pb2 import (
    ActorTaskDefinitionEvent,
)
from ray.core.generated.events_task_definition_event_pb2 import TaskDefinitionEvent
from ray.core.generated.events_task_execution_event_pb2 import TaskExecutionEvent
from ray.core.generated.events_task_profile_events_pb2 import TaskProfileEvents

logger = logging.getLogger(__name__)


def _convert_task_definition_event(
    event: TaskDefinitionEvent,
) -> gcs_pb2.TaskEvents:
    """Convert TaskDefinitionEvent to TaskEvents.

    Args:
        event: The TaskDefinitionEvent to convert

    Returns:
        TaskEvents protobuf message
    """
    task_event = gcs_pb2.TaskEvents()

    # Set top-level fields
    task_event.task_id = event.task_id
    task_event.attempt_number = event.task_attempt
    task_event.job_id = event.job_id

    # Populate task_info
    task_info = task_event.task_info
    task_info.type = event.task_type
    task_info.name = event.task_name
    task_info.task_id = event.task_id
    task_info.job_id = event.job_id
    task_info.parent_task_id = event.parent_task_id

    if event.placement_group_id:
        task_info.placement_group_id = event.placement_group_id

    # Populate runtime and function info
    _populate_task_runtime_and_function_info(
        event.serialized_runtime_env,
        event.task_func,
        event.required_resources,
        event.language,
        task_info,
    )

    return task_event


def _convert_task_execution_event(
    event: TaskExecutionEvent,
) -> gcs_pb2.TaskEvents:
    """Convert TaskExecutionEvent to TaskEvents.

    Args:
        event: The TaskExecutionEvent to convert

    Returns:
        TaskEvents protobuf message
    """
    task_event = gcs_pb2.TaskEvents()

    # Set top-level fields
    task_event.task_id = event.task_id
    task_event.attempt_number = event.task_attempt
    task_event.job_id = event.job_id

    # Populate state_updates
    state_update = task_event.state_updates
    state_update.node_id = event.node_id
    state_update.worker_id = event.worker_id
    state_update.worker_pid = event.worker_pid

    # Copy error info if present
    if event.HasField("ray_error_info"):
        state_update.error_info.CopyFrom(event.ray_error_info)

    # Convert task state timestamps
    for state, timestamp in event.task_state.items():
        # Convert protobuf timestamp to nanoseconds
        timestamp_ns = timestamp.seconds * 1_000_000_000 + timestamp.nanos
        state_update.state_ts_ns[state] = timestamp_ns

    return task_event


def _convert_actor_task_definition_event(
    event: ActorTaskDefinitionEvent,
) -> gcs_pb2.TaskEvents:
    """Convert ActorTaskDefinitionEvent to TaskEvents.

    Args:
        event: The ActorTaskDefinitionEvent to convert

    Returns:
        TaskEvents protobuf message
    """
    task_event = gcs_pb2.TaskEvents()

    # Set top-level fields
    task_event.task_id = event.task_id
    task_event.attempt_number = event.task_attempt
    task_event.job_id = event.job_id

    # Populate task_info
    task_info = task_event.task_info
    task_info.type = gcs_pb2.TaskType.ACTOR_TASK
    task_info.name = event.actor_task_name
    task_info.task_id = event.task_id
    task_info.job_id = event.job_id
    task_info.parent_task_id = event.parent_task_id

    if event.placement_group_id:
        task_info.placement_group_id = event.placement_group_id

    if event.actor_id:
        task_info.actor_id = event.actor_id

    # Populate runtime and function info
    _populate_task_runtime_and_function_info(
        event.serialized_runtime_env,
        event.actor_func,
        event.required_resources,
        event.language,
        task_info,
    )

    return task_event


def _convert_task_profile_events(
    event: TaskProfileEvents,
) -> gcs_pb2.TaskEvents:
    """Convert TaskProfileEvents to TaskEvents.

    Args:
        event: The TaskProfileEvents to convert

    Returns:
        TaskEvents protobuf message
    """
    task_event = gcs_pb2.TaskEvents()

    # Set top-level fields
    task_event.task_id = event.task_id
    task_event.attempt_number = event.attempt_number
    task_event.job_id = event.job_id

    # Deep copy profile events
    task_event.profile_events.CopyFrom(event.profile_events)

    return task_event


def _populate_task_runtime_and_function_info(
    serialized_runtime_env: str,
    function_descriptor,
    required_resources: Dict[str, float],
    language: int,
    task_info: common_pb2.TaskInfoEntry,
) -> None:
    """Populate TaskInfoEntry with runtime env, function descriptor, and resources.

    Args:
        serialized_runtime_env: Serialized runtime environment string
        function_descriptor: FunctionDescriptor protobuf message
        required_resources: Map of resource name to amount
        language: Language enum value
        task_info: TaskInfoEntry to populate (modified in place)
    """
    task_info.language = language
    task_info.runtime_env_info.serialized_runtime_env = serialized_runtime_env

    # Extract function name based on language
    if language == common_pb2.Language.CPP:
        if function_descriptor.HasField("cpp_function_descriptor"):
            task_info.func_or_class_name = (
                function_descriptor.cpp_function_descriptor.function_name
            )
    elif language == common_pb2.Language.PYTHON:
        if function_descriptor.HasField("python_function_descriptor"):
            task_info.func_or_class_name = (
                function_descriptor.python_function_descriptor.function_name
            )
    elif language == common_pb2.Language.JAVA:
        if function_descriptor.HasField("java_function_descriptor"):
            task_info.func_or_class_name = (
                function_descriptor.java_function_descriptor.function_name
            )
    else:
        logger.warning(f"Unsupported language: {language}")

    # Copy required resources
    for key, value in required_resources.items():
        task_info.required_resources[key] = value


def _add_dropped_task_attempts_to_requests(
    metadata: events_event_aggregator_service_pb2.TaskEventsMetadata,
    requests_by_job_id: Dict[bytes, gcs_service_pb2.AddTaskEventDataRequest],
) -> None:
    """Add dropped task attempts to the appropriate job-grouped request.

    Args:
        metadata: TaskEventsMetadata containing dropped task attempts
        requests_by_job_id: Dictionary mapping job_id to AddTaskEventDataRequest
                           (modified in place)
    """

    for dropped_attempt in metadata.dropped_task_attempts:
        # Extract job_id from task_id
        # TaskID is 20 bytes, first 4 bytes are job_id
        task_id_bytes = dropped_attempt.task_id
        if len(task_id_bytes) >= 4:
            job_id = task_id_bytes[:4]
        else:
            logger.warning(f"Invalid task_id length: {len(task_id_bytes)}")
            continue

        # Get or create request for this job_id
        if job_id not in requests_by_job_id:
            requests_by_job_id[job_id] = gcs_service_pb2.AddTaskEventDataRequest()
            requests_by_job_id[job_id].data.job_id = job_id

        # Add dropped attempt to the request
        requests_by_job_id[job_id].data.dropped_task_attempts.append(dropped_attempt)


def convert_to_task_event_data_requests(
    events: List[events_base_event_pb2.RayEvent],
    metadata: Optional[events_event_aggregator_service_pb2.TaskEventsMetadata] = None,
) -> List[gcs_service_pb2.AddTaskEventDataRequest]:
    """Convert RayEvents to AddTaskEventDataRequest objects grouped by job_id.

    This function mirrors the C++ ConvertToTaskEventDataRequests function,
    converting RayEvent objects to TaskEvents and grouping them by job_id.

    Args:
        events: List of RayEvent protobuf messages to convert
        metadata: Optional TaskEventsMetadata with dropped task attempts

    Returns:
        List of AddTaskEventDataRequest objects, one per job_id
    """
    # Dictionary mapping job_id to AddTaskEventDataRequest
    requests_by_job_id: Dict[bytes, gcs_service_pb2.AddTaskEventDataRequest] = {}

    # Convert each event based on its type
    for event in events:
        task_event: Optional[gcs_pb2.TaskEvents] = None

        # Filter out empty events - skip events that don't have their corresponding
        # event data set
        should_skip = False
        event_type = event.event_type

        if event_type == events_base_event_pb2.RayEvent.TASK_DEFINITION_EVENT:
            if not event.HasField("task_definition_event"):
                logger.debug("Skipping empty TASK_DEFINITION_EVENT")
                should_skip = True
        elif event_type == events_base_event_pb2.RayEvent.TASK_EXECUTION_EVENT:
            if not event.HasField("task_execution_event"):
                logger.debug("Skipping empty TASK_EXECUTION_EVENT")
                should_skip = True
        elif event_type == events_base_event_pb2.RayEvent.TASK_PROFILE_EVENT:
            if not event.HasField("task_profile_events"):
                logger.debug("Skipping empty TASK_PROFILE_EVENT")
                should_skip = True
        elif event_type == events_base_event_pb2.RayEvent.ACTOR_TASK_DEFINITION_EVENT:
            if not event.HasField("actor_task_definition_event"):
                logger.debug("Skipping empty ACTOR_TASK_DEFINITION_EVENT")
                should_skip = True
        else:
            # Unsupported event types are skipped
            logger.debug(f"Skipping unsupported event type: {event_type}")
            should_skip = True

        if should_skip:
            continue

        # Convert the event based on its type
        try:
            if event_type == events_base_event_pb2.RayEvent.TASK_DEFINITION_EVENT:
                task_event = _convert_task_definition_event(event.task_definition_event)
            elif event_type == events_base_event_pb2.RayEvent.TASK_EXECUTION_EVENT:
                task_event = _convert_task_execution_event(event.task_execution_event)
            elif event_type == events_base_event_pb2.RayEvent.TASK_PROFILE_EVENT:
                task_event = _convert_task_profile_events(event.task_profile_events)
            elif (
                event_type == events_base_event_pb2.RayEvent.ACTOR_TASK_DEFINITION_EVENT
            ):
                task_event = _convert_actor_task_definition_event(
                    event.actor_task_definition_event
                )
        except Exception as e:
            logger.error(f"Failed to convert event type {event_type}: {e}")
            continue

        if task_event is None:
            continue

        # Group by job_id
        job_id = task_event.job_id

        # Get or create request for this job_id
        if job_id not in requests_by_job_id:
            requests_by_job_id[job_id] = gcs_service_pb2.AddTaskEventDataRequest()
            requests_by_job_id[job_id].data.job_id = job_id

        # Add task event to the request
        requests_by_job_id[job_id].data.events_by_task.append(task_event)

    # Add dropped task attempts if present
    if metadata and metadata.dropped_task_attempts:
        _add_dropped_task_attempts_to_requests(metadata, requests_by_job_id)

    # Convert dictionary to list
    return list(requests_by_job_id.values())
