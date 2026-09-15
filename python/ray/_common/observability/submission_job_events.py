"""Event builders for submission job events.

This module provides concrete InternalEventBuilder subclasses for emitting
submission job definition and lifecycle events via the ONE-Event framework.
"""

import time
from typing import Dict, Optional, Union

from google.protobuf.timestamp_pb2 import Timestamp

from ray._common.observability.internal_event import InternalEventBuilder
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto
from ray.core.generated.events_submission_job_definition_event_pb2 import (
    SubmissionJobDefinitionEvent,
)
from ray.core.generated.events_submission_job_lifecycle_event_pb2 import (
    SubmissionJobLifecycleEvent,
)


class SubmissionJobDefinitionEventBuilder(InternalEventBuilder):
    """Builds a SubmissionJobDefinitionEvent for a newly submitted job.

    Emitted once per job when the job is first submitted.
    """

    def __init__(
        self,
        submission_id: str,
        entrypoint: str,
        serialized_runtime_env: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        entrypoint_num_cpus: Optional[Union[int, float]] = None,
        entrypoint_num_gpus: Optional[Union[int, float]] = None,
        entrypoint_memory: Optional[int] = None,
        entrypoint_resources: Optional[Dict[str, float]] = None,
        session_name: str = "",
    ):
        super().__init__(
            source_type=RayEventProto.SourceType.JOBS,
            event_type=RayEventProto.EventType.SUBMISSION_JOB_DEFINITION_EVENT,
            nested_event_field_number=RayEventProto.SUBMISSION_JOB_DEFINITION_EVENT_FIELD_NUMBER,
            session_name=session_name,
        )
        self._submission_id = submission_id
        self._entrypoint = entrypoint
        self._serialized_runtime_env = serialized_runtime_env
        self._metadata = metadata
        self._entrypoint_num_cpus = entrypoint_num_cpus
        self._entrypoint_num_gpus = entrypoint_num_gpus
        self._entrypoint_memory = entrypoint_memory
        self._entrypoint_resources = entrypoint_resources

    def get_entity_id(self) -> str:
        return self._submission_id

    def serialize_event_data(self) -> bytes:
        event = SubmissionJobDefinitionEvent()
        event.submission_id = self._submission_id
        event.entrypoint = self._entrypoint

        if self._serialized_runtime_env is not None:
            event.config.serialized_runtime_env = self._serialized_runtime_env
        if self._metadata:
            for k, v in self._metadata.items():
                event.config.metadata[k] = v

        if self._entrypoint_num_cpus is not None:
            event.entrypoint_resources.num_cpus = self._entrypoint_num_cpus
        if self._entrypoint_num_gpus is not None:
            event.entrypoint_resources.num_gpus = self._entrypoint_num_gpus
        if self._entrypoint_memory is not None:
            event.entrypoint_resources.memory = self._entrypoint_memory
        if self._entrypoint_resources:
            for k, v in self._entrypoint_resources.items():
                event.entrypoint_resources.custom_resources[k] = v

        return event.SerializeToString()


# Mapping from JobStatus name to SubmissionJobLifecycleEvent.State enum value.
_JOB_STATUS_TO_PROTO_STATE = {
    "PENDING": SubmissionJobLifecycleEvent.State.PENDING,
    "RUNNING": SubmissionJobLifecycleEvent.State.RUNNING,
    "STOPPED": SubmissionJobLifecycleEvent.State.STOPPED,
    "SUCCEEDED": SubmissionJobLifecycleEvent.State.SUCCEEDED,
    "FAILED": SubmissionJobLifecycleEvent.State.FAILED,
}


def job_status_to_proto_state(
    status_name: str,
) -> Optional[int]:
    """Convert a JobStatus name to the proto State enum value.

    Returns None if the status name is not recognized.
    """
    return _JOB_STATUS_TO_PROTO_STATE.get(status_name)


class SubmissionJobLifecycleEventBuilder(InternalEventBuilder):
    """Builds a SubmissionJobLifecycleEvent for a job state transition.

    Emitted on each job status change.
    """

    def __init__(
        self,
        submission_id: str,
        state: int,
        message: Optional[str] = None,
        error_type: Optional[str] = None,
        driver_node_id: Optional[str] = None,
        driver_agent_http_address: Optional[str] = None,
        driver_exit_code: Optional[int] = None,
        session_name: str = "",
    ):
        super().__init__(
            source_type=RayEventProto.SourceType.JOBS,
            event_type=RayEventProto.EventType.SUBMISSION_JOB_LIFECYCLE_EVENT,
            nested_event_field_number=RayEventProto.SUBMISSION_JOB_LIFECYCLE_EVENT_FIELD_NUMBER,
            session_name=session_name,
        )
        self._submission_id = submission_id
        self._state = state
        self._message = message
        self._error_type = error_type
        self._driver_node_id = driver_node_id
        self._driver_agent_http_address = driver_agent_http_address
        self._driver_exit_code = driver_exit_code

    def get_entity_id(self) -> str:
        return self._submission_id

    def serialize_event_data(self) -> bytes:
        event = SubmissionJobLifecycleEvent()
        event.submission_id = self._submission_id

        transition = event.state_transitions.add()
        transition.state = self._state

        # Set timestamp to current time
        now = time.time()
        transition.timestamp.CopyFrom(
            Timestamp(seconds=int(now), nanos=int((now % 1) * 1e9))
        )

        if self._message is not None:
            transition.message = self._message
        if self._error_type is not None:
            transition.error_type = self._error_type
        if self._driver_node_id is not None:
            transition.driver_node_id = bytes.fromhex(self._driver_node_id)
        if self._driver_agent_http_address is not None:
            transition.driver_agent_http_address = self._driver_agent_http_address
        if self._driver_exit_code is not None:
            transition.driver_exit_code = self._driver_exit_code

        return event.SerializeToString()
