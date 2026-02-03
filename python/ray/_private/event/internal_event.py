"""Base class for building internal Ray events.

This module provides the base class for creating event builders that can
emit events to the internal Ray event aggregator via Cython bindings.

Example usage for creating a new event type:

    from ray._private.event.internal_event import InternalEventBuilder
    from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto

    class MyEventBuilder(InternalEventBuilder):
        def __init__(self, entity_id: str, ...):
            super().__init__(
                source_type=RayEventProto.SourceType.JOBS,
                event_type=RayEventProto.EventType.MY_EVENT_TYPE,
            )
            self._entity_id = entity_id
            # ... store other fields ...

        def get_entity_id(self) -> str:
            return self._entity_id

        def serialize_event_data(self) -> bytes:
            # Create and serialize your protobuf message
            event = MyEventProto()
            # ... populate fields ...
            return event.SerializeToString()
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray._raylet import RayEvent


class InternalEventBuilder(ABC):
    """Abstract base class for building internal Ray events.

    Subclasses implement specific event types (submission job, data operator, etc.)
    and must implement get_entity_id() and serialize_event_data().

    The source_type, event_type, and severity should use the proto enum values from
    ray.core.generated.events_base_event_pb2.RayEvent.
    """

    def __init__(
        self,
        source_type: int,
        event_type: int,
        severity: int = 3,  # INFO = 3 (default)
        message: str = "",
        session_name: str = "",
    ):
        """Initialize the event builder.

        Args:
            source_type: RayEvent.SourceType enum value (e.g., JOBS = 6).
            event_type: RayEvent.EventType enum value.
            severity: RayEvent.Severity enum value (default INFO = 3).
            message: Optional message associated with the event.
            session_name: The Ray session name.
        """
        self._source_type = source_type
        self._event_type = event_type
        self._severity = severity
        self._message = message
        self._session_name = session_name

    @abstractmethod
    def get_entity_id(self) -> str:
        """Return the unique entity ID for this event.

        The entity ID is used to associate related events (e.g., definition
        and lifecycle events for the same job).

        Returns:
            A string identifier unique to this entity (e.g., submission_id).
        """
        pass

    @abstractmethod
    def serialize_event_data(self) -> bytes:
        """Serialize the event-specific protobuf data to bytes.

        Returns:
            Serialized protobuf bytes of the nested event message
            (e.g., SubmissionJobDefinitionEvent.SerializeToString()).
        """
        pass

    def build(self) -> "RayEvent":
        """Build the Cython RayEvent object for submission.

        Returns:
            A RayEvent object that can be passed to emit_event() or emit_events().
        """
        from ray._raylet import RayEvent

        return RayEvent(
            source_type=self._source_type,
            event_type=self._event_type,
            severity=self._severity,
            entity_id=self.get_entity_id(),
            message=self._message,
            session_name=self._session_name,
            serialized_data=self.serialize_event_data(),
        )
