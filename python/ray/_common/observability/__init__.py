"""Ray ONE-Event (Observability aNd Events) Python API.

This module provides the Python API for emitting internal Ray events
via the ONE-Event system. Events are buffered and exported through
the C++ RayEventRecorder.

Example usage:

    from ray._common.observability import emit_event, InternalEventBuilder
    from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto

    class MyEventBuilder(InternalEventBuilder):
        def __init__(self, entity_id: str, data: str):
            super().__init__(
                source_type=RayEventProto.SourceType.JOBS,
                event_type=RayEventProto.EventType.MY_EVENT_TYPE,
            )
            self._entity_id = entity_id
            self._data = data

        def get_entity_id(self) -> str:
            return self._entity_id

        def serialize_event_data(self) -> bytes:
            event = MyEventProto()
            event.data = self._data
            return event.SerializeToString()

    # Emit the event
    event = MyEventBuilder(entity_id="123", data="example").build()
    emit_event(event)
"""

from ray._common.observability.emitter import emit_event, emit_events
from ray._common.observability.internal_event import InternalEventBuilder

__all__ = [
    "emit_event",
    "emit_events",
    "InternalEventBuilder",
]
