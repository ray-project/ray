from typing import Dict, Optional

from ray._common.observability.internal_event import InternalEventBuilder
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto
from ray.core.generated.platform_event_pb2 import PlatformEvent, Source
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class PlatformEventBuilder(InternalEventBuilder):
    """Builder for creating infrastructure PlatformEvents (e.g., from Kubernetes)."""

    def __init__(
        self,
        event_uid: str,
        platform: int = Source.Platform.PLATFORM_UNSPECIFIED,  # Source.Platform enum
        object_kind: str = "",
        object_name: str = "",
        reason: str = "",
        message: str = "",
        severity: int = RayEventProto.Severity.INFO,
        component: str = "",
        source_metadata: Optional[Dict[str, str]] = None,
        custom_fields: Optional[Dict[str, str]] = None,
        session_name: str = "",
    ):
        super().__init__(
            source_type=RayEventProto.SourceType.CLUSTER_LIFECYCLE,
            event_type=RayEventProto.EventType.PLATFORM_EVENT,
            nested_event_field_number=RayEventProto.PLATFORM_EVENT_FIELD_NUMBER,
            severity=severity,
            message=message,
            session_name=session_name,
        )
        self._event_uid = event_uid
        self._platform = platform
        self._object_kind = object_kind
        self._object_name = object_name
        self._reason = reason
        self._component = component
        self._source_metadata = source_metadata
        self._custom_fields = custom_fields

    def get_entity_id(self) -> str:
        return self._event_uid

    def serialize_event_data(self) -> bytes:
        source_proto = Source(
            platform=self._platform,
            component=self._component,
        )
        if self._source_metadata:
            for k, v in self._source_metadata.items():
                source_proto.metadata[k] = v

        event = PlatformEvent(
            source=source_proto,
            object_kind=self._object_kind,
            object_name=self._object_name,
            message=self._message,
            reason=self._reason,
        )
        if self._custom_fields:
            for k, v in self._custom_fields.items():
                event.custom_fields[k] = v

        return event.SerializeToString()
