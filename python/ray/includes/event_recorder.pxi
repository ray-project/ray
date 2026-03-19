from ray.includes.event_recorder cimport (
    CRayEventInterface,
    CreatePythonRayEvent,
    SerializeEventsToRayEventsDataJson,
)
from ray.includes.common cimport move
from libcpp.memory cimport unique_ptr
from libcpp.vector cimport vector as c_vector
from libcpp.string cimport string as c_string


cdef class RayEvent:
    """Python wrapper holding event data for transfer to C++.

    This class stores event metadata and serialized protobuf data. When added
    to the EventRecorder, it creates the underlying C++ RayEvent object.

    Args:
        source_type: Integer value of RayEvent.SourceType enum.
        event_type: Integer value of RayEvent.EventType enum.
        severity: Integer value of RayEvent.Severity enum.
        entity_id: Unique identifier for the event entity (e.g., submission_id).
        message: Optional message associated with the event.
        session_name: The Ray session name.
        serialized_data: Serialized protobuf bytes of the nested event message.
        nested_event_field_number: The field number in RayEvent proto for the
            nested event message. Use RayEventProto.<FIELD>_FIELD_NUMBER constants.
    """
    cdef:
        int _source_type
        int _event_type
        int _severity
        str _entity_id
        str _message
        str _session_name
        bytes _serialized_data
        int _nested_event_field_number

    def __init__(
        self,
        int source_type,
        int event_type,
        int severity,
        str entity_id,
        str message,
        str session_name,
        bytes serialized_data,
        int nested_event_field_number,
    ):
        self._source_type = source_type
        self._event_type = event_type
        self._severity = severity
        self._entity_id = entity_id
        self._message = message
        self._session_name = session_name
        self._serialized_data = serialized_data
        self._nested_event_field_number = nested_event_field_number

    cdef unique_ptr[CRayEventInterface] to_cpp_event(self):
        """Create the underlying C++ event. Ownership is transferred to caller."""
        return CreatePythonRayEvent(
            self._source_type,
            self._event_type,
            self._severity,
            self._entity_id.encode("utf-8"),
            self._message.encode("utf-8"),
            self._session_name.encode("utf-8"),
            self._serialized_data,
            self._nested_event_field_number,
        )

    @property
    def entity_id(self):
        return self._entity_id

    @property
    def event_type(self):
        return self._event_type


def serialize_events_to_ray_events_data_json(list events):
    """Serialize RayEvent objects directly to a JSON array string."""
    cdef c_vector[unique_ptr[CRayEventInterface]] cpp_events
    cdef RayEvent ev
    cdef c_string json_data

    for ev in events:
        cpp_events.push_back(move(ev.to_cpp_event()))

    with nogil:
        json_data = SerializeEventsToRayEventsDataJson(move(cpp_events))
    return json_data
