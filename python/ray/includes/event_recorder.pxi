"""Cython bindings for RayEventRecorder.

This module provides Python access to the C++ RayEventRecorder for emitting
internal Ray events from Python code (e.g., submission job events).
"""

from ray.includes.event_recorder cimport (
    CRayEventInterface,
    CPythonEventRecorder,
    CreatePythonRayEvent,
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


cdef class EventRecorder:
    """Python wrapper for PythonEventRecorder (singleton)."""
    cdef unique_ptr[CPythonEventRecorder] _recorder

    def is_initialized(self):
        """Check if the recorder has been initialized."""
        return self._recorder.get() != NULL

    def initialize(
        self,
        str aggregator_address,
        int aggregator_port,
        str node_ip,
        str node_id_hex,
        int max_buffer_size,
    ):
        """Initialize the underlying C++ PythonEventRecorder.

        No-op if already initialized.

        Args:
            aggregator_address: Address of the event aggregator server.
            aggregator_port: Port of the event aggregator server.
            node_ip: IP address of the current node.
            node_id_hex: Hex-encoded node ID.
            max_buffer_size: Maximum number of events to buffer.
        """
        if self._recorder.get() != NULL:
            return

        self._recorder.reset(
            new CPythonEventRecorder(
                aggregator_address.encode("utf-8"),
                aggregator_port,
                node_ip.encode("utf-8"),
                node_id_hex.encode("utf-8"),
                max_buffer_size,
            )
        )

    def shutdown(self):
        """Shutdown the recorder.

        Stops exporting events, performs a final flush, and releases all
        C++ resources. After this call, events will be silently dropped
        until initialize() is called again.
        """
        if self._recorder.get() != NULL:
            self._recorder.get().Shutdown()
        self._recorder.reset()

    def add_events(self, list events):
        """Add events to the recorder buffer.

        Events will be periodically sent to the event aggregator.

        Args:
            events: List of RayEvent objects to add.

        Returns:
            True if events were added, False if recorder not initialized.
        """
        if self._recorder.get() == NULL:
            return False

        cdef c_vector[unique_ptr[CRayEventInterface]] cpp_events
        cdef RayEvent event

        for event in events:
            cpp_events.push_back(move(event.to_cpp_event()))

        with nogil:
            self._recorder.get().AddEvents(move(cpp_events))

        return True


# Global singleton EventRecorder instance.
cdef EventRecorder _global_event_recorder = EventRecorder()


def _get_global_event_recorder():
    """Get the global EventRecorder singleton.

    Returns:
        The global EventRecorder instance (may not be initialized).
    """
    return _global_event_recorder


def initialize_event_recorder(
    str aggregator_address,
    int aggregator_port,
    str node_ip,
    str node_id_hex,
    int max_buffer_size,
):
    """Initialize the global event recorder.

    Args:
        aggregator_address: Address of the event aggregator server.
        aggregator_port: Port of the event aggregator server.
        node_ip: IP address of the current node.
        node_id_hex: Hex-encoded node ID.
        max_buffer_size: Maximum number of events to buffer.
    """
    _global_event_recorder.initialize(
        aggregator_address, aggregator_port, node_ip, node_id_hex, max_buffer_size,
    )


def shutdown_event_recorder():
    """Shutdown the global event recorder.

    Stops exporting events, performs a final flush, and releases all
    C++ resources. After this call, events will be silently dropped
    until initialize_event_recorder() is called again.
    """
    _global_event_recorder.shutdown()
