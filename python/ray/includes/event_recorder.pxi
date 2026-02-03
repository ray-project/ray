"""Cython bindings for RayEventRecorder.

This module provides Python access to the C++ RayEventRecorder for emitting
internal Ray events from Python code (e.g., submission job events).
"""

from ray.includes.event_recorder cimport (
    CRayEventInterface,
    CRayEventRecorderInterface,
    CreatePythonRayEvent,
)
from ray.includes.common cimport move
from libcpp.memory cimport unique_ptr
from libcpp.vector cimport vector as c_vector
from libcpp.string cimport string as c_string


cdef class RayEvent:
    """Python wrapper holding event data for transfer to C++.

    This class stores event metadata and serialized protobuf data. When added
    to the EventRecorder, it creates the underlying C++ event object.

    Args:
        source_type: Integer value of RayEvent.SourceType enum.
        event_type: Integer value of RayEvent.EventType enum.
        severity: Integer value of RayEvent.Severity enum.
        entity_id: Unique identifier for the event entity (e.g., submission_id).
        message: Optional message associated with the event.
        session_name: The Ray session name.
        serialized_data: Serialized protobuf bytes of the nested event message.
    """
    cdef:
        int _source_type
        int _event_type
        int _severity
        str _entity_id
        str _message
        str _session_name
        bytes _serialized_data

    def __init__(
        self,
        int source_type,
        int event_type,
        int severity,
        str entity_id,
        str message,
        str session_name,
        bytes serialized_data,
    ):
        self._source_type = source_type
        self._event_type = event_type
        self._severity = severity
        self._entity_id = entity_id
        self._message = message
        self._session_name = session_name
        self._serialized_data = serialized_data

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
        )

    @property
    def entity_id(self):
        return self._entity_id

    @property
    def event_type(self):
        return self._event_type


cdef class EventRecorder:
    """Python wrapper for RayEventRecorderInterface (singleton).

    This class wraps the C++ event recorder and provides methods to add
    events from Python. The underlying recorder is set during CoreWorker
    or GCS initialization.
    """
    cdef CRayEventRecorderInterface* _recorder

    def __cinit__(self):
        self._recorder = NULL

    cdef void _set_recorder(self, CRayEventRecorderInterface* recorder):
        """Set the underlying C++ recorder. Called during worker init."""
        self._recorder = recorder

    def is_initialized(self):
        """Check if the recorder has been initialized."""
        return self._recorder != NULL

    def add_events(self, list events):
        """Add events to the recorder buffer.

        Events will be periodically sent to the event aggregator.

        Args:
            events: List of RayEvent objects to add.

        Returns:
            True if events were added, False if recorder not initialized.
        """
        if self._recorder == NULL:
            return False

        cdef c_vector[unique_ptr[CRayEventInterface]] cpp_events
        cdef RayEvent event

        for event in events:
            cpp_events.push_back(move(event.to_cpp_event()))

        with nogil:
            self._recorder.AddEvents(move(cpp_events))

        return True


# Global singleton EventRecorder instance.
# Note: The recorder needs to be initialized by calling set_event_recorder()
# with a valid CRayEventRecorderInterface pointer from C++ code.
# Until that happens, is_initialized() returns False and events are dropped.
cdef EventRecorder _global_event_recorder = EventRecorder()


def _get_global_event_recorder():
    """Get the global EventRecorder singleton.

    Returns:
        The global EventRecorder instance (may not be initialized).
    """
    return _global_event_recorder
