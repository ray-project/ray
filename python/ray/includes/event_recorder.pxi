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

import logging

logger = logging.getLogger(__name__)


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
    """Per-process singleton for recording Ray events.

    Access the singleton and its lifecycle via static methods::

        # Initialization (once per process)
        EventRecorder.initialize(
            aggregator_address, aggregator_port,
            node_ip, node_id_hex, max_buffer_size,
        )

        # Emit events (no-op if not initialized)
        EventRecorder.emit(event)
        EventRecorder.emit_batch([event1, event2])

        # Shutdown (flushes buffered events)
        EventRecorder.shutdown()

    Thread safety: initialize() and shutdown() are expected to be called from
    the main thread (same as ray.init/ray.shutdown). emit() and emit_batch()
    are safe to call from any thread — the GIL serializes Python-level access,
    and the underlying C++ RayEventRecorder is protected by absl::Mutex.
    """
    cdef unique_ptr[CPythonEventRecorder] _recorder

    def __dealloc__(self):
        """Safety-net cleanup. C++ destructor also calls Shutdown()."""
        if self._recorder.get() != NULL:
            self._recorder.get().Shutdown()
        self._recorder.reset()

    @staticmethod
    def initialize(
        str aggregator_address,
        int aggregator_port,
        str node_ip,
        str node_id_hex,
        int max_buffer_size,
        str metric_source = "python",
    ):
        """Initialize the per-process event recorder.

        Creates the underlying C++ PythonEventRecorder with a background I/O
        thread and gRPC client. No-op if already initialized.

        Args:
            aggregator_address: Address of the event aggregator server.
            aggregator_port: Port of the event aggregator server.
            node_ip: IP address of the current node.
            node_id_hex: Hex-encoded node ID.
            max_buffer_size: Maximum number of events to buffer.
            metric_source: Label for the "Source" tag on dropped-events metrics
                (default "python").
        """
        if EventRecorder._instance is not None:
            return

        cdef EventRecorder rec = EventRecorder()
        rec._recorder.reset(
            new CPythonEventRecorder(
                aggregator_address.encode("utf-8"),
                aggregator_port,
                node_ip.encode("utf-8"),
                node_id_hex.encode("utf-8"),
                max_buffer_size,
                metric_source.encode("utf-8"),
            )
        )
        EventRecorder._instance = rec

    @staticmethod
    def instance():
        """Get the per-process EventRecorder singleton.

        Returns:
            The EventRecorder instance if initialized, None otherwise.
        """
        return EventRecorder._instance

    @staticmethod
    def shutdown():
        """Shutdown the event recorder.

        Stops exporting events, performs a final flush, and releases all
        C++ resources. After this call, emit() and emit_batch() will be
        no-ops until initialize() is called again.
        """
        if EventRecorder._instance is None:
            return

        cdef EventRecorder rec = <EventRecorder>EventRecorder._instance
        if rec._recorder.get() != NULL:
            rec._recorder.get().Shutdown()
        rec._recorder.reset()
        EventRecorder._instance = None

    @staticmethod
    def emit(RayEvent event):
        """Emit a single event. No-op if not initialized.

        Args:
            event: A RayEvent object (created via InternalEventBuilder.build()).

        Returns:
            True if the event was successfully queued, False otherwise.
        """
        return EventRecorder.emit_batch([event])

    @staticmethod
    def emit_batch(list events):
        """Emit multiple events. No-op if not initialized.

        Args:
            events: List of RayEvent objects to emit.

        Returns:
            True if events were successfully queued, False otherwise.
        """
        if not events:
            return True

        if EventRecorder._instance is None:
            logger.debug(
                "Event recorder not initialized, dropping %d events",
                len(events),
            )
            return False

        cdef EventRecorder rec = <EventRecorder>EventRecorder._instance
        cdef c_vector[unique_ptr[CRayEventInterface]] cpp_events
        cdef RayEvent ev

        for ev in events:
            cpp_events.push_back(move(ev.to_cpp_event()))

        with nogil:
            rec._recorder.get().AddEvents(move(cpp_events))

        return True


# Singleton state lives on the class itself as a Python class attribute.
# Cython cdef class doesn't support class-level attributes in the body,
# so we set it after the class definition.
EventRecorder._instance = None
