import logging
import pathlib
import json
import random
import string
import socket
import os
import threading

from typing import Dict, Optional
from datetime import datetime

from google.protobuf.json_format import MessageToDict, Parse

from ray.core.generated.event_pb2 import Event

global_logger = logging.getLogger(__name__)


def get_event_id():
    return "".join([random.choice(string.hexdigits) for _ in range(36)])


class EventLoggerAdapter:
    def __init__(self, source: Event.SourceType, logger: logging.Logger):
        """Adapter for the Python logger that's used to emit events.

        When events are emitted, they are aggregated and available via
        state API and dashboard.

        This class is thread-safe.
        """
        self.logger = logger
        # Aligned with `event.proto`'s `message Event``
        self.source = source
        self.source_hostname = socket.gethostname()
        self.source_pid = os.getpid()

        # The below fields must be protected by this lock.
        self.lock = threading.Lock()
        # {str -> str} typed dict
        self.global_context = {}

    def set_global_context(self, global_context: Dict[str, str] = None):
        """Set the global metadata.

        This method overwrites the global metadata if it is called more than once.
        """
        with self.lock:
            self.global_context = {} if not global_context else global_context

    def trace(self, message: str, **kwargs):
        self._emit(Event.Severity.TRACE, message, **kwargs)

    def debug(self, message: str, **kwargs):
        self._emit(Event.Severity.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        self._emit(Event.Severity.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._emit(Event.Severity.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        self._emit(Event.Severity.ERROR, message, **kwargs)

    def fatal(self, message: str, **kwargs):
        self._emit(Event.Severity.FATAL, message, **kwargs)

    def _emit(self, severity: Event.Severity, message: str, **kwargs):
        # NOTE: Python logger is thread-safe,
        # so we don't need to protect it using locks.
        event = Event()
        event.event_id = get_event_id()
        event.timestamp = int(datetime.now().timestamp())
        event.message = message
        event.severity = severity
        # TODO(sang): Support event type & schema.
        event.label = ""
        event.source_type = self.source
        event.source_hostname = self.source_hostname
        event.source_pid = self.source_pid
        custom_fields = event.custom_fields
        with self.lock:
            for k, v in self.global_context.items():
                if v is not None and k is not None:
                    custom_fields[k] = v
        for k, v in kwargs.items():
            if v is not None and k is not None:
                custom_fields[k] = v

        self.logger.info(
            json.dumps(
                MessageToDict(
                    event,
                    including_default_value_fields=True,
                    preserving_proto_field_name=True,
                    use_integers_for_enums=False,
                )
            )
        )

        # Force flush so that we won't lose events
        self.logger.handlers[0].flush()


def _build_event_file_logger(source: Event.SourceType, sink_dir: str):
    logger = logging.getLogger("_ray_event_logger")
    logger.setLevel(logging.INFO)
    dir_path = pathlib.Path(sink_dir) / "events"
    filepath = dir_path / f"event_{source}.log"
    dir_path.mkdir(exist_ok=True)
    filepath.touch(exist_ok=True)
    # Configure the logger.
    handler = logging.FileHandler(filepath)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# This lock must be used when accessing or updating global event logger dict.
_event_logger_lock = threading.Lock()
_event_logger = {}


def get_event_logger(source: Event.SourceType, sink_dir: str):
    """Get the event logger of the current process.

    There's only 1 event logger per (process, source).

    TODO(sang): Support more impl than file-based logging.
                Currently, the interface also ties to the
                file-based logging impl.

    Args:
        source: The source of the event.
        sink_dir: The directory to sink event logs.
    """
    with _event_logger_lock:
        global _event_logger
        source_name = Event.SourceType.Name(source)
        if source_name not in _event_logger:
            logger = _build_event_file_logger(source_name, sink_dir)
            _event_logger[source_name] = EventLoggerAdapter(source, logger)

        return _event_logger[source_name]


def parse_event(event_str: str) -> Optional[Event]:
    """Parse an event from a string.

    Args:
        event_str: The string to parse. Expect to be a JSON serialized
            Event protobuf.

    Returns:
        The parsed event if parsable, else None
    """
    try:
        return Parse(event_str, Event())
    except Exception:
        global_logger.exception(f"Failed to parse event: {event_str}")
        return None


def filter_event_by_level(event: Event, filter_event_level: str) -> bool:
    """Filter an event based on event level.

    Args:
        event: The event to filter.
        filter_event_level: The event level string to filter by. Any events
            that are lower than this level will be filtered.

    Returns:
        True if the event should be filtered, else False.
    """

    event_levels = {
        Event.Severity.TRACE: 0,
        Event.Severity.DEBUG: 1,
        Event.Severity.INFO: 2,
        Event.Severity.WARNING: 3,
        Event.Severity.ERROR: 4,
        Event.Severity.FATAL: 5,
    }

    filter_event_level = filter_event_level.upper()
    filter_event_level = Event.Severity.Value(filter_event_level)

    if event_levels[event.severity] < event_levels[filter_event_level]:
        return True

    return False
