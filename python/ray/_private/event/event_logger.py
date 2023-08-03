import logging
import pathlib
import json
import random
import string
import socket
import os
import threading

from typing import Dict
from datetime import datetime

from google.protobuf.json_format import MessageToDict

from ray.core.generated.event_pb2 import Event


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
