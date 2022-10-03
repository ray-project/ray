import logging
import pathlib
import json
import random
import string
import socket
import os

from typing import Dict
from dataclasses import dataclass
from datetime import datetime

from google.protobuf.json_format import MessageToDict

from ray._private.event.event_types import EventTypes
from ray.core.generated.event_pb2 import Event

logger = logging.getLogger(__name__)


def get_event_id():
    return "".join([random.choice(string.hexdigits) for _ in range(36)])


class EventLoggerAdapter:
    def __init__(self, source: Event.SourceType, logger: logging.Logger):
        """Adapter for the Python logger that's used to emit events."""
        self.logger = logger
        # Aligned with `event.proto`'s `message Event``
        self.source = source
        self.source_hostname = socket.gethostname()
        self.source_pid = os.getpid()
        # {str -> str} typed dict
        self.global_context = {}

    def set_global_context(self, global_context: Dict[str, str] = None):
        """Set the global metadata.
        
        This method overwrites the global metadata if it is called more than once.
        """
        self.global_context = {} if not global_context else global_context

    def info(self, event_type: EventTypes, message: str, **kwargs):
        self._emit(Event.Severity.INFO, event_type, message, **kwargs)

    def warning(self, event_type: EventTypes, message: str, **kwargs):
        self._emit(Event.Severity.WARNING, event_type, message, **kwargs)

    def error(self, event_type: EventTypes, message: str, **kwargs):
        self._emit(Event.Severity.ERROR, event_type, message, **kwargs)

    def fatal(self, event_type: EventTypes, message: str, **kwargs):
        self._emit(Event.Severity.FATAL, event_type, message, **kwargs)

    def _emit(self, severity: Event.Severity, event_type: EventTypes, message: str, **kwargs):
        event = Event()
        event.event_id = get_event_id()
        event.timestamp = datetime.now().timestamp()
        event.message = message
        event.severity = severity
        event.label = event_type.value
        event.source_type = self.source
        event.source_hostname = self.source_hostname
        event.source_pid = self.source_pid
        custom_fields = event.custom_fields
        for k, v in self.global_context.items():
            custom_fields[k] = v
        for k, v in self.kwargs.items():
            custom_fields[k] = v

        MessageToDict(event, including_default_value_fields=True, preserving_proto_field_name=True, use_integers_for_enums=True)
        self.logger.info(
            json.dumps(MessageToDict(event, including_default_value_fields=True, preserving_proto_field_name=True, use_integers_for_enums=True))
        )
        # Force flush so that we won't lose events
        self.logger.handlers[0].flush()


@dataclass
class EventLoggerOption:
    sink_dir: str
    source: str
    type: str = "file"


def _build_event_file_logger(option: EventLoggerOption):
    # TODO(sang): Support different logger setup based on the env var.
    logger = logging.getLogger("_ray_event_logger")
    logger.setLevel(logging.INFO)
    dir_path = pathlib.Path(option.sink_dir) / "events"
    filepath = dir_path / f"event_{option.source}.log"
    dir_path.mkdir(exist_ok=True)
    filepath.touch(exist_ok=True)
    # Configure the logger.
    handler = logging.FileHandler(filepath)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


_event_logger = None


def get_event_logger(option: EventLoggerOption):
    """Get the event logger of the current process.

    There's only 1 event logger per process.
    """
    global _event_logger
    if _event_logger is None:
        logger = _build_event_file_logger(option)
        _event_logger = EventLoggerAdapter(option.source, logger)

    return _event_logger
