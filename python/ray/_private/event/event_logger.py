import logging
import pathlib
import json
import random
import string
import socket
import os

from dataclasses import dataclass
from datetime import datetime

from ray._private.event.event_types import EventTypes

logger = logging.getLogger(__name__)

def get_event_id():
    return "".join([random.choice(string.hexdigits) for _ in range(36)])


class EventLoggerAdapter:
    def __init__(self, source: str, logger: logging.Logger):
        """Adapter for the Python logger that's used to emit events."""
        self.logger = logger
        # Aligned with `event.proto`'s `message Event``
        self.global_context = {
            "source": source,
            "source_hostname": socket.gethostname(),
            "source_pid": os.getpid(),
            "metadata": {},
        }
    
    def set_global_context(self, metadata: dict = None):
        metadata = {} if not metadata else metadata
        self.global_context["metadata"] = metadata
    
    def info(self, event_type: EventTypes, message: str, **kwargs):
        self._emit("INFO", event_type, message, **kwargs)

    def debug(self, event_type: EventTypes, message: str, **kwargs):
        self._emit("DEBUG", event_type, message, **kwargs)

    def warning(self, event_type: EventTypes, message: str, **kwargs):
        self._emit("WARNING", event_type, message, **kwargs)

    def error(self, event_type: EventTypes, message: str, **kwargs):
        self._emit("ERROR", event_type, message, **kwargs)

    def fatal(self, event_type: EventTypes, message: str, **kwargs):
        self._emit("FATAL", event_type, message, **kwargs)

    def _emit(self, severity: str, event_type: EventTypes, message: str, **kwargs):
        allowed_severity = ["INFO", "WARNING", "ERROR", "DEBUG", "FATAL"]
        if severity not in allowed_severity:
            assert False, "Invalid severity {}.".format(severity)
        
        self.global_context["metadata"].update(**kwargs)

        self.logger.info(
            json.dumps(
                {
                    "event_id": get_event_id(),
                    "timestamp": datetime.now().timestamp(),
                    "type": event_type.value,
                    "message": message,
                    "severity": severity,
                    **self.global_context,
                }
            )
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
