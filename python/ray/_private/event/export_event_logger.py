import logging
import pathlib
import json
import random
import string
import threading

from typing import Union
from datetime import datetime
from ray._private import ray_constants
from ray.core.generated.export_event_pb2 import ExportEvent
from ray.core.generated.export_submission_job_event_pb2 import (
    ExportSubmissionJobEventData,
)
from ray._private.protobuf_compat import message_to_dict

global_logger = logging.getLogger(__name__)

# This contains the union of export event data types which emit events
# using the python ExportEventLoggerAdapter
ExportEventDataType = Union[ExportSubmissionJobEventData]


def generate_event_id():
    return "".join([random.choice(string.hexdigits) for _ in range(18)])


class ExportEventLoggerAdapter:
    def __init__(self, source: ExportEvent.SourceType, logger: logging.Logger):
        """Adapter for the Python logger that's used to emit export events."""
        self.logger = logger
        self.source = source

    def send_event(self, event_data: ExportEventDataType):
        # NOTE: Python logger is thread-safe,
        # so we don't need to protect it using locks.
        try:
            event = self._create_export_event(event_data)
        except TypeError:
            global_logger.exception(
                "Failed to create ExportEvent from event_data so no "
                "event will be written to file."
            )
            return

        event_as_str = self._export_event_to_string(event)

        self.logger.info(event_as_str)
        # Force flush so that we won't lose events
        self.logger.handlers[0].flush()

    def _create_export_event(self, event_data: ExportEventDataType) -> ExportEvent:
        event = ExportEvent()
        event.event_id = generate_event_id()
        event.timestamp = int(datetime.now().timestamp())
        if isinstance(event_data, ExportSubmissionJobEventData):
            event.submission_job_event_data.CopyFrom(event_data)
            event.source_type = ExportEvent.SourceType.EXPORT_SUBMISSION_JOB
        else:
            raise TypeError(f"Invalid event_data type: {type(event_data)}")
        if event.source_type != self.source:
            global_logger.error(
                f"event_data has source type {event.source_type}, however "
                f"the event was sent to a logger with source {self.source}. "
                f"The event will still be written to the file of {self.source} "
                "but this indicates a bug in the code."
            )
            pass
        return event

    def _export_event_to_string(self, event: ExportEvent) -> str:
        event_data_json = {}
        proto_to_dict_options = {
            "always_print_fields_with_no_presence": True,
            "preserving_proto_field_name": True,
            "use_integers_for_enums": False,
        }
        event_data_field_set = event.WhichOneof("event_data")
        if event_data_field_set:
            event_data_json = message_to_dict(
                getattr(event, event_data_field_set),
                **proto_to_dict_options,
            )
        else:
            global_logger.error(
                f"event_data missing from export event with id {event.event_id} "
                f"and type {event.source_type}. An empty event will be written, "
                "but this indicates a bug in the code. "
            )
            pass
        event_json = {
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "source_type": ExportEvent.SourceType.Name(event.source_type),
            "event_data": event_data_json,
        }
        return json.dumps(event_json)


def _build_export_event_file_logger(
    source: ExportEvent.SourceType, sink_dir: str
) -> logging.Logger:
    logger = logging.getLogger("_ray_export_event_logger_" + source)
    logger.setLevel(logging.INFO)
    dir_path = pathlib.Path(sink_dir) / "export_events"
    filepath = dir_path / f"event_{source}.log"
    dir_path.mkdir(exist_ok=True)
    filepath.touch(exist_ok=True)
    # Configure the logger.
    # Default is 100 MB max file size
    handler = logging.handlers.RotatingFileHandler(
        filepath,
        maxBytes=(ray_constants.RAY_EXPORT_EVENT_MAX_FILE_SIZE_BYTES),
        backupCount=ray_constants.RAY_EXPORT_EVENT_MAX_BACKUP_COUNT,
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# This lock must be used when accessing or updating global event logger dict.
_export_event_logger_lock = threading.Lock()
_export_event_logger = {}


def get_export_event_logger(
    source: ExportEvent.SourceType, sink_dir: str
) -> logging.Logger:
    """Get the export event logger of the current process.

    There's only one logger per export event source.

    Args:
        source: The source of the export event.
        sink_dir: The directory to sink event logs.
    """
    with _export_event_logger_lock:
        global _export_event_logger
        source_name = ExportEvent.SourceType.Name(source)
        if source_name not in _export_event_logger:
            logger = _build_export_event_file_logger(source_name, sink_dir)
            _export_event_logger[source_name] = ExportEventLoggerAdapter(source, logger)

        return _export_event_logger[source_name]


def check_export_api_enabled(
    source: ExportEvent.SourceType,
) -> bool:
    """
    Check RAY_ENABLE_EXPORT_API_WRITE and RAY_ENABLE_EXPORT_API_WRITE_CONFIG environment
    variables to verify if export events should be written for the given source type.

    Args:
        source: The source of the export event.
    """
    if ray_constants.RAY_ENABLE_EXPORT_API_WRITE:
        return True
    source_name = ExportEvent.SourceType.Name(source)
    return (
        source_name in ray_constants.RAY_ENABLE_EXPORT_API_WRITE_CONFIG
        if ray_constants.RAY_ENABLE_EXPORT_API_WRITE_CONFIG
        else False
    )
