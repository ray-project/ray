import json
import logging
import pathlib
import random
import string
import threading
from datetime import datetime
from enum import Enum
from typing import Union

from ray._private import ray_constants
from ray._private.protobuf_compat import message_to_dict
from ray.core.generated.export_dataset_metadata_pb2 import (
    ExportDatasetMetadata,
)
from ray.core.generated.export_dataset_operator_event_pb2 import (
    ExportDatasetOperatorEventData,
)
from ray.core.generated.export_event_pb2 import ExportEvent
from ray.core.generated.export_submission_job_event_pb2 import (
    ExportSubmissionJobEventData,
)
from ray.core.generated.export_train_state_pb2 import (
    ExportTrainRunAttemptEventData,
    ExportTrainRunEventData,
)

global_logger = logging.getLogger(__name__)

# This contains the union of export event data types which emit events
# using the python ExportEventLoggerAdapter
ExportEventDataType = Union[
    ExportSubmissionJobEventData,
    ExportTrainRunEventData,
    ExportTrainRunAttemptEventData,
    ExportDatasetMetadata,
    ExportDatasetOperatorEventData,
]


class EventLogType(Enum):
    """Enum class representing different types of export event logs.

    Each enum value contains a log type name and a set of supported event data types.

    Attributes:
        TRAIN_STATE: Export events related to training state, supporting train run and attempt events.
        SUBMISSION_JOB: Export events related to job submissions.
        DATASET_METADATA: Export events related to dataset metadata.
        DATASET_OPERATOR_EVENT: Export events related to Ray Data operator.
    """

    TRAIN_STATE = (
        "EXPORT_TRAIN_STATE",
        {ExportTrainRunEventData, ExportTrainRunAttemptEventData},
    )
    SUBMISSION_JOB = ("EXPORT_SUBMISSION_JOB", {ExportSubmissionJobEventData})
    DATASET_METADATA = ("EXPORT_DATASET_METADATA", {ExportDatasetMetadata})
    DATASET_OPERATOR_EVENT = (
        "EXPORT_DATASET_OPERATOR_EVENT",
        {ExportDatasetOperatorEventData},
    )

    def __init__(self, log_type_name: str, event_types: set[ExportEventDataType]):
        """Initialize an EventLogType enum value.

        Args:
            log_type_name: String identifier for the log type. This name is used to construct the log file name.
                See `_build_export_event_file_logger` for more details.
            event_types: Set of event data types that this log type supports.
        """
        self.log_type_name = log_type_name
        self.event_types = event_types

    def supports_event_type(self, event_type: ExportEventDataType) -> bool:
        """Check if this log type supports the given event data type.

        Args:
            event_type: The event data type to check for support.

        Returns:
            bool: True if the event type is supported, False otherwise.
        """
        return type(event_type) in self.event_types


def generate_event_id():
    return "".join([random.choice(string.hexdigits) for _ in range(18)])


class ExportEventLoggerAdapter:
    def __init__(self, log_type: EventLogType, logger: logging.Logger):
        """Adapter for the Python logger that's used to emit export events."""
        self.logger = logger
        self.log_type = log_type

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
        elif isinstance(event_data, ExportTrainRunEventData):
            event.train_run_event_data.CopyFrom(event_data)
            event.source_type = ExportEvent.SourceType.EXPORT_TRAIN_RUN
        elif isinstance(event_data, ExportTrainRunAttemptEventData):
            event.train_run_attempt_event_data.CopyFrom(event_data)
            event.source_type = ExportEvent.SourceType.EXPORT_TRAIN_RUN_ATTEMPT
        elif isinstance(event_data, ExportDatasetMetadata):
            event.dataset_metadata.CopyFrom(event_data)
            event.source_type = ExportEvent.SourceType.EXPORT_DATASET_METADATA
        elif isinstance(event_data, ExportDatasetOperatorEventData):
            event.dataset_operator_event_data.CopyFrom(event_data)
            event.source_type = ExportEvent.SourceType.EXPORT_DATASET_OPERATOR_EVENT
        else:
            raise TypeError(f"Invalid event_data type: {type(event_data)}")
        if not self.log_type.supports_event_type(event_data):
            global_logger.error(
                f"event_data has source type {event.source_type}, however "
                f"the event was sent to a logger with log type {self.log_type.log_type_name}. "
                f"The event will still be written to the file of {self.log_type.log_type_name} "
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
    log_type_name: str, sink_dir: str
) -> logging.Logger:
    logger = logging.getLogger("_ray_export_event_logger_" + log_type_name)
    logger.setLevel(logging.INFO)
    dir_path = pathlib.Path(sink_dir) / "export_events"
    filepath = dir_path / f"event_{log_type_name}.log"
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


def get_export_event_logger(log_type: EventLogType, sink_dir: str) -> logging.Logger:
    """Get the export event logger of the current process.

    There's only one logger per export event source.

    Args:
        log_type: The type of the export event.
        sink_dir: The directory to sink event logs.
    """
    with _export_event_logger_lock:
        global _export_event_logger
        log_type_name = log_type.log_type_name
        if log_type_name not in _export_event_logger:
            logger = _build_export_event_file_logger(log_type.log_type_name, sink_dir)
            _export_event_logger[log_type_name] = ExportEventLoggerAdapter(
                log_type, logger
            )

        return _export_event_logger[log_type_name]


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
