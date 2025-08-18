"""Exporter API for Ray Data operator events."""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    get_export_event_logger,
)

logger = logging.getLogger(__name__)


@dataclass
class OperatorEvent:
    """Represents an Ray Data operator event, such as issue detection

    Attributes:
        dataset_id: The id of the dataset.
        operator_id: The id of the operator within the DAG structure, typically
            incorporating a position or index (e.g., "ReadParquet_0")
        operator_name: The name of the operator.
        event_time: The timestamp when the event is emitted (in seconds since epoch).
        event_type: The type of the event.
        message: The content of the event message.
    """

    dataset_id: str
    operator_id: str
    operator_name: str
    event_time: float
    event_type: str
    message: str


def operator_event_to_proto(operator_event: OperatorEvent) -> Any:
    """Convert the operator event to a protobuf message.

    Args:
        operator_event: OperatorEvent object containing the event details

    Returns:
        The protobuf message representing the operator event.
    """

    from ray.core.generated.export_dataset_operator_event_pb2 import (
        ExportDatasetOperatorEventData as ProtoOperatorEventData,
    )

    # Create the protobuf message
    proto_operator_event_data = ProtoOperatorEventData(
        dataset_id=operator_event.dataset_id,
        operator_id=operator_event.operator_id,
        operator_name=operator_event.operator_name,
        event_time=operator_event.event_time,
        event_type=ProtoOperatorEventData.DatasetOperatorEventType.Value(
            operator_event.event_type
        ),
        message=operator_event.message,
    )

    return proto_operator_event_data


def get_operator_event_exporter() -> "OperatorEventExporter":
    """Get the operator event exporter instance.

    Returns:
        The operator event exporter instance.
    """
    return LoggerOperatorEventExporter.create_event_exporter()


class OperatorEventExporter(ABC):
    """Abstract base class for operator event exporters.

    Implementations of this interface can export Ray Data operator event to various
    destinations like log files, databases, or monitoring systems.
    """

    @abstractmethod
    def export_operator_event(self, operator_event: OperatorEvent) -> None:
        """Export operator event to the destination.

        Args:
            operator_event: OperatorEvent object containing operator event details.
        """
        pass

    @classmethod
    @abstractmethod
    def create_event_exporter(cls) -> Optional["OperatorEventExporter"]:
        """Create an event exporter instance

        Returns:
            An event exporter instance.
        """
        pass


class LoggerOperatorEventExporter(OperatorEventExporter):
    """Operator event exporter implementation that uses the Ray export event logger.

    This exporter writes operator event to log files using Ray's export event system.
    """

    def __init__(self, logger: logging.Logger):
        """Initialize with a configured export event logger.

        Args:
            logger: The export event logger to use for writing events.
        """
        self._export_logger = logger

    def export_operator_event(self, operator_event: OperatorEvent) -> None:
        """Export operator event using the export event logger.

        Args:
            operator_event: OperatorEvent object containing operator event details.
        """
        operator_event_proto = operator_event_to_proto(operator_event)
        self._export_logger.send_event(operator_event_proto)

    @classmethod
    def create_event_exporter(cls) -> Optional["LoggerOperatorEventExporter"]:
        """Create a logger-based exporter.

        Returns:
            A LoggerOperatorEventExporter instance.
        """
        log_directory = os.path.join(
            ray._private.worker._global_node.get_session_dir_path(), "logs"
        )

        try:
            logger = get_export_event_logger(
                EventLogType.DATASET_OPERATOR_EVENT,
                log_directory,
            )
            return LoggerOperatorEventExporter(logger)
        except Exception:
            logger.exception(
                "Unable to initialize the export event logger, so no operator export "
                "events will be written."
            )
            return None
