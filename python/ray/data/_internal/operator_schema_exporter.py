"""Exporter API for Ray Data operator schema."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    check_export_api_enabled,
    get_export_event_logger,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OperatorSchema:
    """Represents a Ray Data operator schema

    Attributes:
        operator_uuid: The uuid of the operator.
        schema_fields: The schema fields of the operator.
    """

    operator_uuid: str
    schema_fields: Dict[str, str]  # Mapping from name to type


def operator_schema_to_proto(operator_schema: OperatorSchema) -> Any:
    """Convert the operator schema to a protobuf message.

    Args:
        operator_schema: OperatorSchema object containing the schema details

    Returns:
        The protobuf message representing the operator schema.
    """

    from ray.core.generated.export_dataset_operator_schema_pb2 import (
        ExportDatasetOperatorSchema as ProtoOperatorSchema,
    )

    # Create the protobuf message
    proto_operator_schema = ProtoOperatorSchema(
        operator_uuid=operator_schema.operator_uuid,
        schema_fields=operator_schema.schema_fields,
    )

    return proto_operator_schema


def get_operator_schema_exporter() -> Optional["LoggerOperatorSchemaExporter"]:
    """Get the operator schema exporter instance.

    Returns:
        The operator schema exporter instance.
    """
    return LoggerOperatorSchemaExporter.create_if_enabled()


class OperatorSchemaExporter(ABC):
    """Abstract base class for operator schema exporters.

    Implementations of this interface can export Ray Data operator schema to various
    destinations like log files, databases, or monitoring systems.
    """

    @abstractmethod
    def export_operator_schema(self, operator_schema: OperatorSchema) -> None:
        """Export operator schema to the destination.

        Args:
            operator_schema: OperatorSchema object containing operator schema details.
        """
        pass

    @classmethod
    @abstractmethod
    def create_if_enabled(cls) -> Optional["OperatorSchemaExporter"]:
        """Create a schema exporter instance if the export functionality is enabled.

        Returns:
            A schema exporter instance if enabled, none otherwise.
        """
        pass


class LoggerOperatorSchemaExporter(OperatorSchemaExporter):
    """Operator schema exporter implementation that uses the Ray export event logger.

    This exporter writes operator schema to log files using Ray's export event system.
    """

    def __init__(self, logger: logging.Logger):
        """Initialize with a configured export event logger.

        Args:
            logger: The export event logger to use for writing events.
        """
        self._export_logger = logger

    def export_operator_schema(self, operator_schema: OperatorSchema) -> None:
        """Export operator schema using the export event logger.

        Args:
            operator_schema: OperatorSchema object containing operator event details.
        """
        operator_schema_proto = operator_schema_to_proto(operator_schema)
        self._export_logger.send_event(operator_schema_proto)

    @classmethod
    def create_if_enabled(cls) -> Optional["LoggerOperatorSchemaExporter"]:
        """Create a logger-based exporter if the export API is enabled.

        Returns:
            A LoggerOperatorSchemaExporter instance, none otherwise.
        """
        from ray.core.generated.export_event_pb2 import ExportEvent

        is_operator_schema_export_api_enabled = check_export_api_enabled(
            ExportEvent.SourceType.EXPORT_DATASET_OPERATOR_SCHEMA
        )
        if not is_operator_schema_export_api_enabled:
            # The export API is not enabled, so we shouldn't create an exporter
            return None

        log_directory = ray._private.worker._global_node.get_logs_dir_path()

        try:
            export_logger = get_export_event_logger(
                EventLogType.DATASET_OPERATOR_SCHEMA,
                log_directory,
            )
            return LoggerOperatorSchemaExporter(export_logger)
        except Exception:
            logger.exception(
                "Unable to initialize the export event logger, so no operator export "
                "schema will be written."
            )
            return None
