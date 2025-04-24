"""Metadata exporter API for Ray Data datasets."""

from abc import ABC, abstractmethod
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    get_export_event_logger,
    check_export_api_enabled,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )

UNKNOWN = "unknown"

# NOTE: These dataclasses need to be updated in sync with the protobuf definitions in
# src/ray/protobuf/export_api/export_dataset_metadata.proto
@dataclass
class SubStage:
    """Represents a sub-stage within an operator in the DAG.

    Attributes:
        name: The name of the sub-stage.
        id: The unique identifier of the sub-stage.
    """

    name: str
    id: str


@dataclass
class Operator:
    """Represents a data processing operator in the DAG.

    Attributes:
        name: The name of the operator.
        id: The unique identifier of the operator within the DAG structure, typically
            incorporating a position or index (e.g., "ReadParquet_0"). This is used for
            referencing operators within the DAG topology.
        uuid: The system-generated UUID of the physical operator instance. This is the
            internal unique identifier created when the operator instance is initialized
            and remains consistent throughout its lifetime.
        input_dependencies: List of operator IDs that this operator depends on for input.
        sub_stages: List of sub-stages contained within this operator.
    """

    name: str
    id: str
    uuid: str
    input_dependencies: List[str] = field(default_factory=list)
    sub_stages: List[SubStage] = field(default_factory=list)


@dataclass
class Topology:
    """Represents the complete structure of the operator DAG.

    Attributes:
        operators: List of all operators in the DAG.
    """

    operators: List[Operator] = field(default_factory=list)

    @staticmethod
    def create_topology_metadata(
        dag: "PhysicalOperator", op_to_id: Dict["PhysicalOperator", str]
    ) -> "Topology":
        """Create a Topology structure from the physical operator DAG.

        Args:
            dag: The operator DAG to analyze.

        Returns:
            A Topology object representing the operator DAG structure.
        """
        # Create the result structure
        result = Topology()

        # Add detailed operator information with dependencies
        for op in dag.post_order_iter():
            op_id = op_to_id[op]

            # Create operator object
            operator = Operator(
                name=op.name,
                id=op_id,
                uuid=op.id,
                input_dependencies=[
                    op_to_id[dep] for dep in op.input_dependencies if dep in op_to_id
                ],
            )

            # Add sub-stages if they exist
            if hasattr(op, "_sub_progress_bar_names") and op._sub_progress_bar_names:
                for j, sub_name in enumerate(op._sub_progress_bar_names):
                    sub_stage_id = f"{op_id}_sub_{j}"
                    operator.sub_stages.append(SubStage(name=sub_name, id=sub_stage_id))

            result.operators.append(operator)

        return result


@dataclass
class DatasetMetadata:
    """Metadata about a Ray Data dataset.

    This class represents the metadata associated with a dataset, including its provenance
    information and execution details.

    Attributes:
        job_id: The ID of the job running this dataset.
        topology: The structure of the dataset's operator DAG.
        dataset_id: The unique ID of the dataset.
        start_time: The timestamp when the dataset execution started.
    """

    job_id: str
    topology: Topology
    dataset_id: str
    start_time: float


def dataset_metadata_to_proto(dataset_metadata: DatasetMetadata) -> Any:
    """Convert the dataset metadata to a protobuf message.

    Args:
        dataset_metadata: DatasetMetadata object containing the dataset's
            information and DAG structure.

    Returns:
        The protobuf message representing the dataset metadata.
    """
    from ray.core.generated.export_dataset_metadata_pb2 import (
        ExportDatasetMetadata as ProtoDatasetMetadata,
        Topology as ProtoTopology,
        Operator as ProtoOperator,
        SubStage as ProtoSubStage,
    )

    # Create the protobuf message
    proto_dataset_metadata = ProtoDatasetMetadata()
    proto_topology = ProtoTopology()

    # Add operators to the DAG
    for op in dataset_metadata.topology.operators:
        proto_operator = ProtoOperator(
            name=op.name,
            id=op.id,
            uuid=op.uuid,
        )

        # Add input dependencies
        for dep_id in op.input_dependencies:
            proto_operator.input_dependencies.append(dep_id)

        # Add sub-stages
        for sub_stage in op.sub_stages:
            proto_sub_stage = ProtoSubStage(
                name=sub_stage.name,
                id=sub_stage.id,
            )
            proto_operator.sub_stages.append(proto_sub_stage)

        # Add the operator to the DAG
        proto_topology.operators.append(proto_operator)

    # Populate the data metadata proto
    proto_dataset_metadata = ProtoDatasetMetadata(
        dataset_id=dataset_metadata.dataset_id,
        job_id=dataset_metadata.job_id,
        start_time=dataset_metadata.start_time,
    )
    proto_dataset_metadata.topology.CopyFrom(proto_topology)

    return proto_dataset_metadata


def get_dataset_metadata_exporter() -> "DatasetMetadataExporter":
    """Get the dataset metadata exporter instance.

    Returns:
        The dataset metadata exporter instance.
    """
    return LoggerDatasetMetadataExporter.create_if_enabled()


class DatasetMetadataExporter(ABC):
    """Abstract base class for dataset metadata exporters.

    Implementations of this interface can export Ray Data metadata to various destinations
    like log files, databases, or monitoring systems.
    """

    @abstractmethod
    def export_dataset_metadata(self, dataset_metadata: DatasetMetadata) -> None:
        """Export dataset metadata to the destination.

        Args:
            dataset_metadata: DatasetMetadata object containing dataset information.
        """
        pass

    @classmethod
    @abstractmethod
    def create_if_enabled(cls) -> Optional["DatasetMetadataExporter"]:
        """Create an exporter instance if the export functionality is enabled.

        Returns:
            An exporter instance if enabled, None otherwise.
        """
        pass


class LoggerDatasetMetadataExporter(DatasetMetadataExporter):
    """Dataset metadata exporter implementation that uses the Ray export event logger.

    This exporter writes dataset metadata to log files using Ray's export event system.
    """

    def __init__(self, logger: logging.Logger):
        """Initialize with a configured export event logger.

        Args:
            logger: The export event logger to use for writing events.
        """
        self._export_logger = logger

    def export_dataset_metadata(self, dataset_metadata: DatasetMetadata) -> None:
        """Export dataset metadata using the export event logger.

        Args:
            dataset_metadata: DatasetMetadata object containing dataset information.
        """
        data_metadata_proto = dataset_metadata_to_proto(dataset_metadata)
        self._export_logger.send_event(data_metadata_proto)

    @classmethod
    def create_if_enabled(cls) -> Optional["LoggerDatasetMetadataExporter"]:
        """Create a logger-based exporter if the export API is enabled.

        Returns:
            A LoggerDatasetMetadataExporter instance if enabled, None otherwise.
        """
        # Proto schemas should be imported here to prevent serialization errors
        from ray.core.generated.export_event_pb2 import ExportEvent

        is_dataset_metadata_export_api_enabled = check_export_api_enabled(
            ExportEvent.SourceType.EXPORT_DATASET_METADATA
        )
        if not is_dataset_metadata_export_api_enabled:
            # The export API is not enabled, so we shouldn't create an exporter
            return None

        log_directory = os.path.join(
            ray._private.worker._global_node.get_session_dir_path(), "logs"
        )

        try:
            logger = get_export_event_logger(
                EventLogType.DATASET_METADATA,
                log_directory,
            )
            return LoggerDatasetMetadataExporter(logger)
        except Exception:
            logger.exception(
                "Unable to initialize the export event logger, so no Dataset Metadata export "
                "events will be written."
            )
            return None
