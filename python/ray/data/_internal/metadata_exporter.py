"""Metadata exporter API for Ray Data datasets."""

from abc import ABC, abstractmethod
import logging
import os
from typing import Any, Dict, Optional, Tuple

import ray
from ray._private.event.export_event_logger import (
    EventLogType,
    get_export_event_logger,
    check_export_api_enabled,
)

UNKNOWN = "unknown"

class DataMetadataExporter(ABC):
    """Abstract base class for data metadata exporters.
    
    Implementations of this interface can export Ray Data metadata to various destinations
    like log files, databases, or monitoring systems.
    """
    
    @abstractmethod
    def export_dataset_metadata(self, dataset_metadata: Dict[str, Any]) -> None:
        """Export dataset metadata to the destination.
        
        Args:
            dataset_metadata: Dictionary containing dataset metadata.
                Expected to have fields like job_id, dataset_id, start_time, and dag_structure.
        """
        pass
    
    @classmethod
    @abstractmethod
    def create_if_enabled(cls) -> Optional["DataMetadataExporter"]:
        """Create an exporter instance if the export functionality is enabled.
        
        Returns:
            An exporter instance if enabled, None otherwise.
        """
        pass


class LoggerDataMetadataExporter(DataMetadataExporter):
    """Data metadata exporter implementation that uses the Ray export event logger.
    
    This exporter writes dataset metadata to log files using Ray's export event system.
    """
    
    def __init__(self, logger: logging.Logger):
        """Initialize with a configured export event logger.
        
        Args:
            logger: The export event logger to use for writing events.
            is_enabled: Whether this exporter is enabled.
        """
        self._export_logger = logger
    
    def export_dataset_metadata(self, dataset_metadata: Dict[str, Any]) -> None:
        """Export dataset metadata using the export event logger.
        
        Args:
            dataset_metadata: Dictionary containing dataset metadata.
        """
        data_metadata_proto = self._dataset_metadata_to_proto(dataset_metadata)
        self._export_logger.send_event(data_metadata_proto)
    
    
    @classmethod
    def create_if_enabled(cls) -> Optional["LoggerDataMetadataExporter"]:
        """Create a logger-based exporter if the export API is enabled.
        
        Returns:
            A LoggerDataMetadataExporter instance if enabled, None otherwise.
        """
        # Proto schemas should be imported here to prevent serialization errors
        from ray.core.generated.export_event_pb2 import ExportEvent
        
        is_data_metadata_export_api_enabled = check_export_api_enabled(
            ExportEvent.SourceType.EXPORT_DATA_METADATA
        )
        if not is_data_metadata_export_api_enabled:
            # The export API is not enabled, so we shouldn't create an exporter
            return None
        
        log_directory = os.path.join(
            ray._private.worker._global_node.get_session_dir_path(), "logs"
        )
        
        logger = None
        try:
            logger = get_export_event_logger(
                EventLogType.DATA_METADATA,
                log_directory,
            )
        except Exception:
            logger.exception(
                "Unable to initialize the export event logger, so no Data Metadata export "
                "events will be written."
            )

        if logger is None:
            return None

        return LoggerDataMetadataExporter(logger)
    
    def _dataset_metadata_to_proto(self, dataset_metadata: Dict[str, Any]) -> Any:
        """Convert the DAG structure dictionary to a protobuf message.

        Args:
            dag_structure: Dictionary representation of the DAG structure. This
            is passed as a dictionary to avoid serializing the protobuf message.

        Returns:
            The protobuf message representing the DAG structure.
        """
        from ray.core.generated.export_data_metadata_pb2 import (
            ExportDataMetadata,
            OperatorDAG,
            Operator,
            SubOperator,
        )

        # Create the protobuf message
        data_metadata = ExportDataMetadata()
        dag = OperatorDAG()

        # Add operators to the DAG
        dag_structure = dataset_metadata["dag_structure"]
        if "operators" in dag_structure:
            for op_dict in dag_structure["operators"]:
                operator = Operator()
                operator.name = op_dict.get("name", UNKNOWN)
                operator.id = op_dict.get("id", UNKNOWN)
                operator.uuid = op_dict.get("uuid", UNKNOWN)

                # Add input dependencies if they exist
                if "input_dependencies" in op_dict:
                    for dep_id in op_dict["input_dependencies"]:
                        operator.input_dependencies.append(dep_id)

                # Add sub-operators if they exist
                if "sub_operators" in op_dict:
                    for sub_op_dict in op_dict["sub_operators"]:
                        sub_op = SubOperator()
                        sub_op.name = sub_op_dict.get("name", UNKNOWN)
                        sub_op.id = sub_op_dict.get("id", UNKNOWN)
                        operator.sub_operators.append(sub_op)

                # Add the operator to the DAG
                dag.operators.append(operator)

        # Populate the data metadata proto
        data_metadata.dag.CopyFrom(dag)
        data_metadata.job_id = dataset_metadata.get("job_id", None)
        data_metadata.start_time = dataset_metadata.get("start_time", None)
        data_metadata.dataset_id = dataset_metadata.get("dataset_id", None)

        return data_metadata
