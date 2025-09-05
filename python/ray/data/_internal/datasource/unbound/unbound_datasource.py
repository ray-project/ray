"""Base class for unbound datasources.

This module provides the core abstract base class for all unbounded online data sources
in Ray Data. These are different from Ray Data's "streaming execution" - they represent
sources of unbounded data like Kafka topics, Kinesis streams, etc.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pyarrow as pa

from ray.data._internal.datasource.datasource import Datasource, ReadTask
from ray.data._internal.datasource.unbound.position import UnboundPosition
from ray.data._internal.datasource.unbound.metrics import UnboundMetrics


class UnboundDatasource(Datasource, ABC):
    """Abstract base class for unbounded online data sources.

    This class provides the foundation for streaming data sources that represent
    unbounded data streams (like Kafka, Kinesis, Flink). Note that this is distinct
    from Ray Data's "streaming execution" engine - this handles the ingestion of
    unbounded online data.
    """

    def __init__(self, source_type: str):
        """Initialize unbound datasource.

        Args:
            source_type: Type of unbound source (kafka, kinesis, flink)
        """
        self.source_type = source_type
        self.metrics = UnboundMetrics()
        self._state = "initialized"

    # Abstract methods that must be implemented by subclasses

    @abstractmethod
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate configuration for this datasource.

        Args:
            config: Configuration dictionary

        Raises:
            ValueError: If configuration is invalid
        """
        pass

    @abstractmethod
    def get_unbound_partitions(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get list of partitions for this unbound source.

        Args:
            config: Configuration dictionary

        Returns:
            List of partition configurations
        """
        pass

    @abstractmethod
    def _create_unbound_read_task(
        self,
        partition_config: Dict[str, Any],
        position: Optional[UnboundPosition] = None,
    ) -> ReadTask:
        """Create a read task for a specific partition.

        Args:
            partition_config: Configuration for this partition
            position: Starting position for reading

        Returns:
            ReadTask for this partition
        """
        pass

    @abstractmethod
    def get_unbound_schema(self, config: Dict[str, Any]) -> Optional[pa.Schema]:
        """Get schema for the unbound data.

        Args:
            config: Configuration dictionary

        Returns:
            PyArrow schema or None if schema cannot be determined
        """
        pass

    # Concrete implementations

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Get read tasks for this datasource.

        Args:
            parallelism: Desired parallelism level

        Returns:
            List of read tasks
        """
        # This method should be implemented by subclasses
        # The streaming operator will call this for "once" triggers
        raise NotImplementedError("Subclasses must implement get_read_tasks")

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size.

        Returns:
            None for unbound sources (unbounded)
        """
        return None

    def get_name(self) -> str:
        """Get name of this datasource.

        Returns:
            Name of the datasource
        """
        return f"{self.source_type}_unbound_datasource"

    def get_schema(self, **kwargs) -> Optional[pa.Schema]:
        """Get schema for this datasource.

        Returns:
            PyArrow schema or None
        """
        config = kwargs.get("config", {})
        return self.get_unbound_schema(config)

    def _schemas_compatible(self, schema1: pa.Schema, schema2: pa.Schema) -> bool:
        """Check if two schemas are compatible.

        Args:
            schema1: First schema
            schema2: Second schema

        Returns:
            True if schemas are compatible
        """
        return schema1.equals(schema2)

    def _types_compatible(self, type1: pa.DataType, type2: pa.DataType) -> bool:
        """Check if two types are compatible.

        Args:
            type1: First type
            type2: Second type

        Returns:
            True if types are compatible
        """
        return type1.equals(type2)

    # Lifecycle methods

    def start(self) -> None:
        """Start the unbound datasource."""
        self._state = "running"

    def stop(self) -> None:
        """Stop the unbound datasource."""
        self._state = "stopped"

    def pause(self) -> None:
        """Pause the unbound datasource."""
        self._state = "paused"

    def resume(self) -> None:
        """Resume the unbound datasource."""
        self._state = "running"

    def get_state(self) -> str:
        """Get current state of the datasource.

        Returns:
            Current state (initialized, running, paused, stopped)
        """
        return self._state

    def get_runtime_info(self) -> Dict[str, Any]:
        """Get runtime information about the datasource.

        Returns:
            Dictionary containing runtime information
        """
        return {
            "source_type": self.source_type,
            "state": self._state,
            "metrics": self.metrics.get_comprehensive_stats(),
        }
