"""Unbound datasource base classes and utilities.

This module provides the core abstract base class and utilities for all unbounded 
online data sources in Ray Data. These are different from Ray Data's "streaming 
execution" - they represent sources of unbounded data like Kafka topics, Kinesis 
streams, etc.

Example:
    Create a custom unbound datasource by inheriting from UnboundDatasource:

    >>> class MyUnboundDatasource(UnboundDatasource):
    ...     def _get_read_tasks_for_partition(self, partition_info, parallelism):
    ...         # Implementation here
    ...         pass
    
    >>> datasource = MyUnboundDatasource("my_source")
    >>> read_tasks = datasource.get_read_tasks(parallelism=4)
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, Iterable,List, Optional, Union

import pyarrow as pa

from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.block import Block, BlockMetadata
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class UnboundPosition:
    """Represents a position in an unbound source."""

    def __init__(self, value: Union[str, int], position_type: str = "offset"):
        """Initialize an unbound position.
        
        Args:
            value: Position value (string or integer)
            position_type: Type of position (offset, timestamp, sequence)
        """
        self.value = value
        self.position_type = position_type

    def __eq__(self, other: object) -> bool:
        """Compare positions for equality."""
        if not isinstance(other, UnboundPosition):
            return False
        return self.position_type == other.position_type and self.value == other.value

    def __str__(self) -> str:
        """String representation of the position."""
        return f"{self.position_type}:{self.value}"

    @classmethod
    def from_string(cls, position_str: str) -> "UnboundPosition":
        """Parse position from string representation."""
        if ":" in position_str:
            position_type, value = position_str.split(":", 1)
            return cls(value, position_type)
        else:
            return cls(position_str, "offset")


@DeveloperAPI
class UnboundMetrics:
    """Simple metrics for unbound sources."""

    def __init__(self):
        """Initialize unbound metrics."""
        self.records_read = 0
        self.bytes_read = 0
        self.read_errors = 0
        self.start_time = datetime.now()

    def record_read(self, record_count: int, byte_count: int) -> None:
        """Record successful read."""
        self.records_read += record_count
        self.bytes_read += byte_count

    def record_error(self) -> None:
        """Record read error."""
        self.read_errors += 1

    def get_throughput(self) -> Dict[str, float]:
        """Calculate throughput metrics."""
        duration = (datetime.now() - self.start_time).total_seconds()
        if duration > 0:
            return {
                "records_per_second": self.records_read / duration,
                "bytes_per_second": self.bytes_read / duration,
                "error_rate": self.read_errors / max(1, self.records_read),
            }
        return {"records_per_second": 0, "bytes_per_second": 0, "error_rate": 0}

    def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics."""
        return {
            "records_read": self.records_read,
            "bytes_read": self.bytes_read,
            "read_errors": self.read_errors,
        }


class UnboundDatasource(Datasource, ABC):
    """Abstract base class for unbounded online data sources.

    This class provides the foundation for streaming data sources that represent
    unbounded data streams (like Kafka, Kinesis, Flink). Note that this is distinct
    from Ray Data's "streaming execution" engine - this handles the ingestion of
    unbounded online data.

    Subclasses must implement the abstract method:
        - `_get_read_tasks_for_partition`: Create read tasks for data partitions

    Example:
        >>> class MyDatasource(UnboundDatasource):
        ...     def _get_read_tasks_for_partition(self, partition_info, parallelism):
        ...         # Create and return ReadTask objects
        ...         return [create_unbound_read_task(...)]
        
        >>> ds = MyDatasource("my_source")
        >>> tasks = ds.get_read_tasks(parallelism=2)
    """

    def __init__(self, source_type: str):
        """Initialize unbound datasource.

        Args:
            source_type: Type of unbound source (kafka, kinesis, flink)
        """
        self.source_type = source_type
        self.metrics = UnboundMetrics()

    # Abstract methods that must be implemented by subclasses

    @abstractmethod
    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for a specific partition.

        Args:
            partition_info: Information about the partition to read
            parallelism: Desired parallelism level

        Returns:
            List of ReadTask objects for this partition
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
        # Default implementation that subclasses can override
        # Get partition information - this is source-specific
        partition_info = {"default_partition": True}
        
        # Create read tasks for the partition
        return self._get_read_tasks_for_partition(partition_info, parallelism)

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
        return f"{self.source_type.title()}Unbound"

    def get_schema(self, **kwargs) -> Optional[pa.Schema]:
        """Get schema for this datasource.

        Returns:
            PyArrow schema or None if schema cannot be determined
        """
        # For unbound sources, schema may not be determinable upfront
        return None

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads.
        
        Returns:
            True - unbound datasources support distributed reads
        """
        return True




def create_unbound_read_task(
    read_fn: Callable[[], Iterable[Block]],
    metadata: BlockMetadata,
    schema: Optional[pa.Schema] = None,
) -> ReadTask:
    """Factory function to create unbound read tasks.

    Args:
        read_fn: Function that reads and yields blocks
        metadata: Block metadata for this task
        schema: Optional PyArrow schema

    Returns:
        ReadTask configured for unbound reading.
    """
    return ReadTask(read_fn, metadata, schema=schema)




def infer_schema_from_records(records: List[Dict[str, Any]]) -> Optional[pa.Schema]:
    """Infer PyArrow schema from record samples.

    Args:
        records: Sample records to infer schema from

    Returns:
        Inferred PyArrow schema or None if inference fails
    """
    if not records:
        return None
        
    try:
        table = pa.Table.from_pylist(records)
        return table.schema
    except Exception:
        return None


__all__ = [
    "UnboundDatasource",
    "UnboundPosition", 
    "UnboundMetrics",
    "create_unbound_read_task",
    "infer_schema_from_records",
]
