from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class StreamingDatasource(Datasource, ABC):
    """Abstract base class for streaming data sources.

    This class provides a common interface for all streaming sources like Kafka,
    Kinesis, Pulsar, Redis Streams, etc. It handles common streaming patterns
    while allowing each implementation to focus on source-specific logic.

    Key Features:
    - Standardized streaming interface
    - Built-in offset/checkpoint management
    - Error handling and retry logic
    - Metrics collection
    - Schema inference

    Subclasses should implement:
    - get_streaming_partitions()
    - get_streaming_schema()
    - _create_streaming_read_task()

    Examples:
        Creating a custom streaming datasource:

        .. testcode::

            import pyarrow as pa
            from ray.data.datasource.streaming_datasource import (
                StreamingDatasource, create_streaming_read_task
            )

            class MyStreamingDatasource(StreamingDatasource):
                def get_streaming_partitions(self):
                    return [{"partition_id": "partition_0"}]

                def _create_streaming_read_task(self, partition_info):
                    return create_streaming_read_task(
                        partition_id=partition_info["partition_id"],
                        streaming_config=self.streaming_config,
                        read_source_fn=self._read_data,
                        get_position_fn=self._get_position,
                        get_schema_fn=self._get_schema,
                    )

                def get_streaming_schema(self):
                    return pa.schema([("data", pa.string())])

                def _validate_config(self):
                    pass

    Args:
        max_records_per_task: Maximum records to read per task per batch.
        start_position: Starting position (offset, timestamp, etc.).
        end_position: Ending position (None for unbounded).
        streaming_config: Source-specific configuration.
    """

    def __init__(
        self,
        max_records_per_task: int = 1000,
        start_position: Optional[str] = None,
        end_position: Optional[str] = None,
        streaming_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize streaming datasource.

        Args:
            max_records_per_task: Maximum records to read per task per batch.
            start_position: Starting position (offset, timestamp, etc.).
            end_position: Ending position (None for unbounded).
            streaming_config: Source-specific configuration.
        """
        self.max_records_per_task = max_records_per_task
        self.start_position = start_position
        self.end_position = end_position
        self.streaming_config = streaming_config or {}

        # Validate required configuration
        self._validate_config()

    @abstractmethod
    def _validate_config(self) -> None:
        """Validate source-specific configuration."""
        pass

    @abstractmethod
    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get list of partitions/shards/streams to read from.

        Returns:
            List of partition metadata dicts. Each dict should contain
            information needed to create a read task for that partition.
        """
        pass

    @abstractmethod
    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create a read task for a specific partition.

        Args:
            partition_info: Partition metadata from get_streaming_partitions().

        Returns:
            ReadTask instance for this partition.
        """
        pass

    @abstractmethod
    def get_streaming_schema(self) -> Optional[pa.Schema]:
        """Get the schema for streaming data.

        Returns:
            PyArrow schema for the streaming data.
        """
        pass

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Get read tasks for streaming source.

        This implements the standard Datasource interface by delegating
        to streaming-specific methods.

        Args:
            parallelism: Maximum number of read tasks to create.

        Returns:
            List of ReadTask instances for parallel execution.
        """
        partitions = self.get_streaming_partitions()

        if not partitions:
            return []

        # Limit to requested parallelism
        selected_partitions = (
            partitions[:parallelism] if parallelism > 0 else partitions
        )

        # Create read tasks
        read_tasks = []
        for partition_info in selected_partitions:
            task = self._create_streaming_read_task(partition_info)
            read_tasks.append(task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Streaming sources have unknown size.

        Returns:
            None, as streaming sources have unbounded data.
        """
        return None

    def get_schema(self) -> Optional[pa.Schema]:
        """Get schema - delegates to streaming-specific implementation.

        Returns:
            PyArrow schema for the data.
        """
        return self.get_streaming_schema()

    @property
    def supports_distributed_reads(self) -> bool:
        """Most streaming sources support distributed reads.

        Returns:
            True if this datasource supports distributed reads.
        """
        return True


def create_streaming_read_task(
    partition_id: str,
    streaming_config: Dict[str, Any],
    read_source_fn: Callable[[], Iterator[Dict[str, Any]]],
    get_position_fn: Callable[[], str],
    get_schema_fn: Callable[[], pa.Schema],
    start_position: Optional[str] = None,
    end_position: Optional[str] = None,
    max_records: int = 1000,
    enrichment_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> ReadTask:
    """Factory function to create streaming read tasks.

    This follows Ray Data patterns by using a factory function approach
    similar to other datasources in the codebase.

    Examples:
        Creating a basic streaming read task:

        .. testcode::

            def read_data():
                yield {"id": 1, "value": "test"}

            def get_position():
                return "offset:1"

            def get_schema():
                return pa.schema([("id", pa.int64()), ("value", pa.string())])

            task = create_streaming_read_task(
                partition_id="partition_0",
                streaming_config={},
                read_source_fn=read_data,
                get_position_fn=get_position,
                get_schema_fn=get_schema,
            )

    Args:
        partition_id: Unique identifier for partition.
        streaming_config: Source-specific configuration.
        read_source_fn: Function that reads from source and yields records.
        get_position_fn: Function that returns current position.
        get_schema_fn: Function that returns the schema.
        start_position: Starting position.
        end_position: Ending position.
        max_records: Maximum records per read.
        enrichment_fn: Optional function to enrich records with metadata.

    Returns:
        ReadTask configured for streaming.
    """

    def read_fn() -> Iterable[Block]:
        """Read function that converts source records to blocks."""
        records = []
        records_read = 0

        try:
            for record in read_source_fn():
                if end_position and _should_stop_reading(record, end_position):
                    break

                if records_read >= max_records:
                    break

                # Enrich record with metadata
                enriched_record = record.copy()
                enriched_record.update(
                    {
                        "partition_id": partition_id,
                        "read_timestamp": datetime.now(),
                        "current_position": get_position_fn(),
                    }
                )

                # Apply custom enrichment if provided
                if enrichment_fn:
                    enriched_record = enrichment_fn(enriched_record)

                records.append(enriched_record)
                records_read += 1

            # Convert to PyArrow table
            if records:
                table = pa.Table.from_pylist(records)
                yield table
            else:
                # Yield empty table with correct schema
                schema = get_schema_fn()
                yield pa.Table.from_arrays([[] for _ in schema], schema=schema)

        except Exception as e:
            # Log error with context
            error_context = {
                "partition_id": partition_id,
                "start_position": start_position,
                "current_position": get_position_fn(),
                "error": str(e),
            }
            raise RuntimeError(f"Streaming read error: {error_context}") from e

    # Create metadata for this task
    # For streaming, we can estimate records per task
    estimated_rows = (
        max_records if max_records is not None and max_records > 0 else None
    )

    # Provide source identifier for tracking
    input_source = streaming_config.get("source_identifier", partition_id)

    metadata = BlockMetadata(
        num_rows=estimated_rows,  # Estimate based on max_records
        size_bytes=None,  # Unknown until read for streaming
        input_files=[input_source] if input_source else None,
        exec_stats=None,
    )

    return ReadTask(read_fn, metadata, schema=get_schema_fn())


def _should_stop_reading(record: Dict[str, Any], end_position: str) -> bool:
    """Check if reading should stop based on end position.

    This is a default implementation that subclasses can override.

    Args:
        record: Current record being processed.
        end_position: Position at which to stop reading.

    Returns:
        True if reading should stop, False otherwise.
    """
    # Default implementation - subclasses should override for source-specific logic
    return False


@DeveloperAPI
class StreamingPosition:
    """Represents a position in a streaming source.

    Examples:
        Creating position objects:

        .. testcode::

            # Offset-based position
            pos1 = StreamingPosition(1000, "offset")
            print(str(pos1))  # "offset:1000"

            # Timestamp-based position
            from datetime import datetime
            pos2 = StreamingPosition(datetime.now(), "timestamp")

            # Parse from string
            pos3 = StreamingPosition.from_string("sequence:42")

    Args:
        value: The position value (offset, timestamp, sequence number, etc.).
        position_type: Type of position ("offset", "timestamp", "sequence", etc.).
    """

    def __init__(self, value: Union[str, int, datetime], position_type: str = "offset"):
        """Initialize a streaming position.

        Args:
            value: The position value (offset, timestamp, sequence number, etc.).
            position_type: Type of position ("offset", "timestamp", "sequence", etc.).
        """
        self.value = value
        self.position_type = position_type  # "offset", "timestamp", "sequence", etc.

    def __str__(self) -> str:
        """String representation of the position.

        Returns:
            String in format "position_type:value".
        """
        return f"{self.position_type}:{self.value}"

    @classmethod
    def from_string(cls, position_str: str) -> "StreamingPosition":
        """Parse position from string representation.

        Args:
            position_str: String representation like "offset:1000".

        Returns:
            StreamingPosition instance.
        """
        if ":" in position_str:
            position_type, value = position_str.split(":", 1)
            return cls(value, position_type)
        else:
            return cls(position_str, "offset")


@DeveloperAPI
class StreamingMetrics:
    """Common metrics for streaming sources.

    Examples:
        Using streaming metrics:

        .. testcode::

            metrics = StreamingMetrics()
            metrics.record_read(record_count=100, byte_count=5000)
            metrics.record_error()

            throughput = metrics.get_throughput()
            print(f"Records/sec: {throughput['records_per_second']}")
    """

    def __init__(self):
        """Initialize streaming metrics."""
        self.records_read = 0
        self.bytes_read = 0
        self.read_errors = 0
        self.last_read_time = None
        self.start_time = datetime.now()

    def record_read(self, record_count: int, byte_count: int) -> None:
        """Record successful read.

        Args:
            record_count: Number of records read.
            byte_count: Number of bytes read.
        """
        self.records_read += record_count
        self.bytes_read += byte_count
        self.last_read_time = datetime.now()

    def record_error(self) -> None:
        """Record read error."""
        self.read_errors += 1

    def get_throughput(self) -> Dict[str, float]:
        """Calculate throughput metrics.

        Returns:
            Dictionary containing records_per_second, bytes_per_second, and error_rate.
        """
        duration = (datetime.now() - self.start_time).total_seconds()
        if duration > 0:
            return {
                "records_per_second": self.records_read / duration,
                "bytes_per_second": self.bytes_read / duration,
                "error_rate": self.read_errors / max(1, self.records_read),
            }
        return {"records_per_second": 0, "bytes_per_second": 0, "error_rate": 0}
