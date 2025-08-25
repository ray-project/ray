import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


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
        max_records_per_task: Optional[int] = None,
        start_position: Optional[str] = None,
        end_position: Optional[str] = None,
        streaming_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize streaming datasource.

        Args:
            max_records_per_task: Maximum records to read per task per batch.
                If None, uses the default from Ray Data context.
            start_position: Starting position (offset, timestamp, etc.).
            end_position: Ending position (None for unbounded).
            streaming_config: Source-specific configuration.
        """
        # Get current Ray Data context for proper integration
        from ray.data.context import DataContext

        self._data_context = DataContext.get_current()

        # Use context defaults if not specified
        self.max_records_per_task = (
            max_records_per_task or self._data_context.streaming_batch_size
        )
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
        try:
            partitions = self.get_streaming_partitions()

            if not partitions:
                logger.info("No streaming partitions available")
                return []

            # Limit to requested parallelism, ensuring we don't exceed available
            # partitions
            if parallelism > 0:
                selected_partitions = partitions[:parallelism]
                logger.info(
                    f"Selected {len(selected_partitions)} partitions from "
                    f"{len(partitions)} available"
                )
            else:
                selected_partitions = partitions
                logger.info(
                    f"Using all {len(selected_partitions)} available " f"partitions"
                )

            # Create read tasks
            read_tasks = []
            for partition_info in selected_partitions:
                try:
                    task = self._create_streaming_read_task(partition_info)
                    read_tasks.append(task)
                except Exception as e:
                    logger.error(
                        f"Failed to create read task for partition "
                        f"{partition_info.get('partition_id', 'unknown')}: {e}"
                    )
                    # Continue with other partitions rather than failing completely
                    continue

            return read_tasks

        except Exception as e:
            logger.error(f"Failed to get streaming read tasks: {e}")
            raise RuntimeError(f"Failed to get streaming read tasks: {e}") from e

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
        try:
            schema = self.get_streaming_schema()

            # Validate schema consistency for streaming operations
            if hasattr(self, "_last_schema") and self._last_schema is not None:
                if not self._schemas_compatible(self._last_schema, schema):
                    logger.warning(
                        "Schema evolution detected - schema has changed from "
                        "previous version"
                    )
                    # In production, you might want to handle schema evolution more
                    # gracefully
                    # For now, we'll log the change and continue

            self._last_schema = schema
            return schema

        except Exception as e:
            logger.warning(f"Failed to get streaming schema: {e}")
            return None

    def _schemas_compatible(self, old_schema: pa.Schema, new_schema: pa.Schema) -> bool:
        """Check if two schemas are compatible for streaming operations.

        Args:
            old_schema: Previous schema version
            new_schema: Current schema version

        Returns:
            True if schemas are compatible, False otherwise
        """
        try:
            # Check if all columns from old schema exist in new schema
            old_columns = set(old_schema.names)
            new_columns = set(new_schema.names)

            # Missing columns are not compatible
            if not old_columns.issubset(new_columns):
                missing = old_columns - new_columns
                logger.warning(f"Schema incompatibility: missing columns {missing}")
                return False

            # Check type compatibility for existing columns
            for col_name in old_columns:
                old_type = old_schema.field(col_name).type
                new_type = new_schema.field(col_name).type

                # For streaming, we allow some type widening (e.g., int32 -> int64)
                if not self._types_compatible(old_type, new_type):
                    logger.warning(
                        f"Schema incompatibility: column {col_name} type "
                        f"changed from {old_type} to {new_type}"
                    )
                    return False

            return True

        except Exception as e:
            logger.debug(f"Error checking schema compatibility: {e}")
            return False

    def _types_compatible(self, old_type: pa.DataType, new_type: pa.DataType) -> bool:
        """Check if two PyArrow types are compatible for streaming operations.

        Args:
            old_type: Previous type
            new_type: Current type

        Returns:
            True if types are compatible, False otherwise
        """
        try:
            # Same type is always compatible
            if old_type == new_type:
                return True

            # Allow some type widening for streaming
            if pa.types.is_integer(old_type) and pa.types.is_integer(new_type):
                # Allow widening (e.g., int32 -> int64)
                return True

            if pa.types.is_floating(old_type) and pa.types.is_floating(new_type):
                # Allow widening (e.g., float32 -> float64)
                return True

            if pa.types.is_string(old_type) and pa.types.is_string(new_type):
                # String types are generally compatible
                return True

            # For other types, require exact match
            return False

        except Exception as e:
            logger.debug(f"Error checking type compatibility: {e}")
            return False

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
    max_records: Optional[int] = None,
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
        # Get Ray Data context for retry configuration
        from ray.data.context import DataContext

        data_context = DataContext.get_current()
        max_retries = data_context.streaming_max_retries
        retry_delay = data_context.streaming_retry_delay

        # Use context default if max_records not specified
        local_max_records = max_records
        if local_max_records is None:
            local_max_records = data_context.streaming_batch_size

        records = []
        records_read = 0
        retry_count = 0

        while retry_count <= max_retries:
            try:
                for record in read_source_fn():
                    if end_position and _should_stop_reading(record, end_position):
                        break

                    if records_read >= local_max_records:
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

                # Success - break out of retry loop
                break

            except Exception as e:
                retry_count += 1
                error_context = {
                    "partition_id": partition_id,
                    "start_position": start_position,
                    "current_position": get_position_fn(),
                    "error": str(e),
                    "retry_count": retry_count,
                    "max_retries": max_retries,
                }

                if retry_count <= max_retries:
                    logger.warning(
                        f"Streaming read error (retry {retry_count}/{max_retries}): "
                        f"{error_context}"
                    )
                    import time

                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(
                        f"Streaming read failed after {max_retries} retries: "
                        f"{error_context}"
                    )
                    raise RuntimeError(f"Streaming read error: {error_context}") from e

    # Create metadata for this task
    # For streaming, we can estimate records per task
    estimated_rows = max_records if max_records is not None else None

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

    This class provides a standardized way to represent and manage positions
    across different streaming sources, similar to Ray Data's position tracking
    in other datasources.

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

        # Validate position type
        valid_types = {"offset", "timestamp", "sequence", "checkpoint", "custom"}
        if position_type not in valid_types:
            logger.warning(
                f"Unknown position type: {position_type}. "
                f"Valid types: {valid_types}"
            )

    def __eq__(self, other: object) -> bool:
        """Compare positions for equality."""
        if not isinstance(other, StreamingPosition):
            return False
        return self.position_type == other.position_type and self.value == other.value

    def __lt__(self, other: "StreamingPosition") -> bool:
        """Compare positions for ordering (if comparable)."""
        if self.position_type != other.position_type:
            raise ValueError(
                f"Cannot compare positions of different types: "
                f"{self.position_type} vs {other.position_type}"
            )

        if isinstance(self.value, (int, float)) and isinstance(
            other.value, (int, float)
        ):
            return self.value < other.value
        elif isinstance(self.value, datetime) and isinstance(other.value, datetime):
            return self.value < other.value
        else:
            raise ValueError(
                f"Cannot compare position values of type {type(self.value)}"
            )

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"StreamingPosition({self.value!r}, {self.position_type!r})"

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

    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary for serialization.

        Returns:
            Dictionary representation of the position.
        """
        return {
            "value": self.value,
            "position_type": self.position_type,
            "timestamp": (
                self.timestamp.isoformat() if hasattr(self, "timestamp") else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamingPosition":
        """Create position from dictionary representation.

        Args:
            data: Dictionary containing position data.

        Returns:
            StreamingPosition instance.
        """
        position = cls(data["value"], data["position_type"])
        if "timestamp" in data and data["timestamp"]:
            from datetime import datetime

            position.timestamp = datetime.fromisoformat(data["timestamp"])
        return position

    def advance(self, increment: Union[int, float, timedelta]) -> "StreamingPosition":
        """Advance the position by the given increment.

        Args:
            increment: Amount to advance by.

        Returns:
            New StreamingPosition with advanced value.

        Raises:
            ValueError: If the position type doesn't support advancement.
        """
        if self.position_type == "offset" and isinstance(self.value, int):
            return StreamingPosition(self.value + increment, self.position_type)
        elif self.position_type == "timestamp" and isinstance(self.value, datetime):
            if isinstance(increment, timedelta):
                return StreamingPosition(self.value + increment, self.position_type)
            else:
                raise ValueError(
                    "Timestamp positions can only be advanced by timedelta"
                )
        elif self.position_type == "sequence" and isinstance(self.value, int):
            return StreamingPosition(self.value + increment, self.position_type)
        else:
            raise ValueError(
                f"Cannot advance position of type {self.position_type} "
                f"with value {self.value}"
            )


@DeveloperAPI
class StreamingMetrics:
    """Common metrics for streaming sources.

    This class provides performance tracking and monitoring capabilities
    similar to Ray Data's built-in metrics system.

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

        # Performance tracking
        self._read_latencies = []
        self._batch_sizes = []
        self._partition_metrics = {}  # Track metrics per partition
        self._error_details = []  # Track error details for debugging

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

    def record_batch_performance(self, batch_size: int, latency_ms: float) -> None:
        """Record batch performance metrics for monitoring.

        Args:
            batch_size: Number of records in the batch
            latency_ms: Time taken to process the batch in milliseconds
        """
        self._batch_sizes.append(batch_size)
        self._read_latencies.append(latency_ms)

        # Keep only recent metrics to avoid memory bloat
        if len(self._batch_sizes) > 1000:
            self._batch_sizes = self._batch_sizes[-500:]
            self._read_latencies = self._read_latencies[-500:]

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics.

        Returns:
            Dictionary containing performance metrics and statistics.
        """
        stats = self.get_throughput()

        if self._batch_sizes:
            stats.update(
                {
                    "avg_batch_size": sum(self._batch_sizes) / len(self._batch_sizes),
                    "max_batch_size": max(self._batch_sizes),
                    "min_batch_size": min(self._batch_sizes),
                }
            )

        if self._read_latencies:
            stats.update(
                {
                    "avg_latency_ms": sum(self._read_latencies)
                    / len(self._read_latencies),
                    "max_latency_ms": max(self._read_latencies),
                    "min_latency_ms": min(self._read_latencies),
                }
            )

        return stats

    def record_partition_metrics(
        self, partition_id: str, record_count: int, byte_count: int
    ) -> None:
        """Record metrics for a specific partition.

        Args:
            partition_id: Identifier for the partition
            record_count: Number of records read from this partition
            byte_count: Number of bytes read from this partition
        """
        if partition_id not in self._partition_metrics:
            self._partition_metrics[partition_id] = {
                "records_read": 0,
                "bytes_read": 0,
                "last_read_time": None,
            }

        self._partition_metrics[partition_id]["records_read"] += record_count
        self._partition_metrics[partition_id]["bytes_read"] += byte_count
        self._partition_metrics[partition_id]["last_read_time"] = datetime.now()

    def record_error_detail(
        self,
        error: Exception,
        partition_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record detailed error information for debugging.

        Args:
            error: The exception that occurred
            partition_id: Optional partition identifier where the error occurred
            context: Optional additional context about the error
        """
        error_detail = {
            "timestamp": datetime.now(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "partition_id": partition_id,
            "context": context or {},
        }
        self._error_details.append(error_detail)

        # Keep only recent error details to avoid memory bloat
        if len(self._error_details) > 100:
            self._error_details = self._error_details[-50:]

    def get_partition_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all partitions.

        Returns:
            Dictionary mapping partition IDs to their metrics
        """
        return self._partition_metrics.copy()

    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors encountered.

        Returns:
            Dictionary containing error statistics and recent error details
        """
        if not self._error_details:
            return {"total_errors": 0, "error_types": {}, "recent_errors": []}

        error_types = {}
        for error_detail in self._error_details:
            error_type = error_detail["error_type"]
            error_types[error_type] = error_types.get(error_type, 0) + 1

        return {
            "total_errors": len(self._error_details),
            "error_types": error_types,
            "recent_errors": self._error_details[-10:],  # Last 10 errors
            "error_rate": self.read_errors / max(1, self.records_read),
        }

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance and operational statistics.

        Returns:
            Dictionary containing all available metrics and statistics
        """
        stats = self.get_performance_stats()
        stats.update(self.get_error_summary())
        stats.update(self.get_partition_metrics())

        # Add operational metrics
        if hasattr(self, "start_time"):
            uptime = (datetime.now() - self.start_time).total_seconds()
            stats.update(
                {
                    "uptime_seconds": uptime,
                    "uptime_hours": uptime / 3600,
                    "records_per_hour": (
                        (self.records_read / uptime * 3600) if uptime > 0 else 0
                    ),
                    "bytes_per_hour": (
                        (self.bytes_read / uptime * 3600) if uptime > 0 else 0
                    ),
                }
            )

        # Add health indicators
        if self.records_read > 0:
            error_rate = self.read_errors / self.records_read
            if error_rate < 0.01:
                health_status = "healthy"
            elif error_rate < 0.05:
                health_status = "warning"
            else:
                health_status = "critical"

            stats["health_status"] = health_status
            stats["error_rate_percent"] = error_rate * 100

        return stats

    def export_metrics(self, format: str = "dict") -> Union[Dict[str, Any], str]:
        """Export metrics in various formats for external monitoring systems.

        Args:
            format: Output format ("dict", "json", "prometheus")

        Returns:
            Metrics in the specified format
        """
        if format == "dict":
            return self.get_comprehensive_stats()
        elif format == "json":
            import json

            return json.dumps(self.get_comprehensive_stats(), default=str)
        elif format == "prometheus":
            return self._to_prometheus_format()
        else:
            raise ValueError(
                f"Unsupported format: {format}. " f"Supported: dict, json, prometheus"
            )

    def _to_prometheus_format(self) -> str:
        """Convert metrics to Prometheus format for monitoring systems."""
        lines = []

        # Basic metrics
        lines.append("# HELP ray_streaming_records_read Total records read")
        lines.append("# TYPE ray_streaming_records_read counter")
        lines.append(f"ray_streaming_records_read {self.records_read}")

        lines.append("# HELP ray_streaming_bytes_read Total bytes read")
        lines.append("# TYPE ray_streaming_bytes_read counter")
        lines.append(f"ray_streaming_bytes_read {self.bytes_read}")

        lines.append("# HELP ray_streaming_errors Total errors encountered")
        lines.append("# TYPE ray_streaming_errors counter")
        lines.append(f"ray_streaming_errors {self.read_errors}")

        # Performance metrics
        if self._read_latencies:
            avg_latency = sum(self._read_latencies) / len(self._read_latencies)
            lines.append(
                "# HELP ray_streaming_avg_latency_ms "
                "Average read latency in milliseconds"
            )
            lines.append("# TYPE ray_streaming_avg_latency_ms gauge")
            lines.append(f"ray_streaming_avg_latency_ms {avg_latency}")

        if self._batch_sizes:
            avg_batch_size = sum(self._batch_sizes) / len(self._batch_sizes)
            lines.append("# HELP ray_streaming_avg_batch_size Average batch size")
            lines.append("# TYPE ray_streaming_avg_batch_size gauge")
            lines.append(f"ray_streaming_avg_batch_size {avg_batch_size}")

        return "\n".join(lines)
