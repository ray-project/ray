import functools
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator, SourceOperator
from ray.data.block import BlockMetadata, BlockMetadataWithSchema
from ray.data.datasource import Datasource

logger = logging.getLogger(__name__)

# Optional import for cron scheduling support
try:
    from croniter import croniter

    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False


@dataclass
class StreamingTrigger:
    """Configuration for streaming data processing triggers.

    Defines when and how streaming data should be processed, supporting various
    modes from continuous processing to time-based batching and cron scheduling.
    """

    trigger_type: str  # "continuous", "fixed_interval", "once", "available_now", "cron"
    interval: Optional[Union[str, timedelta]] = None  # e.g., "15m", "30s"
    cron_expression: Optional[str] = None  # e.g., "0 */5 * * *" (every 5 hours)
    max_batches: Optional[int] = None  # For testing/bounded streams
    max_files_per_trigger: Optional[int] = None  # Limit files per trigger
    processing_time: bool = True  # Use processing time vs event time

    # Watermark and late data handling
    watermark_column: Optional[str] = None  # Column for event time watermarks
    allow_late_data: bool = True  # Whether to process late arriving data
    late_data_threshold: Optional[timedelta] = None  # Max lateness allowed

    # Output mode configuration
    output_mode: str = "append"  # "append", "update", "complete"

    # Checkpointing configuration
    checkpoint_location: Optional[str] = None
    checkpoint_interval: Optional[timedelta] = field(
        default_factory=lambda: timedelta(minutes=5)
    )

    def __post_init__(self):
        if self.trigger_type == "fixed_interval" and self.interval is None:
            raise ValueError("Fixed interval trigger requires an interval")

        if self.trigger_type == "cron":
            if self.cron_expression is None:
                raise ValueError("Cron trigger requires a cron_expression")
            if not CRONITER_AVAILABLE:
                raise ImportError(
                    "croniter package is required for cron scheduling. "
                    "Install with: pip install croniter"
                )
            self._validate_cron_expression(self.cron_expression)

        # Parse string intervals to timedelta
        if isinstance(self.interval, str):
            self.interval = self._parse_interval(self.interval)

    @staticmethod
    def _validate_cron_expression(cron_expr: str) -> None:
        """Validate that the cron expression is valid."""
        try:
            # Test if the cron expression is valid by creating a croniter instance
            croniter(cron_expr, datetime.now())
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid cron expression '{cron_expr}': {e}")

    def get_next_trigger_time(
        self, base_time: Optional[datetime] = None
    ) -> Optional[datetime]:
        """Get the next trigger time based on the trigger configuration.

        Args:
            base_time: Base time to calculate next trigger from. Defaults to now.

        Returns:
            Next trigger time, or None for continuous/once triggers.
        """
        if base_time is None:
            base_time = datetime.now()

        if self.trigger_type == "cron":
            if not CRONITER_AVAILABLE:
                raise ImportError("croniter package required for cron scheduling")
            cron = croniter(self.cron_expression, base_time)
            return cron.get_next(datetime)
        elif self.trigger_type == "fixed_interval" and self.interval:
            return base_time + self.interval
        else:
            # Continuous, once, or available_now don't have scheduled next times
            return None

    @staticmethod
    def _parse_interval(interval_str: str) -> timedelta:
        """Parse interval strings like '15m', '30s', '1h' to timedelta."""
        if interval_str == "continuous":
            return timedelta(0)  # Special case for continuous processing

        unit_map = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
        }

        unit = interval_str[-1].lower()
        if unit not in unit_map:
            raise ValueError(
                f"Invalid interval unit: {unit}. Use 's', 'm', 'h', or 'd'"
            )

        try:
            value = int(interval_str[:-1])
            return timedelta(**{unit_map[unit]: value})
        except ValueError:
            raise ValueError(f"Invalid interval format: {interval_str}")

    @classmethod
    def continuous(
        cls, checkpoint_location: Optional[str] = None
    ) -> "StreamingTrigger":
        """Create a continuous processing trigger.

        Processes data as soon as it arrives with minimal latency.

        Args:
            checkpoint_location: Optional checkpoint location for fault tolerance.
        """
        return cls(trigger_type="continuous", interval=timedelta(0))

    @classmethod
    def fixed_interval(cls, interval: Union[str, timedelta]) -> "StreamingTrigger":
        """Create a fixed interval trigger.

        Processes data in batches at regular time intervals.

        Args:
            interval: Time interval between batches (e.g., "30s", "5m", "1h").
        """
        return cls(trigger_type="fixed_interval", interval=interval)

    @classmethod
    def cron(
        cls, cron_expression: str, checkpoint_location: Optional[str] = None
    ) -> "StreamingTrigger":
        """Create a cron-scheduled trigger.

        Processes data based on a cron schedule expression.

        Args:
            cron_expression: Cron expression defining the schedule
                (e.g., "0 */2 * * *" for every 2 hours).
            checkpoint_location: Optional checkpoint location for fault tolerance.

        Examples:
            - "0 */5 * * *": Every 5 hours
            - "0 9 * * 1-5": Every weekday at 9:00 AM
            - "*/15 * * * *": Every 15 minutes
            - "0 0 1 * *": First day of every month at midnight
        """
        return cls(
            trigger_type="cron",
            cron_expression=cron_expression,
            checkpoint_location=checkpoint_location,
        )

    @classmethod
    def once(cls) -> "StreamingTrigger":
        """Create a one-time processing trigger.

        Processes all available data once and then stops.
        """
        return cls(trigger_type="once")

    @classmethod
    def available_now(
        cls, max_files_per_trigger: Optional[int] = None
    ) -> "StreamingTrigger":
        """Create an available-now trigger.

        Processes all data available at the time of the trigger, then stops.
        Similar to once, but can be used for incremental batch processing.

        Args:
            max_files_per_trigger: Maximum number of files to process per trigger.
        """
        return cls(
            trigger_type="available_now", max_files_per_trigger=max_files_per_trigger
        )

    @classmethod
    def with_watermark(
        cls,
        trigger_type: str,
        watermark_column: str,
        interval: Optional[Union[str, timedelta]] = None,
        late_data_threshold: Optional[Union[str, timedelta]] = None,
        cron_expression: Optional[str] = None,
    ) -> "StreamingTrigger":
        """Create a trigger with watermark-based event time processing.

        Args:
            trigger_type: Type of trigger ("continuous", "fixed_interval", "cron").
            watermark_column: Column name containing event timestamps.
            interval: Processing interval for fixed_interval triggers.
            late_data_threshold: Maximum allowed lateness for data.
            cron_expression: Cron expression for cron triggers.
        """
        if isinstance(late_data_threshold, str):
            late_data_threshold = cls._parse_interval(late_data_threshold)

        if trigger_type == "cron" and cron_expression is None:
            raise ValueError("Cron trigger with watermark requires cron_expression")

        return cls(
            trigger_type=trigger_type,
            interval=interval,
            cron_expression=cron_expression,
            watermark_column=watermark_column,
            late_data_threshold=late_data_threshold,
            processing_time=False,
        )

    @classmethod
    def with_output_mode(
        cls,
        trigger_type: str,
        output_mode: str,
        interval: Optional[Union[str, timedelta]] = None,
        checkpoint_location: Optional[str] = None,
        cron_expression: Optional[str] = None,
    ) -> "StreamingTrigger":
        """Create a trigger with specific output mode.

        Args:
            trigger_type: Type of trigger.
            output_mode: Output mode ("append", "update", "complete").
            interval: Processing interval for fixed_interval triggers.
            checkpoint_location: Location for checkpointing.
            cron_expression: Cron expression for cron triggers.
        """
        if output_mode not in ["append", "update", "complete"]:
            raise ValueError(f"Invalid output mode: {output_mode}")

        if trigger_type == "cron" and cron_expression is None:
            raise ValueError("Cron trigger with output mode requires cron_expression")

        return cls(
            trigger_type=trigger_type,
            interval=interval,
            cron_expression=cron_expression,
            output_mode=output_mode,
            checkpoint_location=checkpoint_location,
        )


class UnboundedQueueStreamingData(LogicalOperator, SourceOperator):
    """Logical operator for unbounded streaming data sources.

    This operator represents an unbounded data source like Kafka, Kinesis, or
    other streaming systems. Unlike InputData which represents bounded datasets,
    this operator can continuously produce new data based on trigger patterns.
    """

    def __init__(
        self,
        datasource: Datasource,
        trigger: StreamingTrigger,
        datasource_config: Optional[Dict[str, Any]] = None,
        parallelism: int = -1,
    ):
        """Initialize an unbounded streaming data operator.

        Args:
            datasource: The streaming datasource (e.g., KafkaDatasource)
            trigger: Trigger configuration for microbatch processing
            datasource_config: Configuration for the datasource
            parallelism: Number of parallel read tasks
        """
        super().__init__("UnboundedQueueStreamingData", [], None)  # Unbounded output
        self.datasource = datasource
        self.trigger = trigger
        self.datasource_config = datasource_config or {}
        self.parallelism = parallelism

        # Get Ray Data context for configuration
        from ray.data.context import DataContext
        self._data_context = DataContext.get_current()

        # Use context defaults for trigger if not specified
        if trigger.trigger_type == "fixed_interval" and trigger.interval is None:
            from datetime import timedelta
            default_interval = self._data_context.streaming_trigger_interval
            if isinstance(default_interval, str):
                trigger.interval = StreamingTrigger._parse_interval(default_interval)
            else:
                trigger.interval = timedelta(seconds=30)  # Fallback default

    def output_data(self) -> Optional[List[RefBundle]]:
        """Streaming operators don't have pre-computed output data."""
        return None

    def infer_metadata(self) -> BlockMetadata:
        """Return metadata for streaming operator."""
        return self._cached_output_metadata.metadata

    def infer_schema(self):
        """Infer schema from the datasource."""
        return self._cached_output_metadata.schema

    @functools.cached_property
    def _cached_output_metadata(self) -> "BlockMetadataWithSchema":
        """Infer metadata and schema from streaming datasource."""
        from ray.data._internal.util import unify_schemas_with_validation

        # Get a sample of read tasks to infer metadata and schema
        try:
            # Use a small, constant number for schema inference to avoid
            # performance issues
            sample_parallelism = min(
                3, max(1, self.parallelism if self.parallelism > 0 else 1)
            )
            read_tasks = self.datasource.get_read_tasks(sample_parallelism)

            if not read_tasks:
                logger.warning("No read tasks available for schema inference")
                empty_meta = BlockMetadata(None, None, None, None)
                return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

        except Exception as e:
            logger.warning(f"Failed to get read tasks for schema inference: {e}")
            # Fallback to empty schema
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

        if len(read_tasks) == 0:
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

        metadata = [read_task.metadata for read_task in read_tasks]

        # For streaming, we can estimate based on max_records_per_task
        if all(meta is not None and meta.num_rows is not None for meta in metadata):
            estimated_rows_per_batch = sum(meta.num_rows for meta in metadata)
        else:
            estimated_rows_per_batch = None

        # Size bytes is unknown for streaming
        size_bytes = None

        # Collect input files/sources
        input_files = []
        for meta in metadata:
            if meta.input_files is not None:
                input_files.extend(meta.input_files)

        # Create streaming metadata - note we use estimated_rows_per_batch,
        # not total rows
        streaming_meta = BlockMetadata(
            num_rows=estimated_rows_per_batch,
            size_bytes=size_bytes,
            input_files=input_files if input_files else None,
            exec_stats=None,
        )

        # Infer schema from read tasks
        schemas = [
            read_task.schema for read_task in read_tasks if read_task.schema is not None
        ]

        schema = None
        if schemas:
            schema = unify_schemas_with_validation(schemas)

        return BlockMetadataWithSchema(metadata=streaming_meta, schema=schema)

    def is_lineage_serializable(self) -> bool:
        """Streaming operators are not serializable due to active connections."""
        return False

    def estimated_num_outputs(self) -> Optional[int]:
        """Unbounded streams have unknown output count."""
        return None
