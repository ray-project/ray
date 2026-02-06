import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray._raylet import ObjectRefGenerator
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    OpTask,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.logical.operators.unbound_data_operator import (
    StreamingTrigger,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


@dataclass
class _GenState:
    """Tracks one streaming generator task."""
    gen: ObjectRefGenerator  # ObjectRefGenerator returned by remote task with num_returns="streaming"
    next_ref: Optional[ray.ObjectRef] = None  # next yielded ObjectRef from generator
    exhausted: bool = False
    failed: bool = False  # True if generator task failed with an exception
    failure_exception: Optional[Exception] = None  # Exception if generator failed


class UnboundedDataOperator(PhysicalOperator):
    """Physical operator for unbounded online data sources.

    This operator reads from unbounded online data sources (Kafka, Kinesis, Flink, etc.)
    using a MICROBATCH processing pattern similar to Spark Structured Streaming.

    **Microbatch Semantic:**
    - Each trigger creates bounded read tasks (read max_records_per_task)
    - Tasks yield PyArrow tables incrementally for memory efficiency
    - Tasks COMPLETE after reading their batch (not infinite)
    - Triggers control when new batches are created (continuous, interval, once)
    - For continuous: create new batches immediately when previous completes
    - For interval: wait for time interval between batches

    **For Kafka example:**
    - Trigger creates tasks (one per topic/partition)
    - Each task reads up to max_records_per_task messages
    - Task yields tables every 1000 records (incremental)
    - Task completes and Kafka auto-commits offsets
    - Next trigger creates new tasks starting from latest offset

    This provides continuous data flow while maintaining bounded resource usage.

    Note: "streaming" in this context refers to unbounded data sources, not
    Ray Data's "streaming execution" engine (which handles memory management).
    """

    def __init__(
        self,
        data_context: DataContext,
        datasource: Datasource,
        trigger: StreamingTrigger,
        parallelism: int = -1,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the unbounded data operator.

        Args:
            data_context: Ray Data context
            datasource: The online data source (e.g., KafkaDatasource)
            trigger: Trigger configuration for microbatch processing
            parallelism: Number of parallel read tasks
            ray_remote_args: Arguments passed to ray.remote for read tasks
        """
        super().__init__(
            "UnboundedOnlineData", [], data_context, target_max_block_size=None
        )

        self.datasource = datasource
        self.trigger = trigger
        self.parallelism = parallelism if parallelism > 0 else 1
        self.ray_remote_args = ray_remote_args or {}

        # Add online data source specific remote args for better performance
        self._apply_online_data_remote_args()

        # Streaming generator state (num_returns="streaming")
        self._gens: List[_GenState] = []
        self._last_trigger_time = datetime.now()
        self._current_batch_id = 0
        self._completed = False

        # For "once" trigger, we only produce one batch
        self._batch_produced = False

        # Performance tracking
        self._bytes_produced = 0
        self._rows_produced = 0

        # Adaptive backoff for continuous mode
        self._consecutive_empty_batches = 0
        self._last_data_time = datetime.now()  # Track when we last received data
        self._min_backoff_seconds = data_context.streaming_min_backoff_seconds
        self._max_backoff_seconds = data_context.streaming_max_backoff_seconds
        self._total_batches_processed = 0  # Track total batches for stop conditions

        # Adaptive batching state
        self._batch_durations: List[float] = []  # Track recent batch durations
        self._batch_duration_window = data_context.streaming_batch_duration_window
        self._adaptive_batching_enabled = (
            getattr(trigger, "enable_adaptive_batching", False)
            if hasattr(trigger, "enable_adaptive_batching")
            else False
        )
        self._current_max_records_per_trigger = (
            trigger.max_records_per_trigger
            if trigger.max_records_per_trigger
            else data_context.streaming_default_max_records_per_trigger
        )
        self._min_max_records_per_trigger = (
            data_context.streaming_min_max_records_per_trigger
        )
        self._max_max_records_per_trigger = (
            data_context.streaming_max_max_records_per_trigger
        )
        self._batch_start_time: Optional[datetime] = None

        # Graceful shutdown support
        self._shutdown_requested = False
        self._shutdown_reason = None
        self._exception = None

        # Optional datasource checkpointing (Kafka offsets etc.)
        self._checkpoint = None
        if hasattr(self.datasource, "initial_checkpoint"):
            self._checkpoint = self.datasource.initial_checkpoint()
        self._checkpoint_pending_commit = None
        self._commit_token_pending: Any = None

        # Execution-engine backpressure signal (set by notify_in_task_submission_backpressure)
        self._in_engine_backpressure: bool = False
        self._backpressure_since: Optional[datetime] = None

        # Autoscaling state
        self._last_scaling_time = datetime.now()
        self._original_parallelism = parallelism if parallelism > 0 else 1

        # Rate limiting state
        self._rate_limit_window_start = datetime.now()
        self._records_in_window = 0
        self._bytes_in_window = 0
        self._rate_limit_window_seconds = 1.0  # 1 second window

        # Output bundling state (performance optimization)
        # Use context knobs as first-class controls
        self._max_bundled_blocks = data_context.streaming_max_bundled_blocks
        self._max_bundled_bytes = data_context.streaming_max_bundled_bytes

        # Byte-level accounting for memory safety
        self._bytes_in_flight: int = 0
        self._max_bytes_in_flight = data_context.streaming_max_bytes_in_flight

        # Reader actor pool (optional optimization to reduce ray.remote overhead)
        self._use_reader_actors = (
            getattr(trigger, "use_reader_actors", False)
            if hasattr(trigger, "use_reader_actors")
            else False
        )
        self._reader_actors: List[Any] = []  # List of StreamingReaderActor handles
        self._reader_actor_initialized = False

        # Prefetching state (pipeline microbatches for better throughput)
        self._prefetch_enabled = (
            getattr(trigger, "enable_prefetching", False)
            if hasattr(trigger, "enable_prefetching")
            else False
        )
        self._prefetch_queue: List[Tuple[List[ReadTask], Any]] = []  # Prefetched read tasks
        self._prefetch_max_size = data_context.streaming_prefetch_max_size

    def start(self, options: ExecutionOptions) -> None:
        """Start the streaming operator."""
        super().start(options)

        # Initialize reader actors if enabled
        if self._use_reader_actors:
            self._initialize_reader_actors()

        logger.info(
            f"Starting UnboundedDataOperator for {self.datasource.get_name()} "
            f"with trigger type: {self.trigger.trigger_type}, "
            f"parallelism: {self.parallelism}"
        )

        # Log trigger configuration details
        if self.trigger.trigger_type == "fixed_interval":
            logger.info(
                f"Fixed interval trigger with interval: {self.trigger.interval}"
            )
        elif self.trigger.trigger_type == "cron":
            logger.info(f"Cron trigger with expression: {self.trigger.cron_expression}")

        # Initialize first batch of read tasks if trigger condition is met
        if self._should_trigger_new_batch():
            self._start_new_microbatch()

    def request_shutdown(self, reason: str = "user_requested") -> None:
        """Request graceful shutdown of the streaming operator.

        The operator will complete currently running tasks before stopping.

        Args:
            reason: Reason for shutdown (for logging)
        """
        if not self._shutdown_requested:
            logger.info(f"Graceful shutdown requested: {reason}")
            self._shutdown_requested = True
            self._shutdown_reason = reason

    def get_status(self) -> Dict[str, Any]:
        """Get current status of the streaming operator.

        Returns:
            Dictionary containing operator status information
        """
        return {
            "name": self.name,
            "is_active": not self._completed,
            "current_batch_id": self._current_batch_id,
            "total_batches_processed": self._total_batches_processed,
            "consecutive_empty_batches": self._consecutive_empty_batches,
            "bytes_produced": self._bytes_produced,
            "rows_produced": self._rows_produced,
            "active_tasks": len([g for g in self._gens if not g.exhausted]),
            "trigger_type": self.trigger.trigger_type,
            "shutdown_requested": self._shutdown_requested,
            "shutdown_reason": self._shutdown_reason,
        }

    def get_exception(self) -> Optional[Exception]:
        """Get the exception if operator failed.

        Returns:
            Exception if operator failed, None otherwise
        """
        return self._exception

    def export_metrics_prometheus(self) -> str:
        """Export metrics in Prometheus format for monitoring.

        Returns lightweight metrics that can be scraped by Prometheus or similar tools.
        This is optional and complements (not replaces) Anyscale's built-in monitoring.

        Returns:
            String in Prometheus exposition format
        """
        metrics_lines = [
            "# HELP ray_data_streaming_batches_total Total batches processed",
            "# TYPE ray_data_streaming_batches_total counter",
            f'ray_data_streaming_batches_total{{operator_id="{self.id}",name="{self.name}"}} {self._total_batches_processed}',
            "",
            "# HELP ray_data_streaming_records_total Total records processed",
            "# TYPE ray_data_streaming_records_total counter",
            f'ray_data_streaming_records_total{{operator_id="{self.id}",name="{self.name}"}} {self._rows_produced}',
            "",
            "# HELP ray_data_streaming_bytes_total Total bytes processed",
            "# TYPE ray_data_streaming_bytes_total counter",
            f'ray_data_streaming_bytes_total{{operator_id="{self.id}",name="{self.name}"}} {self._bytes_produced}',
            "",
            "# HELP ray_data_streaming_empty_batches_consecutive Current consecutive empty batches",
            "# TYPE ray_data_streaming_empty_batches_consecutive gauge",
            f'ray_data_streaming_empty_batches_consecutive{{operator_id="{self.id}",name="{self.name}"}} {self._consecutive_empty_batches}',
            "",
            "# HELP ray_data_streaming_parallelism Current parallelism level",
            "# TYPE ray_data_streaming_parallelism gauge",
            f'ray_data_streaming_parallelism{{operator_id="{self.id}",name="{self.name}"}} {self.parallelism}',
            "",
            "# HELP ray_data_streaming_active_tasks Current number of active tasks",
            "# TYPE ray_data_streaming_active_tasks gauge",
            f'ray_data_streaming_active_tasks{{operator_id="{self.id}",name="{self.name}"}} {len([g for g in self._gens if not g.exhausted])}',
            "",
            "# HELP ray_data_streaming_is_active Whether operator is currently active",
            "# TYPE ray_data_streaming_is_active gauge",
            f'ray_data_streaming_is_active{{operator_id="{self.id}",name="{self.name}"}} {1 if not self._completed else 0}',
            "",
        ]
        return "\n".join(metrics_lines)

    def _batch_inflight(self) -> bool:
        """True if we are currently draining a microbatch."""
        return any((not g.exhausted and not g.failed) for g in self._gens)

    def _should_trigger_new_batch(self) -> bool:
        """Determine if a new batch should be triggered based on trigger configuration
        and backpressure.

        This method implements the core triggering logic for different streaming modes:
        - once: Trigger exactly one batch
        - continuous: Trigger new batch whenever previous batch completes
        - fixed_interval: Trigger at regular time intervals
        - cron: Trigger based on cron schedule
        - available_now: Trigger until no more data available

        Returns:
            True if a new batch should be triggered, False otherwise
        """
        # Check for graceful shutdown request
        if self._shutdown_requested:
            if not self._batch_inflight():
                logger.info("All tasks completed, shutting down gracefully")
                self._completed = True
            else:
                logger.debug(
                    "Waiting for active generators to complete "
                    "before shutdown"
                )
            return False

        if self.trigger.trigger_type == "once":
            # One-time trigger: only produce batch if we haven't already
            return not self._batch_produced

        # Strong integration point: don't create new microbatches while engine is backpressuring.
        if self._in_engine_backpressure:
            return False

        # Check if we should stop due to idle timeout or consecutive empty batches
        if self._should_stop_continuous():
            logger.info("Stop condition met for continuous streaming")
            self._completed = True
            return False

        # Remove/disable custom backpressure checks; let engine drive backpressure.
        # (Custom checks removed - engine-driven backpressure via notify_in_task_submission_backpressure)

        # Check rate limits before creating new batches
        estimated_records = (
            self.datasource.max_records_per_task
            if hasattr(self.datasource, "max_records_per_task")
            else self.data_context.streaming_default_max_records_per_trigger
        )
        estimated_bytes = (
            estimated_records * self.data_context.streaming_estimated_bytes_per_record
        )
        if not self._check_rate_limit(estimated_records, estimated_bytes):
            logger.debug("Rate limit exceeded, delaying new batch creation")
            return False

        # Apply autoscaling if enabled
        self._check_and_apply_autoscaling()

        if self.trigger.trigger_type == "continuous":
            return not self._batch_inflight()

        elif self.trigger.trigger_type == "fixed_interval":
            now = datetime.now()
            time_since_last = now - self._last_trigger_time
            return (
                time_since_last >= self.trigger.interval
                and not self._batch_inflight()
            )

        elif self.trigger.trigger_type == "cron":
            next_time = self.trigger.get_next_trigger_time(self._last_trigger_time)
            return next_time is not None and datetime.now() >= next_time and not self._batch_inflight()

        elif self.trigger.trigger_type == "available_now":
            # Keep triggering until datasource returns no tasks.
            return not self._batch_inflight()

        return False

    def _should_stop_continuous(self) -> bool:
        """Check if continuous streaming should stop based on configured conditions.

        Returns:
            True if streaming should stop, False otherwise
        """
        # Only apply these checks for continuous mode
        if self.trigger.trigger_type != "continuous":
            return False

        # Check max_consecutive_empty_batches
        if (
            self.trigger.max_consecutive_empty_batches is not None
            and self._consecutive_empty_batches
            >= self.trigger.max_consecutive_empty_batches
        ):
            logger.info(
                f"Stopping continuous stream: reached max consecutive empty batches "
                f"({self._consecutive_empty_batches})"
            )
            return True

        # Check idle_timeout
        if self.trigger.idle_timeout is not None:
            time_since_data = datetime.now() - self._last_data_time
            if time_since_data >= self.trigger.idle_timeout:
                logger.info(
                    f"Stopping continuous stream: idle timeout reached "
                    f"({time_since_data.total_seconds():.1f}s > "
                    f"{self.trigger.idle_timeout.total_seconds():.1f}s)"
                )
                return True

        # Check max_batches if set
        if (
            self.trigger.max_batches is not None
            and self._total_batches_processed >= self.trigger.max_batches
        ):
            logger.info(
                f"Stopping continuous stream: reached max batches "
                f"({self._total_batches_processed})"
            )
            return True

        return False

    def _apply_adaptive_backoff(self) -> None:
        """Apply exponential backoff when consecutive empty batches are detected.

        This prevents busy-waiting when the data source is temporarily idle.
        The backoff delay increases exponentially with consecutive empty batches:
        - 1st empty: 100ms
        - 2nd empty: 200ms
        - 3rd empty: 400ms
        - ...
        - Capped at 5 seconds

        The backoff resets to minimum when data is received.
        """
        if self._consecutive_empty_batches == 0:
            return

        # Calculate exponential backoff: min_delay * (2 ^ consecutive_empty)
        backoff_delay = min(
            self._min_backoff_seconds * (2 ** (self._consecutive_empty_batches - 1)),
            self._max_backoff_seconds,
        )

        if backoff_delay > 0:
            logger.debug(
                f"Applying adaptive backoff: {backoff_delay:.2f}s "
                f"({self._consecutive_empty_batches} consecutive empty batches)"
            )
            time.sleep(backoff_delay)

    def _adjust_batch_size(self) -> None:
        """Dynamically adjust max_records_per_trigger based on performance metrics.

        This implements adaptive batching:
        - Shrink batch size if batches are too slow (high latency)
        - Grow batch size if batches are fast and lag is high (throughput optimization)
        - Respect memory pressure from object store
        """
        if len(self._batch_durations) < 3:
            return  # Need at least 3 samples

        avg_duration = sum(self._batch_durations) / len(self._batch_durations)
        target_duration = getattr(
            self.trigger,
            "target_batch_duration_seconds",
            self.data_context.streaming_default_target_batch_duration_seconds,
        )

        # Get lag metrics if available
        lag_metrics = None
        if hasattr(self.datasource, "get_lag_metrics"):
            lag_metrics = self.datasource.get_lag_metrics()

        # Check memory pressure
        memory_pressure = (
            self._bytes_in_flight / self._max_bytes_in_flight
            if self._max_bytes_in_flight > 0
            else 0.0
        )

        old_size = self._current_max_records_per_trigger

        # Shrink if batches are too slow or memory pressure is high
        if avg_duration > target_duration * 1.5 or memory_pressure > 0.8:
            # Reduce by 20%
            self._current_max_records_per_trigger = max(
                int(self._current_max_records_per_trigger * 0.8),
                self._min_max_records_per_trigger,
            )
            logger.info(
                f"Adaptive batching: Reduced batch size {old_size} -> "
                f"{self._current_max_records_per_trigger} "
                f"(duration: {avg_duration:.2f}s, memory: {memory_pressure:.1%})"
            )

        # Grow if batches are fast and lag is high
        elif (
            avg_duration < target_duration * 0.7
            and lag_metrics
            and lag_metrics.total_lag > self.data_context.streaming_lag_threshold
            and memory_pressure < 0.5
        ):
            # Increase by 25%
            self._current_max_records_per_trigger = min(
                int(self._current_max_records_per_trigger * 1.25),
                self._max_max_records_per_trigger,
            )
            logger.info(
                f"Adaptive batching: Increased batch size {old_size} -> "
                f"{self._current_max_records_per_trigger} "
                f"(duration: {avg_duration:.2f}s, lag: {lag_metrics.total_lag})"
            )

        # Update trigger with new batch size
        if self._current_max_records_per_trigger != old_size:
            self.trigger.max_records_per_trigger = (
                self._current_max_records_per_trigger
            )

    def _check_and_apply_autoscaling(self) -> None:
        """Check if autoscaling should be applied and adjust parallelism.

        Uses lag metrics and batch duration to determine if we should scale up/down.
        Implements lag-aware autoscaling with target_batch_duration and target_lag_seconds.
        Respects cooldown period to avoid thrashing.
        """
        if not self.trigger.enable_autoscaling:
            return

        # Check cooldown period
        now = datetime.now()
        time_since_last_scaling = (now - self._last_scaling_time).total_seconds()
        if time_since_last_scaling < self.trigger.scaling_cooldown_seconds:
            return

        # Get lag metrics from datasource
        lag_metrics = None
        if hasattr(self.datasource, "get_lag_metrics"):
            lag_metrics = self.datasource.get_lag_metrics()

        current_parallelism = self.parallelism
        target_lag_seconds = getattr(
            self.trigger, "target_lag_seconds", 60.0
        )  # Default: 60 seconds
        target_batch_duration = getattr(
            self.trigger,
            "target_batch_duration_seconds",
            self.data_context.streaming_default_target_batch_duration_seconds,
        )

        # Calculate lag-based metrics
        lag_seconds = None
        if lag_metrics and lag_metrics.fetch_rate and lag_metrics.fetch_rate > 0:
            lag_seconds = lag_metrics.total_lag / lag_metrics.fetch_rate

        # Get average batch duration
        avg_batch_duration = None
        if self._batch_durations:
            avg_batch_duration = sum(self._batch_durations) / len(
                self._batch_durations
            )

        # Scale up if lag exceeds target or batch duration is too high
        should_scale_up = False
        if lag_seconds and lag_seconds > target_lag_seconds:
            should_scale_up = True
            reason = f"lag ({lag_seconds:.1f}s > {target_lag_seconds:.1f}s)"
        elif (
            avg_batch_duration
            and avg_batch_duration > target_batch_duration * 1.5
        ):
            should_scale_up = True
            reason = f"batch duration ({avg_batch_duration:.2f}s > {target_batch_duration * 1.5:.2f}s)"

        if should_scale_up:
            new_parallelism = min(
                int(current_parallelism * 1.5),  # 50% increase
                self.trigger.max_parallelism,
                lag_metrics.partitions
                if lag_metrics
                else current_parallelism
                * self.data_context.streaming_partition_count_multiplier,
            )
            if new_parallelism > current_parallelism:
                logger.info(
                    f"Lag-aware autoscaling UP: {current_parallelism} -> {new_parallelism} "
                    f"({reason})"
                )
                self.parallelism = new_parallelism
                self._last_scaling_time = now
                self._cancel_all_read_tasks()
            return

        # Scale down if lag is low, batch duration is fast, and many empty batches
        if (
            (not lag_seconds or lag_seconds < target_lag_seconds * 0.3)
            and (
                not avg_batch_duration
                or avg_batch_duration < target_batch_duration * 0.7
            )
            and self._consecutive_empty_batches > 5
            and current_parallelism > self.trigger.min_parallelism
        ):
            new_parallelism = max(
                int(
                    current_parallelism
                    * self.data_context.streaming_parallelism_decrease_factor
                ),
                self.trigger.min_parallelism,
            )
            if new_parallelism < current_parallelism:
                logger.info(
                    f"Lag-aware autoscaling DOWN: {current_parallelism} -> {new_parallelism} "
                    f"(lag: {lag_seconds:.1f}s if available, "
                    f"batch duration: {avg_batch_duration:.2f}s if available, "
                    f"empty batches: {self._consecutive_empty_batches})"
                )
                self.parallelism = new_parallelism
                self._last_scaling_time = now
                self._cancel_all_read_tasks()

    def _cancel_all_read_tasks(self) -> None:
        """Cancel all current read tasks.

        Used during autoscaling to stop current tasks so they can be recreated
        with new parallelism settings.
        """
        if not self._gens:
            return

        cancelled_count = 0
        for gs in self._gens:
            try:
                # Cancel the generator itself
                ray.cancel(gs.gen, force=False)  # Graceful cancellation
                cancelled_count += 1
            except Exception as e:
                logger.debug(f"Could not cancel generator during autoscaling: {e}")

        logger.debug(
            f"Cancelled {cancelled_count}/{len(self._gens)} generators for autoscaling"
        )
        self._gens.clear()

    def _check_rate_limit(self, estimated_records: int, estimated_bytes: int) -> bool:
        """Check if rate limit allows proceeding with new batch.

        Args:
            estimated_records: Estimated records in next batch
            estimated_bytes: Estimated bytes in next batch

        Returns:
            True if can proceed, False if rate limit exceeded
        """
        if (
            not self.trigger.max_records_per_second
            and not self.trigger.max_bytes_per_second
        ):
            return True  # No rate limiting configured

        now = datetime.now()
        time_in_window = (now - self._rate_limit_window_start).total_seconds()

        # Reset window if needed
        if time_in_window >= self._rate_limit_window_seconds:
            self._rate_limit_window_start = now
            self._records_in_window = 0
            self._bytes_in_window = 0
            time_in_window = 0

        # Check records limit
        if self.trigger.max_records_per_second:
            projected_records = self._records_in_window + estimated_records
            records_rate = projected_records / max(time_in_window, 0.001)
            if records_rate > self.trigger.max_records_per_second:
                logger.debug(
                    f"Rate limit exceeded: {records_rate:.0f} records/s > "
                    f"{self.trigger.max_records_per_second}"
                )
                return False

        # Check bytes limit
        if self.trigger.max_bytes_per_second:
            projected_bytes = self._bytes_in_window + estimated_bytes
            bytes_rate = projected_bytes / max(time_in_window, 0.001)
            if bytes_rate > self.trigger.max_bytes_per_second:
                logger.debug(
                    f"Rate limit exceeded: {bytes_rate/1024/1024:.1f} MB/s > "
                    f"{self.trigger.max_bytes_per_second/1024/1024:.1f} MB/s"
                )
                return False

        # Update counters
        self._records_in_window += estimated_records
        self._bytes_in_window += estimated_bytes
        return True

    def _initialize_reader_actors(self) -> None:
        """Initialize reader actor pool if enabled and datasource supports it.

        Creates long-lived actors that maintain connections to reduce ray.remote overhead.
        """
        if not self._use_reader_actors or self._reader_actor_initialized:
            return

        # Check if datasource supports reader actors
        if not hasattr(self.datasource, "supports_reader_actors") or not self.datasource.supports_reader_actors():
            logger.debug("Datasource does not support reader actors, using task-based reads")
            return

        try:
            from ray.data._internal.streaming.reader_actor import StreamingReaderActor

            # Get partition assignments (simplified - real implementation would use coordinator)
            # For now, create one actor per parallelism unit
            num_actors = min(
                self.parallelism, self.data_context.streaming_max_reader_actors
            )

            for i in range(num_actors):
                # Assign partitions to this actor (simplified round-robin)
                # Real implementation would use PartitionCoordinatorActor
                partition_ids = [f"partition-{i}"]  # Placeholder

                actor = StreamingReaderActor.remote(
                    datasource=self.datasource,
                    partition_ids=partition_ids,
                    checkpoint=self._checkpoint,
                    ray_remote_args=self.ray_remote_args,
                )
                self._reader_actors.append(actor)

            self._reader_actor_initialized = True
            logger.info(
                f"Initialized {len(self._reader_actors)} reader actors for "
                f"parallelism {self.parallelism}"
            )
        except Exception as e:
            logger.warning(f"Failed to initialize reader actors: {e}")
            self._use_reader_actors = False

    def _prefetch_next_batch(self) -> None:
        """Prefetch read tasks for the next microbatch in the background.

        This allows pipelining: while microbatch k is draining, we prepare
        tasks for microbatch k+1. Improves throughput by reducing idle time.
        """
        if len(self._prefetch_queue) >= self._prefetch_max_size:
            return  # Prefetch queue is full

        # Get read tasks for next batch (increment batch ID for prefetch)
        out = self.datasource.get_read_tasks(
            self.parallelism,
            checkpoint=self._checkpoint,
            trigger=self.trigger,
            batch_id=self._current_batch_id + 1,
            max_records_per_trigger=self.trigger.max_records_per_trigger,
            max_bytes_per_trigger=self.trigger.max_bytes_per_trigger,
            max_splits_per_trigger=self.trigger.max_splits_per_trigger,
        )

        if isinstance(out, tuple) and len(out) == 2:
            tasks, next_ckpt = out
        else:
            tasks, next_ckpt = out, self._checkpoint

        if tasks:
            self._prefetch_queue.append((list(tasks), next_ckpt))
        logger.debug(
            f"Prefetched {len(tasks)} read tasks for batch {self._current_batch_id + 1}"
        )

    def _get_read_tasks_with_checkpoint(self) -> Tuple[List[ReadTask], Any]:
        """Get read tasks with checkpoint from datasource.

        Supports both task-based reads and reader actor-based reads.
        Also checks prefetch queue for pipelined microbatches.
        """
        # Check prefetch queue first
        if self._prefetch_queue:
            tasks, checkpoint = self._prefetch_queue.pop(0)
            # Start prefetching next batch in background
            if self._prefetch_enabled:
                self._prefetch_next_batch()
            return tasks, checkpoint

        # Call datasource.get_read_tasks() with checkpoint and trigger info
        out = self.datasource.get_read_tasks(
            self.parallelism,
            checkpoint=self._checkpoint,
            trigger=self.trigger,
            batch_id=self._current_batch_id,
            max_records_per_trigger=getattr(
                self.trigger, "max_records_per_trigger", None
            ),
            max_bytes_per_trigger=getattr(
                self.trigger, "max_bytes_per_trigger", None
            ),
            max_splits_per_trigger=getattr(
                self.trigger, "max_splits_per_trigger", None
            ),
        )

        if isinstance(out, tuple) and len(out) == 2:
            tasks, next_ckpt = out
        else:
            tasks, next_ckpt = out, self._checkpoint

        tasks_list = list(tasks or [])
        # Start prefetching next batch if enabled
        if self._prefetch_enabled and len(self._prefetch_queue) < self._prefetch_max_size:
            self._prefetch_next_batch()
        return tasks_list, next_ckpt

    def _start_new_microbatch(self) -> None:
        """Start a new microbatch by creating streaming generator tasks."""
        # Track batch start time for adaptive batching
        self._batch_start_time = datetime.now()

        read_tasks, next_checkpoint = self._get_read_tasks_with_checkpoint()

        if not read_tasks:
            # Empty microbatch
            if self.trigger.trigger_type == "once":
                self._completed = True
            if self.trigger.trigger_type == "available_now":
                # available_now stops after N consecutive empty batches (drain until stable)
                limit = self.trigger.max_consecutive_empty_batches or 1
                if self._consecutive_empty_batches >= limit:
                    self._completed = True
            logger.info("No read tasks available from datasource")
            self._total_batches_processed += 1
            self._consecutive_empty_batches += 1
            return

        self._gens.clear()
        self._checkpoint_pending_commit = next_checkpoint
        # Prepare commit early (like Spark epoch start); commit later once batch drains.
        if hasattr(self.datasource, "prepare_commit"):
            self._commit_token_pending = self.datasource.prepare_commit(
                next_checkpoint
            )
        else:
            self._commit_token_pending = next_checkpoint

        validated_args = self._validate_ray_remote_args(self.ray_remote_args)

        # Use reader actors if enabled and available, otherwise use regular tasks
        if self._use_reader_actors and self._reader_actors:
            # Use long-lived reader actors to reduce ray.remote overhead
            for actor in self._reader_actors:
                # Call poll_batch() on the actor to get an ObjectRefGenerator
                gen = actor.poll_batch.remote(
                    max_records=self.trigger.max_records_per_trigger,
                    max_bytes=self.trigger.max_bytes_per_trigger,
                    max_splits=self.trigger.max_splits_per_trigger,
                    batch_id=self._current_batch_id,
                )
                self._gens.append(_GenState(gen=gen))
        else:
            # Use regular task-based reads
            for task in read_tasks:
                    # Optimize resource allocation based on task metadata
                task_args = self._optimize_task_resources(task, validated_args)
                remote_fn = ray.remote(**task_args)(task.read_fn)
                gen = remote_fn.remote()  # ObjectRefGenerator
                self._gens.append(_GenState(gen=gen))

        self._last_trigger_time = datetime.now()
        self._current_batch_id += 1
        if self.trigger.trigger_type == "once":
            self._batch_produced = True

        # Prime next refs so has_next() can quickly find work.
        self._prime_generators()

        # Check if all generators failed immediately
        if self._gens and self._all_generators_exhausted():
            if self._any_generator_failed():
                logger.error(
                    f"All generators failed immediately for microbatch {self._current_batch_id}"
                )
                self._finish_microbatch_if_done()
            else:
                # All exhausted normally (empty generators)
                self._finish_microbatch_if_done()

        logger.info(
            f"Started microbatch {self._current_batch_id} with "
            f"{len([g for g in self._gens if not g.exhausted and not g.failed])} active generator tasks"
        )

    def _prime_generators(self) -> None:
        """Ensure each generator has a pending next_ref (unless exhausted or failed).

        Respects byte-level accounting limits to prevent memory pressure.
        """
        for gs in self._gens:
            if gs.exhausted or gs.failed or gs.next_ref is not None:
                continue

            # Check byte-level accounting: don't prime if we're at the limit
            # (This is a conservative check - we don't know block size until we get it)
            if self._bytes_in_flight >= self._max_bytes_in_flight:
                logger.debug(
                    f"Byte limit reached ({self._bytes_in_flight}/{self._max_bytes_in_flight}), "
                    "pausing generator priming"
                )
                break

            try:
                gs.next_ref = next(gs.gen)
            except StopIteration:
                gs.exhausted = True
                gs.next_ref = None
            except Exception as e:
                # Generator task failed - mark as failed and store exception
                logger.error(f"Generator task failed: {e}", exc_info=e)
                gs.failed = True
                gs.failure_exception = e
                gs.exhausted = True  # Treat as exhausted to stop processing
                gs.next_ref = None

    def _optimize_task_resources(
        self, task: ReadTask, base_args: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Optimize Ray remote args based on task metadata.

        Adjusts memory and CPU allocation based on estimated task size to improve
        resource utilization and prevent OOM errors.

        Args:
            task: ReadTask with optional metadata
            base_args: Base Ray remote args to modify

        Returns:
            Optimized Ray remote args dict
        """
        task_args = base_args.copy()
        if hasattr(task, "metadata") and task.metadata:
            estimated_rows = getattr(task.metadata, "num_rows", None)
            estimated_size = getattr(task.metadata, "size_bytes", None)

            # Allocate more memory for large tasks
            if (
                estimated_size
                and estimated_size > self.data_context.streaming_large_task_size_bytes
            ):
                task_args.setdefault(
                    "memory", self.data_context.streaming_large_task_memory_bytes
                )

            # Allocate more CPUs for row-heavy tasks
            if (
                estimated_rows
                and estimated_rows > self.data_context.streaming_large_task_row_threshold
            ):
                task_args.setdefault(
                    "num_cpus", self.data_context.streaming_large_task_cpus
                )

        return task_args

    def _all_generators_exhausted(self) -> bool:
        return all(gs.exhausted for gs in self._gens) if self._gens else True

    def _any_generator_failed(self) -> bool:
        """Check if any generator has failed."""
        return any(gs.failed for gs in self._gens) if self._gens else False

    def _wait_ready_next_ref(self, timeout: float) -> Tuple[Optional[ray.ObjectRef], Optional[int]]:
        """Wait for one ready next_ref among all generators."""
        refs: List[Tuple[int, ray.ObjectRef]] = [
            (i, gs.next_ref) for i, gs in enumerate(self._gens) if gs.next_ref is not None
        ]
        if not refs:
            return None, None
        ready, _ = ray.wait([r for _, r in refs], num_returns=1, timeout=timeout)
        if not ready:
            return None, None
        ready_ref = ready[0]
        for i, r in refs:
            if r == ready_ref:
                return ready_ref, i
        return ready_ref, None

    def _advance_generator(self, idx: int) -> None:
        """Advance generator idx to its next yielded ref."""
        if idx is None or idx >= len(self._gens):
            return
        gs = self._gens[idx]
        if gs.exhausted or gs.failed:
            return
        gs.next_ref = None
        try:
            gs.next_ref = next(gs.gen)
        except StopIteration:
            gs.exhausted = True
            gs.next_ref = None
        except Exception as e:
            # Generator task failed - mark as failed and store exception
            logger.error(f"Generator task failed during advance: {e}", exc_info=e)
            gs.failed = True
            gs.failure_exception = e
            gs.exhausted = True
            gs.next_ref = None

    def _finish_microbatch_if_done(self) -> None:
        """If all generators exhausted, commit checkpoint and end microbatch."""
        if not self._gens:
            return
        if not self._all_generators_exhausted():
            return

        # Track batch duration for adaptive batching
        if self._batch_start_time and self._adaptive_batching_enabled:
            batch_duration = (
                datetime.now() - self._batch_start_time
            ).total_seconds()
            self._batch_durations.append(batch_duration)
            if len(self._batch_durations) > self._batch_duration_window:
                self._batch_durations.pop(0)
            self._adjust_batch_size()

        # Check if any generator failed - if so, don't commit checkpoint
        # to avoid committing partial data
        if self._any_generator_failed():
            failed_gens = [gs for gs in self._gens if gs.failed]
            exceptions = [gs.failure_exception for gs in failed_gens if gs.failure_exception]
            if exceptions:
                # Store first exception for get_exception()
                self._exception = exceptions[0]
            logger.error(
                f"Microbatch {self._current_batch_id} failed: "
                f"{len(failed_gens)}/{len(self._gens)} generators failed. "
                "Checkpoint not committed to avoid data inconsistency."
            )
            # Don't commit checkpoint on failure - allows retry from previous checkpoint
            self._checkpoint_pending_commit = None
        else:
            # All generators completed successfully - safe to commit checkpoint
            if self._commit_token_pending is not None:
                if hasattr(self.datasource, "commit"):
                    self.datasource.commit(self._commit_token_pending)
                elif hasattr(self.datasource, "commit_checkpoint"):
                    self.datasource.commit_checkpoint(self._checkpoint_pending_commit)

            self._checkpoint = self._checkpoint_pending_commit

            # Update reader actor checkpoints if using reader actors
            if self._use_reader_actors and self._reader_actors:
                for actor in self._reader_actors:
                    actor.update_checkpoint.remote(self._checkpoint)

        self._checkpoint_pending_commit = None
        self._commit_token_pending = None

        # clear gens -> microbatch complete
        self._gens.clear()
        self._total_batches_processed += 1

    def should_add_input(self) -> bool:
        """Check if operator can accept more input.

        Streaming operators are source operators - they generate data from external
        systems rather than processing input from upstream operators.

        Returns:
            Always False for streaming operators
        """
        return False

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Add input to this operator (not supported for streaming sources).

        Args:
            refs: Input bundle to add
            input_index: Index of the input dependency

        Raises:
            RuntimeError: Always, as streaming operators don't accept input
        """
        raise RuntimeError("UnboundedDataOperator does not accept input")

    def has_next(self) -> bool:
        """Check if there are available results or if more batches should be triggered.

        This is a key method in Ray Data's execution engine interface. It's called
        repeatedly to determine if the operator can produce more output.

        The logic handles different trigger modes:
        - once: Has next until single batch is consumed
        - continuous: Always has next unless explicitly stopped
        - fixed_interval: Has next when interval triggers or tasks are pending
        - cron: Has next when cron schedule triggers
        - available_now: Has next until no more data available

        Returns:
            True if operator can produce more output, False otherwise
        """
        if self._completed:
            return False

        # Always keep generators primed
        self._prime_generators()

        # If any next_ref is ready, we can produce output
        ready_ref, _ = self._wait_ready_next_ref(timeout=0.0)
        if ready_ref is not None:
                return True

        # If microbatch inflight (generators exist), we still may produce soon
        if self._gens:
            return True

        # Otherwise try to trigger a new microbatch
        if self._should_trigger_new_batch():
            self._start_new_microbatch()
            return bool(self._gens)

        # Continuous/interval/cron run until stopped; once/available_now stop when empty
        if self.trigger.trigger_type in ("continuous", "fixed_interval", "cron"):
            return not self._completed

        return False


    def _get_next_inner(self) -> RefBundle:
        """Get the next result from streaming generators with output bundling.

        This method implements the core output logic with bundling optimization:
        1. Collect multiple ready blocks (up to limits)
        2. Bundle them into a single RefBundle
        3. Track bytes in-flight for memory safety
        4. Update performance metrics
        5. Advance generators and finish microbatch if done

        Returns:
            RefBundle containing one or more blocks from ready generators

        Raises:
            StopIteration: When no blocks are ready or an error occurs
        """
        if not self._gens:
            raise StopIteration("No inflight microbatch")

        # Ensure pending next_refs exist (respects byte limits)
        self._prime_generators()

        # Collect and bundle ready blocks
        bundled_blocks, bundled_bytes = self._collect_ready_blocks()

        if not bundled_blocks:
            # No blocks ready - check if batch is done
            self._finish_microbatch_if_done()
            raise StopIteration("No blocks ready")

        # Create bundled RefBundle
        bundle = RefBundle(
            blocks=tuple(bundled_blocks),
            schema=None,
            owns_blocks=True,
        )

        # Update byte-level accounting
        self._bytes_in_flight += bundled_bytes
        logger.debug(
            f"Emitted bundle: {len(bundled_blocks)} blocks, "
            f"{bundled_bytes} bytes, "
            f"total in-flight: {self._bytes_in_flight}/{self._max_bytes_in_flight}"
        )

        # Update empty-batch logic: seeing blocks == data arrived
        self._consecutive_empty_batches = 0
        self._last_data_time = datetime.now()

        self._update_performance_metrics(bundle)

        # Check if microbatch is done after advancing generators
        self._finish_microbatch_if_done()

        return bundle

    def _collect_ready_blocks(
        self,
    ) -> Tuple[List[Tuple[ray.ObjectRef, Any]], int]:
        """Collect multiple ready blocks for bundling.

        This helper method extracts the block collection logic from _get_next_inner()
        to improve readability and maintainability.

        Returns:
            Tuple of (bundled_blocks, bundled_bytes) where:
            - bundled_blocks: List of (block_ref, metadata) tuples
            - bundled_bytes: Total estimated bytes in bundled blocks
        """
        bundled_blocks: List[Tuple[ray.ObjectRef, Any]] = []
        bundled_bytes = 0

        # Collect ready blocks up to limits
        while len(bundled_blocks) < self._max_bundled_blocks:
            ready_ref, gen_idx = self._wait_ready_next_ref(
                timeout=0.0 if bundled_blocks else 1.0
            )
            if ready_ref is None:
                break  # No more ready blocks

            try:
                block_ref, meta, block_size = self._process_ready_block(ready_ref)
            except Exception as e:
                # Task failed - mark the generator as failed
                if gen_idx is not None and gen_idx < len(self._gens):
                    gs = self._gens[gen_idx]
                    gs.failed = True
                    gs.failure_exception = e
                    gs.exhausted = True
                    gs.next_ref = None

                logger.error(
                    f"Error getting streaming block from generator {gen_idx}: {e}",
                    exc_info=e,
                )
                self._exception = e
                raise

            # Check bundling limits
            if bundled_blocks and (
                bundled_bytes + block_size > self._max_bundled_bytes
            ):
                # Would exceed byte limit - emit what we have
                break

            bundled_blocks.append((block_ref, meta))
            bundled_bytes += block_size

            # Advance this generator
            self._advance_generator(gen_idx)

        return bundled_blocks, bundled_bytes

    def _process_ready_block(
        self, ready_ref: ray.ObjectRef
    ) -> Tuple[ray.ObjectRef, Any, int]:
        """Process a ready block reference and extract metadata.

        Handles both (block, metadata) tuples and raw blocks, computing
        metadata when necessary.

        Args:
            ready_ref: ObjectRef to the ready block or tuple

        Returns:
            Tuple of (block_ref, metadata, block_size)
        """
        # Get the yielded item - datasource yields (block, metadata) tuple
        item = ray.get(ready_ref)
        if not (isinstance(item, tuple) and len(item) == 2):
            raise ValueError(
                f"Datasource read_fn must yield (block, metadata) tuples, "
                f"got {type(item)}"
            )

        block, meta = item
        # Put the *block* itself so downstream sees a block ref, not the tuple ref
        block_ref = ray.put(block)
        block_size = (
            meta.size_bytes if meta and hasattr(meta, "size_bytes") else 0
        )

        return block_ref, meta, block_size

    def _do_shutdown(self, force: bool = False) -> None:
        """Shutdown the operator and cancel all active generators.

        Args:
            force: If True, force cancel all generators immediately.
        """
        # Cancel all active generators
        for gs in self._gens:
            if not gs.exhausted and not gs.failed:
                ray.cancel(gs.gen, force=force)

        # Clear generator state
        self._gens.clear()

        # Shutdown reader actors if used
        if self._reader_actors:
            for actor in self._reader_actors:
                actor.shutdown.remote()
            self._reader_actors.clear()

        # Clear prefetch queue
        self._prefetch_queue.clear()

        # Don't commit pending checkpoint on shutdown - allows recovery
        if self._checkpoint_pending_commit is not None:
            logger.info(
                "Shutting down with pending checkpoint. "
                "Checkpoint not committed to allow recovery."
            )
            # Abort if two-phase commit is supported
            if self._commit_token_pending is not None and hasattr(
                self.datasource, "abort_commit"
            ):
                self.datasource.abort_commit(self._commit_token_pending)
            self._checkpoint_pending_commit = None
            self._commit_token_pending = None

        # Call base class shutdown to cancel active tasks
        super()._do_shutdown(force)

    def input_done(self, input_index: int) -> None:
        """Called when upstream input is done.

        Not applicable for source operators as they have no upstream dependencies.

        Args:
            input_index: Index of the completed input dependency
        """
        pass

    def all_inputs_done(self) -> None:
        """Called when all upstream inputs are done.

        Not applicable for source operators as they have no upstream dependencies.
        """
        pass

    def completed(self) -> bool:
        """Check if the streaming operator has completed execution.

        Completion logic varies by trigger type:
        - once: Complete when single batch is produced and consumed
        - continuous: Never complete (runs until explicitly stopped)
        - fixed_interval: Runs until explicitly stopped
        - cron: Runs until explicitly stopped
        - available_now: Complete when no more data available

        Returns:
            True if operator has completed execution
        """
        if self.trigger.trigger_type == "once":
            # For one-time trigger: complete when batch is produced AND all generators exhausted
            return self._batch_produced and not self._batch_inflight()

        # Continuous and interval streams run indefinitely unless explicitly stopped
        # This allows them to keep producing data as it arrives
        return self._completed

    def throttling_disabled(self) -> bool:
        """Check if execution throttling should be disabled for this operator.

        Streaming operators implement their own throttling via trigger configurations
        (continuous, fixed_interval, etc.), so Ray Data's default throttling should
        be disabled to avoid conflicts.

        Returns:
            Always True for streaming operators
        """
        return True

    def get_active_tasks(self) -> List[OpTask]:
        """Get list of active streaming tasks for monitoring.

        Returns task wrappers that implement the OpTask interface, allowing
        Ray Data's execution engine to monitor and manage streaming tasks.

        Returns:
            List of _StreamingTaskWrapper objects representing active tasks
        """
        # Return wrappers for all non-exhausted, non-failed generators
        # The execution engine waits on the generator itself, not individual refs
        return [
            _StreamingTaskWrapper(gs.gen, i, self)
            for i, gs in enumerate(self._gens)
            if not gs.exhausted and not gs.failed
        ]

    def num_active_tasks(self) -> int:
        """Get the number of currently active streaming tasks.

        Used by Ray Data's execution engine for progress tracking and
        resource management decisions.

        Returns:
            Number of active streaming read tasks (excludes failed generators)
        """
        return len([g for g in self._gens if not g.exhausted and not g.failed])

    def _apply_online_data_remote_args(self) -> None:
        """Apply streaming-specific Ray remote args for optimal performance.

        This configures Ray's task execution to work well with streaming data:
        1. Generator backpressure: Prevents overwhelming object store
        2. Streaming returns: Enables incremental result delivery
        3. Operator labels: Enables tracking and debugging

        Note: "streaming" here refers to Ray Data's execution engine, not the
        unbounded data source. Both concepts work together for efficient processing.
        """
        # Configure generator backpressure if not already set
        # This prevents the read tasks from producing data faster than Ray can handle
        # Important for unbounded sources that could produce infinite data
        if (
            "_generator_backpressure_num_objects" not in self.ray_remote_args
            and self.data_context._max_num_blocks_in_streaming_gen_buffer is not None
        ):
            # Datasource read tasks yield only blocks (not block+metadata pairs)
            # Use the buffer size directly since we yield one object per block
            self.ray_remote_args["_generator_backpressure_num_objects"] = (
                self.data_context._max_num_blocks_in_streaming_gen_buffer
            )

        # Enable streaming returns for better memory management
        # This allows Ray to return results incrementally rather than buffering all
        if "num_returns" not in self.ray_remote_args:
            self.ray_remote_args["num_returns"] = "streaming"

        # Add operator ID label for tracking and debugging
        # This shows up in Ray dashboard and metrics
        if "_labels" not in self.ray_remote_args:
            self.ray_remote_args["_labels"] = {}
        self.ray_remote_args["_labels"][self._OPERATOR_ID_LABEL_KEY] = self.id

    def can_add_input(self, bundle: RefBundle) -> bool:
        """Determine if this operator can accept a specific input bundle.

        Streaming operators are source operators - they generate their own data
        from external systems (Kafka, Kinesis, Flink) rather than consuming
        data from upstream operators in the execution DAG.

        Args:
            bundle: The input bundle to potentially add

        Returns:
            Always False for streaming source operators
        """
        return False

    def implements_accurate_memory_accounting(self) -> bool:
        """Check if this operator implements accurate memory accounting.

        Streaming operators use estimated memory accounting because:
        - Data size is unknown until read from external system
        - Records arrive continuously with varying sizes
        - Exact tracking would add significant overhead

        The estimates are conservative to prevent OOM errors.

        Returns:
            False to indicate estimated rather than precise accounting
        """
        return False

    def notify_in_task_submission_backpressure(
        self, in_backpressure: bool, policy_name: Optional[str] = None
    ) -> None:
        """Notification callback about task submission backpressure state.

        Called by Ray Data's execution engine when the system enters or exits
        backpressure. This allows the operator to adapt its behavior dynamically.

        Args:
            in_backpressure: True if system is currently in backpressure
            policy_name: Name of the backpressure policy that triggered
        """
        # Call base class to update metrics
        super().notify_in_task_submission_backpressure(in_backpressure, policy_name)

        if in_backpressure and not self._in_engine_backpressure:
            self._backpressure_since = datetime.now()
            logger.info(
                f"Entered Ray Data engine backpressure (policy: {policy_name}); "
                f"pausing microbatch triggers"
            )
        if (not in_backpressure) and self._in_engine_backpressure:
            dur = (
                (datetime.now() - self._backpressure_since).total_seconds()
                if self._backpressure_since
                else 0.0
            )
            logger.info(
                f"Exited Ray Data engine backpressure after {dur:.2f}s; resuming triggers"
            )
            self._backpressure_since = None
        self._in_engine_backpressure = in_backpressure

    def _update_performance_metrics(self, bundle: RefBundle) -> None:
        """Update performance tracking metrics."""
        try:
            # Estimate bytes and rows from bundle
            estimated_bytes = 0
            estimated_rows = 0

            for _, metadata in bundle.blocks:
                if metadata:
                    if getattr(metadata, "size_bytes", None):
                        estimated_bytes += metadata.size_bytes
                    if getattr(metadata, "num_rows", None):
                        estimated_rows += metadata.num_rows

            # If no metadata available, use conservative estimates
            if estimated_bytes == 0:
                estimated_bytes = (
                    len(bundle.blocks)
                    * self.data_context.streaming_estimated_bytes_per_record
                )
            if estimated_rows == 0:
                estimated_rows = (
                    len(bundle.blocks)
                    * self.data_context.streaming_estimated_rows_per_block
                )

            self._bytes_produced += estimated_bytes
            self._rows_produced += estimated_rows

            # Log performance metrics periodically
            if (
                self._current_batch_id
                % self.data_context.streaming_batch_logging_interval
                == 0
            ):
                from datetime import datetime

                duration = (datetime.now() - self._last_trigger_time).total_seconds()
                if duration > 0:
                    throughput_rows = self._rows_produced / duration
                    throughput_bytes = self._bytes_produced / duration
                    logger.info(
                        f"Streaming performance - Batch {self._current_batch_id}: "
                        f"{throughput_rows:.1f} rows/sec, "
                        f"{throughput_bytes/1024/1024:.1f} MB/sec, "
                        f"Total: {self._rows_produced} rows, "
                        f"{self._bytes_produced/1024/1024:.1f} MB"
                    )

        except Exception as e:
            # Don't fail streaming on metrics errors, but log the issue
            logger.debug(f"Error updating performance metrics: {e}")

    def get_stats(self) -> StatsDict:
        """Get operator statistics."""
        base_stats = super().get_stats()
        streaming_stats = {
            "current_batch_id": self._current_batch_id,
            "active_generators": len([g for g in self._gens if not g.exhausted and not g.failed]),
            "trigger_type": self.trigger.trigger_type,
            "batch_produced": self._batch_produced,
            "total_bytes_produced": self._bytes_produced,
            "total_rows_produced": self._rows_produced,
            "parallelism": self.parallelism,
            "last_trigger_time": (
                self._last_trigger_time.isoformat() if self._last_trigger_time else None
            ),
            "in_engine_backpressure": getattr(self, "_in_engine_backpressure", False),
            "consecutive_empty_batches": self._consecutive_empty_batches,
            "trigger_config": {
                "type": self.trigger.trigger_type,
                "interval": (
                    str(self.trigger.interval)
                    if hasattr(self.trigger, "interval") and self.trigger.interval
                    else None
                ),
                "cron_expression": getattr(self.trigger, "cron_expression", None),
            },
        }

        # Optional datasource lag metrics (Kafka offsets lag)
        if hasattr(self.datasource, "get_lag_metrics"):
            try:
                lm = self.datasource.get_lag_metrics()
                if lm is not None:
                    streaming_stats["lag_total"] = getattr(lm, "total_lag", None)
                    streaming_stats["lag_partitions"] = getattr(lm, "partitions", None)
            except Exception:
                pass

        base_stats.update(streaming_stats)
        return base_stats

    def _validate_ray_remote_args(
        self, ray_remote_args: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate and sanitize ray_remote_args to prevent security issues.

        Args:
            ray_remote_args: Raw ray remote arguments from user.

        Returns:
            Validated and sanitized ray remote arguments.

        Raises:
            ValueError: If invalid or potentially dangerous arguments are found.
        """
        if not ray_remote_args:
            return {}

        # Define allowed ray.remote argument keys
        allowed_keys = {
            "num_cpus",
            "num_gpus",
            "memory",
            "object_store_memory",
            "resources",
            "max_concurrency",
            "max_restarts",
            "max_task_retries",
            "retry_exceptions",
            "runtime_env",
            "placement_group",
            "placement_group_bundle_index",
            "placement_group_capture_child_tasks",
            "scheduling_strategy",
            "_metadata",
            "_labels",
            "num_returns",
            "_generator_backpressure_num_objects",
        }

        validated_args = {}
        for key, value in ray_remote_args.items():
            if key not in allowed_keys:
                raise ValueError(
                    f"Invalid ray_remote_arg key '{key}'. "
                    f"Allowed keys are: {sorted(allowed_keys)}"
                )
            validated_args[key] = value

        return validated_args


class _StreamingTaskWrapper(OpTask):
    """Wrapper to make streaming generator tasks compatible with OpTask interface."""

    def __init__(
        self,
        generator: ObjectRefGenerator,
        task_index: int,
        operator: "UnboundedDataOperator" = None,
    ):
        super().__init__(task_index)
        self._generator = generator
        self._operator = operator

    def get_waitable(self) -> ObjectRefGenerator:
        """Return the ObjectRefGenerator to wait on.

        The execution engine waits on the generator itself, which allows it to
        detect when new refs are yielded or when the generator completes.
        """
        return self._generator

    def get_requested_resource_bundle(self) -> Optional[Any]:
        return None  # Not tracked for streaming tasks

    def _cancel(self, force: bool = False) -> None:
        """Cancel the generator task."""
        ray.cancel(self._generator, force=force)

    def progress_str(self) -> str:
        """Get progress string for monitoring."""
        if self._operator:
            return (
                f"Batch: {self._operator._current_batch_id}, "
                f"Active generators: {len([g for g in self._operator._gens if not g.exhausted])}"
            )
        return f"Task: {self.task_index()}"

    def num_outputs_total(self) -> Optional[int]:
        """Streaming operators have unknown total outputs."""
        return None

    def current_processor_usage(self) -> ExecutionResources:
        """Get current CPU/GPU usage from active streaming tasks.

        This method is called by the resource manager to track current
        resource utilization for backpressure and scheduling decisions.
        """
        if self._operator:
            # Each generator represents one active task
            return ExecutionResources(
                cpu=self._operator.ray_remote_args.get("num_cpus", 0),
                gpu=self._operator.ray_remote_args.get("num_gpus", 0),
            )
        return ExecutionResources()

    def pending_processor_usage(self) -> ExecutionResources:
        """Get pending CPU/GPU usage from tasks being submitted.

        For streaming operators, this is typically zero since we don't
        pre-submit tasks beyond the current batch.
        """
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        """Get incremental resources needed to process additional input.

        For streaming operators, this represents the resources needed
        to launch one additional streaming task. This is used by the
        resource manager for backpressure decisions.
        """
        if self._operator:
            return ExecutionResources(
                cpu=self._operator.ray_remote_args.get("num_cpus", 0),
                gpu=self._operator.ray_remote_args.get("num_gpus", 0),
                memory=self._operator.ray_remote_args.get("memory", 0),
                # Object store memory estimate for streaming output
                object_store_memory=self._estimate_output_memory_per_task(),
            )
        return ExecutionResources()

    def min_max_resource_requirements(
        self,
    ) -> Tuple[ExecutionResources, ExecutionResources]:
        """Get minimum and maximum resource requirements for this operator.

        Returns:
            Tuple of (min_resources, max_resources) where:
            - min_resources: Minimum resources needed to make progress (1 task)
            - max_resources: Maximum resources this operator will use
              (parallelism tasks)
        """
        if self._operator:
            min_resources = ExecutionResources(
                cpu=self._operator.ray_remote_args.get("num_cpus", 0),
                gpu=self._operator.ray_remote_args.get("num_gpus", 0),
                memory=self._operator.ray_remote_args.get("memory", 0),
            )

            max_resources = ExecutionResources(
                cpu=self._operator.ray_remote_args.get("num_cpus", 0)
                * self._operator.parallelism,
                gpu=self._operator.ray_remote_args.get("num_gpus", 0)
                * self._operator.parallelism,
                memory=self._operator.ray_remote_args.get("memory", 0)
                * self._operator.parallelism,
            )

            return min_resources, max_resources

        return ExecutionResources(), ExecutionResources()

    def _estimate_output_memory_per_task(self) -> float:
        """Estimate object store memory usage per streaming task.

        This is used for resource management and backpressure decisions.
        We estimate based on max_records_per_task from the datasource.
        """
        if not self._operator:
            return 1024 * 1024  # 1MB default

        ctx = self._operator.data_context
        if hasattr(self._operator.datasource, "streaming_config"):
            max_records = self._operator.datasource.streaming_config.get(
                "max_records_per_task",
                ctx.streaming_default_max_records_per_trigger,
            )
        else:
            max_records = ctx.streaming_default_max_records_per_trigger

        return max_records * ctx.streaming_estimated_bytes_per_record
