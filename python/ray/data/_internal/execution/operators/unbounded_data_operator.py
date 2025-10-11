import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
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

        # Online data reading state
        self._current_read_tasks: List[ray.ObjectRef] = []
        self._last_trigger_time = datetime.now()
        self._current_batch_id = 0
        self._completed = False

        # For "once" trigger, we only produce one batch
        self._batch_produced = False

        # Performance tracking
        self._bytes_produced = 0
        self._rows_produced = 0

    def start(self, options: ExecutionOptions) -> None:
        """Start the streaming operator."""
        super().start(options)
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
            self._create_read_tasks()

    def _should_trigger_new_batch(self) -> bool:
        """Determine if a new batch should be triggered based on trigger configuration
        and backpressure.

        This method implements the core triggering logic for different streaming modes:
        - once: Trigger exactly one batch
        - continuous: Trigger new batch whenever previous batch completes
        - fixed_interval: Trigger at regular time intervals

        Returns:
            True if a new batch should be triggered, False otherwise
        """
        if self.trigger.trigger_type == "once":
            # One-time trigger: only produce batch if we haven't already
            return not self._batch_produced

        # Check system backpressure before creating new batches
        # This prevents overwhelming downstream operators or running out of memory
        if self._check_backpressure():
            logger.debug("Backpressure detected, delaying new batch creation")
            return False

        elif self.trigger.trigger_type == "continuous":
            # Continuous mode: create new tasks immediately when current batch finishes
            # This minimizes latency for real-time streaming
            return len(self._current_read_tasks) == 0

        elif self.trigger.trigger_type == "fixed_interval":
            # Interval-based trigger: wait for time interval AND previous batch completion
            now = datetime.now()
            time_since_last = now - self._last_trigger_time
            # Both conditions must be met to avoid task buildup
            return (
                time_since_last >= self.trigger.interval
                and len(self._current_read_tasks) == 0
            )

        return False

    def _check_backpressure(self) -> bool:
        """Check if backpressure conditions are met to prevent overwhelming the system.

        Backpressure detection helps prevent:
        - Memory overflow from producing data faster than it can be consumed
        - Task queue buildup that could cause OOM errors
        - Overwhelming downstream operators

        Checks both memory pressure and concurrent task limits.

        Returns:
            True if backpressure is detected and we should pause new batch creation
        """
        try:
            from ray.data.context import DataContext

            data_context = DataContext.get_current()

            # Get backpressure threshold from context (default 80% memory usage)
            # This is configurable per application based on available resources
            backpressure_threshold = getattr(
                data_context, "streaming_backpressure_threshold", 0.8
            )

            # Check system memory usage with fallback chain
            memory_pressure = False
            try:
                import psutil

                # Use psutil to check actual system memory (most accurate)
                memory_percent = psutil.virtual_memory().percent / 100.0
                memory_pressure = memory_percent > backpressure_threshold

                if memory_pressure:
                    logger.warning(
                        f"Memory usage {memory_percent:.1%} exceeds "
                        f"backpressure threshold {backpressure_threshold:.1%}"
                    )
            except ImportError:
                # psutil not available - try Ray's object store monitoring
                # This is less accurate but better than nothing
                try:
                    import ray

                    memory_info = ray.cluster_resources()
                    object_store_memory = memory_info.get("object_store_memory", 0)
                    if object_store_memory > 0:
                        # Note: We can't easily get current usage, only total capacity
                        # This is a limitation of the Ray API
                        # In production, install psutil for better memory monitoring
                        pass
                except Exception:
                    # If all memory checks fail, proceed conservatively
                    # Better to create tasks than deadlock waiting for memory info
                    pass

            # Check concurrent task limit to prevent unbounded task creation
            # This protects against scenarios where tasks complete slowly
            max_concurrent = getattr(data_context, "streaming_max_concurrent_tasks", 10)
            if len(self._current_read_tasks) >= max_concurrent:
                logger.debug(
                    f"Concurrent tasks {len(self._current_read_tasks)} at "
                    f"limit {max_concurrent}"
                )
                return True

            return memory_pressure

        except Exception as e:
            logger.warning(
                f"Error checking backpressure, assuming no backpressure: {e}"
            )
            return False

    def _create_read_tasks(self) -> None:
        """Create new read tasks from the datasource.

        This method:
        1. Retrieves ReadTask objects from the datasource
        2. Submits them as remote Ray tasks for distributed execution
        3. Tracks task references and start times for monitoring
        4. Applies resource optimization based on task characteristics

        The read tasks are executed remotely to enable:
        - Distributed reading across multiple Ray workers
        - Parallel data ingestion from multiple sources (topics, shards, etc.)
        - Isolation from the driver process
        """
        try:
            # Get read tasks from the datasource
            # Each task represents reading from one partition/shard/topic
            read_tasks: List[ReadTask] = self.datasource.get_read_tasks(
                self.parallelism
            )

            if not read_tasks:
                # No tasks available - datasource may be empty or unavailable
                # For "once" trigger, mark as completed so execution can finish
                if self.trigger.trigger_type == "once":
                    self._completed = True
                logger.info("No read tasks available from datasource")
                return

            # Execute read tasks remotely on Ray workers
            self._current_read_tasks = []
            self._task_start_times = (
                []
            )  # Track when each task was created for timeout detection
            for i, task in enumerate(read_tasks):
                try:
                    # Validate ray_remote_args to prevent security issues
                    # This whitelist approach prevents injection of dangerous parameters
                    validated_args = self._validate_ray_remote_args(
                        self.ray_remote_args
                    )

                    # Optimize resource allocation based on task metadata
                    # This allows streaming tasks with known characteristics to request
                    # appropriate resources upfront, improving scheduling efficiency
                    if hasattr(task, "metadata") and task.metadata:
                        # Use task metadata to optimize resource allocation
                        estimated_rows = getattr(task.metadata, "num_rows", None)
                        estimated_size = getattr(task.metadata, "size_bytes", None)

                        # Large data tasks (> 100MB) get more memory
                        # This prevents OOM errors on workers processing large messages
                        if estimated_size and estimated_size > 100 * 1024 * 1024:
                            validated_args.setdefault(
                                "memory", 2 * 1024 * 1024 * 1024
                            )  # 2GB

                        # High-row-count tasks (> 100k rows) get more CPUs
                        # This helps with deserialization and processing overhead
                        if estimated_rows and estimated_rows > 100000:
                            validated_args.setdefault("num_cpus", 2)

                    # Convert the ReadTask's read_fn to a remote function
                    # This executes it on a Ray worker rather than the driver
                    # With num_returns="streaming", this handles generators efficiently:
                    # - read_fn can yield multiple tables incrementally
                    # - Ray streams them back without buffering all in memory
                    # - When generator completes, we get all yielded values
                    remote_fn = ray.remote(**validated_args)(task.read_fn)
                    task_ref = (
                        remote_fn.remote()
                    )  # Returns ObjectRefGenerator if read_fn is generator
                    self._current_read_tasks.append(task_ref)
                    self._task_start_times.append(
                        datetime.now()
                    )  # Track start time for timeout detection

                    # Log task creation with resource details for debugging
                    logger.debug(
                        f"Created task {i+1}/{len(read_tasks)} with "
                        f"resources: {validated_args}"
                    )

                except Exception as e:
                    logger.error(f"Failed to create remote task {i+1}: {e}")
                    # Continue with other tasks rather than failing the entire batch
                    # This provides better fault tolerance for partial failures
                    continue

            # Update timing and batch tracking
            self._last_trigger_time = datetime.now()
            self._current_batch_id += 1

            # Mark batch as produced for "once" trigger mode
            if self.trigger.trigger_type == "once":
                self._batch_produced = True

            logger.info(
                f"Created {len(self._current_read_tasks)} read tasks for "
                f"batch {self._current_batch_id}"
            )

        except Exception as e:
            # Log error but don't fail operator completely
            # This allows recovery on next trigger attempt
            logger.error(f"Error creating read tasks: {e}")
            self._current_read_tasks = []

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

        Returns:
            True if operator can produce more output, False otherwise
        """
        # First check if we have any completed tasks ready to return
        # This is the fast path for the common case
        if self._current_read_tasks:
            ready, _ = ray.wait(self._current_read_tasks, num_returns=1, timeout=0)
            if ready:
                return True

            # Check for tasks that have been running too long
            # This prevents indefinite blocking on stuck tasks
            self._check_task_timeouts()

        # No ready tasks - check if we should trigger a new batch
        # This implements the trigger logic (once, continuous, interval, etc.)
        if self._should_trigger_new_batch():
            self._create_read_tasks()
            return len(self._current_read_tasks) > 0

        # For continuous streams, keep running indefinitely unless explicitly stopped
        # This is the key difference from bounded operators
        if self.trigger.trigger_type == "continuous":
            return not self._completed

        # For "once" trigger, we're done when the batch is consumed
        return False

    def _check_task_timeouts(self) -> None:
        """Check for timed-out tasks and clean them up.

        Long-running tasks can indicate:
        - Network issues connecting to external systems (Kafka, Kinesis, Flink)
        - Deadlocks in the read function
        - Resource starvation on workers

        This method identifies and cancels stuck tasks to prevent indefinite blocking.
        Configurable timeout via data_context.streaming_task_timeout (default: 5 minutes).
        """
        try:
            from ray.data.context import DataContext

            data_context = DataContext.get_current()
            # Get timeout from context (default 5 minutes for streaming tasks)
            # This is longer than typical task timeouts to account for network delays
            timeout_seconds = getattr(
                data_context, "streaming_task_timeout", 300
            )  # 5 min default

            if not timeout_seconds or not self._current_read_tasks:
                return

            # Check if any tasks have been running too long
            now = datetime.now()
            timed_out_tasks = []

            for i, task_ref in enumerate(self._current_read_tasks):
                if hasattr(self, "_task_start_times") and i < len(
                    self._task_start_times
                ):
                    task_start = self._task_start_times[i]
                    task_duration = (now - task_start).total_seconds()

                    if task_duration > timeout_seconds:
                        # Verify the task is actually stuck before cancelling
                        # Don't cancel tasks that finished naturally but haven't been collected
                        try:
                            ready, _ = ray.wait([task_ref], timeout=0)
                            if not ready:  # Task is still running (potentially stuck)
                                logger.warning(
                                    f"Task {i} has been running for {task_duration:.1f}s "
                                    f"(timeout: {timeout_seconds}s), marking for cancellation"
                                )
                                timed_out_tasks.append(i)
                            # If task is ready, it finished naturally, don't timeout
                        except Exception as e:
                            logger.debug(f"Error checking task {i} status: {e}")
                            # If we can't check status, err on side of caution and timeout
                            # This prevents indefinite blocking on invalid task refs
                            timed_out_tasks.append(i)

            # Clean up timed-out tasks
            if timed_out_tasks:
                successfully_cancelled = 0
                # Process in reverse order to avoid index shifting during removal
                for i in reversed(timed_out_tasks):
                    if i < len(self._current_read_tasks):
                        # Cancel the timed-out task (force=True to interrupt immediately)
                        try:
                            ray.cancel(self._current_read_tasks[i], force=True)
                            successfully_cancelled += 1
                        except Exception as e:
                            logger.warning(f"Could not cancel timed-out task {i}: {e}")

                        # Remove from tracking lists
                        # This prevents us from waiting on stuck tasks forever
                        self._current_read_tasks.pop(i)
                        if hasattr(self, "_task_start_times") and i < len(
                            self._task_start_times
                        ):
                            self._task_start_times.pop(i)

                logger.info(
                    f"Cleaned up {successfully_cancelled}/{len(timed_out_tasks)} timed-out tasks"
                )

        except Exception as e:
            logger.warning(f"Error checking task timeouts: {e}")

    def _get_next_inner(self) -> RefBundle:
        """Get the next result from completed read tasks.

        This method implements the core output logic for the operator:
        1. Wait for at least one read task to complete
        2. Retrieve the result (PyArrow tables)
        3. Convert to RefBundle for Ray Data's execution engine
        4. Update performance metrics

        Returns:
            RefBundle containing blocks from the completed read task

        Raises:
            StopIteration: When no tasks are ready or an error occurs
        """
        if not self._current_read_tasks:
            raise StopIteration("No read tasks available")

        # Wait for at least one task to complete (1 second timeout)
        # Short timeout allows us to check for new trigger conditions frequently
        # Note: With num_returns="streaming", ready[0] is an ObjectRefGenerator
        # ray.wait() waits for the generator to COMPLETE (all yields done)
        # This is correct for microbatch - we want the full batch before continuing
        ready, remaining = ray.wait(
            self._current_read_tasks, num_returns=1, timeout=1.0
        )

        if not ready:
            # No tasks completed yet - for continuous mode, create new batch
            # This ensures continuous streaming doesn't stall between microbatches
            if (
                self.trigger.trigger_type == "continuous"
                and self._should_trigger_new_batch()
            ):
                self._create_read_tasks()
            raise StopIteration("No tasks ready")

        # Update remaining tasks - remove the completed one
        self._current_read_tasks = remaining

        # Retrieve the result from the completed task
        # For generators (num_returns="streaming"), ray.get() returns all yielded values
        # This is perfect for microbatch - we get all tables from this batch
        try:
            result = ray.get(ready[0])

            # Convert result to RefBundle format expected by Ray Data
            # For Kafka/Kinesis/Flink, result is an iterable of PyArrow tables
            if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
                # Result is an iterable (list or materialized generator) of blocks
                # Each block is a PyArrow table from the incremental yields
                block_refs = []
                metadata_list = []

                for block in result:
                    # Put each table in Ray's object store
                    # This allows downstream operators to access them
                    block_ref = ray.put(block)
                    block_refs.append(block_ref)
                    # Metadata will be inferred by downstream operators
                    # (num_rows, size_bytes, schema, etc.)
                    metadata_list.append(None)

                bundle = RefBundle(refs=block_refs, metadata=metadata_list)
                self._update_performance_metrics(bundle)
                return bundle
            else:
                # Single block result (less common, for simple datasources)
                block_ref = ray.put(result)
                bundle = RefBundle(refs=[block_ref], metadata=[None])
                self._update_performance_metrics(bundle)
                return bundle

        except Exception as e:
            # Log and propagate errors from read tasks
            # This could be network errors, deserialization failures, etc.
            logger.error(f"Error getting result from read task: {e}")
            raise StopIteration(f"Error in read task: {e}")

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

        Returns:
            True if operator has completed execution
        """
        if self.trigger.trigger_type == "once":
            # For one-time trigger: complete when batch is produced AND all tasks consumed
            return self._batch_produced and len(self._current_read_tasks) == 0

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

    def get_active_tasks(self) -> List[Any]:
        """Get list of active streaming tasks for monitoring.

        Returns task wrappers that implement the OpTask interface, allowing
        Ray Data's execution engine to monitor and manage streaming tasks.

        Returns:
            List of _StreamingTaskWrapper objects representing active tasks
        """
        # Wrap each ObjectRef in a task wrapper for monitoring compatibility
        return [
            _StreamingTaskWrapper(ref, i, self)
            for i, ref in enumerate(self._current_read_tasks)
        ]

    def num_active_tasks(self) -> int:
        """Get the number of currently active streaming tasks.

        Used by Ray Data's execution engine for progress tracking and
        resource management decisions.

        Returns:
            Number of active streaming read tasks
        """
        return len(self._current_read_tasks)

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
            # For unbounded data operators, we yield block + metadata per record batch
            # Double the buffer size to account for both objects
            self.ray_remote_args["_generator_backpressure_num_objects"] = (
                2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
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

    def notify_in_task_submission_backpressure(self, in_backpressure: bool) -> None:
        """Notification callback about task submission backpressure state.

        Called by Ray Data's execution engine when the system enters or exits
        backpressure. This allows the operator to adapt its behavior dynamically.

        For streaming operators, we could potentially:
        - Pause trigger firing during backpressure
        - Increase batch sizes to reduce task overhead
        - Adjust polling intervals

        Args:
            in_backpressure: True if system is currently in backpressure
        """
        if in_backpressure:
            # Hook for future adaptive behavior
            # Could slow down data production or increase batch sizes
            # Currently a no-op, but provides extension point
            pass

    def _update_performance_metrics(self, bundle: RefBundle) -> None:
        """Update performance tracking metrics."""
        try:
            # Estimate bytes and rows from bundle
            estimated_bytes = 0
            estimated_rows = 0

            for metadata in bundle.metadata:
                if metadata:
                    if hasattr(metadata, "size_bytes") and metadata.size_bytes:
                        estimated_bytes += metadata.size_bytes
                    if hasattr(metadata, "num_rows") and metadata.num_rows:
                        estimated_rows += metadata.num_rows

            # If no metadata available, use conservative estimates
            if estimated_bytes == 0:
                estimated_bytes = len(bundle.refs) * 1024  # 1KB per block estimate
            if estimated_rows == 0:
                estimated_rows = len(bundle.refs) * 100  # 100 rows per block estimate

            self._bytes_produced += estimated_bytes
            self._rows_produced += estimated_rows

            # Log performance metrics periodically
            if self._current_batch_id % 10 == 0:  # Every 10 batches
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
            "active_tasks": len(self._current_read_tasks),
            "trigger_type": self.trigger.trigger_type,
            "batch_produced": self._batch_produced,
            "total_bytes_produced": self._bytes_produced,
            "total_rows_produced": self._rows_produced,
            "parallelism": self.parallelism,
            "last_trigger_time": (
                self._last_trigger_time.isoformat() if self._last_trigger_time else None
            ),
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


class _StreamingTaskWrapper:
    """Wrapper to make streaming task refs compatible with OpTask interface."""

    def __init__(
        self,
        task_ref: ray.ObjectRef,
        task_index: int,
        operator: "UnboundedDataOperator" = None,
    ):
        self._task_ref = task_ref
        self._task_index = task_index
        self._operator = operator

    def task_index(self) -> int:
        return self._task_index

    def get_waitable(self) -> ray.ObjectRef:
        return self._task_ref

    def get_requested_resource_bundle(self) -> Optional[Any]:
        return None  # Not tracked for streaming tasks

    def cancel(self, force: bool = False) -> None:
        ray.cancel(self._task_ref, force=force)

    def progress_str(self) -> str:
        """Get progress string for monitoring."""
        if self._operator:
            return (
                f"Batch: {self._operator._current_batch_id}, "
                f"Active tasks: {len(self._operator._current_read_tasks)}"
            )
        return f"Task: {self._task_index}"

    def num_outputs_total(self) -> Optional[int]:
        """Streaming operators have unknown total outputs."""
        return None

    def current_processor_usage(self) -> ExecutionResources:
        """Get current CPU/GPU usage from active streaming tasks.

        This method is called by the resource manager to track current
        resource utilization for backpressure and scheduling decisions.
        """
        if self._operator:
            num_active_tasks = len(self._operator._current_read_tasks)
            return ExecutionResources(
                cpu=self._operator.ray_remote_args.get("num_cpus", 0)
                * num_active_tasks,
                gpu=self._operator.ray_remote_args.get("num_gpus", 0)
                * num_active_tasks,
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
        try:
            if self._operator:
                # Try to get max_records_per_task from datasource config
                if hasattr(self._operator.datasource, "streaming_config"):
                    max_records = self._operator.datasource.streaming_config.get(
                        "max_records_per_task", 1000
                    )
                else:
                    max_records = 1000  # Default estimate

                # Estimate ~1KB per record for typical streaming data
                # This is conservative and should be configurable in production
                estimated_bytes_per_record = 1024
                return max_records * estimated_bytes_per_record

        except Exception:
            # Fallback to conservative estimate
            pass

        return 1024 * 1024  # 1MB default
