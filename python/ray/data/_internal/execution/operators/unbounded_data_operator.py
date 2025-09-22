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

    This operator reads from unbounded online data sources like Kafka, Kinesis, etc.
    using the standard Ray Data ReadTask pattern. It leverages Ray's distributed
    task execution to handle continuous data reading based on trigger patterns.

    Note: This is for online/unbounded data sources, not Ray Data's "streaming execution"
    which refers to Ray's execution engine. This operator handles data that continuously
    arrives from external systems.
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

        # Initialize first batch of read tasks if needed
        if self._should_trigger_new_batch():
            self._create_read_tasks()

    def _should_trigger_new_batch(self) -> bool:
        """Determine if a new batch should be triggered based on trigger configuration
        and backpressure."""
        if self.trigger.trigger_type == "once":
            return not self._batch_produced

        # Check backpressure conditions before triggering new batches
        if self._check_backpressure():
            logger.debug("Backpressure detected, delaying new batch creation")
            return False

        elif self.trigger.trigger_type == "continuous":
            # For continuous, always create new tasks when current ones are done
            return len(self._current_read_tasks) == 0

        elif self.trigger.trigger_type == "fixed_interval":
            now = datetime.now()
            time_since_last = now - self._last_trigger_time
            # Trigger if interval has passed and no tasks are running
            return (
                time_since_last >= self.trigger.interval
                and len(self._current_read_tasks) == 0
            )

        return False

    def _check_backpressure(self) -> bool:
        """Check if backpressure conditions are met to prevent overwhelming the
        system."""
        try:
            from ray.data.context import DataContext

            data_context = DataContext.get_current()

            # Get backpressure threshold with fallback
            backpressure_threshold = getattr(
                data_context, "streaming_backpressure_threshold", 0.8
            )

            # Check memory usage with fallback to Ray memory monitoring
            memory_pressure = False
            try:
                import psutil

                memory_percent = psutil.virtual_memory().percent / 100.0
                memory_pressure = memory_percent > backpressure_threshold

                if memory_pressure:
                    logger.warning(
                        f"Memory usage {memory_percent:.1%} exceeds "
                        f"backpressure threshold {backpressure_threshold:.1%}"
                    )
            except ImportError:
                # Fallback to Ray's object store memory monitoring
                try:
                    import ray

                    memory_info = ray.cluster_resources()
                    object_store_memory = memory_info.get("object_store_memory", 0)
                    if object_store_memory > 0:
                        # Simple heuristic: if we're using a lot of object store memory
                        # This is a rough approximation since we can't get current usage easily
                        pass
                except Exception:
                    # If all memory checks fail, be conservative
                    pass

            # Check if we have too many concurrent tasks
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
        """Create new read tasks from the datasource."""
        try:
            # Get read tasks from datasource
            read_tasks: List[ReadTask] = self.datasource.get_read_tasks(
                self.parallelism
            )

            if not read_tasks:
                # No tasks available, mark as completed for "once" trigger
                if self.trigger.trigger_type == "once":
                    self._completed = True
                logger.info("No read tasks available from datasource")
                return

            # Execute read tasks remotely
            self._current_read_tasks = []
            self._task_start_times = []  # Track when each task was created
            for i, task in enumerate(read_tasks):
                try:
                    # Create remote task with proper Ray configuration
                    # The ReadTask's read_fn should be executed remotely
                    # Validate ray_remote_args to prevent security issues
                    validated_args = self._validate_ray_remote_args(
                        self.ray_remote_args
                    )

                    # Optimize resource allocation based on task characteristics
                    if hasattr(task, "metadata") and task.metadata:
                        # Use task metadata to optimize resource allocation
                        estimated_rows = getattr(task.metadata, "num_rows", None)
                        estimated_size = getattr(task.metadata, "size_bytes", None)

                        # > 100MB
                        if estimated_size and estimated_size > 100 * 1024 * 1024:
                            # Large data tasks get more memory
                            validated_args.setdefault(
                                "memory", 2 * 1024 * 1024 * 1024
                            )  # 2GB

                        if estimated_rows and estimated_rows > 100000:  # > 100k rows
                            # High-row-count tasks get more CPUs
                            validated_args.setdefault("num_cpus", 2)

                    remote_fn = ray.remote(**validated_args)(task.read_fn)
                    task_ref = remote_fn.remote()
                    self._current_read_tasks.append(task_ref)
                    self._task_start_times.append(datetime.now())  # Track start time

                    # Log task creation with resource details
                    logger.debug(
                        f"Created task {i+1}/{len(read_tasks)} with "
                        f"resources: {validated_args}"
                    )

                except Exception as e:
                    logger.error(f"Failed to create remote task {i+1}: {e}")
                    # Continue with other tasks rather than failing completely
                    continue

            # Update timing and batch tracking
            self._last_trigger_time = datetime.now()
            self._current_batch_id += 1

            if self.trigger.trigger_type == "once":
                self._batch_produced = True

            logger.info(
                f"Created {len(self._current_read_tasks)} read tasks for "
                f"batch {self._current_batch_id}"
            )

        except Exception as e:
            # Log error but don't fail completely
            logger.error(f"Error creating read tasks: {e}")
            self._current_read_tasks = []

    def should_add_input(self) -> bool:
        """Streaming operators don't accept external input."""
        return False

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Streaming operators don't accept external input."""
        raise RuntimeError("UnboundedDataOperator does not accept input")

    def has_next(self) -> bool:
        """Check if there are available results or if more batches should be
        triggered."""
        # Check for completed tasks with timeout management
        if self._current_read_tasks:
            ready, _ = ray.wait(self._current_read_tasks, num_returns=1, timeout=0)
            if ready:
                return True

            # Check for timed-out tasks
            self._check_task_timeouts()

        # Check if we should trigger new batch
        if self._should_trigger_new_batch():
            self._create_read_tasks()
            return len(self._current_read_tasks) > 0

        # For continuous streams, keep running unless explicitly stopped
        if self.trigger.trigger_type == "continuous":
            return not self._completed

        # For "once" trigger, complete when batch is done
        return False

    def _check_task_timeouts(self) -> None:
        """Check for timed-out tasks and clean them up."""
        try:
            from ray.data.context import DataContext

            data_context = DataContext.get_current()
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
                        # Before marking as timed out, check if task is actually stuck
                        # by seeing if it's still running (not finished)
                        try:
                            ready, _ = ray.wait([task_ref], timeout=0)
                            if not ready:  # Task is still running
                                logger.warning(
                                    f"Task {i} has been running for {task_duration:.1f}s "
                                    f"(timeout: {timeout_seconds}s), marking for cancellation"
                                )
                                timed_out_tasks.append(i)
                            # If task is ready, it finished naturally, don't timeout
                        except Exception as e:
                            logger.debug(f"Error checking task {i} status: {e}")
                            # If we can't check status, err on side of caution and timeout
                            timed_out_tasks.append(i)

            # Remove timed-out tasks
            if timed_out_tasks:
                successfully_cancelled = 0
                for i in reversed(timed_out_tasks):
                    if i < len(self._current_read_tasks):
                        # Cancel the timed-out task
                        try:
                            ray.cancel(self._current_read_tasks[i], force=True)
                            successfully_cancelled += 1
                        except Exception as e:
                            logger.warning(f"Could not cancel timed-out task {i}: {e}")

                        # Remove from our tracking
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
        """Get the next result from completed read tasks."""
        if not self._current_read_tasks:
            raise StopIteration("No read tasks available")

        # Wait for at least one task to complete
        ready, remaining = ray.wait(
            self._current_read_tasks, num_returns=1, timeout=1.0
        )

        if not ready:
            # For continuous triggers, create new tasks if none are ready
            if (
                self.trigger.trigger_type == "continuous"
                and self._should_trigger_new_batch()
            ):
                self._create_read_tasks()
            raise StopIteration("No tasks ready")

        # Update remaining tasks
        self._current_read_tasks = remaining

        # Get the result
        try:
            result = ray.get(ready[0])

            # Convert result to RefBundle
            if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
                # Result is iterable (list of blocks)
                block_refs = []
                metadata_list = []

                for block in result:
                    block_ref = ray.put(block)
                    block_refs.append(block_ref)
                    # Create basic metadata for the block
                    metadata_list.append(None)  # Will be inferred later

                bundle = RefBundle(refs=block_refs, metadata=metadata_list)
                self._update_performance_metrics(bundle)
                return bundle
            else:
                # Single block result
                block_ref = ray.put(result)
                bundle = RefBundle(refs=[block_ref], metadata=[None])
                self._update_performance_metrics(bundle)
                return bundle

        except Exception as e:
            logger.error(f"Error getting result from read task: {e}")
            raise StopIteration(f"Error in read task: {e}")

    def input_done(self, input_index: int) -> None:
        """Called when upstream input is done (not applicable for source operators)."""
        pass

    def all_inputs_done(self) -> None:
        """Called when all inputs are done (not applicable for source operators)."""
        pass

    def completed(self) -> bool:
        """Check if the streaming operator has completed."""
        if self.trigger.trigger_type == "once":
            return self._batch_produced and len(self._current_read_tasks) == 0

        # Continuous streams run indefinitely unless explicitly stopped
        return self._completed

    def can_add_input(self, bundle: RefBundle) -> bool:
        """Streaming operators don't accept external input."""
        return False

    def throttling_disabled(self) -> bool:
        """Streaming operators handle their own throttling via triggers."""
        return True

    def get_active_tasks(self) -> List[Any]:
        """Get list of active streaming tasks."""
        # Return task references as OpTask-like objects for monitoring
        return [
            _StreamingTaskWrapper(ref, i, self)
            for i, ref in enumerate(self._current_read_tasks)
        ]

    def num_active_tasks(self) -> int:
        """Return number of active streaming tasks."""
        return len(self._current_read_tasks)


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

    def _apply_online_data_remote_args(self) -> None:
        """Apply online data source specific Ray remote args for optimal performance."""
        # Set up Ray Data streaming execution generator backpressure if not already configured
        # Note: This is Ray Data's streaming execution, not the online data source
        if (
            "_generator_backpressure_num_objects" not in self.ray_remote_args
            and self.data_context._max_num_blocks_in_streaming_gen_buffer is not None
        ):
            # For online data operators, we yield block + metadata per record batch
            self.ray_remote_args["_generator_backpressure_num_objects"] = (
                2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
            )

        # Enable Ray Data streaming execution returns for better memory management
        if "num_returns" not in self.ray_remote_args:
            self.ray_remote_args["num_returns"] = "streaming"

        # Add operator ID for better tracking
        if "_labels" not in self.ray_remote_args:
            self.ray_remote_args["_labels"] = {}
        self.ray_remote_args["_labels"][self._OPERATOR_ID_LABEL_KEY] = self.id

    def can_add_input(self, bundle: RefBundle) -> bool:
        """Determine if this operator can accept a specific input bundle.

        For streaming operators, this always returns False since they
        generate their own data and don't accept external inputs.

        Args:
            bundle: The input bundle to potentially add.

        Returns:
            Always False for streaming operators.
        """
        return False

    def implements_accurate_memory_accounting(self) -> bool:
        """Whether this operator implements accurate memory accounting.

        For streaming operators, memory usage is estimated rather than
        precisely tracked due to the dynamic nature of streaming data.

        Returns:
            False to indicate estimates rather than precise accounting.
        """
        return False

    def notify_in_task_submission_backpressure(self, in_backpressure: bool) -> None:
        """Notification about task submission backpressure state.

        This can be used by streaming operators to adapt their behavior
        when the system is under backpressure.

        Args:
            in_backpressure: Whether the system is currently in backpressure.
        """
        if in_backpressure:
            # Hook for future adaptive behavior
            # Could potentially slow down data production or increase batch sizes
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
