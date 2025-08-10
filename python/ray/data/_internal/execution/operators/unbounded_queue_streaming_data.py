from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
    ExecutionResources,
)
from ray.data._internal.logical.operators.streaming_data_operator import StreamingTrigger
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask


class UnboundedQueueStreamingDataOperator(PhysicalOperator):
    """Physical operator for unbounded streaming data sources.
    
    This operator reads from streaming sources like Kafka using the standard
    Ray Data ReadTask pattern. Instead of using threading, it leverages Ray's
    distributed task execution to handle continuous data reading based on
    trigger patterns.
    """

    def __init__(
        self,
        data_context: DataContext,
        datasource: Datasource,
        trigger: StreamingTrigger,
        parallelism: int = -1,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the unbounded streaming operator.
        
        Args:
            data_context: Ray Data context
            datasource: The streaming datasource (e.g., KafkaDatasource)
            trigger: Trigger configuration for microbatch processing
            parallelism: Number of parallel read tasks
            ray_remote_args: Arguments passed to ray.remote for read tasks
        """
        super().__init__("UnboundedQueueStreamingData", [], data_context, target_max_block_size=None)
        
        self.datasource = datasource
        self.trigger = trigger
        self.parallelism = parallelism if parallelism > 0 else 1
        self.ray_remote_args = ray_remote_args or {}
        
        # Add streaming-specific remote args for better performance
        self._apply_streaming_remote_args()
        
        # Streaming state
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
        # Initialize first batch of read tasks if needed
        if self._should_trigger_new_batch():
            self._create_read_tasks()

    def _should_trigger_new_batch(self) -> bool:
        """Determine if a new batch should be triggered based on trigger configuration."""
        if self.trigger.trigger_type == "once":
            return not self._batch_produced
            
        elif self.trigger.trigger_type == "continuous":
            # For continuous, always create new tasks when current ones are done
            return len(self._current_read_tasks) == 0
            
        elif self.trigger.trigger_type == "fixed_interval":
            now = datetime.now()
            time_since_last = now - self._last_trigger_time
            # Trigger if interval has passed and no tasks are running
            return (time_since_last >= self.trigger.interval and 
                    len(self._current_read_tasks) == 0)
        
        return False

    def _create_read_tasks(self) -> None:
        """Create new read tasks from the datasource."""
        try:
            # Get read tasks from datasource
            read_tasks: List[ReadTask] = self.datasource.get_read_tasks(self.parallelism)
            
            if not read_tasks:
                # No tasks available, mark as completed for "once" trigger
                if self.trigger.trigger_type == "once":
                    self._completed = True
                return
            
            # Execute read tasks remotely
            self._current_read_tasks = []
            for task in read_tasks:
                # Create remote task with proper Ray configuration
                # The ReadTask's read_fn should be executed remotely
                remote_fn = ray.remote(**self.ray_remote_args)(task.read_fn)
                task_ref = remote_fn.remote()
                self._current_read_tasks.append(task_ref)
            
            # Update timing and batch tracking
            self._last_trigger_time = datetime.now()
            self._current_batch_id += 1
            
            if self.trigger.trigger_type == "once":
                self._batch_produced = True
                
        except Exception as e:
            # Log error but don't fail completely
            print(f"Error creating read tasks: {e}")
            self._current_read_tasks = []

    def should_add_input(self) -> bool:
        """Streaming operators don't accept external input."""
        return False

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Streaming operators don't accept external input."""
        raise RuntimeError("UnboundedQueueStreamingDataOperator does not accept input")

    def has_next(self) -> bool:
        """Check if there are available results or if more batches should be triggered."""
        # Check for completed tasks
        if self._current_read_tasks:
            ready, _ = ray.wait(self._current_read_tasks, num_returns=1, timeout=0)
            if ready:
                return True
        
        # Check if we should trigger new batch
        if self._should_trigger_new_batch():
            self._create_read_tasks()
            return len(self._current_read_tasks) > 0
        
        # For continuous streams, keep running unless explicitly stopped
        if self.trigger.trigger_type == "continuous":
            return not self._completed
            
        # For "once" trigger, complete when batch is done
        return False

    def get_next(self) -> RefBundle:
        """Get the next result from completed read tasks."""
        if not self._current_read_tasks:
            raise StopIteration("No read tasks available")
        
        # Wait for at least one task to complete
        ready, remaining = ray.wait(self._current_read_tasks, num_returns=1, timeout=1.0)
        
        if not ready:
            # For continuous triggers, create new tasks if none are ready
            if self.trigger.trigger_type == "continuous" and self._should_trigger_new_batch():
                self._create_read_tasks()
            raise StopIteration("No tasks ready")
        
        # Update remaining tasks
        self._current_read_tasks = remaining
        
        # Get the result
        try:
            result = ray.get(ready[0])
            
            # Convert result to RefBundle
            if hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
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
            print(f"Error getting result from read task: {e}")
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

    def shutdown(self) -> None:
        """Shutdown the streaming operator."""
        # Cancel any remaining tasks
        for task_ref in self._current_read_tasks:
            ray.cancel(task_ref, force=True)
        self._current_read_tasks = []
        self._completed = True
        super().shutdown()

    def progress_str(self) -> str:
        """Get progress string for monitoring."""
        return f"Batch: {self._current_batch_id}, Active tasks: {len(self._current_read_tasks)}"

    def num_outputs_total(self) -> Optional[int]:
        """Streaming operators have unknown total outputs."""
        return None

    def current_processor_usage(self) -> ExecutionResources:
        """Get current CPU/GPU usage from active streaming tasks.
        
        This method is called by the resource manager to track current
        resource utilization for backpressure and scheduling decisions.
        """
        num_active_tasks = len(self._current_read_tasks)
        return ExecutionResources(
            cpu=self.ray_remote_args.get("num_cpus", 0) * num_active_tasks,
            gpu=self.ray_remote_args.get("num_gpus", 0) * num_active_tasks,
        )

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
        return ExecutionResources(
            cpu=self.ray_remote_args.get("num_cpus", 0),
            gpu=self.ray_remote_args.get("num_gpus", 0),
            memory=self.ray_remote_args.get("memory", 0),
            # Object store memory estimate for streaming output
            object_store_memory=self._estimate_output_memory_per_task(),
        )

    def min_max_resource_requirements(self) -> Tuple[ExecutionResources, ExecutionResources]:
        """Get minimum and maximum resource requirements for this operator.
        
        Returns:
            Tuple of (min_resources, max_resources) where:
            - min_resources: Minimum resources needed to make progress (1 task)
            - max_resources: Maximum resources this operator will use (parallelism tasks)
        """
        min_resources = ExecutionResources(
            cpu=self.ray_remote_args.get("num_cpus", 0),
            gpu=self.ray_remote_args.get("num_gpus", 0),
            memory=self.ray_remote_args.get("memory", 0),
        )
        
        max_resources = ExecutionResources(
            cpu=self.ray_remote_args.get("num_cpus", 0) * self.parallelism,
            gpu=self.ray_remote_args.get("num_gpus", 0) * self.parallelism,
            memory=self.ray_remote_args.get("memory", 0) * self.parallelism,
        )
        
        return min_resources, max_resources

    def _estimate_output_memory_per_task(self) -> float:
        """Estimate object store memory usage per streaming task.
        
        This is used for resource management and backpressure decisions.
        We estimate based on max_records_per_task from the datasource.
        """
        try:
            # Try to get max_records_per_task from datasource config
            if hasattr(self.datasource, 'streaming_config'):
                max_records = self.datasource.streaming_config.get('max_records_per_task', 1000)
            else:
                max_records = 1000  # Default estimate
            
            # Estimate ~1KB per record for typical streaming data
            # This is conservative and should be configurable in production
            estimated_bytes_per_record = 1024
            return max_records * estimated_bytes_per_record
            
        except Exception:
            # Fallback to conservative estimate
            return 1024 * 1024  # 1MB default

    def _apply_streaming_remote_args(self) -> None:
        """Apply streaming-specific Ray remote args for optimal performance."""
        # Set up streaming generator backpressure if not already configured
        if ("_generator_backpressure_num_objects" not in self.ray_remote_args and 
            self.data_context._max_num_blocks_in_streaming_gen_buffer is not None):
            # For streaming operators, we yield block + metadata per record batch
            self.ray_remote_args["_generator_backpressure_num_objects"] = (
                2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
            )
        
        # Enable streaming returns for better memory management
        if "num_returns" not in self.ray_remote_args:
            self.ray_remote_args["num_returns"] = "streaming"
        
        # Add operator ID for better tracking
        if "_labels" not in self.ray_remote_args:
            self.ray_remote_args["_labels"] = {}
        self.ray_remote_args["_labels"][self._OPERATOR_ID_LABEL_KEY] = self.id

    def should_add_input(self) -> bool:
        """Determine if this operator can accept new inputs.
        
        For streaming operators, this depends on trigger conditions and
        current task load rather than traditional input queues.
        """
        if self._completed:
            return False
            
        # Check if we should trigger based on our streaming trigger
        if not self._should_trigger_new_batch():
            return False
        
        # Check if we're at our parallelism limit
        return len(self._current_read_tasks) < self.parallelism

    def can_add_input(self, bundle: RefBundle) -> bool:
        """Check if we can add input for backpressure management.
        
        This integrates with Ray Data's backpressure system.
        """
        return self.should_add_input()

    def implements_accurate_memory_accounting(self) -> bool:
        """Whether this operator provides accurate memory usage reporting.
        
        Streaming operators provide estimates but not exact memory accounting.
        """
        return False

    def notify_in_task_submission_backpressure(self, in_backpressure: bool) -> None:
        """Notification that this operator is under task submission backpressure.
        
        This is called by the resource manager when backpressure policies
        are triggered. We can use this to adjust our streaming behavior.
        """
        if in_backpressure:
            # When backpressured, we could slow down trigger timing
            # This is a hook for future optimizations
            pass

    def _update_performance_metrics(self, bundle: RefBundle) -> None:
        """Update performance tracking metrics."""
        try:
            # Estimate bytes and rows from bundle
            estimated_bytes = 0
            estimated_rows = 0
            
            for metadata in bundle.metadata:
                if metadata:
                    if hasattr(metadata, 'size_bytes') and metadata.size_bytes:
                        estimated_bytes += metadata.size_bytes
                    if hasattr(metadata, 'num_rows') and metadata.num_rows:
                        estimated_rows += metadata.num_rows
            
            # If no metadata available, use conservative estimates
            if estimated_bytes == 0:
                estimated_bytes = len(bundle.refs) * 1024  # 1KB per block estimate
            if estimated_rows == 0:
                estimated_rows = len(bundle.refs) * 100  # 100 rows per block estimate
            
            self._bytes_produced += estimated_bytes
            self._rows_produced += estimated_rows
            
        except Exception:
            # Don't fail streaming on metrics errors
            pass

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
            "trigger_config": {
                "type": self.trigger.trigger_type,
                "interval": getattr(self.trigger, 'interval', None),
                "cron_expression": getattr(self.trigger, 'cron_expression', None),
            },
        }
        base_stats.update(streaming_stats)
        return base_stats 