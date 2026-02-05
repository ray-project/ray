import logging
import math
import threading
import time
import typing
from typing import Dict, List, Optional, Tuple

from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.data._internal.actor_autoscaler import (
    create_actor_autoscaler,
)
from ray.data._internal.cluster_autoscaler import create_cluster_autoscaler
from ray.data._internal.execution import create_ranker
from ray.data._internal.execution.backpressure_policy import (
    BackpressurePolicy,
    get_backpressure_policies,
)
from ray.data._internal.execution.dataset_state import DatasetState
from ray.data._internal.execution.execution_callback import get_execution_callbacks
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    Executor,
    OutputIterator,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.resource_manager import (
    ResourceManager,
)
from ray.data._internal.execution.streaming_executor_state import (
    OpState,
    Topology,
    build_streaming_topology,
    format_op_state_summary,
    process_completed_tasks,
    select_operator_to_run,
    update_operator_states,
)
from ray.data._internal.logging import (
    get_log_directory,
    register_dataset_logger,
    unregister_dataset_logger,
)
from ray.data._internal.metadata_exporter import Topology as TopologyMetadata
from ray.data._internal.operator_schema_exporter import (
    OperatorSchema,
    get_operator_schema_exporter,
)
from ray.data._internal.progress import get_progress_manager
from ray.data._internal.stats import DatasetStats, Timer, _StatsManager
from ray.data.context import OK_PREFIX, WARN_PREFIX, DataContext
from ray.util.metrics import Gauge

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseExecutionProgressManager
    from ray.data.block import Schema

logger = logging.getLogger(__name__)

# Interval for logging execution progress updates and operator metrics.
DEBUG_LOG_INTERVAL_SECONDS = 5

# Visible for testing.
_num_shutdown = 0


class StreamingExecutor(Executor, threading.Thread):
    """A streaming Dataset executor.

    This implementation executes Dataset DAGs in a fully streamed way. It runs
    by setting up the operator topology, and then routing blocks through operators in
    a way that maximizes throughput under resource constraints.
    """

    UPDATE_METRICS_INTERVAL_S: float = 5.0

    def __init__(
        self,
        data_context: DataContext,
        dataset_id: str = "unknown_dataset",
    ):
        self._data_context = data_context
        self._ranker = create_ranker()
        self._start_time: Optional[float] = None
        self._initial_stats: Optional[DatasetStats] = None
        self._final_stats: Optional[DatasetStats] = None
        self._progress_manager: Optional["BaseExecutionProgressManager"] = None

        # The executor can be shutdown while still running.
        self._shutdown_lock = threading.RLock()
        self._execution_started = False
        self._shutdown = False

        # Internal execution state shared across thread boundaries. We run the control
        # loop on a separate thread so that it doesn't become stalled between
        # generator `yield`s.
        self._topology: Optional[Topology] = None
        self._output_node: Optional[Tuple[PhysicalOperator, OpState]] = None
        self._backpressure_policies: List[BackpressurePolicy] = []
        self._op_schema: Dict[PhysicalOperator, Schema] = {}

        self._dataset_id = dataset_id
        # Stores if an operator is completed,
        # used for marking when an op has just completed.
        self._has_op_completed: Optional[Dict[PhysicalOperator, bool]] = None
        self._max_errored_blocks = self._data_context.max_errored_blocks
        self._num_errored_blocks = 0

        self._last_debug_log_time = 0

        self._data_context.set_dataset_logger_id(
            register_dataset_logger(self._dataset_id)
        )

        # This stores the last time we updated the metrics.
        # This allows us to update metrics on some interval,
        # by comparing it with the current timestamp.
        self._metrics_last_updated: float = 0.0

        self._initialize_metrics_gauges()

        Executor.__init__(self, self._data_context.execution_options)
        thread_name = f"StreamingExecutor-{self._dataset_id}"
        threading.Thread.__init__(self, daemon=True, name=thread_name)

    def _initialize_metrics_gauges(self) -> None:
        """Initialize all Prometheus-style metrics gauges for monitoring execution."""
        self._sched_loop_duration_s = Gauge(
            "data_sched_loop_duration_s",
            description="Duration of the scheduling loop in seconds",
            tag_keys=("dataset",),
        )

        self._cpu_budget_gauge: Gauge = Gauge(
            "data_cpu_budget",
            "Budget (CPU) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._gpu_budget_gauge: Gauge = Gauge(
            "data_gpu_budget",
            "Budget (GPU) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._memory_budget_gauge: Gauge = Gauge(
            "data_memory_budget",
            "Budget (Memory) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._osm_budget_gauge: Gauge = Gauge(
            "data_object_store_memory_budget",
            "Budget (Object Store Memory) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._max_bytes_to_read_gauge: Gauge = Gauge(
            "data_max_bytes_to_read",
            description="Maximum bytes to read from streaming generator buffer.",
            tag_keys=("dataset", "operator"),
        )

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> OutputIterator:
        """Executes the DAG using a streaming execution strategy.

        We take an event-loop approach to scheduling. We block on the next scheduling
        event using `ray.wait`, updating operator state and dispatching new tasks.
        """

        self._initial_stats = initial_stats
        self._start_time = time.perf_counter()

        if not isinstance(dag, InputDataBuffer):
            if self._data_context.print_on_execution_start:
                message = f"Starting execution of Dataset {self._dataset_id}."
                log_path = get_log_directory()
                if log_path is not None:
                    message += f" Full logs are in {log_path}"
                logger.info(message)
                logger.info(
                    f"Execution plan of Dataset {self._dataset_id}: {dag.dag_str}"
                )

            logger.debug("Execution config: %s", self._options)

        # Setup the streaming DAG topology and start the runner thread.
        self._topology = build_streaming_topology(dag, self._options)

        self._resource_manager = ResourceManager(
            self._topology,
            self._options,
            lambda: self._cluster_autoscaler.get_total_resources(),
            self._data_context,
        )

        # Setup progress manager
        self._progress_manager = get_progress_manager(
            self._data_context,
            self._dataset_id,
            self._topology,
            self._options.verbose_progress,
        )
        self._progress_manager.start()

        self._backpressure_policies = get_backpressure_policies(
            self._data_context, self._topology, self._resource_manager
        )
        self._cluster_autoscaler = create_cluster_autoscaler(
            self._topology,
            self._resource_manager,
            self._data_context,
            execution_id=self._dataset_id,
        )
        self._actor_autoscaler = create_actor_autoscaler(
            self._topology,
            self._resource_manager,
            config=self._data_context.autoscaling_config,
        )

        self._has_op_completed = dict.fromkeys(self._topology, False)

        self._output_node = dag, self._topology[dag]

        op_to_id = {
            op: self._get_operator_id(op, i) for i, op in enumerate(self._topology)
        }
        _StatsManager.register_dataset_to_stats_actor(
            self._dataset_id,
            self._get_operator_tags(),
            TopologyMetadata.create_topology_metadata(dag, op_to_id),
            self._data_context,
        )
        for callback in get_execution_callbacks(self._data_context):
            callback.before_execution_starts(self)

        self.start()
        self._execution_started = True

        return _ClosingIterator(self)

    def __del__(self):
        # NOTE: Upon garbage-collection we're allowing running tasks
        #       to be terminated asynchronously (ie avoid unnecessary
        #       synchronization on their completion)
        self.shutdown(force=False)

    def shutdown(self, force: bool, exception: Optional[Exception] = None):
        global _num_shutdown

        with self._shutdown_lock:
            if not self._execution_started or self._shutdown:
                return

            start = time.perf_counter()

            status_detail = (
                f"failed with {exception}" if exception else "completed successfully"
            )

            logger.debug(
                f"Shutting down executor for dataset {self._dataset_id} "
                f"({status_detail})"
            )

            _num_shutdown += 1
            self._shutdown = True
            # Give the scheduling loop some time to finish processing.
            self.join(timeout=2.0)
            self._update_stats_metrics(
                state=DatasetState.FINISHED.name
                if exception is None
                else DatasetState.FAILED.name,
                force_update=True,
            )
            # Freeze the stats and save it.
            self._final_stats = self._generate_stats()
            stats_summary_string = self._final_stats.to_summary().to_string(
                include_parent=False
            )
            # Reset the scheduling loop duration gauge + resource manager budgets/usages.
            self._resource_manager.update_usages()
            self.update_metrics(0)
            if self._data_context.enable_auto_log_stats:
                logger.info(stats_summary_string)
            # Close the progress manager with a finishing message.
            if exception is None:
                desc = (
                    f"{OK_PREFIX} Dataset {self._dataset_id} execution finished in "
                    f"{self._final_stats.time_total_s:.2f} seconds"
                )
            else:
                desc = f"{WARN_PREFIX} Dataset {self._dataset_id} execution failed"

            self._progress_manager.close_with_finishing_description(
                desc, exception is None
            )
            logger.info(desc)

            timer = Timer()

            for op in self._topology.keys():
                op.shutdown(timer, force=force)

            min_ = round(timer.min(), 3)
            max_ = round(timer.max(), 3)
            total = round(timer.get(), 3)
            logger.debug(
                f"Shut down operator hierarchy for dataset {self._dataset_id}"
                f" (min/max/total={min_}/{max_}/{total}s)"
            )

            if exception is None:
                for callback in get_execution_callbacks(self._data_context):
                    callback.after_execution_succeeds(self)
            else:
                for callback in get_execution_callbacks(self._data_context):
                    callback.after_execution_fails(self, exception)

            self._cluster_autoscaler.on_executor_shutdown()

            dur = time.perf_counter() - start

            logger.debug(
                f"Shut down executor for dataset {self._dataset_id} "
                f"(took {round(dur, 3)}s)"
            )

            # Unregister should be called after all operators are shut down to
            # capture as many logs as possible.
            self._data_context.set_dataset_logger_id(
                unregister_dataset_logger(self._dataset_id)
            )

    def run(self):
        """Run the control loop in a helper thread.

        Results are returned via the output node's outqueue.
        """
        exc: Optional[Exception] = None
        try:
            # Run scheduling loop until complete.
            while True:
                # Use `perf_counter` rather than `process_time` to ensure we include
                # time spent on IO, like RPCs to Ray Core.
                t_start = time.perf_counter()
                continue_sched = self._scheduling_loop_step(self._topology)

                sched_loop_duration = time.perf_counter() - t_start

                self.update_metrics(sched_loop_duration)
                if self._initial_stats:
                    self._initial_stats.streaming_exec_schedule_s.add(
                        sched_loop_duration
                    )

                for callback in get_execution_callbacks(self._data_context):
                    callback.on_execution_step(self)
                if not continue_sched or self._shutdown:
                    break
        except Exception as e:
            # Propagate it to the result iterator.
            exc = e
        finally:
            # Mark state of outputting operator as finished
            _, state = self._output_node
            state.mark_finished(exc)

    def update_metrics(self, sched_loop_duration: int):
        self._sched_loop_duration_s.set(
            sched_loop_duration, tags={"dataset": self._dataset_id}
        )
        for i, op in enumerate(self._topology):
            tags = {
                "dataset": self._dataset_id,
                "operator": self._get_operator_id(op, i),
            }
            self._update_budget_metrics(op, tags)
            self._update_max_bytes_to_read_metric(op, tags)

    def _update_budget_metrics(self, op: PhysicalOperator, tags: Dict[str, str]):
        budget = self._resource_manager.get_budget(op)
        if budget is None:
            cpu_budget = 0
            gpu_budget = 0
            memory_budget = 0
            object_store_memory_budget = 0
        else:
            # Convert inf to -1 to represent unlimited budget in metrics
            cpu_budget = -1 if math.isinf(budget.cpu) else budget.cpu
            gpu_budget = -1 if math.isinf(budget.gpu) else budget.gpu
            memory_budget = -1 if math.isinf(budget.memory) else budget.memory
            object_store_memory_budget = (
                -1
                if math.isinf(budget.object_store_memory)
                else budget.object_store_memory
            )

        self._cpu_budget_gauge.set(cpu_budget, tags=tags)
        self._gpu_budget_gauge.set(gpu_budget, tags=tags)
        self._memory_budget_gauge.set(memory_budget, tags=tags)
        self._osm_budget_gauge.set(object_store_memory_budget, tags=tags)

    def _update_max_bytes_to_read_metric(
        self, op: PhysicalOperator, tags: Dict[str, str]
    ):
        if self._resource_manager.op_resource_allocator_enabled():
            resource_allocator = self._resource_manager.op_resource_allocator
            output_budget_bytes = resource_allocator.get_output_budget(op)
            if output_budget_bytes is not None:
                if math.isinf(output_budget_bytes):
                    # Convert inf to -1 to represent unlimited bytes to read
                    output_budget_bytes = -1
                self._max_bytes_to_read_gauge.set(output_budget_bytes, tags)

    def get_stats(self):
        """Return the stats object for the streaming execution.

        The stats object will be updated as streaming execution progresses.
        """
        if self._final_stats:
            return self._final_stats
        else:
            return self._generate_stats()

    def set_external_consumer_bytes(self, num_bytes: int) -> None:
        """Set the bytes buffered by external consumers."""
        if self._resource_manager is not None:
            self._resource_manager.set_external_consumer_bytes(num_bytes)

    def _generate_stats(self) -> DatasetStats:
        """Create a new stats object reflecting execution status so far."""
        stats = self._initial_stats or DatasetStats(metadata={}, parent=None)
        for op in self._topology:
            if isinstance(op, InputDataBuffer):
                continue
            builder = stats.child_builder(op.name, override_start_time=self._start_time)
            stats = builder.build_multioperator(op.get_stats())
            stats.extra_metrics = op.metrics.as_dict(skip_internal_metrics=True)
        stats.streaming_exec_schedule_s = (
            self._initial_stats.streaming_exec_schedule_s
            if self._initial_stats
            else None
        )
        return stats

    def _scheduling_loop_step(self, topology: Topology) -> bool:
        """Run one step of the scheduling loop.

        This runs a few general phases:
            1. Waiting for the next task completion using `ray.wait()`.
            2. Pulling completed refs into operator outqueues.
            3. Selecting and dispatching new inputs to operators.

        Returns:
            True if we should continue running the scheduling loop.
        """
        self._resource_manager.update_usages()
        # Note: calling process_completed_tasks() is expensive since it incurs
        # ray.wait() overhead, so make sure to allow multiple dispatch per call for
        # greater parallelism.
        num_errored_blocks = process_completed_tasks(
            topology,
            self._backpressure_policies,
            self._max_errored_blocks,
        )
        if self._max_errored_blocks > 0:
            self._max_errored_blocks -= num_errored_blocks
        self._num_errored_blocks += num_errored_blocks

        self._resource_manager.update_usages()
        # Dispatch as many operators as we can for completed tasks.
        self._report_current_usage()

        i = 0
        while True:
            op = select_operator_to_run(
                topology,
                self._resource_manager,
                self._backpressure_policies,
                # If consumer is idling (there's nothing for it to consume)
                # enforce liveness, ie that at least a single task gets scheduled
                ensure_liveness=self._consumer_idling(),
                ranker=self._ranker,
            )

            if op is None:
                break

            topology[op].dispatch_next_task()

            self._resource_manager.update_usages()

            i += 1
            if i % self._progress_manager.TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS == 0:
                self._refresh_progress_manager(topology)

        # Trigger autoscaling
        self._cluster_autoscaler.try_trigger_scaling()
        self._actor_autoscaler.try_trigger_scaling()

        update_operator_states(topology)
        self._refresh_progress_manager(topology)

        self._update_stats_metrics(state=DatasetState.RUNNING.name)
        if time.time() - self._last_debug_log_time >= DEBUG_LOG_INTERVAL_SECONDS:
            _log_op_metrics(topology)
            _debug_dump_topology(topology, self._resource_manager)
            self._last_debug_log_time = time.time()

        for op, state in topology.items():
            # Export operator schema if it's updated
            if state._schema is not None and self._op_schema.get(op) != state._schema:
                self._op_schema[op] = state._schema
                self._export_operator_schema(op)

            # Log metrics of newly completed operators.
            if op.has_completed() and not self._has_op_completed[op]:
                metrics_dict = op._metrics.as_dict(skip_internal_metrics=True)
                metrics_table = _format_metrics_table(metrics_dict)
                log_str = f"Operator {op} completed. Operator Metrics:\n{metrics_table}"
                logger.debug(log_str)
                self._has_op_completed[op] = True
                self._validate_operator_queues_empty(op, state)

        # Keep going until all operators run to completion.
        return not all(op.has_completed() for op in topology)

    def _refresh_progress_manager(self, topology: Topology):
        # Update the progress manager to reflect scheduling decisions.
        if self._progress_manager:
            for op_state in topology.values():
                if not isinstance(op_state.op, InputDataBuffer):
                    self._progress_manager.update_operator_progress(
                        op_state, self._resource_manager
                    )
            self._progress_manager.refresh()

    def _consumer_idling(self) -> bool:
        """Returns whether the user thread is blocked on topology execution."""
        _, state = self._output_node
        return len(state.output_queue) == 0

    def _export_operator_schema(self, op: PhysicalOperator) -> None:
        schema = self._op_schema.get(op)
        operator_schema_exporter = get_operator_schema_exporter()
        if (
            operator_schema_exporter is not None
            and hasattr(schema, "names")
            and hasattr(schema, "types")
        ):
            names = [str(n) for n in schema.names]
            types = [str(t) for t in schema.types]
            operator_schema = OperatorSchema(
                operator_uuid=op.id,
                schema_fields=dict(zip(names, types)),
            )
            operator_schema_exporter.export_operator_schema(operator_schema)

    def _validate_operator_queues_empty(
        self, op: PhysicalOperator, state: OpState
    ) -> None:
        """Validate that all queues are empty when an operator completes.

        Args:
            op: The completed operator to validate.
            state: The operator's execution state.
        """
        error_msg = "Expected {} Queue for {} to be empty, but found {} bundles"

        if isinstance(op, InternalQueueOperatorMixin):
            # 1) Check Internal Input Queue is empty
            assert op.internal_input_queue_num_blocks() == 0, error_msg.format(
                "Internal Input", op.name, op.internal_input_queue_num_blocks()
            )

            # 2) Check Internal Output Queue is empty
            assert op.internal_output_queue_num_blocks() == 0, error_msg.format(
                "Internal Output",
                op.name,
                op.internal_output_queue_num_blocks(),
            )

        # 3) Check that External Input Queue is empty
        for input_q in state.input_queues:
            assert len(input_q) == 0, error_msg.format(
                "External Input", op.name, len(input_q)
            )

    def _report_current_usage(self) -> None:
        # running_usage is the amount of resources that have been requested but
        # not necessarily available
        # TODO(sofian) https://github.com/ray-project/ray/issues/47520
        # We need to split the reported resources into running, pending-scheduling,
        # pending-node-assignment.
        running_usage = self._resource_manager.get_global_running_usage()
        pending_usage = self._resource_manager.get_global_pending_usage()
        limits = self._resource_manager.get_global_limits()
        resources_status = (
            f"Active & requested resources: "
            f"{running_usage.cpu:.4g}/{limits.cpu:.4g} CPU, "
        )
        if running_usage.gpu > 0:
            resources_status += f"{running_usage.gpu:.4g}/{limits.gpu:.4g} GPU, "
        resources_status += (
            f"{running_usage.object_store_memory_str()}/"
            f"{limits.object_store_memory_str()} object store"
        )

        # Only include pending section when there are pending resources.
        if pending_usage.cpu or pending_usage.gpu:
            if pending_usage.cpu and pending_usage.gpu:
                pending_str = (
                    f"{pending_usage.cpu:.4g} CPU, {pending_usage.gpu:.4g} GPU"
                )
            elif pending_usage.cpu:
                pending_str = f"{pending_usage.cpu:.4g} CPU"
            else:
                pending_str = f"{pending_usage.gpu:.4g} GPU"
            resources_status += f" (pending: {pending_str})"

        self._progress_manager.update_total_resource_status(resources_status)

    def _get_operator_id(self, op: PhysicalOperator, topology_index: int) -> str:
        return f"{op.name}_{topology_index}"

    def _get_operator_tags(self):
        """Returns a list of operator tags."""
        return [
            f"{self._get_operator_id(op, i)}" for i, op in enumerate(self._topology)
        ]

    def _get_state_dict(self, state):
        last_op, last_state = list(self._topology.items())[-1]
        return {
            "state": state,
            "progress": last_state.num_completed_tasks,
            "total": last_op.num_outputs_total(),
            "total_rows": last_op.num_output_rows_total(),
            "end_time": time.time()
            if state in (DatasetState.FINISHED.name, DatasetState.FAILED.name)
            else None,
            "operators": {
                f"{self._get_operator_id(op, i)}": {
                    "name": op.name,
                    "progress": op_state.num_completed_tasks,
                    "total": op.num_outputs_total(),
                    "total_rows": op.num_output_rows_total(),
                    "queued_blocks": op_state.total_enqueued_input_blocks(),
                    "state": DatasetState.FINISHED.name
                    if op.has_execution_finished()
                    else state,
                }
                for i, (op, op_state) in enumerate(self._topology.items())
            },
        }

    def _update_stats_metrics(self, state: str, force_update: bool = False):
        now = time.time()
        if (
            force_update
            or (now - self._metrics_last_updated) > self.UPDATE_METRICS_INTERVAL_S
        ):
            _StatsManager.update_execution_metrics(
                self._dataset_id,
                [op.metrics for op in self._topology],
                self._get_operator_tags(),
                self._get_state_dict(state=state),
            )
            self._metrics_last_updated = now


def _validate_dag(dag: PhysicalOperator, limits: ExecutionResources) -> None:
    """Raises an exception on invalid DAGs.

    It checks if the sum of min actor pool sizes are larger than the resource
    limit, as well as other unsupported resource configurations.

    This should be called prior to creating the topology from the DAG.

    Args:
        dag: The DAG to validate.
        limits: The limits to validate against.
    """

    seen = set()

    def walk(op):
        seen.add(op)
        for parent in op.input_dependencies:
            if parent not in seen:
                yield from walk(parent)
        yield op

    base_usage = ExecutionResources(cpu=1)
    for op in walk(dag):
        min_resource_usage, _ = op.min_max_resource_requirements()
        base_usage = base_usage.add(min_resource_usage)

    if not base_usage.satisfies_limit(limits):
        error_message = (
            "The current cluster doesn't have the required resources to execute your "
            "Dataset pipeline:\n"
        )
        if base_usage.cpu > limits.cpu:
            error_message += (
                f"- Your application needs {base_usage.cpu} CPU(s), but your cluster "
                f"only has {limits.cpu}.\n"
            )
        if base_usage.gpu > limits.gpu:
            error_message += (
                f"- Your application needs {base_usage.gpu} GPU(s), but your cluster "
                f"only has {limits.gpu}.\n"
            )
        if base_usage.object_store_memory > limits.object_store_memory:
            error_message += (
                f"- Your application needs {base_usage.object_store_memory}B object "
                f"store memory, but your cluster only has "
                f"{limits.object_store_memory}B.\n"
            )
        raise ValueError(error_message.strip())


def _debug_dump_topology(topology: Topology, resource_manager: ResourceManager) -> None:
    """Log current execution state for the topology for debugging.

    Args:
        topology: The topology to debug.
        resource_manager: The resource manager for this topology.
    """
    logger.debug("Execution Progress:")
    for i, (op, state) in enumerate(topology.items()):
        summary_str = format_op_state_summary(state, resource_manager, verbose=True)
        logger.debug(
            f"{i}: {op.name} - {summary_str}, "
            f"Blocks Outputted: {state.num_completed_tasks}/{op.num_outputs_total()}"
        )


def _format_metrics_table(metrics_dict: dict) -> str:
    """Format metrics dict as a pivot table, organized by category."""
    if not metrics_dict:
        return "(no metrics)"

    # Define metric categories and their patterns
    categories = [
        (
            "Inputs",
            [
                "num_inputs_received",
                "num_row_inputs_received",
                "bytes_inputs_received",
                "num_task_inputs_processed",
                "bytes_task_inputs_processed",
                "bytes_inputs_of_submitted_tasks",
                "rows_inputs_of_submitted_tasks",
            ],
        ),
        (
            "Outputs",
            [
                "num_outputs_taken",
                "bytes_outputs_taken",
                "row_outputs_taken",
                "block_outputs_taken",
                "num_task_outputs_generated",
                "bytes_task_outputs_generated",
                "rows_task_outputs_generated",
                "num_outputs_of_finished_tasks",
                "bytes_outputs_of_finished_tasks",
                "rows_outputs_of_finished_tasks",
            ],
        ),
        (
            "Tasks",
            [
                "num_tasks_submitted",
                "num_tasks_running",
                "num_tasks_have_outputs",
                "num_tasks_finished",
                "num_tasks_failed",
            ],
        ),
        (
            "Timing",
            [
                "block_generation_time",
                "task_submission_backpressure_time",
                "task_output_backpressure_time",
                "task_completion_time_total_s",
                "task_completion_time",
                "block_completion_time",
                "task_completion_time_excl_backpressure_s",
            ],
        ),
        (
            "Block Stats",
            ["num_output_blocks_per_task_s", "block_size_bytes", "block_size_rows"],
        ),
        (
            "Object Store",
            [
                "obj_store_mem_internal_inqueue",
                "obj_store_mem_internal_outqueue",
                "obj_store_mem_pending_task_inputs",
                "obj_store_mem_internal_inqueue_blocks",
                "obj_store_mem_internal_outqueue_blocks",
                "obj_store_mem_freed",
                "obj_store_mem_spilled",
                "obj_store_mem_used",
                "num_external_inqueue_blocks",
                "num_external_inqueue_bytes",
                "num_external_outqueue_blocks",
                "num_external_outqueue_bytes",
            ],
        ),
        ("Actors", ["num_alive_actors", "num_restarting_actors", "num_pending_actors"]),
        ("Resources", ["cpu_usage", "gpu_usage"]),
        (
            "Averages",
            [
                "average_num_outputs_per_task",
                "average_num_inputs_per_task",
                "average_total_task_completion_time_s",
                "average_task_completion_excl_backpressure_time_s",
                "average_bytes_per_output",
                "average_bytes_inputs_per_task",
                "average_rows_inputs_per_task",
                "average_bytes_outputs_per_task",
                "average_rows_outputs_per_task",
                "average_max_uss_per_task",
            ],
        ),
    ]

    # Build categorized dict, sorting metrics alphabetically within each category
    categorized = {}
    categorized_keys = set()
    for category, keys in categories:
        category_metrics = {}
        for key in keys:
            if key in metrics_dict:
                category_metrics[key] = metrics_dict[key]
                categorized_keys.add(key)
        if category_metrics:
            # Sort metrics alphabetically within each category
            categorized[category] = dict(sorted(category_metrics.items()))

    # Add uncategorized metrics under "Other" (also sorted)
    other_metrics = {k: v for k, v in metrics_dict.items() if k not in categorized_keys}
    if other_metrics:
        categorized["Other"] = dict(sorted(other_metrics.items()))

    # Sort categories alphabetically, keeping "Other" at the end
    sorted_categories = sorted(categorized.keys(), key=lambda c: (c == "Other", c))

    # Build table data
    table_data = []
    for category in sorted_categories:
        cat_metrics = categorized[category]
        first_in_cat = True
        for k, v in cat_metrics.items():
            cat_display = category if first_in_cat else ""
            # Convert None to string "None" since tabulate renders None as empty
            table_data.append([cat_display, k, v if v is not None else "None"])
            first_in_cat = False

    return tabulate(
        table_data,
        headers=["category", "metric", "value"],
        tablefmt="plain",
    )


def _log_op_metrics(topology: Topology) -> None:
    """Logs the metrics of each operator.

    Args:
        topology: The topology to debug.
    """
    log_str = "Operator Metrics:\n"
    for op in topology:
        metrics_dict = op.metrics.as_dict(skip_internal_metrics=True)
        log_str += f"{op.name}:\n{_format_metrics_table(metrics_dict)}\n"
    logger.debug(log_str)


class _ClosingIterator(OutputIterator):
    """Iterator automatically shutting down executor upon exhausting the
    iterable sequence.

    NOTE: If this iterator isn't fully exhausted, executor still have to
          be closed manually by the caller!
    """

    def __init__(self, executor: StreamingExecutor):
        self._executor = executor

    def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
        try:
            op, state = self._executor._output_node
            bundle = state.get_output_blocking(output_split_idx)

            # Update progress-bars
            if self._executor._progress_manager:
                self._executor._progress_manager.update_total_progress(
                    bundle.num_rows() or 0, op.num_output_rows_total()
                )

            return bundle

        # Have to be BaseException to catch ``KeyboardInterrupt``
        #
        # NOTE: This also handles ``StopIteration``
        except BaseException as e:
            # Asynchronously shutdown the executor (ie avoid unnecessary
            # synchronization on tasks termination)
            self._executor.shutdown(
                force=False, exception=e if not isinstance(e, StopIteration) else None
            )
            raise

    def __del__(self):
        # NOTE: Upon garbage-collection we're allowing running tasks
        #       to be terminated asynchronously (ie avoid unnecessary
        #       synchronization on their completion)
        self._executor.shutdown(force=False)
