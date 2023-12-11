import os
import threading
import time
import uuid
from typing import Dict, Iterator, List, Optional

import ray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.backpressure_policy import (
    BackpressurePolicy,
    get_backpressure_policies,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    Executor,
    OutputIterator,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.streaming_executor_state import (
    DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION,
    AutoscalingState,
    OpState,
    Topology,
    TopologyResourceUsage,
    build_streaming_topology,
    process_completed_tasks,
    select_operator_to_run,
    update_operator_states,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats, StatsManager
from ray.data.context import DataContext

logger = DatasetLogger(__name__)

# Set this environment variable for detailed scheduler debugging logs.
# If not set, execution state will still be logged after each scheduling loop.
DEBUG_TRACE_SCHEDULING = "RAY_DATA_TRACE_SCHEDULING" in os.environ

# Force a progress bar update after this many events processed . This avoids the
# progress bar seeming to stall for very large scale workloads.
PROGRESS_BAR_UPDATE_INTERVAL = 50

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

    def __init__(self, options: ExecutionOptions, dataset_tag: str = "unknown_dataset"):
        self._start_time: Optional[float] = None
        self._initial_stats: Optional[DatasetStats] = None
        self._final_stats: Optional[DatasetStats] = None
        self._global_info: Optional[ProgressBar] = None

        self._execution_id = uuid.uuid4().hex
        self._autoscaling_state = AutoscalingState()

        # The executor can be shutdown while still running.
        self._shutdown_lock = threading.RLock()
        self._execution_started = False
        self._shutdown = False

        # Internal execution state shared across thread boundaries. We run the control
        # loop on a separate thread so that it doesn't become stalled between
        # generator `yield`s.
        self._topology: Optional[Topology] = None
        self._output_node: Optional[OpState] = None
        self._backpressure_policies: List[BackpressurePolicy] = []

        self._dataset_tag = dataset_tag
        # Stores if an operator is completed,
        # used for marking when an op has just completed.
        self._has_op_completed: Optional[Dict[PhysicalOperator, bool]] = None
        self._max_errored_blocks = DataContext.get_current().max_errored_blocks
        self._num_errored_blocks = 0

        self._last_debug_log_time = 0

        Executor.__init__(self, options)
        thread_name = f"StreamingExecutor-{self._execution_id}"
        threading.Thread.__init__(self, daemon=True, name=thread_name)

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> Iterator[RefBundle]:
        """Executes the DAG using a streaming execution strategy.

        We take an event-loop approach to scheduling. We block on the next scheduling
        event using `ray.wait`, updating operator state and dispatching new tasks.
        """

        self._initial_stats = initial_stats
        self._start_time = time.perf_counter()

        if not isinstance(dag, InputDataBuffer):
            logger.get_logger().info("Executing DAG %s", dag)
            logger.get_logger().info("Execution config: %s", self._options)
            if not self._options.verbose_progress:
                logger.get_logger().info(
                    "Tip: For detailed progress reporting, run "
                    "`ray.data.DataContext.get_current()."
                    "execution_options.verbose_progress = True`"
                )

        # Setup the streaming DAG topology and start the runner thread.
        self._topology, _ = build_streaming_topology(dag, self._options)
        self._backpressure_policies = get_backpressure_policies(self._topology)

        self._has_op_completed = {op: False for op in self._topology}

        if not isinstance(dag, InputDataBuffer):
            # Note: DAG must be initialized in order to query num_outputs_total.
            self._global_info = ProgressBar("Running", dag.num_outputs_total())

        self._output_node: OpState = self._topology[dag]
        StatsManager.register_dataset_to_stats_actor(
            self._dataset_tag,
            self._get_operator_tags(),
        )
        self.start()
        self._execution_started = True

        class StreamIterator(OutputIterator):
            def __init__(self, outer: Executor):
                self._outer = outer

            def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
                try:
                    item = self._outer._output_node.get_output_blocking(
                        output_split_idx
                    )
                    if self._outer._global_info:
                        self._outer._global_info.update(1, dag._estimated_output_blocks)
                    return item
                # Needs to be BaseException to catch KeyboardInterrupt. Otherwise we
                # can leave dangling progress bars by skipping shutdown.
                except BaseException as e:
                    self._outer.shutdown(isinstance(e, StopIteration))
                    raise

            def __del__(self):
                self._outer.shutdown()

        return StreamIterator(self)

    def __del__(self):
        self.shutdown()

    def shutdown(self, execution_completed: bool = True):
        context = DataContext.get_current()
        global _num_shutdown

        with self._shutdown_lock:
            if not self._execution_started or self._shutdown:
                return
            logger.get_logger().debug(f"Shutting down {self}.")
            _num_shutdown += 1
            self._shutdown = True
            # Give the scheduling loop some time to finish processing.
            self.join(timeout=2.0)
            self._update_stats_metrics(
                state="FINISHED" if execution_completed else "FAILED",
                force_update=True,
            )
            # Clears metrics for this dataset so that they do
            # not persist in the grafana dashboard after execution
            StatsManager.clear_execution_metrics(
                self._dataset_tag, self._get_operator_tags()
            )
            # Freeze the stats and save it.
            self._final_stats = self._generate_stats()
            stats_summary_string = self._final_stats.to_summary().to_string(
                include_parent=False
            )
            logger.get_logger(log_to_stdout=context.enable_auto_log_stats).info(
                stats_summary_string,
            )
            # Close the progress bars from top to bottom to avoid them jumping
            # around in the console after completion.
            if self._global_info:
                self._global_info.close()
            for op, state in self._topology.items():
                op.shutdown()
                state.close_progress_bars()
            # Make request for zero resources to autoscaler for this execution.
            actor = get_or_create_autoscaling_requester_actor()
            actor.request_resources.remote({}, self._execution_id)

    def run(self):
        """Run the control loop in a helper thread.

        Results are returned via the output node's outqueue.
        """
        try:
            # Run scheduling loop until complete.
            while self._scheduling_loop_step(self._topology) and not self._shutdown:
                pass
        except Exception as e:
            # Propagate it to the result iterator.
            self._output_node.mark_finished(e)
        finally:
            # Signal end of results.
            self._output_node.mark_finished()

    def get_stats(self):
        """Return the stats object for the streaming execution.

        The stats object will be updated as streaming execution progresses.
        """
        if self._final_stats:
            return self._final_stats
        else:
            return self._generate_stats()

    def _generate_stats(self) -> DatasetStats:
        """Create a new stats object reflecting execution status so far."""
        stats = self._initial_stats or DatasetStats(stages={}, parent=None)
        for op in self._topology:
            if isinstance(op, InputDataBuffer):
                continue
            builder = stats.child_builder(op.name, override_start_time=self._start_time)
            stats = builder.build_multistage(op.get_stats())
            stats.extra_metrics = op.metrics.as_dict()
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

        if DEBUG_TRACE_SCHEDULING:
            logger.get_logger().info("Scheduling loop step...")

        # Note: calling process_completed_tasks() is expensive since it incurs
        # ray.wait() overhead, so make sure to allow multiple dispatch per call for
        # greater parallelism.
        num_errored_blocks = process_completed_tasks(
            topology, self._backpressure_policies, self._max_errored_blocks
        )
        if self._max_errored_blocks > 0:
            self._max_errored_blocks -= num_errored_blocks
        self._num_errored_blocks += num_errored_blocks

        # Dispatch as many operators as we can for completed tasks.
        limits = self._get_or_refresh_resource_limits()
        cur_usage = TopologyResourceUsage.of(topology)
        self._report_current_usage(cur_usage, limits)
        op = select_operator_to_run(
            topology,
            cur_usage,
            limits,
            self._backpressure_policies,
            ensure_at_least_one_running=self._consumer_idling(),
            execution_id=self._execution_id,
            autoscaling_state=self._autoscaling_state,
        )
        i = 0
        while op is not None:
            i += 1
            if i > PROGRESS_BAR_UPDATE_INTERVAL:
                break
            if DEBUG_TRACE_SCHEDULING:
                _debug_dump_topology(topology)
            topology[op].dispatch_next_task()
            cur_usage = TopologyResourceUsage.of(topology)
            op = select_operator_to_run(
                topology,
                cur_usage,
                limits,
                self._backpressure_policies,
                ensure_at_least_one_running=self._consumer_idling(),
                execution_id=self._execution_id,
                autoscaling_state=self._autoscaling_state,
            )

        update_operator_states(topology)

        # Update the progress bar to reflect scheduling decisions.
        for op_state in topology.values():
            op_state.refresh_progress_bar()

        self._update_stats_metrics(state="RUNNING")
        if time.time() - self._last_debug_log_time >= DEBUG_LOG_INTERVAL_SECONDS:
            _log_op_metrics(topology)
            if not DEBUG_TRACE_SCHEDULING:
                _debug_dump_topology(topology, log_to_stdout=False)
            self._last_debug_log_time = time.time()

        # Log metrics of newly completed operators.
        for op in topology:
            if op.completed() and not self._has_op_completed[op]:
                log_str = (
                    f"Operator {op} completed. "
                    f"Operator Metrics:\n{op._metrics.as_dict()}"
                )
                logger.get_logger(log_to_stdout=False).info(log_str)
                self._has_op_completed[op] = True

        # Keep going until all operators run to completion.
        return not all(op.completed() for op in topology)

    def _consumer_idling(self) -> bool:
        """Returns whether the user thread is blocked on topology execution."""
        return len(self._output_node.outqueue) == 0

    def _get_or_refresh_resource_limits(self) -> ExecutionResources:
        """Return concrete limits for use at the current time.

        This method autodetects any unspecified execution resource limits based on the
        current cluster size, refreshing these values periodically to support cluster
        autoscaling.
        """
        base = self._options.resource_limits
        exclude = self._options.exclude_resources
        cluster = ray.cluster_resources()

        cpu = base.cpu
        if cpu is None:
            cpu = cluster.get("CPU", 0.0) - (exclude.cpu or 0.0)
        gpu = base.gpu
        if gpu is None:
            gpu = cluster.get("GPU", 0.0) - (exclude.gpu or 0.0)
        object_store_memory = base.object_store_memory
        if object_store_memory is None:
            object_store_memory = round(
                DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
                * cluster.get("object_store_memory", 0.0)
            ) - (exclude.object_store_memory or 0)

        return ExecutionResources(
            cpu=cpu,
            gpu=gpu,
            object_store_memory=object_store_memory,
        )

    def _report_current_usage(
        self, cur_usage: TopologyResourceUsage, limits: ExecutionResources
    ) -> None:
        resources_status = (
            "Running: "
            f"{cur_usage.overall.cpu}/{limits.cpu} CPU, "
            f"{cur_usage.overall.gpu}/{limits.gpu} GPU, "
            f"{cur_usage.overall.object_store_memory_str()}/"
            f"{limits.object_store_memory_str()} object_store_memory"
        )
        if self._global_info:
            self._global_info.set_description(resources_status)

    def _get_operator_tags(self):
        """Returns a list of operator tags."""
        return [f"{op.name}{i}" for i, op in enumerate(self._topology)]

    def _get_state_dict(self, state):
        last_op, last_state = list(self._topology.items())[-1]
        return {
            "state": state,
            "progress": last_state.num_completed_tasks,
            "total": last_op.num_outputs_total(),
            "end_time": time.time() if state != "RUNNING" else None,
            "operators": {
                f"{op.name}{i}": {
                    "progress": op_state.num_completed_tasks,
                    "total": op.num_outputs_total(),
                    "state": state,
                }
                for i, (op, op_state) in enumerate(self._topology.items())
            },
        }

    def _update_stats_metrics(self, state: str, force_update: bool = False):
        StatsManager.update_execution_metrics(
            self._dataset_tag,
            [op.metrics for op in self._topology],
            self._get_operator_tags(),
            self._get_state_dict(state=state),
            force_update=force_update,
        )


def _validate_dag(dag: PhysicalOperator, limits: ExecutionResources) -> None:
    """Raises an exception on invalid DAGs.

    It checks if the the sum of min actor pool sizes are larger than the resource
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
        base_usage = base_usage.add(op.base_resource_usage())

    if not base_usage.satisfies_limit(limits):
        error_message = (
            "The current cluster doesn't have the required resources to execute your "
            "Dataset pipeline:\n"
        )
        if (
            base_usage.cpu is not None
            and limits.cpu is not None
            and base_usage.cpu > limits.cpu
        ):
            error_message += (
                f"- Your application needs {base_usage.cpu} CPU(s), but your cluster "
                f"only has {limits.cpu}.\n"
            )
        if (
            base_usage.gpu is not None
            and limits.gpu is not None
            and base_usage.gpu > limits.gpu
        ):
            error_message += (
                f"- Your application needs {base_usage.gpu} GPU(s), but your cluster "
                f"only has {limits.gpu}.\n"
            )
        if (
            base_usage.object_store_memory is not None
            and base_usage.object_store_memory is not None
            and base_usage.object_store_memory > limits.object_store_memory
        ):
            error_message += (
                f"- Your application needs {base_usage.object_store_memory}B object "
                f"store memory, but your cluster only has "
                f"{limits.object_store_memory}B.\n"
            )
        raise ValueError(error_message.strip())


def _debug_dump_topology(topology: Topology, log_to_stdout: bool = True) -> None:
    """Print out current execution state for the topology for debugging.

    Args:
        topology: The topology to debug.
    """
    logger.get_logger(log_to_stdout).info("Execution Progress:")
    for i, (op, state) in enumerate(topology.items()):
        logger.get_logger(log_to_stdout).info(
            f"{i}: {state.summary_str()}, "
            f"Blocks Outputted: {state.num_completed_tasks}/{op.num_outputs_total()}"
        )
    logger.get_logger(log_to_stdout).info("")


def _log_op_metrics(topology: Topology) -> None:
    """Logs the metrics of each operator.

    Args:
        topology: The topology to debug.
    """
    log_str = "Operator Metrics:\n"
    for op in topology:
        log_str += f"{op.name}: {op.metrics.as_dict()}\n"
    logger.get_logger(log_to_stdout=False).info(log_str)
