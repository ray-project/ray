import os
import threading
import time
import uuid
from typing import Iterator, Optional

import ray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
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
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext

logger = DatasetLogger(__name__)

# Set this environment variable for detailed scheduler debugging logs.
DEBUG_TRACE_SCHEDULING = "RAY_DATA_TRACE_SCHEDULING" in os.environ

# Force a progress bar update after this many events processed . This avoids the
# progress bar seeming to stall for very large scale workloads.
PROGRESS_BAR_UPDATE_INTERVAL = 50

# Visible for testing.
_num_shutdown = 0


class StreamingExecutor(Executor, threading.Thread):
    """A streaming Dataset executor.

    This implementation executes Dataset DAGs in a fully streamed way. It runs
    by setting up the operator topology, and then routing blocks through operators in
    a way that maximizes throughput under resource constraints.
    """

    def __init__(self, options: ExecutionOptions):
        self._start_time: Optional[float] = None
        self._initial_stats: Optional[DatasetStats] = None
        self._final_stats: Optional[DatasetStats] = None
        self._global_info: Optional[ProgressBar] = None

        self._execution_id = uuid.uuid4().hex
        self._autoscaling_state = AutoscalingState()

        # The executor can be shutdown while still running.
        self._shutdown_lock = threading.RLock()
        self._shutdown = False

        # Internal execution state shared across thread boundaries. We run the control
        # loop on a separate thread so that it doesn't become stalled between
        # generator `yield`s.
        self._topology: Optional[Topology] = None
        self._output_node: Optional[OpState] = None

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

        if not isinstance(dag, InputDataBuffer):
            # Note: DAG must be initialized in order to query num_outputs_total.
            self._global_info = ProgressBar("Running", dag.num_outputs_total() or 1)

        self._output_node: OpState = self._topology[dag]
        self.start()

        class StreamIterator(OutputIterator):
            def __init__(self, outer: Executor):
                self._outer = outer

            def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
                try:
                    item = self._outer._output_node.get_output_blocking(
                        output_split_idx
                    )
                    # Translate the special sentinel values for MaybeRefBundle into
                    # exceptions.
                    if item is None:
                        if self._outer._shutdown:
                            raise StopIteration(f"{self._outer} is shutdown.")
                        else:
                            raise StopIteration
                    elif isinstance(item, Exception):
                        raise item
                    else:
                        # Otherwise return a concrete RefBundle.
                        if self._outer._global_info:
                            self._outer._global_info.update(1)
                        return item
                # Needs to be BaseException to catch KeyboardInterrupt. Otherwise we
                # can leave dangling progress bars by skipping shutdown.
                except BaseException:
                    self._outer.shutdown()
                    raise

            def __del__(self):
                self._outer.shutdown()

        return StreamIterator(self)

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        context = DataContext.get_current()
        global _num_shutdown

        with self._shutdown_lock:
            if self._shutdown:
                return
            logger.get_logger().debug(f"Shutting down {self}.")
            _num_shutdown += 1
            self._shutdown = True
            # Give the scheduling loop some time to finish processing.
            self.join(timeout=2.0)
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
            self._output_node.outqueue.append(e)
        finally:
            # Signal end of results.
            self._output_node.outqueue.append(None)

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
            stats.extra_metrics = op.get_metrics()
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
        process_completed_tasks(topology)

        # Dispatch as many operators as we can for completed tasks.
        limits = self._get_or_refresh_resource_limits()
        cur_usage = TopologyResourceUsage.of(topology)
        self._report_current_usage(cur_usage, limits)
        op = select_operator_to_run(
            topology,
            cur_usage,
            limits,
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
                ensure_at_least_one_running=self._consumer_idling(),
                execution_id=self._execution_id,
                autoscaling_state=self._autoscaling_state,
            )

        update_operator_states(topology)

        # Update the progress bar to reflect scheduling decisions.
        for op_state in topology.values():
            op_state.refresh_progress_bar()

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
        cluster = ray.cluster_resources()
        return ExecutionResources(
            cpu=base.cpu if base.cpu is not None else cluster.get("CPU", 0.0),
            gpu=base.gpu if base.gpu is not None else cluster.get("GPU", 0.0),
            object_store_memory=base.object_store_memory
            if base.object_store_memory is not None
            else round(
                DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
                * cluster.get("object_store_memory", 0.0)
            ),
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


def _debug_dump_topology(topology: Topology) -> None:
    """Print out current execution state for the topology for debugging.

    Args:
        topology: The topology to debug.
    """
    logger.get_logger().info("vvv scheduling trace vvv")
    for i, (op, state) in enumerate(topology.items()):
        logger.get_logger().info(f"{i}: {state.summary_str()}")
    logger.get_logger().info("^^^ scheduling trace ^^^")
