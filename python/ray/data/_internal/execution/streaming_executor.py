import logging
import threading
import os
from typing import Iterator, Optional

import ray
from ray.data._internal.execution.interfaces import (
    Executor,
    ExecutionOptions,
    ExecutionResources,
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.streaming_executor_state import (
    Topology,
    OpState,
    build_streaming_topology,
    process_completed_tasks,
    select_operator_to_run,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats

logger = logging.getLogger(__name__)

# Set this environment variable for detailed scheduler debugging logs.
DEBUG_TRACE_SCHEDULING = "RAY_DATASET_TRACE_SCHEDULING" in os.environ


class StreamingExecutor(Executor, threading.Thread):
    """A streaming Dataset executor.

    This implementation executes Dataset DAGs in a fully streamed way. It runs
    by setting up the operator topology, and then routing blocks through operators in
    a way that maximizes throughput under resource constraints.
    """

    def __init__(self, options: ExecutionOptions):
        # TODO: implement stats recording. We might want to mutate a single stats
        # object as data is streamed through (similar to how iterating over the output
        # data updates the stats object in legacy code).
        self._stats: Optional[DatasetStats] = None
        self._global_info: Optional[ProgressBar] = None
        self._output_info: Optional[ProgressBar] = None

        # Internal execution state shared across thread boundaries. We run the control
        # loop on a separate thread so that it doesn't become stalled between
        # generator `yield`s.
        self._topology: Optional[Topology] = None
        self._output_node: Optional[OpState] = None

        Executor.__init__(self, options)
        threading.Thread.__init__(self)

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> Iterator[RefBundle]:
        """Executes the DAG using a streaming execution strategy.

        We take an event-loop approach to scheduling. We block on the next scheduling
        event using `ray.wait`, updating operator state and dispatching new tasks.
        """
        if not isinstance(dag, InputDataBuffer):
            logger.info("Executing DAG %s", dag)
            self._global_info = ProgressBar("Resource usage vs limits", 1, 0)

        # Setup the streaming DAG topology and start the runner thread.
        self._topology, self._stats = build_streaming_topology(dag, self._options)
        self._output_info = ProgressBar(
            "Output", dag.num_outputs_total() or 1, len(self._topology)
        )
        _validate_topology(self._topology, self._get_or_refresh_resource_limits())

        self._output_node: OpState = self._topology[dag]
        self.start()

        # Drain items from the runner thread until completion.
        try:
            item = self._output_node.get_output_blocking()
            while item is not None:
                if isinstance(item, Exception):
                    raise item
                else:
                    self._output_info.update(1)
                    yield item
                item = self._output_node.get_output_blocking()
        finally:
            # Close the progress bars from top to bottom to avoid them jumping
            # around in the console after completion.
            if self._global_info:
                self._global_info.close()
            for op, state in self._topology.items():
                op.shutdown()
                if state.progress_bar:
                    state.progress_bar.close()
            if self._output_info:
                self._output_info.close()

    def run(self):
        """Run the control loop in a helper thread.

        Results are returned via the output node's outqueue.
        """
        try:
            # Run scheduling loop until complete.
            while self._scheduling_loop_step(self._topology):
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
        assert self._stats, self._stats
        return self._stats

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
            logger.info("Scheduling loop step...")

        # Note: calling process_completed_tasks() is expensive since it incurs
        # ray.wait() overhead, so make sure to allow multiple dispatch per call for
        # greater parallelism.
        process_completed_tasks(topology)

        # Dispatch as many operators as we can for completed tasks.
        limits = self._get_or_refresh_resource_limits()
        cur_usage = self._get_current_usage(topology)
        self._report_current_usage(cur_usage, limits)
        op = select_operator_to_run(
            topology,
            cur_usage,
            limits,
            ensure_at_least_one_running=self._consumer_idling(),
        )
        while op is not None:
            if DEBUG_TRACE_SCHEDULING:
                _debug_dump_topology(topology)
            topology[op].dispatch_next_task()
            cur_usage = self._get_current_usage(topology)
            op = select_operator_to_run(
                topology,
                cur_usage,
                limits,
                ensure_at_least_one_running=self._consumer_idling(),
            )

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
            else cluster.get("object_store_memory", 0.0) // 4,
        )

    def _get_current_usage(self, topology: Topology) -> ExecutionResources:
        cur_usage = ExecutionResources()
        for op, state in topology.items():
            cur_usage = cur_usage.add(op.current_resource_usage())
            if isinstance(op, InputDataBuffer):
                continue  # Don't count input refs towards dynamic memory usage.
            for bundle in state.outqueue:
                cur_usage.object_store_memory += bundle.size_bytes()
        return cur_usage

    def _report_current_usage(
        self, cur_usage: ExecutionResources, limits: ExecutionResources
    ) -> None:
        if self._global_info:
            self._global_info.set_description(
                "Resource usage vs limits: "
                f"{cur_usage.cpu}/{limits.cpu} CPU, "
                f"{cur_usage.gpu}/{limits.gpu} GPU, "
                f"{cur_usage.object_store_memory_str()}/"
                f"{limits.object_store_memory_str()} object_store_memory"
            )
        if self._output_info:
            self._output_info.set_description(
                f"output: {len(self._output_node.outqueue)} queued"
            )


def _validate_topology(topology: Topology, limits: ExecutionResources) -> None:
    """Raises an exception on invalid topologies.

    It checks if the the sum of min actor pool sizes are larger than the resource
    limit, as well as other unsupported resource configurations.

    Args:
        topology: The topology to validate.
        limits: The limits to validate against.
    """

    base_usage = ExecutionResources(cpu=1)
    for op in topology:
        base_usage = base_usage.add(op.base_resource_usage())
        inc_usage = op.incremental_resource_usage()
        if inc_usage.cpu and inc_usage.gpu:
            raise NotImplementedError(
                "Operator incremental resource usage cannot specify both CPU "
                "and GPU at the same time, since it may cause deadlock."
            )
        elif inc_usage.object_store_memory:
            raise NotImplementedError(
                "Operator incremental resource usage must not include memory."
            )

    if not base_usage.satisfies_limit(limits):
        raise ValueError(
            f"The base resource usage of this topology {base_usage} "
            f"exceeds the execution limits {limits}!"
        )


def _debug_dump_topology(topology: Topology) -> None:
    """Print out current execution state for the topology for debugging.

    Args:
        topology: The topology to debug.
    """
    logger.info("vvv scheduling trace vvv")
    for i, (op, state) in enumerate(topology.items()):
        logger.info(f"{i}: {state.summary_str()}")
    logger.info("^^^ scheduling trace ^^^")
