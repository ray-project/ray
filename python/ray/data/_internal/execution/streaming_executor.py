import logging
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


class StreamingExecutor(Executor):
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
        self._global_info = ProgressBar("Resource usage vs limits", 1, 0)
        if options.locality_with_output:
            raise NotImplementedError("locality with output")
        super().__init__(options)

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> Iterator[RefBundle]:
        """Executes the DAG using a streaming execution strategy.

        We take an event-loop approach to scheduling. We block on the next scheduling
        event using `ray.wait`, updating operator state and dispatching new tasks.
        """
        if not isinstance(dag, InputDataBuffer):
            logger.info("Executing DAG %s", dag)

        # Setup the streaming DAG topology.
        topology, self._stats = build_streaming_topology(dag)

        try:
            self._validate_topology(topology)
            output_node: OpState = topology[dag]

            # Run scheduling loop until complete.
            while self._scheduling_loop_step(topology):
                while output_node.outqueue:
                    yield output_node.outqueue.pop(0)

            # Handle any leftover outputs.
            while output_node.outqueue:
                yield output_node.outqueue.pop(0)
        finally:
            for op in topology:
                op.shutdown()
            self._global_info.close()

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

        # Note: calling process_completed_tasks() is expensive since it incurs
        # ray.wait() overhead, so make sure to allow multiple dispatch per call for
        # greater parallelism.
        process_completed_tasks(topology)

        # Dispatch as many operators as we can for completed tasks.
        limits = self._get_or_refresh_resource_limits()
        cur_usage = self._get_and_report_current_usage(topology, limits)
        op = select_operator_to_run(topology, cur_usage, limits)
        while op is not None:
            topology[op].dispatch_next_task()
            cur_usage = self._get_and_report_current_usage(topology, limits)
            op = select_operator_to_run(topology, cur_usage, limits)

        # Keep going until all operators run to completion.
        return not all(op.completed() for op in topology)

    def _validate_topology(self, topology: Topology):
        """Raises an exception on invalid topologies.

        For example, a topology is invalid if its configuration would require more
        resources than the cluster has to execute.
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

        if not base_usage.satisfies_limits(self._options.resource_limits):
            raise ValueError(
                f"The base resource usage of this topology {base_usage} "
                f"exceeds the execution limits {self._options.resource_limits}!"
            )

    def _get_or_refresh_resource_limits(self) -> ExecutionResources:
        """Return concrete limits for use at the current time.

        This method autodetects any unspecified execution resource limits based on the
        current cluster size, refreshing these values periodically to support cluster
        autoscaling.
        """
        base = self._options.resource_limits
        # TODO: throttle refresh interval
        cluster = ray.cluster_resources()
        return ExecutionResources(
            cpu=base.cpu or cluster.get("CPU", 0.0),
            gpu=base.gpu or cluster.get("GPU", 0.0),
            object_store_memory=base.object_store_memory
            or (cluster.get("object_store_memory", 0.0) // 3),
        )

    def _get_and_report_current_usage(
        self, topology: Topology, limits: ExecutionResources
    ) -> ExecutionResources:
        cur_usage = ExecutionResources()
        for op, state in topology.items():
            cur_usage = cur_usage.add(op.current_resource_usage())
            if isinstance(op, InputDataBuffer):
                continue  # Don't count input refs towards dynamic memory usage.
            for bundle in state.outqueue:
                cur_usage.object_store_memory += bundle.size_bytes()
        self._global_info.set_description(
            "Resource usage vs limits: "
            f"{cur_usage.cpu}/{limits.cpu} CPU, "
            f"{cur_usage.gpu}/{limits.gpu} GPU, "
            f"{cur_usage.object_store_memory_str()}/"
            f"{limits.object_store_memory_str()} object_store_memory"
        )
        return cur_usage
