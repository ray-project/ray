import logging
from typing import Iterator, Optional

from ray.data._internal.execution.interfaces import (
    Executor,
    ExecutionOptions,
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
        for op in topology:
            op.start(self._options)
        output_node: OpState = topology[dag]

        # Run scheduling loop until complete.
        while self._scheduling_loop_step(topology):
            while output_node.outqueue:
                yield output_node.outqueue.pop(0)

        # Handle any leftover outputs.
        while output_node.outqueue:
            yield output_node.outqueue.pop(0)

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
        op = select_operator_to_run(topology)
        while op is not None:
            topology[op].dispatch_next_task()
            op = select_operator_to_run(topology)

        # Keep going until all operators run to completion.
        return not all(op.completed() for op in topology)
