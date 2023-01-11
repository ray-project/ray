import logging
from typing import Dict, List, Iterator, Optional

import ray
from ray.data._internal.execution.interfaces import (
    Executor,
    ExecutionOptions,
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats

logger = logging.getLogger(__name__)


class _OpState:
    """Additional streaming execution state tracked for each PhysicalOperator."""

    def __init__(self, op: PhysicalOperator):
        # Each inqueue is connected to another operator's outqueue.
        self.inqueues: List[List[RefBundle]] = [
            [] for _ in range(len(op.input_dependencies))
        ]
        # The outqueue is connected to another operator's inqueue (they physically
        # share the same Python list reference).
        self.outqueue: List[RefBundle] = []
        self.op = op
        self.progress_bar = None
        self.num_completed_tasks = 0

    def initialize_progress_bar(self, index: int) -> None:
        self.progress_bar = ProgressBar(
            self.op.name, self.op.num_outputs_total(), index
        )

    def num_queued(self) -> int:
        return sum(len(q) for q in self.inqueues)

    def num_active_tasks(self):
        # TODO: optimize this?
        return len(self.op.get_work_refs())

    def add_output(self, ref: RefBundle) -> None:
        self.outqueue.append(ref)
        self.num_completed_tasks += 1
        if self.progress_bar:
            self.progress_bar.update(1)

    def refresh_progress_bar(self) -> None:
        if self.progress_bar:
            queued = self.num_queued()
            self.progress_bar.set_description(
                f"{self.op.name}: {self.num_active_tasks()} active, {queued} queued"
            )


class StreamingExecutor(Executor):
    """A streaming Dataset executor.

    This implementation executes Dataset DAGs in a fully streamed way. It runs
    by setting up the operator topology, and then routing blocks through operators in
    a way that maximizes throughput under resource constraints.
    """

    def __init__(self, options: ExecutionOptions):
        # Operator state for the executing pipeline, populated on execution start.
        self._operator_state: Dict[PhysicalOperator, _OpState] = {}
        self._output_node: Optional[PhysicalOperator] = None
        self._active_tasks: Dict[ray.ObjectRef, PhysicalOperator] = {}
        super().__init__(options)

    # TODO implement stats
    def execute(self, dag: PhysicalOperator, initial_stats) -> Iterator[RefBundle]:
        """Executes the DAG using a streaming execution strategy.

        We take an event-loop approach to scheduling. We block on the next scheduling
        event using `ray.wait`, updating operator state and dispatching new tasks.
        """

        if not isinstance(dag, InputDataBuffer):
            logger.info("Executing DAG %s", dag)

        self._init_operator_state(dag)
        output = self._operator_state[self._output_node]

        # Run scheduling loop until complete.
        while self._scheduling_loop_step():
            while output.outqueue:
                yield output.outqueue.pop(0)

        # Handle any leftover outputs.
        while output.outqueue:
            yield output.outqueue.pop(0)

    def get_stats(self):
        return DatasetStats(stages={}, parent=None)

    def _init_operator_state(self, dag: PhysicalOperator) -> None:
        """Initialize operator state for the given DAG.

        This involves creating the operator state for each operator in the DAG,
        registering it with this class, and wiring up the inqueues/outqueues of
        dependent operator states.
        """
        if self._operator_state:
            raise ValueError("Cannot init operator state twice.")

        def setup_state(node) -> _OpState:
            if node in self._operator_state:
                return self._operator_state[node]

            # Create state if it doesn't exist.
            state = _OpState(node)
            self._operator_state[node] = state

            # Wire up the input outqueues to this node's inqueues.
            for i, parent in enumerate(node.input_dependencies):
                parent_state = setup_state(parent)
                state.inqueues[i] = parent_state.outqueue

            return state

        setup_state(dag)
        self._output_node = dag

        i = 0
        for state in list(self._operator_state.values())[::-1]:
            if not isinstance(state.op, InputDataBuffer):
                state.initialize_progress_bar(i)
                i += 1

    def _scheduling_loop_step(self) -> bool:
        """Run one step of the scheduling loop.

        This runs a few general phases:
            1. Waiting for the next task completion using `ray.wait()`.
            2. Pulling completed refs into operator outqueues.
            3. Selecting and dispatching new inputs to operators.

        Returns:
            True if we should continue running the scheduling loop.
        """
        keep_going = self._process_completed_tasks()
        op = self._select_operator_to_run()
        while op is not None:
            self._dispatch_next_task(op)
            self._process_completed_tasks()
            op = self._select_operator_to_run()
            keep_going = True
        return keep_going

    def _process_completed_tasks(self) -> bool:
        """Process any newly completed tasks and update operator state.

        Returns:
            True if work remains to be run.
        """
        for state in self._operator_state.values():
            state.refresh_progress_bar()

        # Update active tasks.
        self._active_tasks.clear()
        for op in self._operator_state:
            for ref in op.get_work_refs():
                self._active_tasks[ref] = op

        # Process completed Ray tasks and notify operators.
        if self._active_tasks:
            completed, _ = ray.wait(
                list(self._active_tasks),
                num_returns=len(self._active_tasks),
                fetch_local=False,
                timeout=0.1,
            )
            for ref in completed:
                op = self._active_tasks.pop(ref)
                op.notify_work_completed(ref)

        # Pull any operator outputs into the streaming op state.
        for op, state in self._operator_state.items():
            while op.has_next():
                state.add_output(op.get_next())

        return len(self._active_tasks) > 0

    def _select_operator_to_run(self) -> Optional[PhysicalOperator]:
        """Select an operator to run, if possible.

        The objective of this function is to maximize the throughput of the overall
        pipeline, subject to defined memory and parallelism limits.
        """
        # TODO: grab par limit from operator state instead?
        PARALLELISM_LIMIT = self._options.parallelism_limit or 8
        if len(self._active_tasks) >= PARALLELISM_LIMIT:
            return None

        # TODO: improve the prioritization.
        pairs = list(self._operator_state.items())
        pairs.sort(key=lambda p: len(p[1].outqueue) + p[1].num_active_tasks())

        selected = None
        for op, state in pairs:
            if state.num_queued() > 0:
                selected = op
                break

        return selected

    def _dispatch_next_task(self, op: PhysicalOperator) -> None:
        """Schedule the next task for the given operator.

        It is an error to call this if the given operator has no next tasks.

        Args:
            op: The operator to schedule a task for.
        """
        state = self._operator_state[op]
        for i, inqueue in enumerate(state.inqueues):
            if inqueue:
                op.add_input(inqueue.pop(0), input_index=i)
                return
