"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

from typing import Dict, List, Optional

import ray
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats


# Holds the full execution state of the streaming topology. It's a dict mapping each
# operator to tracked streaming exec state.
Topology = Dict[PhysicalOperator, "OpState"]


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[List[RefBundle]]):
        # Each inqueue is connected to another operator's outqueue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.inqueues: List[List[RefBundle]] = inqueues
        # The outqueue is connected to another operator's inqueue (they physically
        # share the same Python list reference).
        self.outqueue: List[RefBundle] = []
        self.op = op
        self.progress_bar = None
        self.num_completed_tasks = 0
        self.inputs_done_called = False

    def initialize_progress_bar(self, index: int) -> None:
        """Create a progress bar at the given index (line offset in console)."""
        self.progress_bar = ProgressBar(
            self.op.name, self.op.num_outputs_total() or 1, index
        )

    def num_queued(self) -> int:
        """Return the number of queued bundles across all inqueues."""
        return sum(len(q) for q in self.inqueues)

    def num_active_tasks(self):
        """Return the number of Ray futures pending for this operator."""
        return self.op.num_active_work_refs()

    def add_output(self, ref: RefBundle) -> None:
        """Move a bundle produced by the operator to its outqueue."""
        self.outqueue.append(ref)
        self.num_completed_tasks += 1
        if self.progress_bar:
            self.progress_bar.update(1)

    def refresh_progress_bar(self) -> None:
        """Update the console with the latest operator progress."""
        if self.progress_bar:
            queued = self.num_queued()
            self.progress_bar.set_description(
                f"{self.op.name}: {self.num_active_tasks()} active, {queued} queued"
            )

    def dispatch_next_task(self) -> None:
        """Move a bundle from the operator inqueue to the operator itself."""
        for i, inqueue in enumerate(self.inqueues):
            if inqueue:
                self.op.add_input(inqueue.pop(0), input_index=i)
                return
        assert False, "Nothing to dispatch"


def build_streaming_topology(dag: PhysicalOperator) -> Topology:
    """Build the streaming operator state topology for the given DAG.

    This involves creating the operator state for each operator in the DAG,
    registering it with this class, and wiring up the inqueues/outqueues of
    dependent operator states.

    Returns:
        The topology dict holding the streaming execution state.
    """

    topology: Topology = {}

    # DFS walk to wire up operator states.
    def setup_state(op: PhysicalOperator) -> OpState:
        if op in topology:
            raise ValueError("An operator can only be present in a topology once.")

        # Wire up the input outqueues to this op's inqueues.
        inqueues = []
        for i, parent in enumerate(op.input_dependencies):
            parent_state = setup_state(parent)
            inqueues.append(parent_state.outqueue)

        # Create state.
        op_state = OpState(op, inqueues)
        topology[op] = op_state
        return op_state

    setup_state(dag)

    # Create the progress bars starting from the first operator to run.
    # Note that the topology dict in in topological sort order.
    i = 0
    for op_state in list(topology.values())[::-1]:
        if not isinstance(op_state.op, InputDataBuffer):
            op_state.initialize_progress_bar(i)
            i += 1

    # TODO: fill out stats.
    return topology, DatasetStats(stages={}, parent=None)


def process_completed_tasks(topology: Topology) -> None:
    """Process any newly completed tasks and update operator state."""
    for op_state in topology.values():
        op_state.refresh_progress_bar()

    # Update active tasks.
    active_tasks: Dict[ray.ObjectRef, PhysicalOperator] = {}

    for op in topology.keys():
        for ref in op.get_work_refs():
            active_tasks[ref] = op

    # Process completed Ray tasks and notify operators.
    if active_tasks:
        completed, _ = ray.wait(
            list(active_tasks),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )
        for ref in completed:
            op = active_tasks.pop(ref)
            op.notify_work_completed(ref)

    # Pull any operator outputs into the streaming op state.
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())

    # Call inputs_done() on ops where no more inputs are coming.
    for op, op_state in topology.items():
        inputs_done = all(
            [
                dep.completed() and not topology[dep].outqueue
                for dep in op.input_dependencies
            ]
        )
        if inputs_done and not op_state.inputs_done_called:
            op.inputs_done()
            op_state.inputs_done_called = True


def select_operator_to_run(topology: Topology) -> Optional[PhysicalOperator]:
    """Select an operator to run, if possible.

    The objective of this function is to maximize the throughput of the overall
    pipeline, subject to defined memory and parallelism limits.

    This is currently implemented by applying backpressure on operators that are
    producing outputs faster than they are consuming them `len(outqueue)`, as well as
    operators with a large number of running tasks `num_active_tasks()`.
    """

    # TODO: set limits properly based on resources and execution options. This is just
    # a hard-coded development placeholder.
    PARALLELISM_LIMIT = 8
    num_active_tasks = sum(
        op_state.num_active_tasks() for op_state in topology.values()
    )
    if num_active_tasks >= PARALLELISM_LIMIT:
        return None

    # Filter to ops that have queued inputs.
    ops = [op for op, state in topology.items() if state.num_queued() > 0]
    if not ops:
        return None

    # Equally penalize outqueue length and active tasks for backpressure.
    return min(
        ops, key=lambda op: len(topology[op].outqueue) + topology[op].num_active_tasks()
    )
