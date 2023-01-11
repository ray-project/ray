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
    """The execution state tracked for each PhysicalOperator."""

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
            return topology[op]

        # Create state if it doesn't exist.
        op_state = OpState(op)
        topology[op] = op_state

        # Wire up the input outqueues to this op's inqueues.
        for i, parent in enumerate(op.input_dependencies):
            parent_state = setup_state(parent)
            op_state.inqueues[i] = parent_state.outqueue

        return op_state

    setup_state(dag)

    # Create the progress bars starting from the first operator to run.
    i = 0
    for op_state in list(topology.values())[::-1]:
        if not isinstance(op_state.op, InputDataBuffer):
            op_state.initialize_progress_bar(i)
            i += 1

    # TODO: fill out stats.
    return topology, DatasetStats(stages={}, parent=None)


def process_completed_tasks(topology: Topology) -> bool:
    """Process any newly completed tasks and update operator state.

    Returns:
        Whether there remain incomplete tasks in the topology.
    """
    for op_state in topology.values():
        op_state.refresh_progress_bar()

    # Update active tasks.
    active_tasks: Dict[ray.ObjectRef, PhysicalOperator] = {}
    for op in topology:
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

    return len(active_tasks)


def select_operator_to_run(topology: Topology) -> Optional[PhysicalOperator]:
    """Select an operator to run, if possible.

    The objective of this function is to maximize the throughput of the overall
    pipeline, subject to defined memory and parallelism limits.
    """

    # TODO: set limits properly based on resources and execution options. This is just
    # a hard-coded development placeholder.
    PARALLELISM_LIMIT = 8
    num_active_tasks = sum(op.num_active_tasks() for op in topology)
    if num_active_tasks >= PARALLELISM_LIMIT:
        return None

    # This implements simple backpressure on operators that are producing outputs
    # faster than they are consuming them `len(outqueue),` as well as operators with
    # a large number of running tasks `num_active_tasks()`.
    pairs = list(topology.items())
    pairs.sort(key=lambda p: len(p[1].outqueue) + p[1].num_active_tasks())

    selected = None
    for op, state in pairs:
        if state.num_queued() > 0:
            selected = op
            break

    return selected


def dispatch_next_task(op_state: OpState) -> None:
    """Schedule the next task for the given operator.

    It is an error to call this if the given operator has no next tasks.

    Args:
        op_state: The operator state to schedule a task for.
    """
    for i, inqueue in enumerate(op_state.inqueues):
        if inqueue:
            op_state.op.add_input(inqueue.pop(0), input_index=i)
            return
    assert False, "Nothing to dispatch"
