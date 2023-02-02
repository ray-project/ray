"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

import math
import time
from collections import deque
from typing import Dict, List, Optional, Deque, Union

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    RefBundle,
    PhysicalOperator,
    ExecutionOptions,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats


# Holds the full execution state of the streaming topology. It's a dict mapping each
# operator to tracked streaming exec state.
Topology = Dict[PhysicalOperator, "OpState"]

# A RefBundle or an exception / end of stream indicator.
MaybeRefBundle = Union[RefBundle, Exception, None]


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.

    Note: we use the `deque` data structure here because it is thread-safe, enabling
    operator queues to be shared across threads.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[Deque[MaybeRefBundle]]):
        # Each inqueue is connected to another operator's outqueue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.inqueues: List[Deque[MaybeRefBundle]] = inqueues
        # The outqueue is connected to another operator's inqueue (they physically
        # share the same Python list reference).
        self.outqueue: Deque[MaybeRefBundle] = deque()
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

    def num_processing(self):
        """Return the number of bundles currently in processing for this operator."""
        return self.op.num_active_work_refs() + self.op.internal_queue_size()

    def add_output(self, ref: RefBundle) -> None:
        """Move a bundle produced by the operator to its outqueue."""
        self.outqueue.append(ref)
        self.num_completed_tasks += 1
        if self.progress_bar:
            self.progress_bar.update(1)

    def refresh_progress_bar(self) -> None:
        """Update the console with the latest operator progress."""
        if self.progress_bar:
            self.progress_bar.set_description(self.summary_str())

    def summary_str(self) -> str:
        queued = self.num_queued() + self.op.internal_queue_size()
        active = self.op.num_active_work_refs()
        desc = f"{self.op.name}: {active} active, {queued} queued"
        suffix = self.op.progress_str()
        if suffix:
            desc += f", {suffix}"
        return desc

    def dispatch_next_task(self) -> None:
        """Move a bundle from the operator inqueue to the operator itself."""
        for i, inqueue in enumerate(self.inqueues):
            if inqueue:
                self.op.add_input(inqueue.popleft(), input_index=i)
                return
        assert False, "Nothing to dispatch"

    def get_output_blocking(self) -> MaybeRefBundle:
        """Get an item from this node's output queue, blocking as needed.

        Returns:
            The RefBundle from the output queue, or an error / end of stream indicator.
        """
        while True:
            try:
                return self.outqueue.popleft()
            except IndexError:
                time.sleep(0.01)


def build_streaming_topology(
    dag: PhysicalOperator, options: ExecutionOptions
) -> Topology:
    """Instantiate the streaming operator state topology for the given DAG.

    This involves creating the operator state for each operator in the DAG,
    registering it with this class, and wiring up the inqueues/outqueues of
    dependent operator states.

    Args:
        dag: The operator DAG to instantiate.
        options: The execution options to use to start operators.

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
        op.start(options)
        return op_state

    setup_state(dag)

    # Create the progress bars starting from the first operator to run.
    # Note that the topology dict is in topological sort order. Index zero is reserved
    # for global progress information.
    i = 1
    for op_state in list(topology.values()):
        if not isinstance(op_state.op, InputDataBuffer):
            op_state.initialize_progress_bar(i)
            i += 1

    # TODO: fill out stats.
    return topology, DatasetStats(stages={}, parent=None)


def process_completed_tasks(topology: Topology) -> None:
    """Process any newly completed tasks and update operator state."""

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


def select_operator_to_run(
    topology: Topology,
    cur_usage: ExecutionResources,
    limits: ExecutionResources,
    ensure_at_least_one_running: bool,
) -> Optional[PhysicalOperator]:
    """Select an operator to run, if possible.

    The objective of this function is to maximize the throughput of the overall
    pipeline, subject to defined memory and parallelism limits.

    This is currently implemented by applying backpressure on operators that are
    producing outputs faster than they are consuming them `len(outqueue)`, as well as
    operators with a large number of running tasks `num_processing()`.

    Note that memory limits also apply to the outqueue of the output operator. This
    provides backpressure if the consumer is slow. However, once a bundle is returned
    to the user, it is no longer tracked.
    """

    # Filter to ops that are eligible for execution.
    ops = [
        op
        for op, state in topology.items()
        if state.num_queued() > 0 and _execution_allowed(op, cur_usage, limits)
    ]

    # To ensure liveness, allow at least 1 op to run regardless of limits. This is
    # gated on `ensure_at_least_one_running`, which is set if the consumer is blocked.
    if (
        ensure_at_least_one_running
        and not ops
        and all(op.num_active_work_refs() == 0 for op in topology)
    ):
        # The topology is entirely idle, so choose from all ready ops ignoring limits.
        ops = [op for op, state in topology.items() if state.num_queued() > 0]

    # Nothing to run.
    if not ops:
        return None

    # Equally penalize outqueue length and num bundles processing for backpressure.
    return min(
        ops, key=lambda op: len(topology[op].outqueue) + topology[op].num_processing()
    )


def _execution_allowed(
    op: PhysicalOperator,
    global_usage: ExecutionResources,
    global_limits: ExecutionResources,
) -> bool:
    """Return whether an operator is allowed to execute given resource usage.

    Args:
        op: The operator to check.
        global_usage: Resource usage across the entire topology.
        global_limits: Execution resource limits.

    Returns:
        Whether the op is allowed to run.
    """
    # To avoid starvation problems when dealing with fractional resource types,
    # convert all quantities to integer (0 or 1) for deciding admissibility. This
    # allows operators with non-integral requests to slightly overshoot the limit.
    global_floored = ExecutionResources(
        cpu=math.floor(global_usage.cpu or 0),
        gpu=math.floor(global_usage.gpu or 0),
        object_store_memory=global_usage.object_store_memory,
    )
    inc = op.incremental_resource_usage()
    inc_indicator = ExecutionResources(
        cpu=1 if inc.cpu else 0,
        gpu=1 if inc.gpu else 0,
        object_store_memory=1 if inc.object_store_memory else 0,
    )
    return global_floored.add(inc_indicator).satisfies_limit(global_limits)
