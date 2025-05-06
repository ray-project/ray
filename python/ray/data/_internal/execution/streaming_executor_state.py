"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import ray
from ray.data._internal.execution.backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    Waitable,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.progress_bar import ProgressBar
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# Holds the full execution state of the streaming topology. It's a dict mapping each
# operator to tracked streaming exec state.
Topology = Dict[PhysicalOperator, "OpState"]


class OpBufferQueue:
    """A FIFO queue to buffer RefBundles between upstream and downstream operators.
    This class is thread-safe.
    """

    def __init__(self):
        self._num_blocks = 0
        self._queue = create_bundle_queue()
        self._num_per_split = defaultdict(int)
        self._lock = threading.Lock()
        # Used to buffer output RefBundles indexed by output splits.
        self._outputs_by_split = defaultdict(create_bundle_queue)
        super().__init__()

    @property
    def memory_usage(self) -> int:
        """The total memory usage of the queue in bytes."""
        with self._lock:
            # The split queues contain bundles popped from the main queue. So, a bundle
            # will either be in the main queue or in one of the split queues, and we
            # don't need to worry about double counting.
            return self._queue.estimate_size_bytes() + sum(
                split_queue.estimate_size_bytes()
                for split_queue in self._outputs_by_split.values()
            )

    @property
    def num_blocks(self) -> int:
        """The total number of blocks in the queue."""
        with self._lock:
            return self._num_blocks

    def __len__(self):
        with self._lock:
            return len(self._queue)

    def has_next(self, output_split_idx: Optional[int] = None) -> bool:
        """Whether next RefBundle is available.

        Args:
            output_split_idx: If specified, only check ref bundles with the
                given output split.
        """
        if output_split_idx is None:
            with self._lock:
                return len(self._queue) > 0
        else:
            with self._lock:
                return self._num_per_split[output_split_idx] > 0

    def append(self, ref: RefBundle):
        """Append a RefBundle to the queue."""
        with self._lock:
            self._queue.add(ref)
            self._num_blocks += len(ref.blocks)
            if ref.output_split_idx is not None:
                self._num_per_split[ref.output_split_idx] += 1

    def pop(self, output_split_idx: Optional[int] = None) -> Optional[RefBundle]:
        """Pop a RefBundle from the queue.
        Args:
            output_split_idx: If specified, only pop a RefBundle
                with the given output split.
        Returns:
            A RefBundle if available, otherwise None.
        """
        ret = None
        if output_split_idx is None:
            try:
                with self._lock:
                    ret = self._queue.pop()
            except IndexError:
                pass
        else:
            with self._lock:
                split_queue = self._outputs_by_split[output_split_idx]
            if len(split_queue) == 0:
                # Move all ref bundles to their indexed queues
                # Note, the reason why we do indexing here instead of in the append
                # is because only the last `OpBufferQueue` in the DAG, which will call
                # pop with output_split_idx, needs indexing.
                # If we also index the `OpBufferQueue`s in the middle, we cannot
                # preserve the order of ref bundles with different output splits.
                with self._lock:
                    while len(self._queue) > 0:
                        ref = self._queue.pop()
                        self._outputs_by_split[ref.output_split_idx].add(ref)
            try:
                ret = split_queue.pop()
            except IndexError:
                pass
        if ret is None:
            return None
        with self._lock:
            self._num_blocks -= len(ret.blocks)
            if ret.output_split_idx is not None:
                self._num_per_split[ret.output_split_idx] -= 1
        return ret

    def clear(self):
        with self._lock:
            self._queue.clear()
            self._num_blocks = 0
            self._num_per_split.clear()


@dataclass
class OpSchedulingStatus:
    """The scheduling status of an operator.

    This will be updated each time when StreamingExecutor makes
    a scheduling decision, i.e., in each `select_operator_to_run`
    call.
    """

    # Whether the op was considered runnable in the last scheduling
    # decision.
    runnable: bool = False
    # Whether the resources were sufficient for the operator to run
    # in the last scheduling decision.
    under_resource_limits: bool = False


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.

    Note: we use the `deque` data structure here because it is thread-safe, enabling
    operator queues to be shared across threads.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[OpBufferQueue]):
        # Each input queue is connected to another operator's output queue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.input_queues: List[OpBufferQueue] = inqueues
        # The output queue is connected to another operator's input queue (same object).
        #
        # Note: this queue is also accessed concurrently from the consumer thread.
        # (in addition to the streaming executor thread). Hence, it must be a
        # thread-safe type such as `deque`.
        self.output_queue: OpBufferQueue = OpBufferQueue()
        self.op = op
        self.progress_bar = None
        self.num_completed_tasks = 0
        self.inputs_done_called = False
        # Tracks whether `input_done` is called for each input op.
        self.input_done_called = [False] * len(op.input_dependencies)
        # Used for StreamingExecutor to signal exception or end of execution
        self._finished: bool = False
        self._exception: Optional[Exception] = None
        self._scheduling_status = OpSchedulingStatus()

    def __repr__(self):
        return f"OpState({self.op.name})"

    def initialize_progress_bars(self, index: int, verbose_progress: bool) -> int:
        """Create progress bars at the given index (line offset in console).

        For AllToAllOperator, zero or more sub progress bar would be created.
        Return the number of enabled progress bars created for this operator.
        """
        is_all_to_all = isinstance(self.op, AllToAllOperator)
        # Only show 1:1 ops when in verbose progress mode.
        ctx = DataContext.get_current()
        progress_bar_enabled = (
            ctx.enable_progress_bars
            and ctx.enable_operator_progress_bars
            and (is_all_to_all or verbose_progress)
        )
        self.progress_bar = ProgressBar(
            "- " + self.op.name,
            self.op.num_output_rows_total(),
            unit="row",
            position=index,
            enabled=progress_bar_enabled,
        )
        num_progress_bars = 1
        if is_all_to_all:
            # Initialize must be called for sub progress bars, even the
            # bars are not enabled via the DataContext.
            num_progress_bars += self.op.initialize_sub_progress_bars(index + 1)
        return num_progress_bars if progress_bar_enabled else 0

    def close_progress_bars(self):
        """Close all progress bars for this operator."""
        if self.progress_bar:
            self.progress_bar.close()
            if isinstance(self.op, AllToAllOperator):
                self.op.close_sub_progress_bars()

    def total_input_enqueued(self) -> int:
        """Return the number of enqueued bundles across all input queues."""
        return sum(len(q) for q in self.input_queues)

    def total_output_enqueued(self) -> int:
        """Return the number of enqueued bundles across all input queues."""
        return len(self.output_queue)

    def add_output(self, ref: RefBundle) -> None:
        """Move a bundle produced by the operator to its outqueue."""
        self.output_queue.append(ref)
        self.num_completed_tasks += 1
        if self.progress_bar:
            assert (
                ref.num_rows() is not None
            ), "RefBundle must have a valid number of rows"
            self.progress_bar.update(ref.num_rows(), self.op.num_output_rows_total())
        active, restarting, pending = self.op.actor_info_counts()
        self.op.metrics.num_alive_actors = active
        self.op.metrics.num_restarting_actors = restarting
        self.op.metrics.num_pending_actors = pending

    def refresh_progress_bar(self, resource_manager: ResourceManager) -> None:
        """Update the console with the latest operator progress."""
        if self.progress_bar:
            self.progress_bar.set_description(self.summary_str(resource_manager))
            self.progress_bar.refresh()

    def summary_str(self, resource_manager: ResourceManager) -> str:
        # Active tasks
        active = self.op.num_active_tasks()
        desc = f"- {self.op.name}: Tasks: {active}"
        if (
            self.op._in_task_submission_backpressure
            or self.op._in_task_output_backpressure
        ):
            backpressure_types = []
            if self.op._in_task_submission_backpressure:
                # The op is backpressured from submitting new tasks.
                backpressure_types.append("tasks")
            if self.op._in_task_output_backpressure:
                # The op is backpressured from producing new outputs.
                backpressure_types.append("outputs")
            desc += f" [backpressured:{','.join(backpressure_types)}]"

        # Actors info
        desc += self.op.actor_info_progress_str()

        # Queued blocks
        queued = self.total_input_enqueued() + self.op.internal_queue_size()
        desc += f"; Queued blocks: {queued}"
        desc += f"; Resources: {resource_manager.get_op_usage_str(self.op)}"

        # Any additional operator specific information.
        suffix = self.op.progress_str()
        if suffix:
            desc += f"; {suffix}"

        return desc

    def dispatch_next_task(self) -> None:
        """Move a bundle from the operator inqueue to the operator itself."""
        for i, inqueue in enumerate(self.input_queues):
            ref = inqueue.pop()
            if ref is not None:
                self.op.add_input(ref, input_index=i)
                return
        assert False, "Nothing to dispatch"

    def get_output_blocking(self, output_split_idx: Optional[int]) -> RefBundle:
        """Get an item from this node's output queue, blocking as needed.

        Returns:
            The RefBundle from the output queue, or an error / end of stream indicator.

        Raises:
            StopIteration: If all outputs are already consumed.
            Exception: If there was an exception raised during execution.
        """
        while True:
            # Check if StreamingExecutor has caught an exception or is done execution.
            if self._exception is not None:
                raise self._exception
            elif self._finished and not self.output_queue.has_next(output_split_idx):
                raise StopIteration()
            ref = self.output_queue.pop(output_split_idx)
            if ref is not None:
                return ref
            time.sleep(0.01)

    def inqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's inqueue."""
        total = 0
        for op, inq in zip(self.op.input_dependencies, self.input_queues):
            # Exclude existing input data items from dynamic memory usage.
            if not isinstance(op, InputDataBuffer):
                total += inq.memory_usage
        return total

    def outqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's outqueue."""
        return self.output_queue.memory_usage

    def outqueue_num_blocks(self) -> int:
        """Return the number of blocks in this operator's outqueue."""
        return self.output_queue.num_blocks

    def mark_finished(self, exception: Optional[Exception] = None):
        """Marks this operator as finished. Used for exiting get_output_blocking."""
        if exception is None:
            self._finished = True
        else:
            self._exception = exception


def build_streaming_topology(
    dag: PhysicalOperator, options: ExecutionOptions
) -> Tuple[Topology, int]:
    """Instantiate the streaming operator state topology for the given DAG.

    This involves creating the operator state for each operator in the DAG,
    registering it with this class, and wiring up the inqueues/outqueues of
    dependent operator states.

    Args:
        dag: The operator DAG to instantiate.
        options: The execution options to use to start operators.

    Returns:
        The topology dict holding the streaming execution state.
        The number of progress bars initialized so far.
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
            inqueues.append(parent_state.output_queue)

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
            i += op_state.initialize_progress_bars(i, options.verbose_progress)

    return (topology, i)


def process_completed_tasks(
    topology: Topology,
    resource_manager: ResourceManager,
    max_errored_blocks: int,
) -> int:
    """Process any newly completed tasks. To update operator
    states, call `update_operator_states()` afterwards.

    Args:
        topology: The toplogy of operators.
        backpressure_policies: The backpressure policies to use.
        max_errored_blocks: Max number of errored blocks to allow,
            unlimited if negative.
    Returns:
        The number of errored blocks.
    """

    # All active tasks, keyed by their waitables.
    active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    max_bytes_to_read_per_op: Dict[OpState, int] = {}
    if resource_manager.op_resource_allocator_enabled():
        for op, state in topology.items():
            max_bytes_to_read = (
                resource_manager.op_resource_allocator.max_task_output_bytes_to_read(op)
            )
            op._in_task_output_backpressure = max_bytes_to_read == 0
            if max_bytes_to_read is not None:
                max_bytes_to_read_per_op[state] = max_bytes_to_read

    # Process completed Ray tasks and notify operators.
    num_errored_blocks = 0
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )

        # Organize tasks by the operator they belong to, and sort them by task index.
        # So that we'll process them in a deterministic order.
        # This is because OpResourceAllocator may limit the number of blocks to read
        # per operator. In this case, we want to have fewer tasks finish quickly and
        # yield resources, instead of having all tasks output blocks together.
        ready_tasks_by_op = defaultdict(list)
        for ref in ready:
            state, task = active_tasks[ref]
            ready_tasks_by_op[state].append(task)

        for state, ready_tasks in ready_tasks_by_op.items():
            ready_tasks = sorted(ready_tasks, key=lambda t: t.task_index())
            for task in ready_tasks:
                if isinstance(task, DataOpTask):
                    try:
                        bytes_read = task.on_data_ready(
                            max_bytes_to_read_per_op.get(state, None)
                        )
                        if state in max_bytes_to_read_per_op:
                            max_bytes_to_read_per_op[state] -= bytes_read
                    except Exception as e:
                        num_errored_blocks += 1
                        should_ignore = (
                            max_errored_blocks < 0
                            or max_errored_blocks >= num_errored_blocks
                        )
                        error_message = (
                            "An exception was raised from a task of "
                            f'operator "{state.op.name}".'
                        )
                        if should_ignore:
                            remaining = (
                                max_errored_blocks - num_errored_blocks
                                if max_errored_blocks >= 0
                                else "unlimited"
                            )
                            error_message += (
                                " Ignoring this exception with remaining"
                                f" max_errored_blocks={remaining}."
                            )
                            logger.error(error_message, exc_info=e)
                        else:
                            error_message += (
                                " Dataset execution will now abort."
                                " To ignore this exception and continue, set"
                                " DataContext.max_errored_blocks."
                            )
                            logger.exception(error_message)
                            raise e from None
                else:
                    assert isinstance(task, MetadataOpTask)
                    task.on_task_finished()

    # Pull any operator outputs into the streaming op state.
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())

    return num_errored_blocks


def update_operator_states(topology: Topology) -> None:
    """Update operator states accordingly for newly completed tasks.
    Should be called after `process_completed_tasks()`."""

    # Call inputs_done() on ops where no more inputs are coming.
    for op, op_state in topology.items():
        if op_state.inputs_done_called:
            continue
        all_inputs_done = True
        for idx, dep in enumerate(op.input_dependencies):
            if dep.completed() and not topology[dep].output_queue:
                if not op_state.input_done_called[idx]:
                    op.input_done(idx)
                    op_state.input_done_called[idx] = True
            else:
                all_inputs_done = False

        if all_inputs_done:
            op.all_inputs_done()
            op_state.inputs_done_called = True

    # Traverse the topology in reverse topological order.
    # For each op, if all of its downstream operators have completed.
    # call mark_execution_finished() to also complete this op.
    for op, op_state in reversed(list(topology.items())):
        if op.completed():
            continue
        dependents_completed = len(op.output_dependencies) > 0 and all(
            dep.completed() for dep in op.output_dependencies
        )
        if dependents_completed:
            op.mark_execution_finished()


def get_eligible_operators(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    resource_manager: ResourceManager,
    *,
    ensure_liveness: bool,
) -> List[PhysicalOperator]:
    """This method returns all operators that are eligible for execution in the current state
    of the pipeline.

    Operator is considered eligible for execution iff:

        1. It's NOT completed
        2. It has at least 1 input block (in the input queue)
        3. It can accept new inputs
        4. It's not currently throttled (for task-submission)

    """

    # Filter to ops that are eligible for execution.
    eligible_ops: List[PhysicalOperator] = []
    for op, state in topology.items():
        assert resource_manager.op_resource_allocator_enabled(), topology

        # Check whether the operator is under its limits imposed by the
        # resource manager
        under_resource_limits = (
            resource_manager.op_resource_allocator.can_submit_new_task(op)
        )
        # Operator is considered being in task-submission back-pressure if
        # both of the following holds true:
        #   - It's exceeding its resource limits
        #   - At least one of the back-pressure policies are violated
        in_backpressure = not under_resource_limits or not all(
            p.can_add_input(op) for p in backpressure_policies
        )

        op_runnable = False

        # Check whether operator could start executing immediately:
        #   - It's not completed
        #   - It can accept at least one input
        #   - Its input queue is not empty
        if (
            not op.completed()
            and op.should_add_input()
            and state.total_input_enqueued() > 0
            and not in_backpressure
        ):
            op_runnable = True
            eligible_ops.append(op)

        # Update scheduling status
        state._scheduling_status = OpSchedulingStatus(
            runnable=op_runnable,
            under_resource_limits=under_resource_limits,
        )

        # Signal whether op in backpressure for stats collections
        op.notify_in_task_submission_backpressure(in_backpressure)

    # To ensure liveness, allow at least 1 operator to schedule tasks regardless of
    # limits in case when topology is entirely idle (no active tasks running)
    if (
        not eligible_ops
        and ensure_liveness
        and all(op.num_active_tasks() == 0 for op in topology)
    ):
        eligible_ops = [
            op
            for op, state in topology.items()
            # Pick only operators that have a non-empty input queue
            if state.total_input_enqueued() > 0 and not op.completed()
        ]

    return eligible_ops


def select_operator_to_run(
    topology: Topology,
    resource_manager: ResourceManager,
    backpressure_policies: List[BackpressurePolicy],
    ensure_liveness: bool,
) -> Optional[PhysicalOperator]:
    """Select next operator to launch new tasks.

    The objective of this method is to maximize the throughput of the overall
    pipeline, subject to defined memory, parallelism and other constraints.

    To achieve that this method implements following protocol:

        1. Collects all _eligible_ to run operators (check `_get_eligible_ops`
           for more details)
        2. Applies stack-ranking algorithm to select the best operator (check
           `_create_eligible_ops_ranker` for more details)

    """
    eligible_ops = get_eligible_operators(
        topology,
        backpressure_policies,
        resource_manager,
        ensure_liveness=ensure_liveness,
    )

    if not eligible_ops:
        return None

    ranks = _rank_operators(eligible_ops, resource_manager)

    assert len(eligible_ops) == len(ranks), (eligible_ops, ranks)

    next_op, _ = min(zip(eligible_ops, ranks), key=lambda t: t[1])

    return next_op


def _rank_operators(
    ops: List[PhysicalOperator], resource_manager: ResourceManager
) -> List[Tuple]:
    """Picks operator to run according to the following semantic:

    Operator to run next is selected as the one with the *smallest* value
    of the lexicographically ordered ranks composed of (in order):

        1. Whether operator's could be throttled (bool)
        2. Operators' object store utilization

    Consider following examples:

    Example 1:

        Operator 1 with rank (True, 1024 bytes)
        Operator 2 with rank (False, 2048 bytes)

    In that case Operator 2 will be selected.

    Example 2:

        Operator 1 with rank (True, 1024 bytes)
        Operator 2 with rank (True, 2048 bytes)

    In that case Operator 1 will be selected.
    """

    assert len(ops) > 0, ops

    def _ranker(op):
        # Rank composition:
        #   1. Whether throttling is enabled
        #   2. Estimated Object Store usage
        return (
            not op.throttling_disabled(),
            resource_manager.get_op_usage(op).object_store_memory,
        )

    return [_ranker(op) for op in ops]
