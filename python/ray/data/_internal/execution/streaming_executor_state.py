"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import ray
# Try importing rich, handle gracefully if not installed
try:
    from rich.progress import TaskID
except ImportError:
    TaskID = None # Define as None if rich isn't available

from ray.data._internal.execution.autoscaler import Autoscaler
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
# Removed ProgressBar import
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

    # Whether the op was selected to run in the last scheduling
    # decision.
    selected: bool = False
    # Whether the op was considered runnable in the last scheduling
    # decision.
    runnable: bool = False
    # Whether the resources were sufficient for the operator to run
    # in the last scheduling decision.
    under_resource_limits: bool = False


# --- Start: Added Helper Classes/Functions for Rich Display ---
@dataclass
class OpResourceUsage:
    """Helper dataclass to hold resource usage for progress display."""
    cpu: float = 0.0
    gpu: float = 0.0
    object_store_memory: int = 0

    def __str__(self) -> str:
        """Formats the resource usage string for display."""
        mem_str = format_memory_str(self.object_store_memory)
        cpu_str = f"{self.cpu:.1f}".replace(".0", "") # Remove .0 for integers
        # Basic format, adjust if GPU needed later
        # Example: "0.0 CPU, 0.0B object store" or "4 CPU, 512.6MB object store"
        return f"{cpu_str} CPU, {mem_str} object store"

def format_memory_str(mem_bytes: int) -> str:
    """Formats memory bytes into a human-readable string (KB, MB, GB)."""
    if mem_bytes < 1024:
        # Show 0.0B for 0 bytes
        s = f"{mem_bytes:.1f}B" if mem_bytes > 0 else "0.0B"
    elif mem_bytes < 1024 * 1024:
        s = f"{mem_bytes/1024:.1f}KB"
    elif mem_bytes < 1024 * 1024 * 1024:
        s = f"{mem_bytes/(1024*1024):.1f}MB"
    else:
        s = f"{mem_bytes/(1024*1024*1024):.1f}GB"
    # Remove trailing .0 for integer values like 1.0MB -> 1MB
    return s.replace(".0KB", "KB").replace(".0MB", "MB").replace(".0GB", "GB")


@dataclass
class OpDisplayInfo:
    """Holds the details needed for the rich display second line."""
    resource_usage: OpResourceUsage = field(default_factory=OpResourceUsage)
    tasks_active: int = 0
    tasks_queued: int = 0
    extra_info: str = "" # For things like [locality off] or Actors: N

    def format_display_line(self) -> str:
        """Formats the second line string for the rich display.

        Examples:
        "0.0 CPU, 0.0B object store; Tasks: 0; Queued blocks: 2;"
        "0.0 CPU, 5.9GB object store; Tasks: 0: [locality off]; Queued blocks: 0;"
        "4.0 CPU, 512.6MB object store; Tasks: 16; Actors: 4; Queued blocks: 0;"
        "32; 0.0 CPU, 1.5MB object store; Tasks: 0; Queued blocks: 0;"
        """
        res_str = str(self.resource_usage)
        task_str = f"Tasks: {self.tasks_active}"
        queue_str = f"Queued blocks: {self.tasks_queued}"

        parts = [res_str, task_str]
        if self.extra_info:
            # Insert extra info right after Tasks: count
            parts[1] = f"{task_str}: {self.extra_info}" # e.g. "Tasks: 0: [locality off]" or "Tasks: 16; Actors: 4"
            if "Actors:" in self.extra_info: # Handle Actors: N slightly differently if needed
                 parts[1] = task_str # Keep task count separate
                 parts.insert(2, self.extra_info) # Insert "Actors: N"

        parts.append(queue_str)

        # Special case for LimitOperator (based on screenshot)
        # The screenshot shows "32; 0.0 CPU, 1.5MB object store; Tasks: 0; Queued blocks:"
        # This seems to prepend the limit value. We don't have the limit value here easily.
        # We'll stick to the standard format for now. Revisit if limit needs special handling.

        return "; ".join(parts) + ";" # Add trailing semicolon
# --- End: Added Helper Classes/Functions for Rich Display ---


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.

    Note: we use the `deque` data structure here because it is thread-safe, enabling
    operator queues to be shared across threads.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[OpBufferQueue]):
        # Each inqueue is connected to another operator's outqueue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.inqueues: List[OpBufferQueue] = inqueues
        # The outqueue is connected to another operator's inqueue (they physically
        # share the same Python list reference).
        self.outqueue: OpBufferQueue = OpBufferQueue()
        self.op = op
        self.num_completed_tasks = 0
        self.inputs_done_called = False
        # Tracks whether `input_done` is called for each input op.
        self.input_done_called = [False] * len(op.input_dependencies)
        # Used for StreamingExecutor to signal exception or end of execution
        self._finished: bool = False
        self._exception: Optional[Exception] = None
        self._scheduling_status = OpSchedulingStatus()

        # --- Rich progress bar related fields ---
        self.rich_task_id: Optional["TaskID"] = None # Use string for optional import
        self.display_info = OpDisplayInfo() # Store details for the second line
        self.last_num_output_rows: int = 0 # Track progress count for rich bar
        # --- End Rich progress bar related fields ---

    def __repr__(self):
        return f"OpState({self.op.name})"

    def num_queued(self) -> int:
        """Return the number of queued bundles across all inqueues."""
        return sum(len(q) for q in self.inqueues)

    def num_processing(self):
        """Return the number of bundles currently in processing for this operator."""
        return self.op.num_active_tasks() + self.op.internal_queue_size()

    # --- Added method to update display details ---
    def update_display_info(self, resource_manager: ResourceManager):
        """Updates the display_info field with current stats."""
        usage = resource_manager.get_op_usage(self.op)
        self.display_info.resource_usage = OpResourceUsage(
            cpu=usage.cpu,
            gpu=usage.gpu,
            object_store_memory=usage.object_store_memory
        )
        self.display_info.tasks_active = self.op.num_active_tasks()
        # Queued = external + internal operator queue
        self.display_info.tasks_queued = self.num_queued() + self.op.internal_queue_size()

        # Attempt to get extra info like locality or actor count
        extra = ""
        try:
            op_extra = self.op.progress_str() # May contain locality, etc.
            if op_extra and "locality" in op_extra:
                 # Basic extraction, might need improvement
                 extra = f"[{op_extra.strip()}]"

            # Check if it's an op likely to have actors (Map/AllToAll)
            # Use actor_info_counts which we already update in add_output
            active_actors, _, _ = self.op.actor_info_counts()
            if active_actors > 0 and not extra: # Don't overwrite locality info
                 extra = f"Actors: {active_actors}"

        except AttributeError:
            pass # Op might not have progress_str or actor_info_counts
        except Exception as e:
            # Log unexpected errors getting extra info
            logger.debug(f"Error getting extra progress info for {self.op.name}: {e}")
        self.display_info.extra_info = extra
    # --- End added method ---

    def add_output(self, ref: RefBundle) -> None:
        """Move a bundle produced by the operator to its outqueue."""
        self.outqueue.append(ref)
        self.num_completed_tasks += 1
        if ref.num_rows() is not None:
             self.last_num_output_rows += ref.num_rows() # Update progress count

        # Update actor counts (remains the same)
        active, restarting, pending = self.op.actor_info_counts()
        self.op.metrics.num_alive_actors = active
        self.op.metrics.num_restarting_actors = restarting
        self.op.metrics.num_pending_actors = pending

        # NOTE: We don't update the rich progress bar *here*.
        # The central executor loop will call update_display_info
        # and then update the rich progress task for all ops.

    def dispatch_next_task(self) -> None:
        """Move a bundle from the operator inqueue to the operator itself."""
        for i, inqueue in enumerate(self.inqueues):
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
            elif self._finished and not self.outqueue.has_next(output_split_idx):
                raise StopIteration()
            ref = self.outqueue.pop(output_split_idx)
            if ref is not None:
                return ref
            # Reduce sleep time slightly for potentially better responsiveness
            time.sleep(0.005)

    def inqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's inqueue."""
        total = 0
        for op, inq in zip(self.op.input_dependencies, self.inqueues):
            # Exclude existing input data items from dynamic memory usage.
            if not isinstance(op, InputDataBuffer):
                total += inq.memory_usage
        return total

    def outqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's outqueue."""
        return self.outqueue.memory_usage

    def outqueue_num_blocks(self) -> int:
        """Return the number of blocks in this operator's outqueue."""
        return self.outqueue.num_blocks

    def mark_finished(self, exception: Optional[Exception] = None):
        """Marks this operator as finished. Used for exiting get_output_blocking."""
        if exception is None:
            self._finished = True
        else:
            self._exception = exception


def build_streaming_topology(
    dag: PhysicalOperator, options: ExecutionOptions
) -> Topology: # Return only topology, progress bar count removed
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

    # Progress bar initialization is now handled externally by the executor
    # using a rich progress manager.

    return topology


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
            timeout=0.1, # Keep timeout reasonable
        )

        # Organize tasks by the operator they belong to, and sort them by task index.
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
                            f"An exception was raised from a task {task.task_index()} of "
                            f'operator "{state.op.name}".'
                        )
                        if should_ignore:
                            remaining = (
                                max_errored_blocks - num_errored_blocks
                                if max_errored_blocks >= 0
                                else "unlimited"
                            )
                            error_message += (
                                f" Ignoring this exception with remaining"
                                f" max_errored_blocks={remaining}."
                            )
                            logger.error(error_message, exc_info=e)
                            # Mark the task as completed with error state for tracking if needed
                            task.on_task_finished(error=e)
                        else:
                            error_message += (
                                " Dataset execution will now abort."
                                " To ignore this exception and continue, set"
                                " DataContext.max_errored_blocks."
                            )
                            logger.error(error_message)
                            # Mark the task as completed with error before raising
                            task.on_task_finished(error=e)
                            raise e from None
                else:
                    assert isinstance(task, MetadataOpTask)
                    task.on_task_finished()

    # Pull any operator outputs into the streaming op state.
    for op, op_state in topology.items():
        # Make sure operator is still running before pulling outputs
        if not op.completed():
            try:
                while op.has_next():
                    op_state.add_output(op.get_next())
            except Exception as e:
                 # If getting output fails, mark op state with exception
                 logger.error(f"Error getting output from operator {op.name}: {e}")
                 op_state.mark_finished(exception=e)


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
            # Check parent state finish status as well
            parent_state = topology.get(dep)
            if parent_state and parent_state._finished and not parent_state.outqueue.has_next():
                 if not op_state.input_done_called[idx]:
                     try:
                         op.input_done(idx)
                         op_state.input_done_called[idx] = True
                     except Exception as e:
                         logger.error(f"Error calling input_done({idx}) for {op.name}: {e}")
                         op_state.mark_finished(exception=e)
                         all_inputs_done = False
                         break # Stop checking if an error occurs during input_done

            # Original completion check (may still be relevant for some ops)
            elif dep.completed() and not topology[dep].outqueue.has_next():
                if not op_state.input_done_called[idx]:
                    try:
                        op.input_done(idx)
                        op_state.input_done_called[idx] = True
                    except Exception as e:
                        logger.error(f"Error calling input_done({idx}) for {op.name}: {e}")
                        op_state.mark_finished(exception=e)
                        all_inputs_done = False
                        break
            else:
                all_inputs_done = False

        if all_inputs_done and not op_state._finished: # Only call if not already finished
            try:
                op.all_inputs_done()
                op_state.inputs_done_called = True
            except Exception as e:
                logger.error(f"Error calling all_inputs_done for {op.name}: {e}")
                op_state.mark_finished(exception=e)


    # Traverse the topology in reverse topological order.
    # For each op, if all of its downstream operators have completed.
    # call mark_execution_finished() to also complete this op.
    # This logic seems less critical now, as completion is driven by inputs_done/errors
    # Keep it for now, but might be removable later.
    for op, op_state in reversed(list(topology.items())):
        if op.completed() or op_state._finished:
            continue
        dependents_completed = len(op.output_dependencies) > 0 and all(
            topology[dep]._finished for dep in op.output_dependencies
        )
        # Also check if operator itself reported completion internally
        if dependents_completed or op.completed():
            try:
                 if not op.completed(): # Avoid calling if already marked by operator
                    op.mark_execution_finished()
                 op_state.mark_finished() # Ensure OpState also marked
            except Exception as e:
                 logger.error(f"Error calling mark_execution_finished for {op.name}: {e}")
                 op_state.mark_finished(exception=e)


def select_operator_to_run(
    topology: Topology,
    resource_manager: ResourceManager,
    backpressure_policies: List[BackpressurePolicy],
    autoscaler: Autoscaler,
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
    ops = []
    for op, state in topology.items():
        if state._finished: # Don't select finished ops
             continue

        # Assume op_resource_allocator is enabled if resource_manager is provided
        under_resource_limits = True
        if resource_manager.op_resource_allocator_enabled():
             under_resource_limits = resource_manager.op_resource_allocator.can_submit_new_task(op)

        in_backpressure = not under_resource_limits or any(
            not p.can_add_input(op) for p in backpressure_policies
        )
        op_runnable = False
        if (
            not in_backpressure
            and not op.completed()
            and state.num_queued() > 0
            and op.should_add_input()
        ):
            ops.append(op)
            op_runnable = True
        # Update scheduling status
        state._scheduling_status = OpSchedulingStatus(
            selected=False,
            runnable=op_runnable,
            under_resource_limits=under_resource_limits,
        )

        # Signal whether op in backpressure for stats collections
        op.notify_in_task_submission_backpressure(in_backpressure)

    # To ensure liveness, allow at least 1 op to run regardless of limits. This is
    # gated on `ensure_at_least_one_running`, which is set if the consumer is blocked.
    if (
        ensure_at_least_one_running
        and not ops
        and all(state.op.num_active_tasks() == 0 for state in topology.values() if not state._finished)
    ):
        # The topology is entirely idle, so choose from all ready ops ignoring limits.
        ops = [
            op
            for op, state in topology.items()
            if not state._finished and state.num_queued() > 0 and not op.completed() and op.should_add_input()
        ]

    selected_op = None
    if ops:
        # Run metadata-only operators first. After that, choose the operator with the
        # least memory usage. Consider other factors?
        selected_op = min(
            ops,
            key=lambda op: (
                not op.throttling_disabled(),
                resource_manager.get_op_usage(op).object_store_memory,
                # Add op name as tie-breaker for deterministic selection
                op.name
            ),
        )
        if selected_op:
            topology[selected_op]._scheduling_status.selected = True

    # Consider moving autoscaler trigger outside this selection logic,
    # maybe based on overall pressure or idle state.
    autoscaler.try_trigger_scaling()
    return selected_op
