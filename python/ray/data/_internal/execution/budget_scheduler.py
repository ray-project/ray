"""Budget-based scheduler for the streaming executor.

This scheduler uses a global memory budget (in bytes) to control how many
tasks can be in-flight at once. It replaces the existing multi-policy
backpressure system with a single, simpler mechanism.

Key ideas:
- A single global budget (max_bytes) caps total estimated in-flight memory.
- Each operator tracks an expected *peak ratio* (peak_concurrent_bytes /
  input_bytes) per task via an EMA. On dispatch, the expected peak is
  ratio * input_bytes. For source operators, use input_bytes=1 and set
  the initial ratio to the target partition size.
- On dispatch, the scheduler pre-allocates the expected peak for that task.
- If actual peak exceeds the estimate, allocated_bytes is ratcheted up.
- On task completion, the EMA is updated with the observed peak ratio.
- Backpressure is applied when the budget is exhausted.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Type alias — at runtime this is Operator, but we avoid importing
# it so the module stays importable without a full Ray build.
Operator = Any

# Default EMA smoothing factor for peak memory updates.
DEFAULT_EMA_ALPHA = 0.3

# Default fraction of object store memory to use as the global budget.
DEFAULT_OBJECT_STORE_FRACTION = 0.5

# Default max budget bytes per CPU slot.
DEFAULT_BYTES_PER_CPU = 256 * 1024 * 1024  # 256 MiB


@dataclass
class TaskState:
    """Tracks per-task state for budget accounting."""

    op: Operator
    # Total input bytes consumed by this task.
    input_bytes: int
    # Expected peak concurrent bytes (pre-allocated at dispatch time).
    expected_peak_bytes: int
    # Total output bytes produced so far.
    produced_bytes: int = 0
    # Total output bytes consumed by downstream so far.
    consumed_bytes: int = 0
    # Peak concurrent bytes observed so far: max(produced - consumed).
    peak_concurrent_bytes: int = 0
    # Whether this task has been marked as backpressured.
    backpressured: bool = False
    # Number of CPUs allocated to this task.
    num_cpus: float = 1.0
    # Whether the task has finished executing (but may still have unconsumed output).
    finished: bool = False

    @property
    def concurrent_bytes(self) -> int:
        """Current bytes in-flight (produced but not yet consumed)."""
        return self.produced_bytes - self.consumed_bytes


@dataclass
class OpSchedulingState:
    """Per-operator scheduling state."""

    # EMA of peak ratio (peak_concurrent_bytes / input_bytes) per task.
    expected_peak_ratio: float = 1.0
    # Total bytes buffered in this operator's output queue.
    buffered_output_bytes: int = 0


class BudgetScheduler:
    """Memory-budget-based scheduler for streaming execution.

    Manages a global memory budget and decides which operator to dispatch
    next, based on available budget and operator priority.

    The budget is denominated in "peak concurrent bytes" -- the max amount
    of output data a task holds at any instant (produced minus consumed by
    downstream). This is much tighter than budgeting total output bytes
    because downstream pipelining reduces how much data accumulates.

    Args:
        max_bytes: Global memory budget in bytes.
        available_cpus: Number of CPU slots available for task execution.
        ema_alpha: Smoothing factor for peak-memory EMA updates.
    """

    def __init__(
        self,
        max_bytes: int,
        available_cpus: float,
        ema_alpha: float = DEFAULT_EMA_ALPHA,
    ):
        self.max_bytes = max_bytes
        self.available_cpus = available_cpus
        self.ema_alpha = ema_alpha

        # Sum of expected_peak_bytes for all running tasks, plus any
        # excess from tasks whose actual peak exceeded the estimate.
        self.allocated_bytes: int = 0

        # Per-task tracking keyed by a unique task id.
        self.task_states: Dict[int, TaskState] = {}

        # Per-operator scheduling state.
        self.op_states: Dict[Operator, OpSchedulingState] = {}

        # Monotonically increasing task id counter.
        self._next_task_id: int = 0

    def register_operator(
        self,
        op: Operator,
        initial_peak_ratio: float = 1.0,
    ) -> None:
        """Register an operator with the scheduler.

        Args:
            op: The physical operator.
            initial_peak_ratio: Initial expected ratio of peak concurrent
                bytes to input bytes. For source operators (input_bytes=1),
                set this to the target partition size.
        """
        self.op_states[op] = OpSchedulingState(
            expected_peak_ratio=initial_peak_ratio,
        )

    def get_op_state(self, op: Operator) -> OpSchedulingState:
        return self.op_states[op]

    def _get_topology_order(self, op: Operator) -> int:
        """Return the topological index of an operator.

        Higher index = further downstream. Used for tie-breaking.
        """
        depth = 0
        current = op
        while current.input_dependencies:
            current = current.input_dependencies[0]
            depth += 1
        return depth

    def select_operator(
        self,
        ops_with_input: Dict[Operator, int],
    ) -> Optional[Operator]:
        """Select the next operator to dispatch a task for.

        Picks the operator with the fewest buffered output bytes.
        Ties are broken by favoring downstream (later) operators.

        Only considers operators whose dispatch would stay within budget.

        Args:
            ops_with_input: Mapping from operators that have pending input
                to the size in bytes of the next input to be consumed.

        Returns:
            The selected operator, or None if nothing is dispatchable.
        """
        best_op = None
        best_key = None

        for op, input_bytes in ops_with_input.items():
            if not self._can_dispatch(op, input_bytes):
                continue

            state = self.op_states.get(op)
            if state is None:
                continue

            # Key: (buffered_output_bytes, -topology_order)
            # Lower buffered bytes is better; higher topology order (downstream)
            # is better for tie-breaking.
            key = (state.buffered_output_bytes, -self._get_topology_order(op))
            if best_key is None or key < best_key:
                best_key = key
                best_op = op

        return best_op

    def _can_dispatch(self, op: Operator, input_bytes: int) -> bool:
        """Check if dispatching a task for this operator would stay in budget."""
        state = self.op_states.get(op)
        if state is None:
            return False

        # Check CPU availability.
        task_cpus = self._get_task_cpus(op)
        if task_cpus > self.available_cpus:
            return False

        # Check if adding this task's expected peak would exceed budget.
        expected_peak = state.expected_peak_ratio * input_bytes
        if self.allocated_bytes + expected_peak > self.max_bytes:
            return False

        return True

    def _get_task_cpus(self, op: Operator) -> float:
        """Get the CPU cost of a task for this operator."""
        resources = op.per_task_resource_allocation()
        if resources and resources.cpu:
            return resources.cpu
        return 1.0

    def dispatch_task(
        self,
        op: Operator,
        input_bytes: int,
        input_task_ids: List[int],
        num_cpus: float = 1.0,
    ) -> int:
        """Record that a task has been dispatched for the given operator.

        Pre-allocates the expected peak concurrent bytes in the global budget.
        Also consumes outputs from the op's input operator(s): decreases
        their buffered_output_bytes and updates consumed_bytes on the
        producing tasks.

        Args:
            op: The operator the task belongs to.
            input_bytes: Actual bytes of the input being consumed.
            input_task_ids: Task IDs that produced the input being consumed.
                Their consumed_bytes will be updated.
            num_cpus: CPUs allocated to this task.

        Returns:
            A unique task_id for tracking this task.
        """
        state = self.op_states[op]
        expected_peak = int(state.expected_peak_ratio * input_bytes)

        task_id = self._next_task_id
        self._next_task_id += 1

        self.task_states[task_id] = TaskState(
            op=op,
            input_bytes=input_bytes,
            expected_peak_bytes=expected_peak,
            num_cpus=num_cpus,
        )

        self.allocated_bytes += expected_peak
        self.available_cpus -= num_cpus

        # Consume outputs from the op's input operators.
        if input_bytes > 0:
            for input_op in op.input_dependencies:
                input_op_state = self.op_states.get(input_op)
                if input_op_state is not None:
                    input_op_state.buffered_output_bytes = max(
                        0, input_op_state.buffered_output_bytes - input_bytes
                    )

        # Update consumed_bytes on the producing tasks.
        if input_task_ids:
            per_task_bytes = input_bytes // len(input_task_ids) if input_task_ids else 0
            remainder = input_bytes - per_task_bytes * len(input_task_ids)
            for i, producing_tid in enumerate(input_task_ids):
                task = self.task_states.get(producing_tid)
                if task is not None:
                    extra = remainder if i == 0 else 0
                    consumed = per_task_bytes + extra
                    task.consumed_bytes += consumed
                    # For finished tasks, allocated_bytes tracks concurrent_bytes,
                    # so reduce it as outputs are consumed.
                    if task.finished:
                        self.allocated_bytes -= consumed
                        if task.concurrent_bytes == 0:
                            del self.task_states[producing_tid]

        return task_id

    def on_task_output(self, task_id: int, output_bytes: int) -> bool:
        """Record that a task produced an output block.

        Updates peak concurrent bytes tracking. If the actual peak exceeds
        the pre-allocated estimate, the excess is charged to the budget.

        Args:
            task_id: The task that produced output.
            output_bytes: Bytes of the output block.

        Returns:
            True if backpressure should be applied to this task.
        """
        task = self.task_states.get(task_id)
        if task is None:
            return False

        task.produced_bytes += output_bytes

        # Update the operator's buffered output bytes.
        op_state = self.op_states[task.op]
        op_state.buffered_output_bytes += output_bytes

        # Update peak tracking.
        old_peak = task.peak_concurrent_bytes
        task.peak_concurrent_bytes = max(
            task.peak_concurrent_bytes, task.concurrent_bytes
        )

        # If actual peak exceeds pre-allocated estimate, charge the excess.
        if task.peak_concurrent_bytes > task.expected_peak_bytes:
            # Only charge the new excess since last update.
            previously_charged = max(0, old_peak - task.expected_peak_bytes)
            new_charge = (
                task.peak_concurrent_bytes - task.expected_peak_bytes
            ) - previously_charged
            if new_charge > 0:
                self.allocated_bytes += new_charge

        # Backpressure if over budget and this task is contributing excess.
        if self.allocated_bytes >= self.max_bytes:
            if task.peak_concurrent_bytes >= task.expected_peak_bytes:
                task.backpressured = True
                return True

        return False

    def on_task_finished(self, task_id: int) -> None:
        """Record that a task has finished.

        Updates the EMA with observed peak concurrent bytes. Reduces
        allocated_bytes to reflect the task's current concurrent_bytes
        (unconsumed output still in-flight). The task state is kept until
        all outputs are consumed.

        Args:
            task_id: The finished task.
        """
        task = self.task_states.get(task_id)
        if task is None:
            return

        op_state = self.op_states[task.op]

        # Update the EMA for this operator's expected peak ratio.
        actual_ratio = task.peak_concurrent_bytes / task.input_bytes
        op_state.expected_peak_ratio = (
            self.ema_alpha * actual_ratio
            + (1 - self.ema_alpha) * op_state.expected_peak_ratio
        )

        # Reduce allocation from peak-based charge down to current concurrent bytes.
        # The task was charged max(expected_peak, actual_peak) while running;
        # now it only needs concurrent_bytes in the budget.
        charged = max(task.expected_peak_bytes, task.peak_concurrent_bytes)
        self.allocated_bytes -= charged - task.concurrent_bytes

        # Release CPU.
        self.available_cpus += task.num_cpus

        # Mark as finished. Clean up if fully consumed.
        task.finished = True
        if task.concurrent_bytes == 0:
            del self.task_states[task_id]

    def is_backpressured(self, op: Operator) -> bool:
        """Check if all running tasks for an operator are backpressured."""
        op_tasks = [t for t in self.task_states.values() if t.op is op]
        if not op_tasks:
            return False
        return all(t.backpressured for t in op_tasks)

    def get_backpressured_tasks(self, op: Operator) -> List[int]:
        """Return task_ids of backpressured tasks for an operator."""
        return [
            tid for tid, t in self.task_states.items() if t.op is op and t.backpressured
        ]
