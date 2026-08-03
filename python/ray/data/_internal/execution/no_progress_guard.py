import time
from typing import Callable, Optional

from ray.data._internal.execution.streaming_executor_state import (
    WAIT_FOR_TASK_COMPLETION_TIMEOUT_S,
    Topology,
)
from ray.data.exceptions import ExecutionTimeoutError

# Most a single interval between checks can add to the stall clock. A spinning
# scheduling loop turns over every `WAIT_FOR_TASK_COMPLETION_TIMEOUT_S`, so an
# interval an order of magnitude past that came from the loop blocking on work
# inside one step rather than from spinning. Erring low only makes the guard
# slower to fire, so the multiple is deliberately close to a normal step.
DEFAULT_MAX_STALL_INTERVAL_S = 10 * WAIT_FOR_TASK_COMPLETION_TIMEOUT_S


class NoProgressGuard:
    """Fails an execution that stops making progress.

    Progress is ``num_outputs_taken`` plus successfully finished tasks, summed
    across the topology, so it ticks whenever data moves anywhere in the
    pipeline or any task completes. Outputs alone aren't enough: a barrier
    operator can spend a long stretch finishing tasks before it emits anything,
    and that is work, not a stall. Failed tasks don't count, so a job stuck
    retrying the same failure is still caught. A hung UDF or an unschedulable
    cluster moves neither counter.

    Progress also freezes when the consumer is the bottleneck, since a slow
    loop between iterations backpressures every operator upstream. Failing
    those executions would be worse than the hang this guards against, so the
    stall clock advances only while the consumer is idling.

    The clock measures time the scheduling loop spent spinning rather than
    wall-clock time, so an operator that blocks the loop for a long stretch of
    real work doesn't count against the timeout.

    Args:
        timeout_s: Seconds without progress before failing. Negative disables
            the guard.
        clock: Monotonic time source, injectable for testing.
        max_stall_interval_s: Most a single interval between checks can add to
            the stall clock.

    Raises:
        ValueError: If ``timeout_s`` is zero, which would fail every execution
            on its first check.
    """

    def __init__(
        self,
        timeout_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_stall_interval_s: float = DEFAULT_MAX_STALL_INTERVAL_S,
    ):
        if timeout_s == 0:
            raise ValueError(
                "execution_no_progress_timeout_s must be positive, or negative "
                "to disable the timeout. Zero would fail every execution as "
                "soon as it started."
            )

        self._timeout_s = timeout_s
        self._clock = clock
        self._max_stall_interval_s = max_stall_interval_s

        # Set on the first check, so the clock starts with the scheduling loop
        # rather than with whatever setup happens before it.
        self._last_check_time: Optional[float] = None
        self._last_progress_count = 0
        self._stalled_s = 0.0

    @property
    def enabled(self) -> bool:
        return self._timeout_s > 0

    def check(self, topology: Topology, consumer_idling: bool) -> None:
        """Record progress since the last call, and fail if there was none.

        Args:
            topology: The topology being executed, read for progress counters.
            consumer_idling: Whether the executor's output queue is empty. When
                False the caller is the bottleneck, so the stall clock resets.
        """
        if not self.enabled:
            return

        current_time = self._clock()
        current_progress_count = self._total_progress_count(topology)

        if self._last_check_time is None:
            self._last_check_time = current_time
            self._last_progress_count = current_progress_count
            return

        interval = current_time - self._last_check_time
        self._last_check_time = current_time

        new_progress = current_progress_count > self._last_progress_count
        # Both conditions must reset the clock. Resetting only on progress
        # would let elapsed time accumulate while a slow consumer holds the
        # pipeline back, then fail the moment it catches up.
        if new_progress or not consumer_idling:
            self._last_progress_count = current_progress_count
            self._stalled_s = 0.0
            return

        # A stalled loop keeps spinning at the `ray.wait` timeout, so its stall
        # builds out of many short intervals. A single long interval means the
        # loop blocked running work inside one step instead, which the progress
        # counters only reflect on the step after -- an all-to-all `bulk_fn`
        # runs a whole sort that way. Capping rather than skipping keeps every
        # interval counting for something.
        self._stalled_s += min(interval, self._max_stall_interval_s)
        if self._stalled_s >= self._timeout_s:
            raise ExecutionTimeoutError(self._error_message(topology))

    def _total_progress_count(self, topology: Topology) -> int:
        return sum(
            op.metrics.num_outputs_taken
            + op.metrics.num_tasks_finished
            - op.metrics.num_tasks_failed
            for op in topology
        )

    def _error_message(self, topology: Topology) -> str:
        lines = [
            f"Dataset execution made no progress for at least "
            f"{self._stalled_s:.0f}s of scheduling time (timeout: "
            f"{self._timeout_s:.0f}s). No operator finished a task or produced "
            f"an output in that window."
        ]

        stalled = [op for op in topology if not op.has_completed()]
        if stalled:
            lines.append("Operators still running:")
            for op in stalled:
                metrics = op.metrics
                lines.append(
                    f"  - {op.name}: {op.num_active_tasks()} active task(s), "
                    f"{metrics.num_tasks_finished} finished, "
                    f"{metrics.num_outputs_taken} outputs taken"
                )

        lines.append(
            "If your UDF is legitimately this slow, raise the timeout for this "
            "Dataset with `ds.context.execution_no_progress_timeout_s = <seconds>`, "
            "or set it to -1 to disable. To change the default, set "
            "`DataContext.get_current().execution_no_progress_timeout_s` before "
            "creating a Dataset, or set "
            "RAY_DATA_EXECUTION_NO_PROGRESS_TIMEOUT_S before starting the process."
        )
        return "\n".join(lines)
