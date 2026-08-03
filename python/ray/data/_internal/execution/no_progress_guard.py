import time
from typing import Callable

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

    Progress is the sum of ``num_outputs_taken`` across the topology, which
    ticks whenever data moves anywhere in the pipeline.

    Hash-based operators keep their finalize phase in a separate metrics object
    exposed through ``extra_metrics``, so that phase is counted too. Even then
    it only reports once a whole partition lands, and the gap in between grows
    with the data, so the clock also pauses while one of those phases has a
    task in flight. That gives up detecting a hang inside a finalize, which no
    counter here could distinguish from work anyway.

    Progress also freezes when the consumer is the bottleneck, since a slow
    loop between iterations backpressures every operator upstream. Failing
    those executions would be worse than the hang this guards against, so the
    stall clock advances only while the pipeline is the one holding things up.

    The clock measures time the scheduling loop spent spinning rather than
    wall-clock time, so an operator that blocks the loop for a long stretch of
    real work doesn't count against the timeout. Two consequences: where a step
    itself outlasts ``max_stall_interval_s``, the timeout fires late by roughly
    ``step_duration / max_stall_interval_s``; and a loop wedged inside a
    blocking call never reaches a check, so it never fires at all.

    Args:
        topology: The topology being executed, read for progress counters.
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
        topology: Topology,
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

        self._topology = topology
        self._timeout_s = timeout_s
        self._clock = clock
        self._max_stall_interval_s = max_stall_interval_s

        self._last_check_time = clock()
        self._last_progress_count = self._total_progress_count()
        self._stalled_s = 0.0

    @property
    def enabled(self) -> bool:
        return self._timeout_s > 0

    def check(self, consumer_ready: bool) -> None:
        """Record progress since the last call, and fail if there was none.

        Args:
            consumer_ready: Whether a consumer is blocked on an empty output
                queue. When False the caller is the bottleneck, so the stall
                clock resets.
        """
        if not self.enabled:
            return

        current_time = self._clock()
        current_progress_count = self._total_progress_count()

        if self._last_check_time is None:
            self._last_check_time = current_time
            self._last_progress_count = current_progress_count
            return

        interval = current_time - self._last_check_time
        self._last_check_time = current_time

        new_progress = current_progress_count > self._last_progress_count
        # Every one of these resets the clock. Resetting only on progress would
        # let elapsed time accumulate while something else held the pipeline
        # back, then fail the moment it caught up.
        if new_progress or not consumer_ready or self._barrier_work_in_flight():
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
            raise ExecutionTimeoutError(self._error_message())

    def _barrier_work_in_flight(self) -> bool:
        """Whether a phase reported under ``extra_metrics`` has a task running.

        Those phases only report once a whole partition completes, so between
        two reports they're indistinguishable from a stall on the counters
        alone.
        """
        return any(
            extra.get("num_tasks_running", 0) > 0
            for op in self._topology
            for extra in op.metrics.extra_metrics.values()
            if isinstance(extra, dict)
        )

    def _total_progress_count(self) -> int:
        total = 0
        for op in self._topology:
            metrics = op.metrics
            total += metrics.num_outputs_taken
            # A hash shuffle, join or aggregate reports its finalize phase
            # under `extra_metrics`; `metrics` itself only covers the shuffle
            # phase, and would look frozen for as long as finalizing runs.
            for extra in metrics.extra_metrics.values():
                if isinstance(extra, dict) and "num_outputs_taken" in extra:
                    total += extra["num_outputs_taken"]
        return total

    def _error_message(self) -> str:
        lines = [
            f"Dataset execution made no progress for at least "
            f"{self._stalled_s:.0f}s of scheduling time (timeout: "
            f"{self._timeout_s:.0f}s). No operator produced or consumed an output "
            f"in that window."
        ]

        stalled = [op for op in self._topology if not op.has_completed()]
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
