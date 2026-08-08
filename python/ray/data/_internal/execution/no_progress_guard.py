import time
from typing import Callable

from ray.data._internal.execution.streaming_executor_state import (
    Topology,
)
from ray.data.exceptions import ExecutionTimeoutError


class NoProgressGuard:
    """
    Raises an ExecutionTimeoutError when no operator makes progress for
    DataContext.execution_no_progress_timeout_s.

    Progress is defined as either an output was taken from an operator, or
    blocks entered or left an operator's queues.

    The clock measures time since the last progress, and
    progress is checked at the end of each scheduling loop step.

    A value of -1 disables the guard.
    """

    def __init__(
        self,
        topology: Topology,
        timeout_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ):
        if timeout_s == 0:
            raise ValueError(
                "execution_no_progress_timeout_s must be positive, or -1 to "
                "disable the timeout. Zero would fail every execution as "
                f"soon as it started. Got: {timeout_s}"
            )

        self._topology = topology
        self._timeout_s = timeout_s
        self._clock = clock

        self._last_progress_time = clock()
        self._last_progress_states = self._total_progress_states()

    @property
    def enabled(self) -> bool:
        return self._timeout_s > 0

    def check(self) -> None:
        if not self.enabled:
            return

        current_time = self._clock()
        current_progress_states = self._total_progress_states()

        execution_made_progress = current_progress_states != self._last_progress_states
        if execution_made_progress:
            self._last_progress_states = current_progress_states
            self._last_progress_time = current_time
            return

        if current_time - self._last_progress_time >= self._timeout_s:
            raise ExecutionTimeoutError(self._error_message())

    def _total_progress_states(self) -> tuple[int, int, int]:
        outputs_taken = 0
        enqueued_input_blocks = 0
        enqueued_output_blocks = 0

        # Since these counts can cancel each other out when summed,
        # track them individually.
        for op, state in self._topology.items():
            outputs_taken += op.metrics.num_outputs_taken
            enqueued_input_blocks += state.total_enqueued_input_blocks()
            enqueued_output_blocks += state.total_enqueued_output_blocks()
        return (outputs_taken, enqueued_input_blocks, enqueued_output_blocks)

    def _error_message(self) -> str:
        stalled_s = self._clock() - self._last_progress_time
        lines = [
            f"Dataset execution made no progress for at least "
            f"{stalled_s:.0f}s of scheduling time (timeout: "
            f"{self._timeout_s:.0f}s). No output was taken and no blocks moved "
            f"through any operator's queues in that window."
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
            "If this is expected, for example a UDF that is legitimately this "
            "slow or a long wait for cluster capacity, raise the timeout for this "
            "Dataset with `ds.context.execution_no_progress_timeout_s = <seconds>`, "
            "or set it to -1 to disable. To change the default, set "
            "`DataContext.get_current().execution_no_progress_timeout_s` before "
            "creating a Dataset, or set "
            "RAY_DATA_EXECUTION_NO_PROGRESS_TIMEOUT_S before starting the process."
        )
        return "\n".join(lines)
