import time
from typing import Callable

from ray.data._internal.execution.streaming_executor_state import (
    Topology,
)
from ray.data.exceptions import ExecutionTimeoutError


class NoProgressGuard:
    def __init__(
        self,
        topology: Topology,
        timeout_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ):
        if timeout_s == 0:
            raise ValueError(
                "execution_no_progress_timeout_s must be positive, or negative "
                "to disable the timeout. Zero would fail every execution as "
                f"soon as it started. Got: {timeout_s}"
            )

        self._topology = topology
        self._timeout_s = timeout_s
        self._clock = clock

        self._last_check_time = clock()
        self._last_progress_states = self._total_progress_states()
        self._stalled_s = 0.0

    @property
    def enabled(self) -> bool:
        return self._timeout_s > 0

    def check(self) -> None:
        if not self.enabled:
            return

        current_time = self._clock()
        current_progress_states = self._total_progress_states()
        interval = current_time - self._last_check_time
        self._last_check_time = current_time

        execution_made_progress = current_progress_states != self._last_progress_states
        if execution_made_progress:
            self._reset_stall(current_progress_states)
            return

        self._stalled_s += interval
        if self._stalled_s >= self._timeout_s:
            raise ExecutionTimeoutError(self._error_message())

    def _reset_stall(self, current_progress_states: tuple[int, int, int]) -> None:
        self._last_progress_states = current_progress_states
        self._stalled_s = 0.0

    def _total_progress_states(self) -> tuple[int, int, int]:
        outputs_taken = 0
        enqueued_input_blocks = 0
        enqueued_output_blocks = 0

        for op, state in self._topology.items():
            outputs_taken += op.metrics.num_outputs_taken
            enqueued_input_blocks += state.total_enqueued_input_blocks()
            enqueued_output_blocks += state.total_enqueued_output_blocks()
        return (outputs_taken, enqueued_input_blocks, enqueued_output_blocks)

    def _error_message(self) -> str:
        lines = [
            f"Dataset execution made no progress for at least "
            f"{self._stalled_s:.0f}s of scheduling time (timeout: "
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
            "If your UDF is legitimately this slow, raise the timeout for this "
            "Dataset with `ds.context.execution_no_progress_timeout_s = <seconds>`, "
            "or set it to -1 to disable. To change the default, set "
            "`DataContext.get_current().execution_no_progress_timeout_s` before "
            "creating a Dataset, or set "
            "RAY_DATA_EXECUTION_NO_PROGRESS_TIMEOUT_S before starting the process."
        )
        return "\n".join(lines)
