import time
from typing import Callable

from ray.data._internal.execution.streaming_executor_state import Topology
from ray.data.exceptions import ExecutionTimeoutError


class NoProgressGuard:
    def __init__(
        self,
        topology: Topology,
        timeout_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._topology = topology
        self._timeout_s = timeout_s
        self._clock = clock

        self._last_progress_time = clock()
        self._last_outputs_taken = self._total_outputs_taken()
        return

    def check(self, consumer_idling: bool) -> None:
        if self._timeout_s < 0:
            return

        current_time = self._clock()
        current_outputs_taken = self._total_outputs_taken()

        new_progress = current_outputs_taken > self._last_outputs_taken
        time_elapsed = current_time - self._last_progress_time
        if new_progress or not consumer_idling:
            self._last_progress_time = current_time
            self._last_outputs_taken = current_outputs_taken
            return

        if time_elapsed > self._timeout_s:
            raise ExecutionTimeoutError(self._error_message)

    def _total_outputs_taken(self) -> int:
        return sum(op.metrics.num_outputs_taken for op in self._topology)

    def _error_message(self, elapsed_s: float) -> str:
        lines = [
            f"Dataset execution made no progress for {elapsed_s:.0f}s "
            f"(timeout: {self._timeout_s:.0f}s). No operator produced or "
            f"consumed an output in that window."
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
