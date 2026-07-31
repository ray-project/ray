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

        self._error_message = (
            f"Execution has made no progress for {self._timeout_s} seconds. "
            "This is likely due to a deadlock or other bug in Ray Data or Ray Core."
            "In case of slow consumers, consider increasing the timeout via `DataContext.execution_no_progress_timeout_s`."
        )
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
