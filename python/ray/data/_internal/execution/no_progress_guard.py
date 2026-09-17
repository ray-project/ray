import time
from dataclasses import dataclass
from functools import cached_property
from typing import Callable, List

from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
)
from ray.data._internal.execution.streaming_executor_state import (
    Topology,
)
from ray.data.exceptions import ExecutionTimeoutError


@dataclass(frozen=True)
class OperatorState:
    """The state the guard checks against to identify hangs.

    Attributes:
        num_outputs_taken: The cumulative number of outputs taken.
        total_enqueued_input_blocks: The current number of inputs queued.
        total_enqueued_output_blocks: The current number of outputs queued.
    """

    num_outputs_taken: int
    total_enqueued_input_blocks: int
    total_enqueued_output_blocks: int


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
        self._last_progress_states = self._current_progress_states()

    @cached_property
    def enabled(self) -> bool:
        # AllToAllOperator and HashShufflingOperatorBase require special-cased logic to
        # implement a no-progress check. Since we're deprecating both of them in favor
        # of the V2 hash shuffle implementation around Ray 2.60, we don't bother
        # supporting them.
        if any(
            isinstance(op, (AllToAllOperator, HashShufflingOperatorBase))
            for op in self._topology
        ):
            return False

        return self._timeout_s > 0

    def check(self) -> None:
        if not self.enabled:
            return

        current_time = self._clock()
        current_progress_states = self._current_progress_states()

        execution_made_progress = current_progress_states != self._last_progress_states
        if execution_made_progress:
            self._last_progress_states = current_progress_states
            self._last_progress_time = current_time
            return

        if current_time - self._last_progress_time >= self._timeout_s:
            raise ExecutionTimeoutError(self._error_message())

    def _current_progress_states(self) -> List[OperatorState]:
        """Return the state of each operator."""
        return [
            OperatorState(
                num_outputs_taken=op.metrics.num_outputs_taken,
                total_enqueued_input_blocks=state.total_enqueued_input_blocks(),
                total_enqueued_output_blocks=state.total_enqueued_output_blocks(),
            )
            for op, state in self._topology.items()
        ]

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
