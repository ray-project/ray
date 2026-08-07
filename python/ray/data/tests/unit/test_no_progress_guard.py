from types import SimpleNamespace

import pytest

from ray.data._internal.execution.no_progress_guard import NoProgressGuard
from ray.data.exceptions import ExecutionTimeoutError


class _FakeOperator:
    """Minimal stand-in for the operator surface the guard reads."""

    def __init__(
        self,
        name: str,
        num_outputs_taken: int = 0,
        num_tasks_finished: int = 0,
        num_active_tasks: int = 0,
        completed: bool = False,
        extra_metrics: dict = None,
    ):
        self.name = name
        self.metrics = SimpleNamespace(
            num_outputs_taken=num_outputs_taken,
            num_tasks_finished=num_tasks_finished,
            extra_metrics=extra_metrics or {},
        )
        self._num_active_tasks = num_active_tasks
        self._completed = completed

    def has_completed(self) -> bool:
        return self._completed

    def num_active_tasks(self) -> int:
        return self._num_active_tasks


class _FakeOpState:
    """Minimal stand-in for the `OpState` surface the guard reads."""

    def __init__(self, input_blocks: int = 0, output_blocks: int = 0):
        self.input_blocks = input_blocks
        self.output_blocks = output_blocks

    def total_enqueued_input_blocks(self) -> int:
        return self.input_blocks

    def total_enqueued_output_blocks(self) -> int:
        return self.output_blocks


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make_guard(ops, timeout_s=100.0):
    clock = _FakeClock()
    topology = {op: _FakeOpState() for op in ops}
    return NoProgressGuard(topology, timeout_s, clock=clock), clock


def test_raises_once_timeout_elapses_without_progress():
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=100.0)

    clock.advance(99.0)
    guard.check()

    clock.advance(1.0)
    with pytest.raises(ExecutionTimeoutError):
        guard.check()


def test_progress_resets_the_clock():
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=100.0)

    for _ in range(5):
        clock.advance(99.0)
        op.metrics.num_outputs_taken += 1
        guard.check()

    # 495s elapsed overall without ever tripping the 100s timeout, because each
    # output restarted the stall clock. The clock restarted at the last check,
    # so a full `timeout_s` has to pass from there to trip it.
    clock.advance(100.0)
    with pytest.raises(ExecutionTimeoutError):
        guard.check()


def test_progress_by_any_operator_counts():
    stalled = _FakeOperator("Sort")
    moving = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([stalled, moving], timeout_s=100.0)

    for _ in range(5):
        clock.advance(99.0)
        moving.metrics.num_outputs_taken += 1
        guard.check()


def test_queue_movement_counts_as_progress():
    op = _FakeOperator("MapBatches(embed)")
    state = _FakeOpState()
    clock = _FakeClock()
    guard = NoProgressGuard({op: state}, 100.0, clock=clock)

    for _ in range(5):
        clock.advance(99.0)
        state.input_blocks += 1
        guard.check()

    for _ in range(5):
        clock.advance(99.0)
        state.output_blocks += 1
        guard.check()

    clock.advance(100.0)
    with pytest.raises(ExecutionTimeoutError):
        guard.check()


def test_zero_timeout_is_rejected():
    """Zero would fail every execution on its first check, so reject it."""
    with pytest.raises(ValueError, match="must be positive"):
        NoProgressGuard({}, 0)


@pytest.mark.parametrize("timeout_s", [-1, -1800])
def test_disabled(timeout_s):
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=timeout_s)

    assert not guard.enabled
    clock.advance(10**6)
    guard.check()


def test_error_message_names_stalled_operators():
    stalled = _FakeOperator(
        "MapBatches(embed)",
        num_outputs_taken=118,
        num_tasks_finished=118,
        num_active_tasks=4,
    )
    completed = _FakeOperator("Sort", completed=True)
    guard, clock = _make_guard([stalled, completed], timeout_s=1800.0)

    clock.advance(1000.0)
    guard.check()

    clock.advance(832.0)
    with pytest.raises(ExecutionTimeoutError) as exc_info:
        guard.check()

    message = str(exc_info.value)
    assert (
        "made no progress for at least 1832s of scheduling time "
        "(timeout: 1800s)" in message
    )
    assert (
        "MapBatches(embed): 4 active task(s), 118 finished, 118 outputs taken"
        in message
    )
    # Completed operators aren't stalled, so they aren't reported.
    assert "Sort" not in message
    assert "execution_no_progress_timeout_s" in message


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
