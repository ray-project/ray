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
    ):
        self.name = name
        self.metrics = SimpleNamespace(
            num_outputs_taken=num_outputs_taken,
            num_tasks_finished=num_tasks_finished,
        )
        self._num_active_tasks = num_active_tasks
        self._completed = completed

    def has_completed(self) -> bool:
        return self._completed

    def num_active_tasks(self) -> int:
        return self._num_active_tasks


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make_guard(ops, timeout_s=100.0, max_stall_interval_s=float("inf")):
    clock = _FakeClock()
    # `Topology` is a dict keyed by operator; the guard only reads the keys.
    topology = {op: None for op in ops}
    guard = NoProgressGuard(
        topology,
        timeout_s,
        clock=clock,
        # Most tests advance the clock in single large jumps, which stands in
        # for many fast scheduling steps. `test_blocking_step_is_not_a_stall`
        # covers the real threshold.
        max_stall_interval_s=max_stall_interval_s,
    )
    return guard, clock


def test_raises_once_timeout_elapses_without_progress():
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=100.0)

    clock.advance(99.0)
    guard.check(consumer_idling=True)

    clock.advance(1.0)
    with pytest.raises(ExecutionTimeoutError):
        guard.check(consumer_idling=True)


def test_progress_resets_the_clock():
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=100.0)

    for _ in range(5):
        clock.advance(99.0)
        op.metrics.num_outputs_taken += 1
        guard.check(consumer_idling=True)

    # 495s elapsed overall without ever tripping the 100s timeout, because each
    # output restarted the stall clock. The clock restarted at the last check,
    # so a full `timeout_s` has to pass from there to trip it.
    clock.advance(100.0)
    with pytest.raises(ExecutionTimeoutError):
        guard.check(consumer_idling=True)


def test_progress_by_any_operator_counts():
    stalled = _FakeOperator("Sort")
    moving = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([stalled, moving], timeout_s=100.0)

    for _ in range(5):
        clock.advance(99.0)
        moving.metrics.num_outputs_taken += 1
        guard.check(consumer_idling=True)


def test_busy_consumer_never_times_out():
    """A slow consumer freezes every counter, but must not fail the execution."""
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=100.0)

    for _ in range(100):
        clock.advance(1000.0)
        guard.check(consumer_idling=False)


def test_clock_resumes_after_consumer_catches_up():
    """Time spent waiting on a busy consumer must not carry over.

    Regression test: if only progress reset the clock, elapsed time would
    accumulate while the consumer held the pipeline back, and the execution
    would fail the moment the consumer caught up.
    """
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=100.0)

    clock.advance(99.0)
    guard.check(consumer_idling=True)

    # Consumer is busy for far longer than the timeout.
    clock.advance(1000.0)
    guard.check(consumer_idling=False)

    # It catches up: the clock restarted, so this is not yet a stall.
    clock.advance(99.0)
    guard.check(consumer_idling=True)

    clock.advance(100.0)
    with pytest.raises(ExecutionTimeoutError):
        guard.check(consumer_idling=True)


def test_finishing_tasks_counts_as_progress():
    """A barrier operator produces no output for a while, but isn't stalled.

    Regression test: a shuffle keeps finishing tasks all through its reduce
    phase and only emits once it's done, so counting outputs alone failed
    shuffles that were still working.
    """
    op = _FakeOperator("Sort")
    guard, clock = _make_guard([op], timeout_s=100.0)

    for _ in range(5):
        clock.advance(99.0)
        op.metrics.num_tasks_finished += 1
        guard.check(consumer_idling=True)


def test_blocking_step_is_not_a_stall():
    """A scheduling step that blocks on real work must not count as a stall.

    Regression test: an all-to-all operator runs its whole `bulk_fn`
    synchronously inside one step, and its outputs are only taken on the step
    after. Counting that gap failed sorts that had actually succeeded.
    """
    op = _FakeOperator("Sort")
    guard, clock = _make_guard([op], timeout_s=100.0, max_stall_interval_s=1.0)

    # `bulk_fn` blocks the loop well past the timeout, then returns.
    clock.advance(1000.0)
    guard.check(consumer_idling=True)

    # The next step drains its output, which is the progress the guard wanted.
    clock.advance(0.1)
    op.metrics.num_outputs_taken += 1
    guard.check(consumer_idling=True)


def test_stall_accumulates_across_steps():
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=1.0, max_stall_interval_s=0.25)

    # A stalled loop keeps spinning at the `ray.wait` timeout, so the stall
    # builds up across many short steps rather than one long gap.
    for _ in range(3):
        clock.advance(0.25)
        guard.check(consumer_idling=True)

    clock.advance(0.25)
    with pytest.raises(ExecutionTimeoutError):
        guard.check(consumer_idling=True)


@pytest.mark.parametrize("timeout_s", [-1, -1800])
def test_disabled(timeout_s):
    op = _FakeOperator("MapBatches(embed)")
    guard, clock = _make_guard([op], timeout_s=timeout_s)

    assert not guard.enabled
    clock.advance(10**6)
    guard.check(consumer_idling=True)


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
    guard.check(consumer_idling=True)

    clock.advance(832.0)
    with pytest.raises(ExecutionTimeoutError) as exc_info:
        guard.check(consumer_idling=True)

    message = str(exc_info.value)
    assert "made no progress for 1832s (timeout: 1800s)" in message
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
