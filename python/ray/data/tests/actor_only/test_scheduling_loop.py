"""Unit tests for the actor-only scheduling loop's wiring: executor selection
and shutdown ordering."""
import sys
import threading
from typing import Any, Callable, List, Optional, Set
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
)
from ray.data._internal.execution.resource_bank import ResourceBankBase
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor import (
    StreamingExecutor,
    get_streaming_executor_cls,
)
from ray.data._internal.experimental.execution.streaming_executor import (
    ExperimentalStreamingExecutor,
)
from ray.data.context import DataContext


def test_executor_is_selected_per_call_from_the_flag(monkeypatch):
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "1")
    assert get_streaming_executor_cls() is ExperimentalStreamingExecutor
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "0")
    assert get_streaming_executor_cls() is StreamingExecutor


def test_executor_selection_honors_dataset_override(monkeypatch):
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "1")
    ctx = DataContext.get_current().copy()
    ctx._actor_only_backend_override = False
    assert get_streaming_executor_cls(ctx) is StreamingExecutor
    assert get_streaming_executor_cls() is ExperimentalStreamingExecutor


def test_shutdown_flushes_metrics_after_the_loop_stops():
    """The final metrics flush must not race the scheduling loop: it runs after
    the base shutdown (which stops the loop) and before the bank is cleared."""
    calls = []
    metrics = MagicMock()
    metrics.maybe_update.side_effect = lambda **kwargs: calls.append("flush")
    bank = MagicMock()
    bank.finalize_dataset.side_effect = lambda **kwargs: calls.append("finalize")
    executor = _bare_executor(
        base_shutdown=lambda force, exception=None: calls.append("base_shutdown"),
        resource_bank=bank,
        metrics=metrics,
    )

    executor.shutdown(force=False)

    assert calls == ["base_shutdown", "flush", "finalize"]


def test_progress_resources_use_logical_usage_under_actor_only():
    """The actor-only loop never calls ``ResourceManager.update_usages``, so the
    progress bar's per-op ``Resources:`` comes from the operator itself."""
    ctx = DataContext.get_current().copy()
    ctx._actor_only_backend_override = True
    op = MagicMock()
    op.data_context = ctx
    op.running_logical_usage.return_value = ExecutionResources(cpu=3, gpu=1)
    resource_manager = MagicMock(spec=ResourceManager)

    usage_str = ResourceManager.get_op_usage_str(resource_manager, op, verbose=True)

    assert usage_str == "3.0 CPU, 1.0 GPU"


class _StubBaseExecutor(StreamingExecutor):

    base_shutdown: Callable[..., None]

    def shutdown(self, force: bool, exception: Optional[Exception] = None):
        self.base_shutdown(force, exception)


class _ExecutorUnderTest(ExperimentalStreamingExecutor, _StubBaseExecutor):
    pass


def _bare_executor(
    *,
    base_shutdown: Callable[..., None],
    resource_bank: ResourceBankBase,
    metrics: Any,
) -> ExperimentalStreamingExecutor:
    executor = _ExecutorUnderTest.__new__(_ExecutorUnderTest)
    executor.base_shutdown = base_shutdown
    executor._resource_bank = resource_bank
    executor._actor_only_metrics = metrics
    executor._topology = {}
    executor._execution_started = True
    executor._shutdown = False
    executor._shutdown_lock = threading.RLock()
    return executor


class _FakeBank(ResourceBankBase):
    """Records each finalize, so a test can tell whether a flush read a bank
    that another thread had already cleared."""

    def __init__(self):
        self.num_finalizes = 0

    def finalize_dataset(self, *, ops: Set[PhysicalOperator]) -> None:
        self.num_finalizes += 1


class _FakeMetrics:
    """Records, for each flush, how many finalizes had run before it."""

    def __init__(self, bank: _FakeBank):
        self._bank = bank
        self.finalizes_seen_by_flush: List[int] = []

    def maybe_update(self, *, topology, resource_bank, force: bool) -> None:
        self.finalizes_seen_by_flush.append(self._bank.num_finalizes)


def test_concurrent_shutdowns_flush_and_finalize_exactly_once():
    """Every split consumer of a streaming_split calls shutdown, so several
    threads can be in it at once. Only one may run the final flush + finalize;
    if two do, one thread's finalize clears the bank while the other is still
    flushing, and the flush reads a node with live outputs but no cumulative
    totals (a negative "freed")."""
    num_threads = 4
    # Hold every caller inside the base shutdown until all have arrived, so
    # each decides whether to finalize before any of them marks the executor
    # shut down.
    all_inside_base = threading.Barrier(num_threads, timeout=10)
    base_lock = threading.Lock()

    def base_shutdown(force, exception=None):
        if executor._shutdown:
            return
        all_inside_base.wait()
        with base_lock:
            executor._shutdown = True

    bank = _FakeBank()
    metrics = _FakeMetrics(bank)
    executor = _bare_executor(
        base_shutdown=base_shutdown, resource_bank=bank, metrics=metrics
    )

    errors: List[Optional[BaseException]] = []

    def call_shutdown():
        try:
            executor.shutdown(force=False)
        except BaseException as e:  # surfaced below
            errors.append(e)

    threads = [threading.Thread(target=call_shutdown) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors
    assert bank.num_finalizes == 1
    # The single flush ran before the bank was cleared.
    assert metrics.finalizes_seen_by_flush == [0]


def test_escape_hatch_sees_consumption_queued_this_step(monkeypatch):
    """Processing completed tasks can drop withheld outputs, which only queues
    their release. The hatch applies that queue first, so it never mistakes
    released outputs for a withheld deadlock."""
    from ray.data._internal.experimental.execution import (
        streaming_executor as executor_mod,
    )

    calls: List[str] = []
    bank = MagicMock()
    bank.drain_consumed_blocks.side_effect = lambda: calls.append("drain")
    monkeypatch.setattr(
        executor_mod,
        "relieve_output_deadlock",
        lambda **kwargs: calls.append("hatch"),
    )
    executor = ExperimentalStreamingExecutor.__new__(ExperimentalStreamingExecutor)
    executor._last_detection_timestamp = 0.0
    executor._idle_detection_interval_s = 0.0
    executor._output_node = (MagicMock(), MagicMock())
    executor._resource_bank = bank
    executor._topology = {}

    executor._maybe_run_escape_hatch()

    assert calls == ["drain", "hatch"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
