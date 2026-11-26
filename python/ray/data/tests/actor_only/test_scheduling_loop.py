"""Unit tests for the actor-only scheduling loop's wiring: executor selection
and shutdown ordering."""
import sys
import threading
from types import SimpleNamespace
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
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
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


def _executor_with(**private_fields: Any) -> ExperimentalStreamingExecutor:
    """An executor built without ``__init__``, with only the given private
    fields and methods set. Name each without its leading underscore."""
    executor = ExperimentalStreamingExecutor.__new__(ExperimentalStreamingExecutor)
    for name, value in private_fields.items():
        setattr(executor, f"_{name}", value)
    return executor


class _Recorder:
    """A stand-in that defines only the methods it is given. Each call
    appends the method's name to ``calls`` and returns the given value; any
    other attribute raises ``AttributeError``."""

    def __init__(self, calls: List[str], **returns: Any):
        self._calls = calls
        self._returns = returns

    def __getattr__(self, name: str) -> Callable[..., Any]:
        if name.startswith("_") or name not in self._returns:
            raise AttributeError(name)

        def method(*args: Any, **kwargs: Any) -> Any:
            self._calls.append(name)
            return self._returns[name]

        return method


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
    monkeypatch.setattr(
        executor_mod,
        "relieve_output_deadlock",
        lambda **kwargs: calls.append("relieve_output_deadlock"),
    )
    executor = _executor_with(
        last_detection_timestamp=0.0,
        idle_detection_interval_s=0.0,
        output_node=(object(), object()),
        resource_bank=_Recorder(calls, drain_consumed_blocks=None),
        topology={},
    )

    executor._maybe_run_escape_hatch()

    assert calls == ["drain_consumed_blocks", "relieve_output_deadlock"]


def test_bottleneck_observer_samples_after_tasks_launch():
    """The observer reads slots at one instant. Sampled before the launch,
    every slot a task freed this step looks idle though the launch refills
    it, which made producers look self-limited. It samples after."""
    calls: List[str] = []
    others: List[str] = []
    executor = _executor_with(
        resource_reporter=_Recorder(
            others,
            get_cluster_view=SimpleNamespace(reserved_by_node={}),
            seconds_since_allocation_change=0.0,
        ),
        resource_bank=_Recorder(others, set_node_view=None, drain_consumed_blocks=None),
        actor_only_metrics=_Recorder(others, maybe_update=None),
        cluster_autoscaler=_Recorder(others, try_trigger_scaling=None),
        sizer=_Recorder(
            calls, maybe_build_cluster_state_snapshot=None, observe_after_launch=None
        ),
        metadata_fetcher=_Recorder(others, emit_ready=[], fire_done_callbacks=[]),
        lineage_tracker=None,
        max_errored_blocks=0,
        num_errored_blocks=0,
        topology={},
        # Not due: the escape hatch returns at once.
        last_detection_timestamp=float("inf"),
        idle_detection_interval_s=0.0,
        launch_tasks=lambda: calls.append("launch"),
        report_current_usage=lambda: None,
        finalize_scheduling_loop_step=lambda topology: True,
    )

    executor._scheduling_loop_step({})

    assert calls.index("launch") < calls.index("observe_after_launch")


class _PlacementFailedOp(ExperimentalActorPoolMapOperator):
    """An actor-pool op whose last actor placement may have been rejected.
    Skips the operator's ``__init__``."""

    def __init__(self, placement_failed: bool):
        self.actor_placement_failed = placement_failed


class _TwoViewReporter:
    """Serves a stale cluster view, and a fresh one when asked to
    recompute."""

    def __init__(self):
        self.stale = SimpleNamespace(reserved_by_node={"stale": None})
        self.fresh = SimpleNamespace(reserved_by_node={"fresh": None})

    def get_cluster_view(self, recompute: bool = False) -> Any:
        return self.fresh if recompute else self.stale

    def seconds_since_allocation_change(self) -> float:
        return 0.0


class _ViewSink:
    """Records each node view the bank receives and each cluster view the
    sizer builds its snapshot from."""

    def __init__(self):
        self.node_views: List[Any] = []
        self.cluster_views: List[Any] = []

    def set_node_view(self, *, node_view: Any) -> None:
        self.node_views.append(node_view)

    def drain_consumed_blocks(self) -> None:
        pass

    def maybe_build_cluster_state_snapshot(self, *, cluster_view: Any, **kwargs):
        self.cluster_views.append(cluster_view)
        return None

    def observe_after_launch(self) -> None:
        pass


@pytest.mark.parametrize(
    "placement_failed, expected_view",
    [
        pytest.param(False, "stale", id="placed"),
        pytest.param(True, "fresh", id="placement-rejected"),
    ],
)
def test_rejected_placement_refreshes_the_cluster_view(
    monkeypatch, placement_failed, expected_view
):
    """When the coordinator rejects an actor placement while completed tasks
    are processed, the step re-reads the reservation before the bank and the
    sizer use it, and clears the op's flag."""
    from ray.data._internal.experimental.execution import (
        streaming_executor as executor_mod,
    )

    monkeypatch.setattr(executor_mod, "process_completed_tasks", lambda **kwargs: 0)
    op = _PlacementFailedOp(placement_failed)
    reporter = _TwoViewReporter()
    sink = _ViewSink()
    others: List[str] = []
    executor = _executor_with(
        resource_reporter=reporter,
        resource_bank=sink,
        sizer=sink,
        actor_only_metrics=_Recorder(others, maybe_update=None),
        cluster_autoscaler=_Recorder(others, try_trigger_scaling=None),
        metadata_fetcher=None,
        lineage_tracker=None,
        max_errored_blocks=0,
        num_errored_blocks=0,
        topology={},
        # Not due: the escape hatch returns at once.
        last_detection_timestamp=float("inf"),
        idle_detection_interval_s=0.0,
        # Steps that read the op's pool, which this op does not have.
        expire_pending_actors=lambda topology: None,
        release_drained_actors_of_completed_ops=lambda topology: None,
        sample_output_backpressure=lambda topology: None,
        launch_tasks=lambda: None,
        report_current_usage=lambda: None,
        finalize_scheduling_loop_step=lambda topology: True,
    )
    topology: Any = {op: None}

    executor._scheduling_loop_step(topology)

    view = getattr(reporter, expected_view)
    assert (sink.cluster_views, sink.node_views[-1], op.actor_placement_failed) == (
        [view],
        view.reserved_by_node,
        False,
    )


class _DrainingOp:
    """An actor-pool op whose draining actors finish one per
    ``process_draining_actors`` call."""

    def __init__(self, num_draining: int):
        self.op = MagicMock(spec=ExperimentalActorPoolMapOperator)
        self.op.has_completed.return_value = False
        self.num_draining = num_draining
        self.num_process_calls = 0
        pool = self.op.actor_pool
        pool.process_draining_actors.side_effect = self._process
        pool.num_draining_actors.side_effect = lambda: self.num_draining

    def _process(self):
        self.num_process_calls += 1
        self.num_draining = max(0, self.num_draining - 1)


def test_completed_op_keeps_releasing_draining_actors_until_none_left():
    fake = _DrainingOp(num_draining=2)
    topology: Any = {fake.op: MagicMock()}
    release = ExperimentalStreamingExecutor._release_drained_actors_of_completed_ops

    # Still running: its own refresh_state drains it.
    release(topology)
    assert fake.num_process_calls == 0

    fake.op.has_completed.return_value = True
    for _ in range(4):
        release(topology)
    # Two calls drain it; with nothing left draining it isn't processed again.
    assert fake.num_process_calls == 2


def test_actors_marked_draining_after_completion_are_released():
    # The sizer marks a completed op's actors draining on a later tick, and
    # lineage reconstruction can run a completed op again: either way they
    # appear after the op completed, and must still be released.
    fake = _DrainingOp(num_draining=0)
    topology: Any = {fake.op: MagicMock()}
    release = ExperimentalStreamingExecutor._release_drained_actors_of_completed_ops
    fake.op.has_completed.return_value = True
    release(topology)
    assert fake.num_process_calls == 0

    fake.num_draining = 3
    for _ in range(3):
        release(topology)

    assert fake.num_draining == 0
    assert fake.num_process_calls == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
