"""Unit tests for ``ActorOnlyMetrics``. The operator, its actor pool and every
gauge/counter are ``MagicMock``s spec'd off the real objects, so the tests need
no Ray runtime."""
import sys
from collections import Counter
from types import SimpleNamespace
from typing import Any, List, cast
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.interfaces.distribution_tracker import (
    DistributionTracker,
)
from ray.data._internal.execution.resource_bank import CumulativeObjStore, LiveObjStore
from ray.data._internal.experimental.execution import actor_only_metrics as aom
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)

# Every recorder on ``ActorOnlyRecorders`` the tests below assert against.
RECORDER_NAMES = [
    "node_obj_store",
    "node_num_actors",
    "node_op_num_actors",
    "node_op_input_locality",
    "output_limit_upgrades",
    "op_output_limit",
    "op_output_backpressured_actors",
    "op_output_overshoots",
    "num_draining_actors",
    "actor_drains_started",
    "actor_drains_completed",
    "actor_drains_interrupted",
    "actor_drains_cancelled",
    "actor_drain_completed_time",
    "actor_drain_interrupted_time",
]


def _writes(metric: MagicMock, method: str, **tags) -> List[float]:
    """Values written to ``metric`` through ``method`` under exactly ``tags``.

    Recorders are always called with the value positionally and the tags by
    keyword (``gauge.set(value, tags=tags)``), so both are read that way.
    """
    key = tuple(sorted(tags.items()))
    return [
        call.args[0]
        for call in getattr(metric, method).call_args_list
        if tuple(sorted(call.kwargs["tags"].items())) == key
    ]


def _latest(metric: MagicMock, **tags) -> float:
    """A gauge's current value: the last ``set`` written under ``tags``."""
    values = _writes(metric, "set", **tags)
    assert values, f"no series was written for {tags}"
    return values[-1]


def _total(metric: MagicMock, **tags) -> float:
    """A counter's value: every ``inc`` written under ``tags``, summed."""
    return sum(_writes(metric, "inc", **tags))


def _observations(metric: MagicMock, **tags) -> int:
    """How many samples a histogram took under ``tags``."""
    return len(_writes(metric, "observe", **tags))


def _tracker(samples: List[float]) -> DistributionTracker:
    tracker = DistributionTracker()
    for s in samples:
        tracker.add_sample(s)
    return tracker


def _mock_actor_pool_op(*, counts_by_node, limits_by_actor, blocked):
    """An ``ExperimentalActorPoolMapOperator`` mock serving only the reads the
    metrics make. ``spec`` keeps the metrics' ``isinstance`` checks passing."""
    pool = MagicMock(spec=_ExperimentalActorPool)
    pool.get_logical_ids.return_value = list(limits_by_actor)
    pool.output_bytes_limit.side_effect = lambda a: limits_by_actor[a][0]
    pool.output_count_limit.side_effect = lambda a: limits_by_actor[a][1]
    pool.num_draining_actors.return_value = 1

    op = MagicMock(spec=ExperimentalActorPoolMapOperator)
    op.actor_pool = pool
    op.actor_counts_by_node.return_value = counts_by_node
    op.cached_output_backpressured_actors.return_value = (blocked, 0)
    return op


@pytest.fixture
def recorders(monkeypatch):
    """Swap every recorder for a mock of the same shape (``Gauge.set``,
    ``Counter.inc``, ``Histogram.observe``) and return them by name."""
    mocks = {
        name: MagicMock(spec=getattr(aom.ActorOnlyRecorders, name))
        for name in RECORDER_NAMES
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(aom.ActorOnlyRecorders, name, mock)
    return mocks


def test_stale_tracking_gauge_zeroes_series_that_stop_being_written():
    gauge_mock = MagicMock(spec=aom.ActorOnlyRecorders.node_num_actors)
    gauge = aom._StaleTrackingGauge(gauge_mock)

    gauge.set(5, {"node": "a"})
    gauge.set(7, {"node": "b"})
    gauge.flush_stale()
    assert _latest(gauge_mock, node="a") == 5
    assert _latest(gauge_mock, node="b") == 7

    gauge.set(6, {"node": "a"})  # "b" is not written this cycle
    gauge.flush_stale()
    assert _latest(gauge_mock, node="a") == 6
    assert _latest(gauge_mock, node="b") == 0

    gauge.flush_stale()  # nothing written: everything from last cycle resets
    assert _latest(gauge_mock, node="a") == 0


def test_counters_are_tagged_per_operator(recorders):
    metrics = aom.ActorOnlyMetrics(dataset_id="ds", update_interval=0)
    metrics.record_output_limit_upgrade("Map_1")
    metrics.record_output_limit_upgrade("Map_1")
    metrics.record_block_size_overshoot("Map_1")
    metrics.record_output_limit_overshoot("Map_1")

    tags = dict(dataset="ds", operator="Map_1")
    assert _total(recorders["output_limit_upgrades"], **tags) == 2
    overshoots = recorders["op_output_overshoots"]
    assert _total(overshoots, **tags, kind="block_size") == 1
    assert _total(overshoots, **tags, kind="output_bytes") == 1


def test_drain_events_are_tagged_by_reason_and_cause(recorders):
    """The pool records drain outcomes through the ``_ActorOnlyOpMetrics``
    seam; each lands on its counter (and duration histogram) under the
    operator's tag and the drain reason, cancellations also by cause."""
    metrics = aom.ActorOnlyMetrics(dataset_id="ds", update_interval=0)
    metrics.record_drain_started("Map_1", "resize")
    metrics.record_drain_started("Map_1", "preemption")
    metrics.record_drain_completed("Map_1", "resize", 12.5)
    metrics.record_drain_interrupted("Map_1", "preemption", 70.0)
    metrics.record_drain_cancelled("Map_1", "resize", "reclaimed")

    tags = dict(dataset="ds", operator="Map_1")
    assert _total(recorders["actor_drains_started"], **tags, reason="resize") == 1
    assert _total(recorders["actor_drains_started"], **tags, reason="preemption") == 1
    assert _total(recorders["actor_drains_completed"], **tags, reason="resize") == 1
    assert (
        _observations(recorders["actor_drain_completed_time"], **tags, reason="resize")
        == 1
    )
    assert (
        _total(recorders["actor_drains_interrupted"], **tags, reason="preemption") == 1
    )
    assert (
        _observations(
            recorders["actor_drain_interrupted_time"], **tags, reason="preemption"
        )
        == 1
    )
    assert (
        _total(
            recorders["actor_drains_cancelled"],
            **tags,
            reason="resize",
            cause="reclaimed",
        )
        == 1
    )


def test_maybe_update_emits_every_family_and_honors_the_interval(recorders):
    op = _mock_actor_pool_op(
        counts_by_node={
            "n1": Counter({ActorStatus.ACTIVE: 2, ActorStatus.IDLE: 1}),
            "n2": Counter({ActorStatus.DRAINING: 1}),
        },
        # (bytes limit, blocks limit) per actor; the unbounded one is excluded.
        limits_by_actor={"a": (100, 10), "b": (300, 30), "c": (float("inf"), 20)},
        blocked=1,
    )
    topology = cast(Any, {op: SimpleNamespace(op_tag=lambda: "Map_1")})

    live = LiveObjStore(
        num_input_bytes=10,
        num_input_blocks=1,
        num_pulled_output_bytes=40,
        num_pulled_output_blocks=2,
        num_prebuffered_output_bytes=20,
        num_prebuffered_output_blocks=1,
    )
    cumulative = CumulativeObjStore(
        output_bytes=_tracker([30, 30, 30]),  # 90 bytes / 3 blocks ever pulled
        input_bytes_local=_tracker([5, 5]),
        input_bytes_remote=_tracker([7]),
    )
    bank = cast(
        Any,
        SimpleNamespace(
            node_ids=lambda: {"n1"},
            op_node_ids=lambda: {(op, "n1")},
            live_object_store=lambda **kw: live,
            cumulative_object_store=lambda **kw: cumulative,
        ),
    )

    metrics = aom.ActorOnlyMetrics(dataset_id="ds", update_interval=1e9)
    metrics.maybe_update(topology=topology, resource_bank=bank)

    node_store = recorders["node_obj_store"]
    base = dict(dataset="ds", node="n1")
    assert _latest(node_store, **base, unit="bytes", breakdown="outputs") == 60
    assert _latest(node_store, **base, unit="blocks", breakdown="outputs") == 3
    assert _latest(node_store, **base, unit="bytes", breakdown="inputs") == 10
    # freed = ever pulled (90 bytes / 3 blocks) - still-live PULLED (40 / 2);
    # prebuffered outputs are not yet counted by the cumulative store.
    assert _latest(node_store, **base, unit="bytes", breakdown="freed") == 50
    assert _latest(node_store, **base, unit="blocks", breakdown="freed") == 1
    # local = local inputs + all outputs; remote = remote inputs only.
    assert _latest(node_store, **base, unit="bytes", breakdown="local") == 100
    assert _latest(node_store, **base, unit="bytes", breakdown="remote") == 7

    node_actors = recorders["node_num_actors"]
    assert _latest(node_actors, dataset="ds", node="n1", status="active") == 2
    assert _latest(node_actors, dataset="ds", node="n2", status="draining") == 1
    assert (
        _latest(
            recorders["node_op_num_actors"],
            dataset="ds",
            node="n1",
            operator="Map_1",
            status="idle",
        )
        == 1
    )

    limit = recorders["op_output_limit"]
    op_tags = dict(dataset="ds", operator="Map_1")
    assert _latest(limit, **op_tags, unit="bytes", stat="min") == 100
    assert _latest(limit, **op_tags, unit="bytes", stat="max") == 300
    assert _latest(limit, **op_tags, unit="bytes", stat="mean") == 200
    assert _latest(limit, **op_tags, unit="blocks", stat="max") == 30

    locality = recorders["node_op_input_locality"]
    loc = dict(dataset="ds", node="n1", operator="Map_1")
    assert _latest(locality, **loc, unit="bytes", breakdown="local") == 10
    assert _latest(locality, **loc, unit="blocks", breakdown="local") == 2
    assert _latest(locality, **loc, unit="bytes", breakdown="remote") == 7

    assert _latest(recorders["op_output_backpressured_actors"], **op_tags) == 1
    assert _latest(recorders["num_draining_actors"], **op_tags) == 1

    # Within the interval nothing is re-read; ``force`` bypasses it.
    op.cached_output_backpressured_actors.return_value = (3, 0)
    metrics.maybe_update(topology=topology, resource_bank=bank)
    assert _latest(recorders["op_output_backpressured_actors"], **op_tags) == 1
    metrics.maybe_update(topology=topology, resource_bank=bank, force=True)
    assert _latest(recorders["op_output_backpressured_actors"], **op_tags) == 3


def test_departed_node_series_reset_to_zero(recorders):
    op = _mock_actor_pool_op(
        counts_by_node={"n1": Counter({ActorStatus.ACTIVE: 1})},
        limits_by_actor={},
        blocked=0,
    )
    topology = cast(Any, {op: SimpleNamespace(op_tag=lambda: "Map_1")})
    bank = cast(
        Any,
        SimpleNamespace(
            node_ids=lambda: set(),
            op_node_ids=lambda: set(),
            live_object_store=lambda **kw: LiveObjStore(),
            cumulative_object_store=lambda **kw: CumulativeObjStore(),
        ),
    )
    metrics = aom.ActorOnlyMetrics(dataset_id="ds", update_interval=0)
    metrics.maybe_update(topology=topology, resource_bank=bank, force=True)
    node_actors = recorders["node_num_actors"]
    assert _latest(node_actors, dataset="ds", node="n1", status="active") == 1

    # The actor leaves n1: the series must read 0, not linger at 1.
    op.actor_counts_by_node.return_value = {}
    metrics.maybe_update(topology=topology, resource_bank=bank, force=True)
    assert _latest(node_actors, dataset="ds", node="n1", status="active") == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
