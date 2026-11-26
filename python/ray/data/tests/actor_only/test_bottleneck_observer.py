"""Tests for the observation-only bottleneck model."""

from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple, cast

import pytest

from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    SlotUsage,
)
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    Adjacency,
    Arrow,
    BottleneckObserver,
    Edge,
    EdgeShareBreakdown,
    OpShareBreakdown,
    OpShareWindow,
    Snapshot,
    analyze,
    classify_edge_direction,
    format_snapshot,
    why_should_not_upscale_blocker,
)
from ray.data.tests.actor_only.actor_only_test_utils import FakeClock

ARROW_MARGIN = 0.1
MIN_SEVERITY = 0.1


def _chain(*ops: str) -> Tuple[Adjacency[str], Adjacency[str]]:
    """(prev_ops, next_ops) for a linear pipeline."""
    prev_ops: Adjacency[str] = {
        op: (ops[i - 1],) if i > 0 else () for i, op in enumerate(ops)
    }
    next_ops: Adjacency[str] = {
        op: (ops[i + 1],) if i < len(ops) - 1 else () for i, op in enumerate(ops)
    }
    return prev_ops, next_ops


@pytest.mark.parametrize(
    "blocked, self_limited, consumer_starved, expected",
    [
        pytest.param(0.98, 0.0, 0.0, Arrow.DOWNSTREAM, id="producer-waits"),
        pytest.param(0.0, 0.0, 0.9, Arrow.UPSTREAM, id="consumer-waits"),
        pytest.param(0.2, 0.0, 0.25, Arrow.TIE, id="within-margin"),
        pytest.param(0.5, 0.0, 0.4, Arrow.TIE, id="exactly-the-margin"),
        # A 1-actor producer, held back by its own output budget, feeds a
        # starved consumer: the producer limits the edge.
        pytest.param(0.0, 0.8, 0.57, Arrow.UPSTREAM, id="producer-self-limited"),
    ],
)
def test_classify_edge_direction(blocked, self_limited, consumer_starved, expected):
    assert (
        classify_edge_direction(
            producer_blocked=blocked,
            producer_self_limited=self_limited,
            consumer_starved=consumer_starved,
            margin=ARROW_MARGIN,
        )
        is expected
    )


def _shares(busy, obp, starved, actors) -> OpShareBreakdown:
    return OpShareBreakdown(busy=busy, obp=obp, starved=starved, actors=actors)


@pytest.mark.parametrize(
    "shares, chain, expected_arrows, expected_bottlenecks",
    [
        pytest.param(
            # Two producers are output-backpressured and the sink is starved,
            # so every arrow points at the op between them.
            {
                "Read": _shares(0.1, 0.9, 0.0, 8),
                "Preprocess": _shares(0.1, 0.9, 0.0, 31),
                "Predict": _shares(0.95, 0.05, 0.0, 8),
                "Write": _shares(0.2, 0.0, 0.8, 4),
            },
            ("Read", "Preprocess", "Predict", "Write"),
            (Arrow.DOWNSTREAM, Arrow.DOWNSTREAM, Arrow.UPSTREAM),
            (("Predict", 1.65),),
            id="converging-arrows",
        ),
        pytest.param(
            # B -> C is a near-tie (0.2 vs 0.15), so neither has an outgoing
            # arrow. A waits on B and D on C, so both are named, B first.
            {
                "A": _shares(0.1, 0.9, 0.0, 10),
                "B": _shares(0.8, 0.2, 0.0, 8),
                "C": _shares(0.85, 0.0, 0.15, 6),
                "D": _shares(0.7, 0.0, 0.3, 4),
            },
            ("A", "B", "C", "D"),
            (Arrow.DOWNSTREAM, Arrow.TIE, Arrow.UPSTREAM),
            (("B", 0.85), ("C", 0.35)),
            id="near-tie-both-named",
        ),
        pytest.param(
            # During ramp-up the first edge is a tie (0.00 vs 0.03), so A has no
            # outgoing arrow, but its severity is under the floor.
            {
                "A": _shares(0.99, 0.0, 0.0, 33),
                "B": _shares(0.58, 0.38, 0.03, 31),
                "C": _shares(0.73, 0.0, 0.16, 7),
            },
            ("A", "B", "C"),
            (Arrow.TIE, Arrow.DOWNSTREAM),
            (("C", 0.22),),
            id="tie-below-severity-floor",
        ),
        pytest.param(
            # Within the margin, but Write is starved (0.25) more than B is
            # blocked on it (0.2): Write's severity is negative.
            {
                "A": _shares(0.1, 0.9, 0.0, 10),
                "B": _shares(0.8, 0.2, 0.0, 8),
                "Write": _shares(0.75, 0.0, 0.25, 27),
            },
            ("A", "B", "Write"),
            (Arrow.DOWNSTREAM, Arrow.TIE),
            (("B", 0.95),),
            id="negative-severity-dropped",
        ),
        pytest.param(
            # A sink that limits the pipeline is found from its producer's
            # output backpressure alone.
            {"B": _shares(0.1, 0.9, 0.0, 6), "Write": _shares(1.0, 0.0, 0.0, 3)},
            ("B", "Write"),
            (Arrow.DOWNSTREAM,),
            (("Write", 0.9),),
            id="sink-from-producer-obp",
        ),
        pytest.param(
            # Only the middle op has a full window: there are no edges, and no
            # measured neighbour waits on it.
            {"B": _shares(1.0, 0.0, 0.0, 2)},
            ("A", "B", "C"),
            (),
            (),
            id="missing-neighbours",
        ),
        pytest.param(
            # B is output-backpressured nearly all the time, so it waits on
            # something downstream, even with no shares for that consumer.
            {"A": _shares(0.1, 0.9, 0.0, 4), "B": _shares(0.05, 0.95, 0.0, 5)},
            ("A", "B", "Write"),
            (Arrow.DOWNSTREAM,),
            (),
            id="blocked-on-unmodeled-consumer",
        ),
    ],
)
def test_analyze_names_bottlenecks(
    shares, chain, expected_arrows, expected_bottlenecks
):
    prev_ops, next_ops = _chain(*chain)
    snapshot = analyze(
        shares=shares,
        prev_ops=prev_ops,
        next_ops=next_ops,
        arrow_margin=ARROW_MARGIN,
        min_severity=MIN_SEVERITY,
    )

    assert tuple(edge.arrow for edge in snapshot.edges) == expected_arrows
    assert snapshot.bottlenecks == tuple(
        (op, pytest.approx(sev)) for op, sev in expected_bottlenecks
    )


def test_share_window_is_slot_time_weighted_and_slides():
    window = OpShareWindow(window_s=10.0)
    window.record(dt=0.0, actors=4, slot_s=0, non_obp_busy_s=0, obp_s=0, starved_s=0)
    window.record(dt=5.0, actors=4, slot_s=20, non_obp_busy_s=20, obp_s=0, starved_s=0)
    assert window.shares() is None  # half a window

    window.record(dt=5.0, actors=4, slot_s=20, non_obp_busy_s=0, obp_s=20, starved_s=0)
    assert window.shares() == _shares(0.5, 0.5, 0.0, 4)

    # Another 10s of starvation pushes the earlier samples out.
    window.record(dt=10.0, actors=2, slot_s=20, non_obp_busy_s=0, obp_s=0, starved_s=20)
    assert window.shares() == _shares(0.0, 0.0, 1.0, 2)


def test_share_window_counts_unstaffed_time_as_one_busy_slot():
    window = OpShareWindow(window_s=10.0)
    window.record(
        dt=5.0,
        actors=0,
        slot_s=0,
        non_obp_busy_s=0,
        obp_s=0,
        starved_s=0,
        unstaffed=True,
    )
    window.record(dt=5.0, actors=1, slot_s=5, non_obp_busy_s=0, obp_s=5, starved_s=0)

    assert window.shares() == OpShareBreakdown(
        busy=0.5, obp=0.5, starved=0.0, actors=1, unstaffed=0.5
    )


def test_share_window_without_actors_or_input_has_no_shares():
    window = OpShareWindow(window_s=10.0)
    window.record(dt=10.0, actors=0, slot_s=0, non_obp_busy_s=0, obp_s=0, starved_s=0)

    assert window.shares() is None


class _Op:
    """Fake op: slot usage set per test; ``busy_slots`` advance the busy-slot
    integral by that many slot-seconds per observed second."""

    def __init__(self, name, *, serving, obp_actors=0, busy_slots=0.0, conc=1):
        self.name = name
        self.id = name
        self.serving = serving
        self.obp_actors = obp_actors
        self.busy_slots = busy_slots
        self.conc = conc
        self.busy_slot_seconds = 0.0
        self.slot_seconds = 0.0
        # Slots busy right now, for the joint edge signal: by default every
        # slot not output-backpressured is busy while busy_slots > 0.
        self.busy_now = (serving - obp_actors) * conc if busy_slots > 0 else 0
        self.completed = False
        # Set by ``_observer``: the op's queue, which says whether a bundle
        # is ready to dispatch.
        self.op_state: Optional[_OpState] = None

    def has_completed(self):
        return self.completed

    def has_queued_bundle(self):
        assert self.op_state is not None
        return self.op_state.has_queued_bundle()

    def slot_usage(self):
        return SlotUsage(
            serving_actors=self.serving,
            slots=self.serving * self.conc,
            slot_seconds=self.slot_seconds,
            obp_slots=self.obp_actors * self.conc,
            obp_busy_slots=0,
            busy_slots=self.busy_now,
            busy_slot_seconds=self.busy_slot_seconds,
        )


class _OpState:
    def __init__(self, queued, partial=False):
        self.queued = queued
        # Whether the queued blocks form only a partial bundle, which can't
        # dispatch until more input arrives.
        self.partial = partial

    def total_enqueued_input_blocks(self):
        return self.queued

    def has_queued_bundle(self):
        return self.queued > 0 and not self.partial


def _observer(*ops, queued, partial=False, window_s=4.0):
    """An observer over the chain ``ops`` and a stepper that advances the
    fake clock by 1s, moves each op's slot and busy-slot integrals, and
    observes."""
    prev_ops, next_ops = _chain(*ops)
    profiles = SimpleNamespace(
        actor_pool_ops=ops,
        non_pg_ops=ops,
        prev_ops=prev_ops,
        next_ops=next_ops,
        is_pg_managed=lambda op: False,
    )
    topology = cast(
        Any, {op: _OpState(queued=q, partial=partial) for op, q in zip(ops, queued)}
    )
    for op in ops:
        op.op_state = topology[op]
    clock = FakeClock()
    observer = BottleneckObserver(cast(Any, profiles), clock=clock, window_s=window_s)

    def step(n=1):
        for _ in range(n):
            observer.observe(topology)
            clock.now += 1.0
            for op in ops:
                op.busy_slot_seconds += op.busy_slots
                op.slot_seconds += op.serving * op.conc

    return observer, topology, step


def _shares_of(observer: BottleneckObserver) -> Dict[Any, OpShareBreakdown]:
    return cast(Dict[Any, OpShareBreakdown], observer.snapshot().shares)


@pytest.mark.parametrize(
    "queued, partial, expected_starved",
    [
        # The idle slot has queued work: a dispatch gap, not starvation.
        pytest.param(3, False, 0.0, id="ready-bundle"),
        pytest.param(0, False, 0.5, id="empty-queue"),
        # Queued blocks that form only a partial bundle can't dispatch, so
        # the idle slot waits on the producer.
        pytest.param(3, True, 0.5, id="partial-bundle"),
    ],
)
def test_observer_counts_idle_as_starved_only_without_a_ready_bundle(
    queued, partial, expected_starved
):
    producer = _Op("P", serving=4, obp_actors=4)
    consumer = _Op("C", serving=2, busy_slots=1.0)
    observer, _, step = _observer(
        producer, consumer, queued=(0, queued), partial=partial
    )

    step(5)

    assert _shares_of(observer)[consumer] == _shares(0.5, 0.0, expected_starved, 2)


def test_consumer_without_actors_but_with_queued_input_is_named():
    """A consumer with queued input and no actor stays in the model as
    unstaffed, and the producer waiting on it names it."""
    producer = _Op("P", serving=4, obp_actors=4)
    consumer = _Op("C", serving=0)
    observer, _, step = _observer(producer, consumer, queued=(0, 3))

    step(5)

    snapshot = observer.snapshot()
    assert _shares_of(observer)[consumer].unstaffed == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]


def test_completed_op_leaves_the_model():
    producer = _Op("P", serving=4, obp_actors=4)
    consumer = _Op("C", serving=2, busy_slots=1.0)
    observer, _, step = _observer(producer, consumer, queued=(0, 3))
    step(5)

    producer.completed = True
    step()

    assert producer not in observer.snapshot().shares


def test_format_snapshot():
    snapshot = Snapshot(
        shares={
            "P": _shares(0.0, 1.0, 0.0, 4),
            "C": OpShareBreakdown(
                busy=1.0, obp=0.0, starved=0.0, actors=0, unstaffed=1.0
            ),
        },
        edges=(
            Edge(
                producer="P",
                consumer="C",
                shares=EdgeShareBreakdown(
                    producer_blocked=1.0, producer_self_limited=0.0
                ),
                consumer_starved=0.0,
                arrow=Arrow.DOWNSTREAM,
            ),
        ),
        bottlenecks=(("C", 1.0),),
    )

    assert format_snapshot(snapshot=snapshot, names={"P": "Read", "C": "Write"}) == (
        "bottlenecks=[Write(sev=1.00, n=0)] | "
        "edges=[Read->Write blocked=1.00/self=0.00/starved=0.00 [->]] | "
        "ops=[Read busy=0.00 obp=1.00 starved=0.00 n=4, "
        "Write busy=1.00 obp=0.00 starved=0.00 n=0 unstaffed=1.00]"
    )


@pytest.mark.parametrize(
    "consumer_busy_now, expected_edge, expected",
    [
        pytest.param(
            2,
            EdgeShareBreakdown(producer_blocked=1.0, producer_self_limited=0.0),
            "C",
            id="consumer-saturated",
        ),
        pytest.param(
            1,
            EdgeShareBreakdown(producer_blocked=0.0, producer_self_limited=1.0),
            "P",
            id="consumer-has-an-idle-slot",
        ),
    ],
)
def test_observer_attributes_output_backpressure_by_consumer_state(
    consumer_busy_now, expected_edge, expected
):
    """P's one actor is output-backpressured every step. If C has no idle slot
    at those moments, C holds P back. If C has an idle slot (and, with its
    queue empty, is starved), P's own output budget holds it back, so P is
    named: more P actors means more blocks in flight."""
    producer = _Op("P", serving=1, obp_actors=1)
    consumer = _Op("C", serving=2, busy_slots=float(consumer_busy_now))
    consumer.busy_now = consumer_busy_now
    observer, _, step = _observer(producer, consumer, queued=(0, 0))

    step(5)

    snapshot = observer.snapshot()
    (edge,) = snapshot.edges
    assert edge.shares == expected_edge
    names: Dict[Any, str] = {producer: "P", consumer: "C"}
    assert [names[op] for op, _ in snapshot.bottlenecks] == [expected]


def test_resize_at_sample_uses_integrated_slots():
    """One busy slot throughout; one of two actors leaves just before a
    sample. The interval's capacity is what served during it (2 slots, half
    starved), not the count at the sample (1 slot, never starved)."""
    op = _Op("C", serving=2, busy_slots=1.0)
    observer, _, step = _observer(op, queued=(0,), window_s=1.0)
    step(3)

    op.serving = 1
    step()

    assert _shares_of(observer)[op].starved == pytest.approx(0.5)


def test_short_tasks_between_samples_count_as_busy():
    """Each 1s step the op runs 0.4 slot-seconds of tasks that start and
    finish between samples, so no sample sees a busy slot. The busy-slot
    integral still counts them."""
    op = _Op("C", serving=1, busy_slots=0.4)
    op.busy_now = 0
    observer, _, step = _observer(op, queued=(5,))

    step(5)

    assert _shares_of(observer)[op].busy == pytest.approx(0.4)


@pytest.mark.parametrize(
    "named, self_limited, op, expected",
    [
        # C became the most severe bottleneck while B was still named. B's
        # input is empty, so more B actors would not help.
        pytest.param(
            [("C", 0.92), ("B", 0.45)],
            0.0,
            "B",
            "a more severe bottleneck is upstream",
            id="upstream-more-severe",
        ),
        pytest.param(
            [("C", 0.92), ("B", 0.45)], 0.0, "C", None, id="most-severe-upstream"
        ),
        # The producer feeding C turns self-limited, so new C actors would
        # starve.
        pytest.param(
            [("C", 0.97)],
            0.12,
            "C",
            "input self-limited (0.12)",
            id="input-self-limited",
        ),
        pytest.param([("C", 0.97)], 0.05, "C", None, id="self-below-margin"),
    ],
)
def test_why_should_not_upscale_blocker(named, self_limited, op, expected):
    """A named bottleneck can use more actors only with no more severe named
    op feeding it and no self-limited producer feeding it."""
    _, next_ops = _chain("R", "C", "B", "W")
    edge = Edge(
        producer="R",
        consumer="C",
        shares=EdgeShareBreakdown(
            producer_blocked=1.0 - self_limited, producer_self_limited=self_limited
        ),
        consumer_starved=0.0,
        arrow=Arrow.DOWNSTREAM,
    )
    snapshot = Snapshot(shares={}, edges=(edge,), bottlenecks=tuple(named))
    assert (
        why_should_not_upscale_blocker(
            op=op, snapshot=snapshot, next_ops=next_ops, margin=ARROW_MARGIN
        )
        == expected
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
