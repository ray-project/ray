"""Tests for the observation-only bottleneck model."""

from types import SimpleNamespace
from typing import Any, Dict, Tuple, cast

import pytest

from ray.data._internal.actor_autoscaler import AutoscalingActorPool
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    SlotUsage,
)
from ray.data._internal.experimental.execution.sizer import bottleneck_observer
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    Adjacency,
    Arrow,
    BottleneckObserver,
    EdgeShares,
    OpShareBreakdown,
    ShareWindow,
    analyze,
    classify_edge,
    format_snapshot,
    why_should_not_upscale_blocker,
)

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


def _analyze(shares, *ops):
    prev_ops, next_ops = _chain(*ops)
    return analyze(
        shares,
        prev_ops,
        next_ops,
        arrow_margin=ARROW_MARGIN,
        min_severity=MIN_SEVERITY,
    )


@pytest.mark.parametrize(
    "blocked, self_limited, consumer_starved, expected",
    [
        (0.98, 0.0, 0.0, Arrow.DOWNSTREAM),  # producer waits on consumer
        (0.0, 0.0, 0.9, Arrow.UPSTREAM),  # consumer waits on producer
        (0.2, 0.0, 0.25, Arrow.TIE),  # within the margin
        (0.5, 0.0, 0.4, Arrow.TIE),  # exactly the margin is still a tie
        # Output-backpressured by its own budget while the consumer has room:
        # the producer limits the edge (video: 1 GPU actor, Write starved).
        (0.0, 0.8, 0.57, Arrow.UPSTREAM),
    ],
)
def test_classify_edge(blocked, self_limited, consumer_starved, expected):
    assert (
        classify_edge(blocked, self_limited, consumer_starved, ARROW_MARGIN) is expected
    )


def test_label_contention_names_the_labeled_stage():
    """autoscale_contention: GenStage is output-backpressured on LabeledStage,
    Write is starved, so every arrow points at LabeledStage."""
    shares = {
        "Gen": OpShareBreakdown(busy=0.02, obp=0.98, starved=0.0, actors=15),
        "Labeled": OpShareBreakdown(busy=1.0, obp=0.0, starved=0.0, actors=2),
        "Write": OpShareBreakdown(busy=0.1, obp=0.0, starved=0.9, actors=1),
    }
    snapshot = _analyze(shares, "Gen", "Labeled", "Write")

    assert [op for op, _ in snapshot.bottlenecks] == ["Labeled"]


def test_image_classification_never_names_the_upstream_reader():
    """image_classification: Preprocess is output-backpressured on the GPU
    Predictor; ReadFiles is not a bottleneck, so nothing would flow to it."""
    shares = {
        "ReadFiles": OpShareBreakdown(busy=0.1, obp=0.9, starved=0.0, actors=8),
        "Preprocess": OpShareBreakdown(busy=0.1, obp=0.9, starved=0.0, actors=31),
        "Predictor": OpShareBreakdown(busy=0.95, obp=0.05, starved=0.0, actors=8),
        "Write": OpShareBreakdown(busy=0.2, obp=0.0, starved=0.8, actors=4),
    }
    snapshot = _analyze(shares, "ReadFiles", "Preprocess", "Predictor", "Write")

    assert [op for op, _ in snapshot.bottlenecks] == ["Predictor"]


def test_near_tie_reports_both_sides_most_severe_first():
    """B -> C is a near-tie (0.2 vs 0.15), so neither has an outgoing arrow;
    both have neighbours waiting on them (A on B, D on C), so both are
    reported, B first."""
    shares = {
        "A": OpShareBreakdown(busy=0.1, obp=0.9, starved=0.0, actors=10),
        "B": OpShareBreakdown(busy=0.8, obp=0.2, starved=0.0, actors=8),
        "C": OpShareBreakdown(busy=0.85, obp=0.0, starved=0.15, actors=6),
        "D": OpShareBreakdown(busy=0.7, obp=0.0, starved=0.3, actors=4),
    }
    snapshot = _analyze(shares, "A", "B", "C", "D")

    assert snapshot.edges[1].arrow is Arrow.TIE
    assert snapshot.bottlenecks == (
        ("B", pytest.approx(0.85)),
        ("C", pytest.approx(0.35)),
    )


def test_ramp_up_tie_is_below_the_severity_floor():
    """image_classification ramp-up: ReadFiles -> Preprocess is a tie (0.00
    vs 0.03), so ReadFiles had no outgoing arrow, but at severity 0.03 it is
    noise, not a bottleneck: only Predictor is named."""
    shares = {
        "ReadFiles": OpShareBreakdown(busy=0.99, obp=0.0, starved=0.0, actors=33),
        "Preprocess": OpShareBreakdown(busy=0.58, obp=0.38, starved=0.03, actors=31),
        "Predictor": OpShareBreakdown(busy=0.73, obp=0.0, starved=0.16, actors=7),
    }
    snapshot = _analyze(shares, "ReadFiles", "Preprocess", "Predictor")

    assert snapshot.edges[0].arrow is Arrow.TIE
    assert [op for op, _ in snapshot.bottlenecks] == ["Predictor"]


def test_tie_side_that_waits_more_than_it_is_waited_on_is_dropped():
    """Within the arrow margin, but Write is starved (0.25) more than B is
    blocked on it (0.2): Write's severity is negative, so only B is named."""
    shares = {
        "A": OpShareBreakdown(busy=0.1, obp=0.9, starved=0.0, actors=10),
        "B": OpShareBreakdown(busy=0.8, obp=0.2, starved=0.0, actors=8),
        "Write": OpShareBreakdown(busy=0.75, obp=0.0, starved=0.25, actors=27),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert snapshot.edges[1].arrow is Arrow.TIE
    assert [op for op, _ in snapshot.bottlenecks] == ["B"]


def test_last_op_working_through_its_tail_is_not_a_bottleneck():
    """image_classification tail: every upstream op has completed and left
    the model, so nothing waits on Predictor (severity -0.05)."""
    shares = {
        "Predictor": OpShareBreakdown(busy=0.93, obp=0.0, starved=0.05, actors=10),
    }
    snapshot = _analyze(shares, "ReadFiles", "Preprocess", "Predictor")

    assert snapshot.bottlenecks == ()


def test_sink_bottleneck_uses_only_its_upstream_edge():
    """A sink that limits the pipeline is found from its producer's output
    backpressure alone."""
    shares = {
        "B": OpShareBreakdown(busy=0.1, obp=0.9, starved=0.0, actors=6),
        "Write": OpShareBreakdown(busy=1.0, obp=0.0, starved=0.0, actors=3),
    }
    snapshot = _analyze(shares, "B", "Write")

    assert snapshot.bottlenecks == (("Write", pytest.approx(0.9)),)


def test_ops_without_a_full_window_are_left_out():
    # Gen and Write have no shares yet: no edges, and with no measured
    # neighbour waiting on it, Labeled is not named either.
    shares = {"Labeled": OpShareBreakdown(busy=1.0, obp=0.0, starved=0.0, actors=2)}
    snapshot = _analyze(shares, "Gen", "Labeled", "Write")

    assert snapshot.edges == ()
    assert snapshot.bottlenecks == ()


def test_sink_with_no_actors_is_named_not_its_blocked_producer():
    """video_object_detection stall: Write never placed an actor, so its
    queued input is unstaffed time (one fully busy actor), and B, fully
    output-backpressured on it, points its arrow at it."""
    shares = {
        "A": OpShareBreakdown(busy=0.0, obp=1.0, starved=0.0, actors=6),
        "B": OpShareBreakdown(busy=0.0, obp=1.0, starved=0.0, actors=5),
        "Write": OpShareBreakdown(
            busy=1.0, obp=0.0, starved=0.0, actors=0, unstaffed=1.0
        ),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert snapshot.bottlenecks == (("Write", pytest.approx(1.0)),)
    assert snapshot.edges[1].arrow is Arrow.DOWNSTREAM


def test_op_blocked_on_an_unmodeled_consumer_is_not_a_bottleneck():
    """An op output-backpressured nearly all the time is waiting on something
    downstream, even when that consumer has no shares to draw an arrow to."""
    shares = {
        "A": OpShareBreakdown(busy=0.1, obp=0.9, starved=0.0, actors=4),
        "B": OpShareBreakdown(busy=0.05, obp=0.95, starved=0.0, actors=5),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert snapshot.bottlenecks == ()


@pytest.mark.parametrize(
    "consumer_saturated, expected",
    [(True, "W"), (False, "B")],
    ids=["consumer-saturated", "producer-budget-limited"],
)
def test_output_backpressure_is_attributed_by_what_the_consumer_was_doing(
    consumer_saturated, expected
):
    """B is output-backpressured 80% of the time. If W had no idle slot then
    (saturated), W holds B back and W is the bottleneck. If W had idle slots
    and was starved, B's own output budget held it back, so B is: more B
    actors means more blocks in flight. Raw obp alone cannot tell these apart
    (video_object_detection: 1 GPU actor, Write starved 57%)."""
    shares = {
        "A": OpShareBreakdown(busy=0.0, obp=1.0, starved=0.0, actors=7),
        "B": OpShareBreakdown(busy=0.2, obp=0.8, starved=0.0, actors=1),
        "W": OpShareBreakdown(
            busy=1.0 if consumer_saturated else 0.4,
            obp=0.0,
            starved=0.0 if consumer_saturated else 0.57,
            actors=7,
        ),
    }
    b_out = (
        EdgeShares(blocked=0.8, self_limited=0.0)
        if consumer_saturated
        else EdgeShares(blocked=0.0, self_limited=0.8)
    )
    edge_shares = {
        ("A", "B"): EdgeShares(blocked=1.0, self_limited=0.0),
        ("B", "W"): b_out,
    }
    prev_ops, next_ops = _chain("A", "B", "W")
    snapshot = analyze(
        shares,
        prev_ops,
        next_ops,
        arrow_margin=ARROW_MARGIN,
        min_severity=MIN_SEVERITY,
        edge_shares=edge_shares,
    )

    assert [op for op, _ in snapshot.bottlenecks] == [expected]


def test_share_window_is_slot_time_weighted_and_slides():
    window = ShareWindow(window_s=10.0)
    window.record(dt=0.0, actors=4, slot_s=0, busy_s=0, obp_s=0, starved_s=0)
    window.record(dt=5.0, actors=4, slot_s=20, busy_s=20, obp_s=0, starved_s=0)
    assert window.shares() is None  # half a window

    window.record(dt=5.0, actors=4, slot_s=20, busy_s=0, obp_s=20, starved_s=0)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.starved) == (0.5, 0.5, 0.0)
    assert shares.actors == 4

    # Another 10s of starvation pushes the earlier samples out.
    window.record(dt=10.0, actors=2, slot_s=20, busy_s=0, obp_s=0, starved_s=20)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.starved) == (0.0, 0.0, 1.0)
    assert shares.actors == 2


def test_share_window_counts_unstaffed_time_as_one_busy_slot():
    window = ShareWindow(window_s=10.0)
    window.record(
        dt=5.0, actors=0, slot_s=0, busy_s=0, obp_s=0, starved_s=0, unstaffed=True
    )
    window.record(dt=5.0, actors=1, slot_s=5, busy_s=0, obp_s=5, starved_s=0)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.unstaffed) == (0.5, 0.5, 0.5)
    assert shares.actors == 1

    # No actors and nothing queued is no data, not a busy stage.
    idle = ShareWindow(window_s=10.0)
    idle.record(dt=10.0, actors=0, slot_s=0, busy_s=0, obp_s=0, starved_s=0)
    assert idle.shares() is None


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class _Pool:
    def __init__(self, states=None, max_size=10, per_actor=None):
        self.states = states or {}
        self._max_size = max_size
        self._per_actor = per_actor or ExecutionResources(cpu=1)

    def per_actor_resource_usage(self):
        return self._per_actor

    def serving_size(self):
        return sum(len(by_actor) for by_actor in self.states.values())

    def max_size(self):
        return self._max_size

    bounded_max_size = AutoscalingActorPool.bounded_max_size

    def max_actor_concurrency(self):
        return 1

    def alive_actor_states_by_node(self):
        return self.states


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
        # Slots busy right now, for the joint edge signal: by default every
        # slot not output-backpressured is busy while busy_slots > 0.
        self.busy_now = (serving - obp_actors) * conc if busy_slots > 0 else 0
        self.completed = False
        self.metrics = SimpleNamespace(
            num_task_inputs_processed=0,
            num_outputs_taken=0,
            num_task_outputs_generated=0,
            num_tasks_finished=0,
        )
        self.actor_pool = _Pool()

    def has_completed(self):
        return self.completed

    def slot_usage(self):
        return SlotUsage(
            serving_actors=self.serving,
            slots=self.serving * self.conc,
            obp_slots=self.obp_actors * self.conc,
            obp_busy_slots=0,
            busy_slots=self.busy_now,
            busy_slot_seconds=self.busy_slot_seconds,
        )


class _OpState:
    def __init__(self, queued):
        self.queued = queued

    def total_enqueued_input_blocks(self):
        return self.queued


def _observer(*ops, queued, window_s=4.0):
    """An observer over the chain ``ops`` and a stepper that advances the
    fake clock by 1s, moves each op's busy-slot integral, and observes."""
    prev_ops, next_ops = _chain(*ops)
    profiles = SimpleNamespace(
        actor_pool_ops=ops,
        non_pg_ops=ops,
        prev_ops=prev_ops,
        next_ops=next_ops,
        is_pg_managed=lambda op: False,
    )
    topology = cast(Any, {op: _OpState(queued=q) for op, q in zip(ops, queued)})
    clock = _Clock()
    observer = BottleneckObserver(cast(Any, profiles), clock=clock, window_s=window_s)

    def step(n=1):
        for _ in range(n):
            observer.observe(topology)
            clock.now += 1.0
            for op in ops:
                op.busy_slot_seconds += op.busy_slots

    return observer, topology, step


def test_observer_counts_idle_as_starved_only_with_an_empty_queue():
    producer = _Op("P", serving=4, obp_actors=4)
    consumer = _Op("C", serving=2, busy_slots=1.0)
    observer, topology, step = _observer(producer, consumer, queued=(0, 3))

    step(5)
    snapshot = observer.snapshot()
    shares = cast(Dict[Any, OpShareBreakdown], snapshot.shares)

    # Consumer's idle actor has queued work: a dispatch gap, not starvation.
    assert shares[consumer].starved == 0.0
    assert shares[consumer].busy == 0.5
    assert shares[producer].obp == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]
    assert "bottlenecks=[C(" in format_snapshot(
        snapshot=snapshot, names={producer: "P", consumer: "C"}
    )

    # With its queue empty, the consumer's idle slot is starved.
    topology[consumer].queued = 0
    step(5)
    assert (
        cast(Dict[Any, OpShareBreakdown], observer.snapshot().shares)[consumer].starved
        == 0.5
    )

    # The consumer loses every actor with input queued: it stays in the
    # model as unstaffed, and is still the one named.
    topology[consumer].queued = 3
    consumer.serving, consumer.busy_slots = 0, 0.0
    step(5)
    snapshot = observer.snapshot()
    shares = cast(Dict[Any, OpShareBreakdown], snapshot.shares)
    assert shares[consumer].unstaffed == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]
    assert "unstaffed=1.00" in format_snapshot(
        snapshot=snapshot, names={producer: "P", consumer: "C"}
    )

    # A completed op leaves the model.
    producer.completed = True
    step()
    assert producer not in observer.snapshot().shares


@pytest.mark.parametrize(
    "consumer_busy_now, expected",
    [(2, "C"), (1, "P")],
    ids=["consumer-saturated", "consumer-has-an-idle-slot"],
)
def test_observer_attributes_output_backpressure_by_consumer_state(
    consumer_busy_now, expected
):
    # P's one actor is output-backpressured every step. If C has no idle slot
    # at those moments, C holds P back; if C has an idle slot (and, with its
    # queue empty, is starved), P's own budget does, so P is named.
    producer = _Op("P", serving=1, obp_actors=1)
    consumer = _Op("C", serving=2, busy_slots=float(consumer_busy_now))
    consumer.busy_now = consumer_busy_now
    observer, _, step = _observer(producer, consumer, queued=(0, 0))
    step(5)
    snapshot = observer.snapshot()

    (edge,) = snapshot.edges
    if expected == "C":
        assert (edge.producer_blocked, edge.producer_self_limited) == (1.0, 0.0)
    else:
        assert (edge.producer_blocked, edge.producer_self_limited) == (0.0, 1.0)
    names: Dict[Any, str] = {producer: "P", consumer: "C"}
    assert [names[op] for op, _ in snapshot.bottlenecks] == [expected]


def test_short_tasks_between_samples_count_as_busy():
    # Each 1s step the op ran 0.4 slot-seconds of tasks that started and
    # finished between samples: per-step sampling would see it idle.
    op = _Op("C", serving=1, busy_slots=0.4)
    upstream = _Op("P", serving=2, obp_actors=2)
    observer, _, step = _observer(upstream, op, queued=(0, 5))
    step(5)
    shares = cast(Dict[Any, OpShareBreakdown], observer.snapshot().shares)
    assert shares[op].busy == pytest.approx(0.4)


@pytest.mark.parametrize(
    "named, self_limited, op, expected",
    [
        # 21:57:00 in video: resize (C) took over while the GPU op (B) was
        # still named; B's input was empty, so upscaling it would not help.
        (
            [("C", 0.92), ("B", 0.45)],
            0.0,
            "B",
            "a more severe bottleneck is upstream",
        ),
        ([("C", 0.92), ("B", 0.45)], 0.0, "C", None),
        # 22:02:39: resize upscaled while Read was turning self-limited; its
        # new actors starved.
        ([("C", 0.97)], 0.12, "C", "input self-limited (0.12)"),
        ([("C", 0.97)], 0.05, "C", None),
    ],
    ids=[
        "upstream-more-severe",
        "most-severe-upstream",
        "input-self-limited",
        "self-below-margin",
    ],
)
def test_why_should_not_upscale_blocker(named, self_limited, op, expected):
    """A named bottleneck can use more actors only with no more severe named
    op feeding it and no self-limited producer feeding it."""
    _, next_ops = _chain("R", "C", "B", "W")
    edge = bottleneck_observer.Edge(
        producer="R",
        consumer="C",
        producer_blocked=1.0 - self_limited,
        producer_self_limited=self_limited,
        consumer_starved=0.0,
        arrow=Arrow.DOWNSTREAM,
    )
    snapshot = bottleneck_observer.Snapshot(
        shares={},
        edges=(edge,),
        bottlenecks=tuple((name, sev) for name, sev in named),
    )
    assert (
        why_should_not_upscale_blocker(
            op=op, snapshot=snapshot, next_ops=next_ops, margin=ARROW_MARGIN
        )
        == expected
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
