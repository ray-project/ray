"""Tests for the observation-only bottleneck model."""

from types import SimpleNamespace
from typing import Any, Dict, Sequence, Tuple, cast

import pytest

from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    Arrow,
    BottleneckObserver,
    OpShares,
    ShareWindow,
    analyze,
    classify_edge,
    format_snapshot,
    slack,
)

ARROW_MARGIN = 0.1
SLACK_MARGIN = 0.2


def _chain(*ops: str) -> Tuple[Dict[str, Sequence[str]], Dict[str, Sequence[str]]]:
    """(prev_ops, next_ops) for a linear pipeline."""
    prev_ops: Dict[str, Sequence[str]] = {
        op: (ops[i - 1],) if i > 0 else () for i, op in enumerate(ops)
    }
    next_ops: Dict[str, Sequence[str]] = {
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
        slack_margin=SLACK_MARGIN,
    )


@pytest.mark.parametrize(
    "producer_obp, consumer_starved, expected",
    [
        (0.98, 0.0, Arrow.DOWNSTREAM),  # producer waits on consumer
        (0.0, 0.9, Arrow.UPSTREAM),  # consumer waits on producer
        (0.2, 0.25, Arrow.TIE),  # within the margin
        (0.5, 0.4, Arrow.TIE),  # exactly the margin is still a tie
    ],
)
def test_classify_edge(producer_obp, consumer_starved, expected):
    assert classify_edge(producer_obp, consumer_starved, ARROW_MARGIN) is expected


def test_label_contention_names_the_labeled_stage():
    """autoscale_contention: GenStage is output-backpressured on LabeledStage,
    Write is starved, so every arrow points at LabeledStage and GenStage's
    output-backpressured actors are slack."""
    shares = {
        "Gen": OpShares(busy=0.02, obp=0.98, starved=0.0, actors=15),
        "Labeled": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=2),
        "Write": OpShares(busy=0.1, obp=0.0, starved=0.9, actors=1),
    }
    snapshot = _analyze(shares, "Gen", "Labeled", "Write")

    assert [op for op, _ in snapshot.bottlenecks] == ["Labeled"]
    # Labeled grows 2 -> 3: Gen needs ceil(15 * 0.02 * 1.5 * 1.2) = 1 actor.
    assert snapshot.slack["Gen"] == 14


def test_image_classification_never_names_the_upstream_reader():
    """image_classification: Preprocess is output-backpressured on the GPU
    Predictor; ReadFiles is not a bottleneck, so nothing would flow to it."""
    shares = {
        "ReadFiles": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=8),
        "Preprocess": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=31),
        "Predictor": OpShares(busy=0.95, obp=0.05, starved=0.0, actors=8),
        "Write": OpShares(busy=0.2, obp=0.0, starved=0.8, actors=4),
    }
    snapshot = _analyze(shares, "ReadFiles", "Preprocess", "Predictor", "Write")

    assert [op for op, _ in snapshot.bottlenecks] == ["Predictor"]
    assert "ReadFiles" in snapshot.slack


def test_near_tie_reports_both_sides_most_severe_first():
    """video_object_detection steady state: B -> Write is a near-tie, so
    neither side has an outgoing arrow and both are reported; B, which its
    upstream waits on, ranks first."""
    shares = {
        "A": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=10),
        "B": OpShares(busy=0.8, obp=0.2, starved=0.0, actors=8),
        "Write": OpShares(busy=0.85, obp=0.0, starved=0.15, actors=27),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert [op for op, _ in snapshot.bottlenecks] == ["B", "Write"]
    assert snapshot.edges[1].arrow is Arrow.TIE


def test_tie_side_that_waits_more_than_it_is_waited_on_is_dropped():
    """Within the arrow margin, but Write is starved (0.25) more than B is
    blocked on it (0.2): Write's severity is negative, so only B is named."""
    shares = {
        "A": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=10),
        "B": OpShares(busy=0.8, obp=0.2, starved=0.0, actors=8),
        "Write": OpShares(busy=0.75, obp=0.0, starved=0.25, actors=27),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert snapshot.edges[1].arrow is Arrow.TIE
    assert [op for op, _ in snapshot.bottlenecks] == ["B"]


def test_last_op_working_through_its_tail_is_not_a_bottleneck():
    """image_classification tail: every upstream op has completed and left
    the model, so nothing waits on Predictor (severity -0.05)."""
    shares = {
        "Predictor": OpShares(busy=0.93, obp=0.0, starved=0.05, actors=10),
    }
    snapshot = _analyze(shares, "ReadFiles", "Preprocess", "Predictor")

    assert snapshot.bottlenecks == ()


def test_sink_bottleneck_uses_only_its_upstream_edge():
    """A sink that limits the pipeline (the blocks_upstream case) is found
    from its producer's output backpressure alone."""
    shares = {
        "B": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=6),
        "Write": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=3),
    }
    snapshot = _analyze(shares, "B", "Write")

    assert snapshot.bottlenecks == (("Write", pytest.approx(0.9)),)


def test_ops_without_a_full_window_are_left_out():
    # Gen and Write have no shares yet: no edges, and with no measured
    # neighbour waiting on it, Labeled is not named either.
    shares = {"Labeled": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=2)}
    snapshot = _analyze(shares, "Gen", "Labeled", "Write")

    assert snapshot.edges == ()
    assert snapshot.bottlenecks == ()


def test_sink_with_no_actors_is_named_not_its_blocked_producer():
    """video_object_detection stall: Write never placed an actor, so its
    queued input is unstaffed time (one fully busy actor), and B, fully
    output-backpressured on it, points its arrow at it."""
    shares = {
        "A": OpShares(busy=0.0, obp=1.0, starved=0.0, actors=6),
        "B": OpShares(busy=0.0, obp=1.0, starved=0.0, actors=5),
        "Write": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=0, unstaffed=1.0),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert snapshot.bottlenecks == (("Write", pytest.approx(1.0)),)
    assert snapshot.edges[1].arrow is Arrow.DOWNSTREAM
    assert snapshot.slack["B"] == 5


def test_op_blocked_on_an_unmodeled_consumer_is_not_a_bottleneck():
    """An op output-backpressured nearly all the time is waiting on something
    downstream, even when that consumer has no shares to draw an arrow to."""
    shares = {
        "A": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=4),
        "B": OpShares(busy=0.05, obp=0.95, starved=0.0, actors=5),
    }
    snapshot = _analyze(shares, "A", "B", "Write")

    assert snapshot.bottlenecks == ()


@pytest.mark.parametrize(
    "actors, busy, growth, expected",
    [
        (15, 0.02, 1.5, 14),
        (10, 1.0, 1.0, -2),  # fully busy: short once margin is added
        (0, 0.5, 2.0, 0),
    ],
)
def test_slack(actors, busy, growth, expected):
    assert slack(actors, busy, growth, SLACK_MARGIN) == expected


def test_share_window_is_time_weighted_and_slides():
    window = ShareWindow(window_s=10.0)
    window.record(dt=0.0, serving=4, busy=4, obp=0, starved=0)
    window.record(dt=5.0, serving=4, busy=4, obp=0, starved=0)
    assert window.shares() is None  # half a window

    window.record(dt=5.0, serving=4, busy=0, obp=4, starved=0)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.starved) == (0.5, 0.5, 0.0)
    assert shares.actors == 4

    # Another 10s of starvation pushes the earlier samples out.
    window.record(dt=10.0, serving=2, busy=0, obp=0, starved=2)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.starved) == (0.0, 0.0, 1.0)
    assert shares.actors == 2


def test_share_window_counts_unstaffed_time_as_one_busy_actor():
    window = ShareWindow(window_s=10.0)
    window.record(dt=5.0, serving=0, busy=0, obp=0, starved=0, unstaffed=True)
    window.record(dt=5.0, serving=1, busy=0, obp=1, starved=0)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.unstaffed) == (0.5, 0.5, 0.5)
    assert shares.actors == 1

    # No actors and nothing queued is no data, not a busy stage.
    idle = ShareWindow(window_s=10.0)
    idle.record(dt=10.0, serving=0, busy=0, obp=0, starved=0)
    assert idle.shares() is None


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class _Op:
    def __init__(self, name, breakdown, completed=False):
        self.name = name
        self.id = name
        self.breakdown = breakdown
        self.completed = completed

    def has_completed(self):
        return self.completed

    def serving_actor_breakdown(self):
        return self.breakdown


class _OpState:
    def __init__(self, queued):
        self.queued = queued

    def total_enqueued_input_blocks(self):
        return self.queued


def test_observer_counts_idle_as_starved_only_with_an_empty_queue():
    producer = _Op("P", breakdown=(4, 0, 4, 0))
    consumer = _Op("C", breakdown=(2, 1, 0, 1))
    profiles = SimpleNamespace(
        actor_pool_ops=(producer, consumer),
        prev_ops={producer: (), consumer: (producer,)},
        next_ops={producer: (consumer,), consumer: ()},
    )
    # Fakes stand in for real operators and op states; the observer only
    # calls the methods defined on them.
    topology = cast(Any, {producer: _OpState(queued=0), consumer: _OpState(queued=3)})
    clock = _Clock()
    observer = BottleneckObserver(cast(Any, profiles), clock=clock, window_s=4.0)

    for _ in range(5):
        observer.observe(topology)
        clock.now += 1.0
    snapshot = observer.snapshot()
    shares = cast(Dict[Any, OpShares], snapshot.shares)

    # Consumer's idle actor has queued work: a dispatch gap, not starvation.
    assert shares[consumer].starved == 0.0
    assert shares[producer].obp == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]
    assert "bottlenecks=[C(" in format_snapshot(
        snapshot, {producer: "P", consumer: "C"}
    )

    # The consumer loses every actor with input still queued: it stays in the
    # model as unstaffed, and is still the one named.
    consumer.breakdown = (0, 0, 0, 0)
    for _ in range(5):
        observer.observe(topology)
        clock.now += 1.0
    snapshot = observer.snapshot()
    shares = cast(Dict[Any, OpShares], snapshot.shares)
    assert shares[consumer].unstaffed == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]
    assert "unstaffed=1.00" in format_snapshot(snapshot, {producer: "P", consumer: "C"})

    # A completed op leaves the model.
    producer.completed = True
    observer.observe(topology)
    assert producer not in observer.snapshot().shares


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
