"""Tests for the observation-only bottleneck model."""

import math
from types import SimpleNamespace
from typing import Any, Dict, Sequence, Tuple, cast

import pytest

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    SlotUsage,
)
from ray.data._internal.experimental.execution.sizer import bottleneck_observer
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    Arrow,
    BottleneckObserver,
    DonorVictims,
    EdgeShares,
    OpShares,
    ShareWindow,
    StableBottlenecks,
    analyze,
    classify_edge,
    format_snapshot,
    growth_target,
    is_circular,
    slack,
    slack_toward,
    time_to_free,
    upstream_limiters,
)

ARROW_MARGIN = 0.1
SLACK_MARGIN = 0.2
MIN_SEVERITY = 0.1


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
    """B -> C is a near-tie (0.2 vs 0.15), so neither has an outgoing arrow;
    both have neighbours waiting on them (A on B, D on C), so both are
    reported, B first."""
    shares = {
        "A": OpShares(busy=0.1, obp=0.9, starved=0.0, actors=10),
        "B": OpShares(busy=0.8, obp=0.2, starved=0.0, actors=8),
        "C": OpShares(busy=0.85, obp=0.0, starved=0.15, actors=6),
        "D": OpShares(busy=0.7, obp=0.0, starved=0.3, actors=4),
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
        "ReadFiles": OpShares(busy=0.99, obp=0.0, starved=0.0, actors=33),
        "Preprocess": OpShares(busy=0.58, obp=0.38, starved=0.03, actors=31),
        "Predictor": OpShares(busy=0.73, obp=0.0, starved=0.16, actors=7),
    }
    snapshot = _analyze(shares, "ReadFiles", "Preprocess", "Predictor")

    assert snapshot.edges[0].arrow is Arrow.TIE
    assert [op for op, _ in snapshot.bottlenecks] == ["Predictor"]


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
        "A": OpShares(busy=0.0, obp=1.0, starved=0.0, actors=7),
        "B": OpShares(busy=0.2, obp=0.8, starved=0.0, actors=1),
        "W": OpShares(
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
        slack_margin=SLACK_MARGIN,
        min_severity=MIN_SEVERITY,
        edge_shares=edge_shares,
    )

    assert [op for op, _ in snapshot.bottlenecks] == [expected]


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


def test_share_window_is_slot_time_weighted_and_slides():
    window = ShareWindow(window_s=10.0)
    window.record(0.0, actors=4, slot_s=0, busy_s=0, obp_s=0, starved_s=0)
    window.record(5.0, actors=4, slot_s=20, busy_s=20, obp_s=0, starved_s=0)
    assert window.shares() is None  # half a window

    window.record(5.0, actors=4, slot_s=20, busy_s=0, obp_s=20, starved_s=0)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.starved) == (0.5, 0.5, 0.0)
    assert shares.actors == 4

    # Another 10s of starvation pushes the earlier samples out.
    window.record(10.0, actors=2, slot_s=20, busy_s=0, obp_s=0, starved_s=20)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.starved) == (0.0, 0.0, 1.0)
    assert shares.actors == 2


def test_share_window_counts_unstaffed_time_as_one_busy_slot():
    window = ShareWindow(window_s=10.0)
    window.record(
        5.0, actors=0, slot_s=0, busy_s=0, obp_s=0, starved_s=0, unstaffed=True
    )
    window.record(5.0, actors=1, slot_s=5, busy_s=0, obp_s=5, starved_s=0)
    shares = window.shares()
    assert shares is not None
    assert (shares.busy, shares.obp, shares.unstaffed) == (0.5, 0.5, 0.5)
    assert shares.actors == 1

    # No actors and nothing queued is no data, not a busy stage.
    idle = ShareWindow(window_s=10.0)
    idle.record(10.0, actors=0, slot_s=0, busy_s=0, obp_s=0, starved_s=0)
    assert idle.shares() is None


@pytest.mark.parametrize(
    "work, rate, expected",
    [(8, 2.0, 4.0), (0, 0.0, 0.0), (3, 0.0, math.inf)],
    ids=["drains", "nothing-to-drain", "never-consumed"],
)
def test_time_to_free(work, rate, expected):
    assert time_to_free(work, rate) == expected


def test_circular_only_when_every_op_between_is_output_backpressured():
    shares = {
        "R": OpShares(busy=0.0, obp=1.0, starved=0.0, actors=7),
        "B": OpShares(busy=0.05, obp=0.95, starved=0.0, actors=2),
        "W": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=2),
    }
    _, next_ops = _chain("R", "B", "W")
    assert is_circular("R", "W", shares, next_ops, ARROW_MARGIN)
    assert is_circular("B", "W", shares, next_ops, ARROW_MARGIN)  # adjacent
    assert not is_circular("W", "R", shares, next_ops, ARROW_MARGIN)  # upstream
    flowing = dict(shares, B=OpShares(busy=0.7, obp=0.3, starved=0.0, actors=2))
    assert not is_circular("R", "W", flowing, next_ops, ARROW_MARGIN)


def test_stable_after_a_full_window_named_and_until_a_full_window_unnamed():
    stable = StableBottlenecks(hold_s=10.0)
    present = {"A", "B"}
    assert stable.update({"A"}, present, now=0.0) == set()
    assert stable.update({"A"}, present, now=9.0) == set()
    assert stable.update({"A"}, present, now=10.0) == {"A"}

    # A one-tick blip of B neither unmakes A nor makes B.
    assert stable.update({"B"}, present, now=12.0) == {"A"}
    assert stable.update({"A"}, present, now=14.0) == {"A"}

    # B takes over: A stays stable until it has gone a full window unnamed.
    assert stable.update({"B"}, present, now=16.0) == {"A"}
    assert stable.update({"B"}, present, now=25.0) == {"A"}
    assert stable.update({"B"}, present, now=26.0) == {"B"}

    # An op leaving the model (e.g. completed) is dropped at once.
    assert stable.update(set(), {"A"}, now=27.0) == set()


def test_slack_scales_only_ops_on_the_recipients_path():
    """A -> B, and D on a branch that never meets B. Growing B from 2 to 3
    (b'/b = 1.5) pushes 1.5x the flow through A; D carries the same flow as
    before, so it keeps factor 1."""
    shares = {
        "A": OpShares(busy=0.5, obp=0.5, starved=0.0, actors=10),
        "B": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=2),
        "D": OpShares(busy=0.5, obp=0.5, starved=0.0, actors=10),
    }
    next_ops = {"A": ("B",), "B": (), "D": ()}
    by_op = slack_toward(shares, "B", 3, next_ops, SLACK_MARGIN)

    # A: 10 - ceil(10 x 0.5 x 1.5 x 1.2) = 10 - 9; D: 10 - ceil(10 x 0.5 x 1.2) = 4.
    assert by_op == {"A": 1, "D": 4}
    # No recipient: every op at factor 1.
    assert slack_toward(shares, None, 0, next_ops, SLACK_MARGIN)["A"] == 4


@pytest.mark.parametrize(
    "producer_blocked, consumer_starved, actors, expected",
    [
        # Producer blocked 20%: it could make 1.25x, so 8 -> 10.
        (0.2, 0.0, 8, 10),
        # Consumer starved 50%: 2x, capped at 1.5x, so 6 -> 9.
        (0.0, 0.5, 6, 9),
        # Nothing waits much: still at least one more.
        (0.01, 0.0, 4, 5),
        # Fully blocked producer: the step cap decides.
        (1.0, 0.0, 4, 6),
        # No actors yet: at least one.
        (1.0, 0.0, 0, 1),
    ],
)
def test_growth_target_follows_how_long_neighbours_wait(
    producer_blocked, consumer_starved, actors, expected
):
    shares = {
        "A": OpShares(busy=0.0, obp=producer_blocked, starved=0.0, actors=10),
        "B": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=actors),
        "C": OpShares(busy=0.5, obp=0.0, starved=consumer_starved, actors=4),
    }
    prev_ops, next_ops = _chain("A", "B", "C")
    edge_shares = {("A", "B"): EdgeShares(blocked=producer_blocked, self_limited=0.0)}
    assert (
        growth_target("B", shares, prev_ops, next_ops, edge_shares, 1.5, 100)
        == expected
    )


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class _Pool:
    def __init__(self, states=None, max_size=10):
        self.states = states or {}
        self._max_size = max_size

    def serving_size(self):
        return sum(len(by_actor) for by_actor in self.states.values())

    def max_size(self):
        return self._max_size

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
    shares = cast(Dict[Any, OpShares], snapshot.shares)

    # Consumer's idle actor has queued work: a dispatch gap, not starvation.
    assert shares[consumer].starved == 0.0
    assert shares[consumer].busy == 0.5
    assert shares[producer].obp == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]
    assert "bottlenecks=[C(" in format_snapshot(
        snapshot, {producer: "P", consumer: "C"}
    )

    # With its queue empty, the consumer's idle slot is starved.
    topology[consumer].queued = 0
    step(5)
    assert (
        cast(Dict[Any, OpShares], observer.snapshot().shares)[consumer].starved == 0.5
    )

    # The consumer loses every actor with input queued: it stays in the
    # model as unstaffed, and is still the one named.
    topology[consumer].queued = 3
    consumer.serving, consumer.busy_slots = 0, 0.0
    step(5)
    snapshot = observer.snapshot()
    shares = cast(Dict[Any, OpShares], snapshot.shares)
    assert shares[consumer].unstaffed == 1.0
    assert [op for op, _ in snapshot.bottlenecks] == [consumer]
    assert "unstaffed=1.00" in format_snapshot(snapshot, {producer: "P", consumer: "C"})

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


def test_victim_ttf_is_outputs_and_in_flight_work_over_consume_rate():
    producer = _Op("P", serving=2)
    consumer = _Op("C", serving=1, busy_slots=1.0)
    producer.metrics.num_task_outputs_generated = 30
    producer.metrics.num_tasks_finished = 10  # 3 outputs per task
    observer, _, step = _observer(producer, consumer, queued=(0, 1))
    for _ in range(5):
        consumer.metrics.num_task_inputs_processed += 2  # 2 blocks/s
        step()

    # No task duration known yet: (4 unconsumed + 2 tasks x 3 outputs) / 2/s.
    assert observer.victim_ttf(cast(Any, producer), 2, 4) == pytest.approx(5.0)
    # 30s tasks: the 2 in flight (concurrency 1) finish first, 2 x 30s.
    producer.metrics.average_total_task_completion_time_s = 30.0
    assert observer.victim_ttf(cast(Any, producer), 2, 4) == pytest.approx(65.0)
    # An idle actor with nothing buffered frees at once.
    assert observer.victim_ttf(cast(Any, producer), 0, 0) == 0.0


def test_short_tasks_between_samples_count_as_busy():
    # Each 1s step the op ran 0.4 slot-seconds of tasks that started and
    # finished between samples: per-step sampling would see it idle.
    op = _Op("C", serving=1, busy_slots=0.4)
    upstream = _Op("P", serving=2, obp_actors=2)
    observer, _, step = _observer(upstream, op, queued=(0, 5))
    step(5)
    shares = cast(Dict[Any, OpShares], observer.snapshot().shares)
    assert shares[op].busy == pytest.approx(0.4)


GiB = 2**30
_NEEDS = ExecutionResources(cpu=1, memory=2.4 * GiB)


@pytest.mark.parametrize(
    "free_mem_gib, write_nodes, held, expected",
    [
        (3, {"n1"}, None, "yes: free"),
        # The same room, held for an open transfer's recipient, is not free.
        (3, {"n1"}, ExecutionResources(cpu=1, memory=3 * GiB), "yes: victims"),
        (0.5, {"n1"}, None, "yes: victims"),
        (0.5, {"n3"}, None, "no: short {cpu=1, memory=2.4GiB} after all victims"),
    ],
    ids=["free-room", "free-room-held", "victims-on-its-node", "no-actors-on-its-node"],
)
def test_assessment_eligibility_and_victims(
    monkeypatch, free_mem_gib, write_nodes, held, expected
):
    """video_object_detection stall: Write (W) is the bottleneck. Read (R)
    actors sit on n1; the GPU op (B) is on n2. Victims are listed only for an
    eligible recipient, from donors with actors on its nodes, and R's are
    circular: their outputs clear through B, which is backpressured on W."""

    def actors(n):
        return {f"a{i}": SimpleNamespace(status=ActorStatus.ACTIVE) for i in range(n)}

    read = _Op("R", serving=2, obp_actors=2)
    read.actor_pool = _Pool({"n1": actors(2)})
    read.metrics.num_task_outputs_generated = 20
    read.metrics.num_tasks_finished = 10
    gpu = _Op("B", serving=1, obp_actors=1)
    gpu.actor_pool = _Pool({"n2": actors(1)})
    write = _Op("W", serving=1, busy_slots=1.0)
    write.actor_pool = _Pool({"n2": actors(1)}, max_size=4)
    observer, _, step = _observer(read, gpu, write, queued=(5, 5, 5))
    for _ in range(5):
        gpu.metrics.num_task_inputs_processed += 1  # B consumes 1 block/s
        step()

    # R's two actors on n1: (tasks in flight, unconsumed outputs).
    views = {read: {"n1": [(0, 4), (1, 2)]}, gpu: {"n2": [(0, 1)]}}
    monkeypatch.setattr(
        bottleneck_observer.placement_view,
        "build_op_placement_view",
        lambda op, rank_victims: SimpleNamespace(victim_drain_cost_by_node=views[op]),
    )
    zero = ExecutionResources(cpu=0, memory=0)
    capacity = cast(
        Any,
        SimpleNamespace(
            free_by_node={
                "n1": ExecutionResources(cpu=1, memory=free_mem_gib * GiB),
                "n2": zero,
                "n3": zero,
            },
            schedulable_nodes_by_op={read: {"n1"}, gpu: {"n2"}, write: write_nodes},
            per_actor_resources_by_op={
                read: _NEEDS,
                gpu: ExecutionResources(cpu=1, gpu=1, memory=3.3 * GiB),
                write: _NEEDS,
            },
        ),
    )

    snapshot = observer.snapshot()
    # Named but not yet stable: eligibility is shown, but there is no
    # recipient, so no victims.
    assert observer.assess(snapshot, capacity, stable=set()).recipient is None
    # Still stable but no longer waited on (R's severity is below the floor):
    # stability does not make it a recipient.
    lingering = observer.assess(snapshot, capacity, stable=cast(Any, {read}))
    assert lingering.recipient is None and lingering.stable[0][0] is read
    assessment = observer.assess(
        snapshot,
        capacity,
        stable=cast(Any, {write}),
        held_by_node={"n1": held} if held is not None else None,
    )

    assert [op for op, _ in snapshot.bottlenecks] == [write]
    assert assessment.stable == ((write, pytest.approx(1.0)),)
    assert assessment.eligibility == {write: expected}
    line = format_snapshot(snapshot, {read: "R", gpu: "B", write: "W"}, assessment)
    assert "stable=[W(sev=1.00)]" in line
    assert f"eligible=[W: {expected}]" in line
    if expected.startswith("no:"):
        assert assessment.recipient is None and "victims for" not in line
    elif write_nodes == {"n1"}:
        # 4 outputs, and 2 outputs + 1 task x 2 outputs/task, at 1 block/s.
        assert assessment.victims == {
            read: DonorVictims(count=2, ttf_min_s=4.0, ttf_p50_s=4.0, circular=True)
        }
        assert "victims for W=[R n=2 ttf min=4s p50=4s circular]" in line


def _edge(producer, consumer, arrow):
    return bottleneck_observer.Edge(
        producer=producer,
        consumer=consumer,
        producer_blocked=0.0,
        producer_self_limited=0.0,
        consumer_starved=0.0,
        arrow=arrow,
    )


_VIDEO_NEXT = {"R": ("Z",), "Z": ("E",), "E": ("W",), "W": ()}


@pytest.mark.parametrize(
    "arrows, expected",
    [
        # 22:03 in video: Read limits resize (self 0.96); downstream edges
        # point at Write.
        ((Arrow.UPSTREAM, Arrow.DOWNSTREAM, Arrow.DOWNSTREAM), {"Z": {"R"}}),
        # 23:25: resize limits both ExtractImageFeatures and, through it,
        # Write: Write's limiter is resize, the top of the upstream chain.
        (
            (Arrow.DOWNSTREAM, Arrow.UPSTREAM, Arrow.UPSTREAM),
            {"E": {"Z"}, "W": {"Z"}},
        ),
        ((Arrow.TIE, Arrow.DOWNSTREAM, Arrow.TIE), {}),
    ],
    ids=["read-limits-resize", "resize-limits-chain", "no-upstream-limit"],
)
def test_upstream_limiters_follow_upstream_arrows(arrows, expected):
    chain = ("R", "Z", "E", "W")
    edges = [_edge(p, c, a) for p, c, a in zip(chain, chain[1:], arrows)]
    assert upstream_limiters(edges) == {k: frozenset(v) for k, v in expected.items()}


def test_starved_time_is_reserve_except_toward_its_limiter():
    """video at 22:03: resize (Z) is starved because Read (R) is limited by
    its own budget. Its starved time is not spare toward Write (W), only
    toward Read; ExtractImageFeatures (E), blocked by Write, keeps its slack."""
    shares = {
        "R": OpShares(busy=0.04, obp=0.96, starved=0.0, actors=1),
        "Z": OpShares(busy=0.16, obp=0.38, starved=0.45, actors=6),
        "E": OpShares(busy=0.40, obp=0.60, starved=0.0, actors=4),
        "W": OpShares(busy=0.97, obp=0.0, starved=0.02, actors=9),
    }
    edges = [
        _edge("R", "Z", Arrow.UPSTREAM),
        _edge("Z", "E", Arrow.DOWNSTREAM),
        _edge("E", "W", Arrow.DOWNSTREAM),
    ]
    to_write = slack_toward(shares, "W", 13, _VIDEO_NEXT, SLACK_MARGIN, edges)
    # Z: 6 - ceil(6 x (0.16 + 0.45) x 13/9 x 1.2) = 6 - 7; without the edge
    # rule it would be 6 - ceil(6 x 0.16 x 13/9 x 1.2) = 4.
    assert to_write["Z"] == -1
    assert slack_toward(shares, "W", 13, _VIDEO_NEXT, SLACK_MARGIN)["Z"] == 4
    # E: 4 - ceil(4 x 0.40 x 13/9 x 1.2) = 1.
    assert to_write["E"] == 1
    # Toward Read, its limiter, Z's starved time is spare:
    # 6 - ceil(6 x 0.16 x 2 x 1.2) = 3.
    assert slack_toward(shares, "R", 2, _VIDEO_NEXT, SLACK_MARGIN, edges)["Z"] == 3


def test_starved_time_is_spare_when_the_producer_keeps_up():
    """X is starved, but its producer P is not what limits the edge (a tie):
    the downstream op is. X's idle time is spare toward the downstream
    recipient."""
    shares = {
        "P": OpShares(busy=0.5, obp=0.3, starved=0.0, actors=4),
        "X": OpShares(busy=0.2, obp=0.0, starved=0.4, actors=10),
        "D": OpShares(busy=1.0, obp=0.0, starved=0.0, actors=2),
    }
    next_ops = {"P": ("X",), "X": ("D",), "D": ()}
    edges = [_edge("P", "X", Arrow.TIE), _edge("X", "D", Arrow.DOWNSTREAM)]
    # X: 10 - ceil(10 x 0.2 x 3/2 x 1.2) = 10 - 4.
    assert slack_toward(shares, "D", 3, next_ops, SLACK_MARGIN, edges)["X"] == 6


@pytest.mark.parametrize(
    "named, self_limited, op, expected",
    [
        # 21:57:00 in video: resize (C) took over while the GPU op (B) was
        # still named; B's input was empty, so it is no recipient.
        ([("C", 0.92), ("B", 0.45)], 0.0, "B", "no: C upstream is more severe"),
        ([("C", 0.92), ("B", 0.45)], 0.0, "C", None),
        # Stable but no longer named: nothing waits on it.
        ([("C", 0.92)], 0.0, "B", "no: not named this tick"),
        # 22:02:39: resize grew while Read was turning self-limited; its new
        # actors starved.
        ([("C", 0.97)], 0.12, "C", "no: input from R self-limited (0.12)"),
        ([("C", 0.97)], 0.05, "C", None),
    ],
    ids=[
        "upstream-more-severe",
        "most-severe-upstream",
        "not-named",
        "input-self-limited",
        "self-below-margin",
    ],
)
def test_growth_blocker(named, self_limited, op, expected):
    """An op can use more actors only if named this tick, with no more
    severe named op feeding it and no self-limited producer feeding it."""
    ops = {name: _Op(name, serving=1) for name in "RCBW"}
    observer, _, _ = _observer(*ops.values(), queued=(0, 0, 0, 0))
    edge = bottleneck_observer.Edge(
        producer=ops["R"],
        consumer=ops["C"],
        producer_blocked=1.0 - self_limited,
        producer_self_limited=self_limited,
        consumer_starved=0.0,
        arrow=Arrow.DOWNSTREAM,
    )
    snapshot = bottleneck_observer.Snapshot(
        shares={},
        edges=(edge,),
        bottlenecks=tuple((ops[name], sev) for name, sev in named),
        slack={},
    )
    assert observer._growth_blocker(cast(Any, ops[op]), cast(Any, snapshot)) == (
        expected
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
