"""Unit tests for the sizer's policy: the op status machine, the step sizes,
``SizingPolicy.decide_op`` (one rule per op from its status and signals) and
``SizingPolicy.decide`` (priority, capacity, cooldown). Duck-typed doubles; no
cluster."""

import sys
from collections import Counter
from types import SimpleNamespace
from typing import Any, Dict, Optional, Sequence, Tuple, cast

import pytest

from ray.data._internal.actor_autoscaler import AutoscalingActorPool
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    EdgeShares,
    OpShareBreakdown,
    Snapshot,
)
from ray.data._internal.experimental.execution.sizer.capacity_ledger import (
    ConstraintAwareCapacityLedger,
)
from ray.data._internal.experimental.execution.sizer.models import (
    ClusterStateSnapshot,
)
from ray.data._internal.experimental.execution.sizer.op_status import (
    Cooling,
    Held,
    IllegalStatusTransition,
    OpStatusKind,
    OpStatusMachine,
)
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.sizing_policy import (
    SizingPolicy,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    OpRule,
    SizerPhase,
)
from ray.data._internal.experimental.execution.sizer.policy.steps import (
    Direction,
    StepSizes,
)

# --- doubles -------------------------------------------------------------------


class _Pool:
    def __init__(self, *, current=4, min_size=1, max_size=100, cpu=1, gpu=0):
        self._current = current
        self._min_size = min_size
        self._max_size = max_size
        self._per_actor = ExecutionResources(cpu=cpu, gpu=gpu)
        self.pending = 0
        self.draining = 0

    def serving_size(self):
        return self._current

    def min_size(self):
        return self._min_size

    def max_size(self):
        return self._max_size

    bounded_max_size = AutoscalingActorPool.bounded_max_size

    def per_actor_resource_usage(self):
        return self._per_actor

    def num_pending_actors(self):
        return self.pending

    def num_draining_actors(self):
        return self.draining

    def num_reclaimable_draining_actors(self):
        return 0


class _Op:
    def __init__(self, name, pool=None):
        self.name = name
        self._pool = pool or _Pool()
        self.pending_first_input = False
        self.completed = False
        self.input_dependencies = []
        # Actors per node, as ``actor_counts_by_node`` reports them.
        self.actors_by_node: Dict[str, int] = {}

    @property
    def actor_pool(self):
        return self._pool

    def awaiting_first_input(self):
        return self.pending_first_input

    def has_completed(self):
        return self.completed

    def actor_counts_by_node(self):
        return {
            node: Counter({ActorStatus.ACTIVE: n})
            for node, n in self.actors_by_node.items()
        }


def _op(name, **pool) -> Any:
    return _Op(name, _Pool(**pool))


def _bottleneck_snapshot(
    shares: Dict[Any, Tuple[float, float, float]],
    *,
    bottlenecks: Sequence[Tuple[Any, float]] = (),
    edge_shares: Optional[Dict[Tuple[Any, Any], EdgeShares]] = None,
) -> Snapshot:
    """A bottleneck snapshot over ``shares``: op -> (busy, obp, starved)."""
    return Snapshot(
        shares={
            op: OpShareBreakdown(
                busy=busy, obp=obp, starved=starved, actors=op.actor_pool.serving_size()
            )
            for op, (busy, obp, starved) in shares.items()
        },
        edges=(),
        bottlenecks=tuple(bottlenecks),
        edge_shares=dict(edge_shares or {}),
    )


def _profiles(ops, next_ops=None):
    next_ops = {op: tuple((next_ops or {}).get(op, ())) for op in ops}
    return SimpleNamespace(
        actor_pool_ops=tuple(ops),
        non_pg_ops=tuple(ops),
        next_ops=next_ops,
        prev_ops={op: () for op in ops},
        is_pg_managed=lambda op: False,
    )


def _ctx(
    ops,
    *,
    bottleneck_snapshot=None,
    free_cpu=100.0,
    phase=SizerPhase.STEADY,
    allocation_age_s=float("inf"),
) -> DecisionContext:
    snapshot = ClusterStateSnapshot(
        schedulable_nodes_by_op={op: frozenset({"node"}) for op in ops},
        reserved_by_node={"node": ExecutionResources(cpu=100)},
        free_by_node={"node": ExecutionResources(cpu=free_cpu)},
        ordered_ops=tuple(ops),
        seconds_since_allocation_change=allocation_age_s,
    )
    return DecisionContext(
        topology=cast(Any, {op: SimpleNamespace() for op in ops}),
        phase=phase,
        capacity=ConstraintAwareCapacityLedger(snapshot),
        apmo_profiles=cast(Any, _profiles(ops)),
        snapshot=snapshot,
        now=0.0,
        bottleneck_snapshot=bottleneck_snapshot,
    )


def _policy(ops, next_ops=None, steps=None) -> Tuple[SizingPolicy, OpStatusMachine]:
    statuses = OpStatusMachine(ops=ops, cooldown_s=10.0)
    policy = SizingPolicy(
        statuses=statuses, profiles=cast(Any, _profiles(ops, next_ops)), steps=steps
    )
    return policy, statuses


def _delta(decision) -> int:
    return decision.request.delta if decision.request is not None else 0


# --- OpStatusMachine -----------------------------------------------------------


def test_status_holds_an_op_until_its_first_input_then_activates_it():
    write = _op("Write")
    write.pending_first_input = True
    statuses = OpStatusMachine(ops=[write])
    assert statuses.status(write) == Held(reserved=0)

    statuses.set_reserved(op=write, n=4)
    statuses.on_tick(now=0.0)
    assert statuses.status(write) == Held(reserved=4)  # still waiting

    write.pending_first_input = False
    statuses.on_tick(now=1.0)
    assert statuses.kind(write) is OpStatusKind.ACTIVE


def test_status_an_upscale_cools_once_its_actors_land():
    op = _op("op", current=4)
    statuses = OpStatusMachine(ops=[op], cooldown_s=10.0)
    statuses.start_cooling(op=op, now=0.0, upscale_from=4)
    op.actor_pool.pending = 2

    assert statuses.on_tick(now=0.0) == {}
    # Actors still pending: the cooldown has not started.
    assert statuses.status(op) == Cooling(until=None, upscale_from=4)

    # One pending actor expired without starting: 5 actors landed, not 6.
    op.actor_pool.pending = 0
    op.actor_pool._current = 5
    assert statuses.on_tick(now=5.0) == {op: (4, 5)}
    assert statuses.status(op) == Cooling(until=15.0)  # landed at 5s

    statuses.on_tick(now=14.0)
    assert statuses.kind(op) is OpStatusKind.COOLING
    statuses.on_tick(now=15.0)
    assert statuses.kind(op) is OpStatusKind.ACTIVE


def test_status_a_downscale_cools_from_the_decision_while_actors_drain():
    # Draining actors serve no tasks, so the window describes the new size
    # at once; a long drain does not hold the op in cooling.
    op = _op("op")
    statuses = OpStatusMachine(ops=[op], cooldown_s=10.0)
    statuses.start_cooling(op=op, now=0.0)
    op.actor_pool.draining = 3
    assert statuses.status(op) == Cooling(until=10.0)
    assert statuses.on_tick(now=10.0) == {}
    assert statuses.kind(op) is OpStatusKind.ACTIVE


def test_status_completed_is_derived_and_a_reopened_op_resumes_active():
    """Completed comes from the op itself, and overrides any stored status.
    Lineage reconstruction can reopen a completed op; it then resumes as
    active, not in whatever status it held when it completed."""
    op = _op("op")
    statuses = OpStatusMachine(ops=[op])
    statuses.start_cooling(op=op, now=0.0)
    op.completed = True
    assert statuses.kind(op) is OpStatusKind.COMPLETED
    statuses.on_tick(now=0.0)

    op.completed = False  # reopened
    assert statuses.kind(op) is OpStatusKind.ACTIVE


def test_status_a_held_op_cannot_cool():
    op = _op("op")
    op.pending_first_input = True
    statuses = OpStatusMachine(ops=[op])
    with pytest.raises(IllegalStatusTransition):
        statuses.start_cooling(op=op, now=0.0)


# --- StepSizes -----------------------------------------------------------------


@pytest.mark.parametrize(
    "moves, size, expected",
    [
        # The first step: x1.5 up, x0.8 down.
        ((), 10, {Direction.UP: 5, Direction.DOWN: -2}),
        # The same direction again doubles the step, up to x2 / x0.5.
        ((Direction.UP,), 10, {Direction.UP: 10}),
        ((Direction.UP, Direction.UP), 10, {Direction.UP: 10}),
        ((Direction.DOWN,), 10, {Direction.DOWN: -4}),
        ((Direction.DOWN, Direction.DOWN), 10, {Direction.DOWN: -5}),
        # Turning around halves the step: x0.9 after an upscale, x1.25 after
        # a downscale.
        ((Direction.UP,), 10, {Direction.DOWN: -1}),
        ((Direction.DOWN,), 8, {Direction.UP: 2}),
        # Every step moves at least one actor.
        ((), 1, {Direction.UP: 1, Direction.DOWN: -1}),
        ((), 0, {Direction.UP: 1}),
    ],
)
def test_step_sizes(moves, size, expected):
    op = _op("op")
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    for direction in moves:
        _take_step(steps, op, direction, size)
    for direction, delta in expected.items():
        assert steps.delta(op=op, direction=direction, size=size) == delta


def _take_step(steps: StepSizes, op, direction: Direction, size: int) -> int:
    """One full step of ``op`` from ``size``, recorded as it landed."""
    after = size + steps.delta(op=op, direction=direction, size=size)
    steps.record(op=op, direction=direction, before=size, after=after)
    return after


def test_step_sizes_settle_when_an_op_keeps_turning_around():
    op = _op("op")
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    sizes = [20]
    for direction in [Direction.UP, Direction.DOWN] * 6:
        sizes.append(_take_step(steps, op, direction, sizes[-1]))
    assert sizes == [20, 30, 27, 34, 32, 36, 35, 37, 36, 37, 36, 37, 36]
    # Each turn halves the step, so the swings shrink to one actor.
    swings = [abs(b - a) for a, b in zip(sizes, sizes[1:])]
    assert swings[-5:] == [1, 1, 1, 1, 1]


@pytest.mark.parametrize(
    "after, next_delta",
    [
        # Cut from +5 to +2 (0.2 moved): the next step doubles that, to 0.4.
        (12, 4),
        # Nothing landed: the next try uses the same step.
        (10, 5),
    ],
)
def test_step_sizes_adapt_to_what_landed(after, next_delta):
    op = _op("op")
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    steps.record(op=op, direction=Direction.UP, before=10, after=after)
    assert steps.delta(op=op, direction=Direction.UP, size=10) == next_delta


def test_step_sizes_reset():
    op = _op("op")
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    _take_step(steps, op, Direction.UP, 10)
    steps.reset()
    assert steps.delta(op=op, direction=Direction.UP, size=10) == 5


# --- decide_op: one rule per op from its status and signals --------------------


def test_decide_op_releases_a_completed_pool():
    op = _op("op", current=3)
    policy, _ = _policy([op])
    op.completed = True
    decision = policy.decide_op(op=op, ctx=_ctx([op]))
    assert decision.rule is OpRule.RELEASE_POOL
    request = decision.request
    assert request is not None and request.delta == -3 and request.force


def test_decide_op_leaves_a_held_op_alone():
    op = _op("op", current=0)
    op.pending_first_input = True
    policy, _ = _policy([op])
    assert policy.decide_op(op=op, ctx=_ctx([op])).rule is OpRule.NONE


@pytest.mark.parametrize("cooling", [False, True])
def test_decide_op_upscales_to_min_size_even_while_cooling(cooling):
    op = _op("op", current=1, min_size=3)
    policy, statuses = _policy([op])
    if cooling:
        statuses.start_cooling(op=op, now=0.0)
    decision = policy.decide_op(
        op=op,
        ctx=_ctx([op], bottleneck_snapshot=_bottleneck_snapshot({op: (1.0, 0, 0)})),
    )
    assert (decision.rule, _delta(decision)) == (OpRule.UPSCALE_TO_MIN, 2)


def test_decide_op_leaves_a_cooling_op_alone():
    op = _op("op", current=4)
    policy, statuses = _policy([op])
    statuses.start_cooling(op=op, now=0.0)
    bottleneck_snapshot = _bottleneck_snapshot(
        {op: (1.0, 0, 0)}, bottlenecks=[(op, 0.5)]
    )
    assert (
        policy.decide_op(
            op=op, ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot)
        ).rule
        is OpRule.NONE
    )


def test_decide_op_leaves_a_fixed_size_pool_alone():
    op = _op("op", current=4, min_size=4, max_size=4)
    policy, _ = _policy([op])
    bottleneck_snapshot = _bottleneck_snapshot(
        {op: (1.0, 0, 0)}, bottlenecks=[(op, 0.5)]
    )
    assert (
        policy.decide_op(
            op=op, ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot)
        ).rule
        is OpRule.NONE
    )


@pytest.mark.parametrize(
    "busy, obp, starved, bottleneck, expected",
    [
        # Named bottleneck: one step up (x1.5 from 4).
        (1.0, 0.0, 0.0, True, (OpRule.UPSCALE_BOTTLENECK, 2)),
        # No bottleneck named and nearly always busy: one step up.
        (0.95, 0.0, 0.0, False, (OpRule.UPSCALE_BUSY, 2)),
        # Blocked by its consumer: one step down (x0.8).
        (0.1, 0.85, 0.05, False, (OpRule.DOWNSCALE_BLOCKED, -1)),
        # Idle: one step down.
        (0.02, 0.0, 0.98, False, (OpRule.DOWNSCALE_IDLE, -1)),
        # Wasting 0.2, but 3 actors would be busy 0.8 x 4/3 > 0.9: no change.
        (0.6, 0.2, 0.2, False, (OpRule.NONE, 0)),
        # Wasting less than the noise margin: no change.
        (0.85, 0.05, 0.05, False, (OpRule.NONE, 0)),
    ],
)
def test_decide_op_steps_an_active_op_from_its_window(
    busy, obp, starved, bottleneck, expected
):
    op = _op("op", current=4)
    policy, _ = _policy([op])
    bottleneck_snapshot = _bottleneck_snapshot(
        {op: (busy, obp, starved)}, bottlenecks=[(op, 0.5)] if bottleneck else ()
    )
    decision = policy.decide_op(
        op=op, ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot)
    )
    assert (decision.rule, _delta(decision)) == expected


def test_decide_op_does_not_upscale_busy_ops_while_a_bottleneck_is_named():
    busy, named = _op("busy"), _op("named")
    policy, _ = _policy([busy, named])
    bottleneck_snapshot = _bottleneck_snapshot(
        {busy: (0.95, 0, 0), named: (1.0, 0, 0)}, bottlenecks=[(named, 0.5)]
    )
    assert (
        policy.decide_op(
            op=busy, ctx=_ctx([busy, named], bottleneck_snapshot=bottleneck_snapshot)
        ).rule
        is OpRule.NONE
    )


def test_decide_op_does_not_upscale_a_bottleneck_behind_a_more_severe_one():
    upstream, downstream = _op("upstream"), _op("downstream")
    policy, _ = _policy([upstream, downstream], next_ops={upstream: [downstream]})
    bottleneck_snapshot = _bottleneck_snapshot(
        {upstream: (1.0, 0, 0), downstream: (1.0, 0, 0)},
        bottlenecks=[(upstream, 0.6), (downstream, 0.3)],
    )
    ctx = _ctx([upstream, downstream], bottleneck_snapshot=bottleneck_snapshot)
    assert policy.decide_op(op=downstream, ctx=ctx).rule is OpRule.NONE
    assert policy.decide_op(op=upstream, ctx=ctx).rule is OpRule.UPSCALE_BOTTLENECK


@pytest.mark.parametrize("consumer_starved, downscales", [(0.04, True), (0.1, False)])
def test_decide_op_keeps_a_blocked_op_that_feeds_a_starving_op(
    consumer_starved, downscales
):
    producer, consumer = _op("producer"), _op("consumer")
    policy, _ = _policy([producer, consumer], next_ops={producer: [consumer]})
    bottleneck_snapshot = _bottleneck_snapshot(
        {producer: (0.1, 0.85, 0.05), consumer: (0.5, 0, consumer_starved)},
        edge_shares={(producer, consumer): EdgeShares(blocked=0.85, self_limited=0)},
    )
    decision = policy.decide_op(
        op=producer,
        ctx=_ctx([producer, consumer], bottleneck_snapshot=bottleneck_snapshot),
    )
    assert (decision.rule is OpRule.DOWNSCALE_BLOCKED) == downscales


@pytest.mark.parametrize(
    "size, starved, earlier_downscales, delta",
    [
        # Half the slot time idle: one x0.8 step, above the floor of
        # ceil(100 x 0.5 / 0.9) = 56.
        (100, 0.5, 0, -20),
        # Idle 0.15: the floor ceil(100 x 0.85 / 0.9) = 95 cuts the x0.8 step.
        (100, 0.15, 0, -5),
        # The doubled step (x0.6 -> 48) stops at the floor
        # ceil(80 x 0.625 / 0.9) = 56.
        (80, 0.375, 1, -24),
    ],
)
def test_decide_op_downscales_no_further_than_keeps_the_op_below_busy(
    size, starved, earlier_downscales, delta
):
    op = _op("op", current=size, max_size=200)
    policy, _ = _policy([op])
    for _ in range(earlier_downscales):
        _take_step(policy.steps, op, Direction.DOWN, 10)
    bottleneck_snapshot = _bottleneck_snapshot({op: (1.0 - starved, 0.0, starved)})
    decision = policy.decide_op(
        op=op, ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot)
    )
    assert (decision.rule, _delta(decision)) == (OpRule.DOWNSCALE_IDLE, delta)


def test_decide_op_never_downscales_below_min_size():
    op = _op("op", current=2, min_size=2)
    policy, _ = _policy([op])
    bottleneck_snapshot = _bottleneck_snapshot({op: (0.0, 0.0, 1.0)})
    assert (
        policy.decide_op(
            op=op, ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot)
        ).rule
        is OpRule.NONE
    )


def test_decide_op_upscales_no_further_than_the_pool_max():
    op = _op("op", current=9, max_size=10)
    policy, _ = _policy([op])
    bottleneck_snapshot = _bottleneck_snapshot(
        {op: (1.0, 0, 0)}, bottlenecks=[(op, 0.5)]
    )
    decision = policy.decide_op(
        op=op, ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot)
    )
    assert (decision.rule, _delta(decision)) == (OpRule.UPSCALE_BOTTLENECK, 1)


def test_decide_op_does_nothing_for_an_op_the_model_has_not_seen():
    op = _op("op")
    policy, _ = _policy([op])
    assert (
        policy.decide_op(
            op=op, ctx=_ctx([op], bottleneck_snapshot=_bottleneck_snapshot({}))
        ).rule
        is OpRule.NONE
    )


# --- decide: priority, capacity and cooldown -----------------------------------


def test_decide_grants_free_capacity_in_priority_order():
    """Two CPUs free: the op below min size takes one first, then the more
    severe bottleneck takes the other; the less severe one gets nothing."""
    below_min = _op("below_min", current=0, min_size=1)
    mild = _op("mild", current=2)
    severe = _op("severe", current=2)
    ops = [mild, severe, below_min]
    policy, statuses = _policy(ops)
    bottleneck_snapshot = _bottleneck_snapshot(
        {mild: (1.0, 0, 0), severe: (1.0, 0, 0)},
        bottlenecks=[(severe, 0.6), (mild, 0.3)],
    )
    decisions = policy.decide(
        ctx=_ctx(ops, bottleneck_snapshot=bottleneck_snapshot, free_cpu=2)
    )
    assert list(decisions) == [below_min, severe, mild]
    assert _delta(decisions[below_min]) == 1
    assert _delta(decisions[severe]) == 1  # wanted 1 (x1.5 of 2), got 1
    assert decisions[mild].rule is OpRule.UPSCALE_BOTTLENECK
    assert decisions[mild].request is None  # no room left
    # The part that found no room stays on the decision, for WHERE.
    assert decisions[mild].unmet == 1
    assert decisions[severe].unmet == 0
    # Only the op the policy stepped cools down.
    assert statuses.kind(severe) is OpStatusKind.COOLING
    assert statuses.kind(mild) is OpStatusKind.ACTIVE
    assert statuses.kind(below_min) is OpStatusKind.ACTIVE


def test_decide_adapts_the_step_only_for_a_resize_it_made():
    op = _op("op", current=10)
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    policy, statuses = _policy([op], steps=steps)
    bottleneck_snapshot = _bottleneck_snapshot(
        {op: (1.0, 0, 0)}, bottlenecks=[(op, 0.5)]
    )

    # No room: no resize, so the step stays and the op does not cool.
    policy.decide(ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot, free_cpu=0))
    assert statuses.kind(op) is OpStatusKind.ACTIVE
    assert steps.delta(op=op, direction=Direction.UP, size=10) == 5

    policy.decide(ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot))
    assert statuses.status(op) == Cooling(until=None, upscale_from=10)
    # The step adapts once the actors land, not at the decision.
    assert steps.delta(op=op, direction=Direction.UP, size=10) == 5

    # 3 of the 5 actors started: 0.3 moved, so the next step is 0.6.
    op.actor_pool._current = 13
    policy.record_landed_upscales(landed=statuses.on_tick(now=1.0))
    assert steps.delta(op=op, direction=Direction.UP, size=10) == 6


def test_decide_records_a_downscale_at_the_decision():
    op = _op("op", current=10)
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    policy, statuses = _policy([op], steps=steps)
    policy.decide(
        ctx=_ctx([op], bottleneck_snapshot=_bottleneck_snapshot({op: (0.0, 0.0, 1.0)}))
    )
    assert statuses.status(op) == Cooling(until=10.0)
    assert steps.delta(op=op, direction=Direction.DOWN, size=10) == -4


def test_decide_resets_every_step_when_the_allocation_changes():
    op = _op("op", current=10)
    steps = StepSizes(upscale_step=0.5, downscale_step=0.2)
    policy, _ = _policy([op], steps=steps)
    bottleneck_snapshot = _bottleneck_snapshot({op: (0.0, 0.0, 1.0)})
    policy.decide(
        ctx=_ctx([op], bottleneck_snapshot=bottleneck_snapshot, allocation_age_s=100.0)
    )
    assert steps.delta(op=op, direction=Direction.DOWN, size=10) == -4

    # The allocation changed since the last tick: its age went down.
    policy.decide(
        ctx=_ctx(
            [op], bottleneck_snapshot=_bottleneck_snapshot({}), allocation_age_s=1.0
        )
    )
    assert steps.delta(op=op, direction=Direction.DOWN, size=10) == -2


@pytest.mark.parametrize(
    "producer_nodes, gpu_free_cpus, downscales",
    [
        # The GPU op is short, and g1 has its GPU free but no CPU free: the
        # producer's actors on g1 make room for it, so it downscales.
        pytest.param({"g1": 2, "g2": 2}, 0, True, id="frees-room-for-bottleneck"),
        # The producer holds actors only on g2, whose GPU the GPU op already
        # uses: removing them frees CPUs it cannot use.
        pytest.param({"g2": 4}, 0, False, id="frees-the-wrong-node"),
        # g1 has a free CPU, so the GPU op gets its actor and nobody is
        # short: the blocked actors keep their queued work.
        pytest.param({"g1": 2, "g2": 2}, 1, False, id="nobody-short"),
    ],
)
def test_decide_downscales_a_blocked_op_only_to_make_room(
    producer_nodes, gpu_free_cpus, downscales
):
    producer = _op("producer", current=4, cpu=1)
    producer.actors_by_node = producer_nodes
    gpu_op = _op("gpu_op", current=1, cpu=1, gpu=1)
    ops = [producer, gpu_op]
    policy, _ = _policy(ops, next_ops={producer: [gpu_op]})
    snapshot = ClusterStateSnapshot(
        schedulable_nodes_by_op={op: frozenset({"g1", "g2"}) for op in ops},
        reserved_by_node={
            "g1": ExecutionResources(cpu=8, gpu=1),
            "g2": ExecutionResources(cpu=8, gpu=1),
        },
        free_by_node={
            "g1": ExecutionResources(cpu=gpu_free_cpus, gpu=1),
            "g2": ExecutionResources(cpu=3, gpu=0),
        },
        ordered_ops=tuple(ops),
    )
    ctx = DecisionContext(
        topology=cast(Any, {op: SimpleNamespace() for op in ops}),
        phase=SizerPhase.STEADY,
        capacity=ConstraintAwareCapacityLedger(snapshot),
        apmo_profiles=cast(Any, _profiles(ops)),
        snapshot=snapshot,
        now=0.0,
        bottleneck_snapshot=_bottleneck_snapshot(
            {producer: (0.1, 0.9, 0.0), gpu_op: (1.0, 0.0, 0.0)},
            bottlenecks=[(gpu_op, 0.9)],
        ),
    )
    decisions = policy.decide(ctx=ctx)
    assert decisions[gpu_op].rule is OpRule.UPSCALE_BOTTLENECK
    assert (decisions[gpu_op].unmet > 0) is (gpu_free_cpus == 0)
    expected = OpRule.DOWNSCALE_BLOCKED if downscales else OpRule.NONE
    assert decisions[producer].rule is expected
    assert (decisions[producer].request is not None) is downscales


def test_decide_still_downscales_a_kept_blocked_op_that_is_idle():
    """A blocked op the gate keeps can still downscale as idle: starved
    actors have no queued work to lose."""
    producer = _op("producer", current=10)
    gpu_op = _op("gpu_op", current=1, cpu=1, gpu=1)
    ops = [producer, gpu_op]
    policy, _ = _policy(ops, next_ops={producer: [gpu_op]})
    ctx = _ctx(
        ops,
        bottleneck_snapshot=_bottleneck_snapshot(
            {producer: (0.1, 0.5, 0.4), gpu_op: (0.5, 0.0, 0.0)}
        ),
    )
    decision = policy.decide(ctx=ctx)[producer]
    assert decision.rule is OpRule.DOWNSCALE_IDLE
    assert decision.request is not None and decision.request.delta < 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
