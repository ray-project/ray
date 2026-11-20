"""Unit tests for the pipeline optimizer's rules (``CapacityTransfer``,
``SilentBottleneckGrant``, ``BottleneckTransfer``), the signal bookkeeping
they decide on, and the ``OpScalingGuard`` they share. Duck-typed doubles; no
cluster."""
import logging
import math
import sys
from types import SimpleNamespace
from typing import Any, Dict, cast

import pytest

from ray.data._internal.execution.execution_flags import SIZER_TRANSFER_TIMEOUT_S
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (
    PlacementConstraint,
)
from ray.data._internal.experimental.execution.sizer import (
    bottleneck_observer,
    placement_view,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    OpPlacementView,
)
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    PG_ELIGIBLE,
)
from ray.data._internal.experimental.execution.sizer.models import OpShortfall
from ray.data._internal.experimental.execution.sizer.optimizer.bottleneck_transfer import (  # noqa: E501
    BottleneckTransfer,
    transfer_timeout_s,
)
from ray.data._internal.experimental.execution.sizer.optimizer.capacity_transfer import (  # noqa: E501
    THRASH_WINDOW_S,
    CapacityTransfer,
    DonorCapacity,
    RecentTransfer,
    TransferInFlight,
    VictimCandidate,
    _plan_node,
    assess_donor,
    plan_transfer,
)
from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)
from ray.data._internal.experimental.execution.sizer.optimizer.signal_observer import (  # noqa: E501
    SignalObserver,
)
from ray.data._internal.experimental.execution.sizer.optimizer.silent_bottleneck import (  # noqa: E501
    SilentBottleneckGrant,
)
from ray.data._internal.experimental.execution.sizer.scaling_guard import OpScalingGuard
from ray.data._internal.experimental.execution.sizer.signals import (
    BoolStreak,
    WindowedSignal,
)


@pytest.fixture(autouse=True)
def _views_from_fake_ops(monkeypatch):
    """The doubles serve canned placement views from ``build_placement_view``;
    route the rules' view builder there."""
    monkeypatch.setattr(
        placement_view,
        "build_op_placement_view",
        lambda op, *, rank_victims: op.build_placement_view(rank_victims=rank_victims),
    )


class _Metrics:
    def __init__(self):
        self.actions = []
        self.refusals = []
        self.transfers = []

    def record_outcome(self, optimization, operator, outcome):
        self.actions.append((optimization, operator, outcome))

    def record_refusal(self, optimization, reason):
        self.refusals.append((optimization, reason))

    def record_capacity_transfer(self, donor, recipient, num_actors):
        self.transfers.append((donor, recipient, num_actors))


class _Placement:
    """Ranks nodes by id, so plans are deterministic."""

    def rank_node(self, _view, node):
        return (node,)


class _Pool:
    def __init__(
        self,
        *,
        current=1,
        min_size=1,
        pending=0,
        in_flight=0,
        concurrency=1,
        cpu=1,
        gpu=0,
        pending_ids=None,
        lost_ids=(),
        running_ids=(),
    ):
        self._current = current
        self._min_size = min_size
        self._pending = pending
        self._in_flight = in_flight
        self._concurrency = concurrency
        self._per_actor = ExecutionResources(cpu=cpu, gpu=gpu)
        # Actors the pool is waiting on, per target node; a transfer stays in
        # flight while its claimed ids are still in here.
        self.pending_ids = dict(pending_ids or {})
        # Draining actors an upscale could revive; set by the owning _Op.
        self._reclaimable = 0
        # Claimed actors that left the pool without starting (cancelled/died).
        self.lost_ids = set(lost_ids)
        self.running_ids = list(running_ids)
        # (limit, node_id) of every reclaim_draining_actors call.
        self.reclaims = []

    def total_size(self):
        return self._current

    def serving_size(self):
        # The fake has no draining actors, so serving == current.
        return self._current

    def min_size(self):
        return self._min_size

    def max_size(self):
        return getattr(self, "_max_size", 100)

    def num_pending_actors(self):
        return self._pending

    def num_tasks_in_flight(self):
        return self._in_flight

    def max_actor_concurrency(self):
        return self._concurrency

    def per_actor_resource_usage(self):
        return self._per_actor

    def pending_ids_by_target_node(self):
        return self.pending_ids

    def num_reclaimable_draining_actors(self):
        return self._reclaimable

    def num_draining_actors(self):
        return getattr(self, "draining", 0)

    def has_actor(self, actor_id):
        return actor_id not in self.lost_ids

    def reclaim_draining_actors(self, *, limit, node_id=None):
        self.reclaims.append((limit, node_id))
        return limit

    def get_logical_ids(self):
        pending = [a for ids in self.pending_ids.values() for a in ids]
        return self.running_ids + pending


class _FakeClock:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


class _Op:
    def __init__(
        self, name, pool=None, view=None, output_dependencies=(), reclaimable=0
    ):
        self.name = name
        self._pool = pool or _Pool()
        self._view = view or _view()
        self.output_dependencies = list(output_dependencies)
        self._pool._reclaimable = reclaimable

    def placement_constraint(self):
        return PlacementConstraint()

    def has_completed(self):
        return False

    @property
    def actor_pool(self):
        return self._pool

    def get_autoscaling_actor_pools(self):
        return [self._pool]

    def build_placement_view(self, rank_victims=True):
        return self._view


def _op(*args, **kwargs) -> Any:
    """An ``_Op``, typed Any once here: the rules take real operators, and the
    double is duck-typed."""
    return _Op(*args, **kwargs)


class _State:
    def __init__(self, enqueued=0):
        self._enqueued = enqueued

    def total_enqueued_input_blocks(self):
        return self._enqueued


def _view(**overrides):
    base = dict(
        op_id="op",
        is_source=False,
        per_actor_usage=ExecutionResources(cpu=1),
        input_bytes_per_actor=None,
        actors_by_node={},
        input_queue_bytes_by_node={},
        upstream_actors_by_node={},
        downstream_actors_by_node={},
        victim_ids_by_node={},
        victim_drain_cost_by_node={},
        idle_actors_by_node={},
        pending_ids_by_node={},
    )
    base.update(overrides)
    return OpPlacementView(**base)


def _bool_streak(value):
    streak = BoolStreak(1)
    streak.record(value)
    return streak


def _signal(value):
    signal = WindowedSignal(1)
    signal.record(value)
    return signal


def _ctx(**overrides) -> OptimizationContext:
    """A context over the given ops. By default every op can run on every free
    node, every op is an actor-pool op, and an op's next actor-pool ops are its
    direct output dependencies."""
    base: Dict[str, Any] = dict(
        topology={},
        warmup=False,
        free={"node": ExecutionResources(cpu=0)},
        non_pg_ops=[],
        shortfall_streak={},
        shortfalls={},
        busy_tasks_signal={},
        productive_tasks_signal={},
        backpressured_streak={},
        not_backpressured_streak={},
        seconds_since_allocation_change=999.0,
        now=1000.0,
    )
    base.update(overrides)
    base.setdefault("actor_pool_ops", list(base["non_pg_ops"]))
    ops = (
        set(base["actor_pool_ops"])
        | set(base["non_pg_ops"])
        | set(base["shortfall_streak"])
    )
    base.setdefault("schedulable_nodes", {op: frozenset(base["free"]) for op in ops})
    base.setdefault("next_ops", {op: tuple(op.output_dependencies) for op in ops})
    return OptimizationContext(**base)


def _guard(clock=None) -> OpScalingGuard:
    """A guard nothing has blocked yet."""
    return OpScalingGuard(clock=clock) if clock else OpScalingGuard()


def _transfer_rule(metrics=None, guard=None, reset_signals=None) -> CapacityTransfer:
    return CapacityTransfer(
        cast(Any, metrics or _Metrics()),
        cast(Any, _Placement()),
        guard or _guard(),
        reset_signals=reset_signals or (lambda _op: None),
    )


def _grant_rule(metrics=None, guard=None, placement=None) -> SilentBottleneckGrant:
    return SilentBottleneckGrant(
        cast(Any, metrics or _Metrics()),
        guard or _guard(),
        cast(Any, placement or _Placement()),
    )


def _signal_observer() -> SignalObserver:
    return SignalObserver(lambda _op: (False, 0, 0))


def _donor_op(name: str = "donor", output_dependencies: tuple = ()) -> Any:
    """A 2-actor op with one idle, zero-cost actor on "node" to donate."""
    return _op(
        name,
        pool=_Pool(current=2),
        view=_view(
            actors_by_node={"node": 1},
            victim_ids_by_node={"node": ["d1"]},
            victim_drain_cost_by_node={"node": [(0, 0)]},
            idle_actors_by_node={"node": 1},
        ),
        output_dependencies=output_dependencies,
    )


def _shortfall(num_actors: int, silent: bool = False) -> OpShortfall:
    return OpShortfall(num_actors=num_actors, silent=silent)


def _granted(requests: list) -> list:
    return [
        op for placed in requests for op, req in placed.request.items() if req.delta > 0
    ]


def _owed(state) -> dict:
    """Recipient -> actors an open transfer still owes it (a transfer places
    its recipient only once the donors' capacity is free)."""
    return {
        recipient: sum(transfer.owed_by_node.values())
        for recipient, transfer in state.in_flight.items()
    }


def _donors(requests: list) -> set:
    return {
        op for placed in requests for op, req in placed.request.items() if req.delta < 0
    }


def _bottleneck_ctx(
    bottleneck: Any,
    consumer: Any,
    *,
    free_cpu: float = 10,
    consumer_busy=None,
    consumer_unblocked=None,
    bottleneck_busy=2,
) -> OptimizationContext:
    """The SilentBottleneckGrant scenario: a busy, backpressured ``bottleneck``
    feeding an idle-queued ``consumer``, with a growth delta of 2."""
    busy_tasks_signal = {bottleneck: _signal(bottleneck_busy)}
    if consumer_busy is not None:
        busy_tasks_signal[consumer] = _signal(consumer_busy)
    kwargs = dict(
        topology={consumer: _State(enqueued=0)},
        non_pg_ops=[bottleneck],
        free={"node": ExecutionResources(cpu=free_cpu)},
        busy_tasks_signal=busy_tasks_signal,
        backpressured_streak={bottleneck: _bool_streak(True)},
        growth_delta={bottleneck: 2},
    )
    if consumer_unblocked is not None:
        kwargs["not_backpressured_streak"] = {
            consumer: _bool_streak(consumer_unblocked)
        }
    return _ctx(**kwargs)


# --- CapacityTransfer ----------------------------------------------------------


def test_capacity_transfer_does_not_reuse_donor_capacity_across_recipients():
    donor = _donor_op()
    r1 = _op("r1")
    r2 = _op("r2")
    rule = _transfer_rule()
    ctx = _ctx(
        non_pg_ops=[donor, r1, r2],
        shortfall_streak={r1: _bool_streak(True), r2: _bool_streak(True)},
        shortfalls={r1: _shortfall(1), r2: _shortfall(1)},
        productive_tasks_signal={donor: _signal(1)},
    )

    state = rule.new_state()
    requests = rule.apply(ctx, state)

    donor_downscales = sum(
        -placed.request[donor].delta
        for placed in requests
        if donor in placed.request and placed.request[donor].delta < 0
    )
    assert donor_downscales == 1
    assert sum(_owed(state).values()) == 1
    assert not _granted(requests)  # placed once the donor's capacity frees


def test_capacity_transfer_opens_one_transfer_per_node():
    """Two short recipients, each with its own donor on the same node: only one
    transfer opens there. Holds come out of the shared free pool in in-flight
    order, so a second open transfer could lose its donor's freed slot to the
    first and stall until it times out."""
    donor_a, donor_b = _donor_op("donor_a"), _donor_op("donor_b")
    r1, r2 = _op("r1"), _op("r2")
    rule = _transfer_rule()
    ctx = _ctx(
        non_pg_ops=[donor_a, donor_b, r1, r2],
        shortfall_streak={r1: _bool_streak(True), r2: _bool_streak(True)},
        shortfalls={r1: _shortfall(1), r2: _shortfall(1)},
        productive_tasks_signal={donor_a: _signal(1), donor_b: _signal(1)},
    )

    state = rule.new_state()
    rule.apply(ctx, state)

    assert len(state.in_flight) == 1
    assert sum(_owed(state).values()) == 1


def test_capacity_transfer_debits_the_free_capacity_it_claims():
    # The recipient's actor uses the node's leftover 0.5 CPU plus what the
    # donor frees. Only the leftover was ever free, so it drops to zero; the
    # donor's share stays claimed rather than showing up as free.
    donor = _donor_op()
    recipient = _op("recipient")
    rule = _transfer_rule()
    ctx = _ctx(
        free={"node": ExecutionResources(cpu=0.5)},
        non_pg_ops=[donor, recipient],
        shortfall_streak={recipient: _bool_streak(True)},
        shortfalls={recipient: _shortfall(1)},
        productive_tasks_signal={donor: _signal(1)},
    )
    state = rule.new_state()
    rule.apply(ctx, state)
    assert _owed(state) == {recipient: 1}
    assert ctx.free["node"].cpu == 0


def test_capacity_transfer_skips_non_managed_recipients():
    donor = _op("donor", pool=_Pool(current=2))
    recipient = _op("self_placed")
    rule = _transfer_rule()
    ctx = _ctx(
        non_pg_ops=[donor],
        shortfall_streak={recipient: _bool_streak(True)},
        shortfalls={recipient: _shortfall(1)},
        productive_tasks_signal={donor: _signal(1)},
    )

    assert rule.apply(ctx, rule.new_state()) == []


@pytest.mark.parametrize(
    "ctx_kwargs,expected_refusal",
    [
        # The skip must be visible while the cluster is still resizing, even
        # when no recipient has qualified yet.
        pytest.param(
            dict(seconds_since_allocation_change=1.0),
            "cluster_settling",
            id="cluster-settling",
        ),
        pytest.param(dict(warmup=True), "warmup", id="warmup"),
    ],
)
def test_capacity_transfer_refuses_before_recipient_selection(
    ctx_kwargs, expected_refusal
):
    metrics = _Metrics()
    rule = _transfer_rule(metrics)
    assert rule.apply(_ctx(**ctx_kwargs), rule.new_state()) == []
    assert (rule.name, expected_refusal) in metrics.refusals


@pytest.mark.parametrize("bottleneck", [True, False], ids=["bottleneck", "not"])
def test_capacity_transfer_never_drains_a_silent_bottleneck(bottleneck):
    """A silent bottleneck's output-blocked actors look like surplus, but it
    limits the pipeline: it is never a donor. Without the backpressure that
    makes it one, the same op donates as usual."""
    consumer = _op("consumer", pool=_Pool(current=2))
    donor = _donor_op("producer", output_dependencies=(consumer,))
    recipient = _op("recipient")
    metrics = _Metrics()
    rule = _transfer_rule(metrics)
    ctx = _ctx(
        topology={consumer: _State(enqueued=0)},
        non_pg_ops=[donor, recipient],
        shortfall_streak={recipient: _bool_streak(True)},
        shortfalls={recipient: _shortfall(1)},
        productive_tasks_signal={donor: _signal(1)},
        busy_tasks_signal={donor: _signal(2)},
        backpressured_streak={donor: _bool_streak(bottleneck)},
        free={"node": ExecutionResources(cpu=0.5)},
    )

    requests = rule.apply(ctx, rule.new_state())

    if bottleneck:
        assert requests == []
        assert (rule.name, "no_donor_surplus") in metrics.refusals
    else:
        assert _donors(requests) == {donor}


def test_capacity_transfer_refuses_when_no_donor_has_surplus():
    # Both ops short: recipients exist but the donor list is empty, so
    # the rule refuses with no_donor_surplus instead of planning.
    r1 = _op("r1")
    r2 = _op("r2")
    metrics = _Metrics()
    rule = _transfer_rule(metrics)
    ctx = _ctx(
        non_pg_ops=[r1, r2],
        shortfall_streak={r1: _bool_streak(True), r2: _bool_streak(True)},
        shortfalls={r1: _shortfall(1), r2: _shortfall(1)},
    )

    assert rule.apply(ctx, rule.new_state()) == []
    assert (rule.name, "no_donor_surplus") in metrics.refusals


def test_capacity_transfer_serves_real_asks_before_silent_wants():
    # One donor actor, two short recipients: the op that actually asked
    # and was clamped gets it; the bridged silent want waits.
    donor = _donor_op()
    silent_op = _op("silent_op")
    real_op = _op("real_op")
    rule = _transfer_rule()
    ctx = _ctx(
        non_pg_ops=[donor, silent_op, real_op],
        # Silent op listed first: without the priority sort it would win.
        shortfall_streak={
            silent_op: _bool_streak(True),
            real_op: _bool_streak(True),
        },
        shortfalls={
            silent_op: _shortfall(1, silent=True),
            real_op: _shortfall(1),
        },
        productive_tasks_signal={donor: _signal(1)},
    )

    state = rule.new_state()
    rule.apply(ctx, state)
    assert _owed(state) == {real_op: 1}


def test_feeder_with_short_consumer_donates_instead_of_competing():
    # Both ops read short, but the feeder's consumer IS the short op:
    # the feeder's backlog is phantom, so it must be skipped as a recipient
    # and used as the donor for its consumer.
    consumer = _op("consumer")
    feeder = _donor_op("feeder", output_dependencies=(consumer,))
    rule = _transfer_rule()
    ctx = _ctx(
        non_pg_ops=[feeder, consumer],
        shortfall_streak={
            feeder: _bool_streak(True),
            consumer: _bool_streak(True),
        },
        shortfalls={feeder: _shortfall(31), consumer: _shortfall(1)},
        productive_tasks_signal={feeder: _signal(1)},
    )

    state = rule.new_state()
    requests = rule.apply(ctx, state)

    assert _owed(state) == {consumer: 1}
    assert _donors(requests) == {feeder}


def test_donor_stays_blocked_after_a_transfer_closes():
    # A donor must not be regrown -- by another rule or normal sizing --
    # until its signals reflect the smaller pool: its window is cleared when
    # the transfer closes, and it stays blocked until the window refills.
    donor = _op("donor", pool=_Pool(current=4))
    recipient = _op("recipient")
    clock = _FakeClock()
    guard = _guard(clock=clock)
    reset = []
    rule = _transfer_rule(guard=guard, reset_signals=reset.append)
    state = rule.new_state()
    state.in_flight[recipient] = TransferInFlight(
        recipient=recipient,
        donors=frozenset({donor}),
        started_t=clock.now,
        claimed_ids=("claimed",),
    )
    # What apply() blocks when it fires a transfer.
    guard.block(recipient, "receiving a capacity transfer")
    guard.block(donor, f"donating to {recipient.name}")

    assert not guard.can_scale(donor)  # transfer in flight

    # Recipient has no pendings -> lands; the donor swaps to a cooldown.
    rule.advance(_ctx(now=clock.now), state)
    assert not state.in_flight
    assert guard.can_scale(recipient)
    assert reset == [donor]
    assert not guard.can_scale(donor)  # still blocked after landing

    # Time alone does not end the cooldown; a partly filled window doesn't
    # either.
    clock.now += 3600
    partial = WindowedSignal(2)
    partial.record(1)
    rule.advance(_ctx(productive_tasks_signal={donor: partial}), state)
    assert not guard.can_scale(donor)

    # A full window of fresh samples does.
    rule.advance(_ctx(productive_tasks_signal={donor: _signal(1)}), state)
    assert guard.can_scale(donor)


def test_donor_funding_two_transfers_stays_blocked_until_the_last_closes():
    # A donor with spare capacity can fund several recipients in one pass. The
    # first transfer closing must NOT drop it onto the post-donation cooldown:
    # that cooldown (one signal window) is far shorter than the transfer
    # timeout, so the donor could be regrown while the other transfer is still
    # draining it.
    donor = _donor_op()
    landed = _op("landed")
    still_draining = _op(
        "still_draining", pool=_Pool(pending_ids={"node": ["draining-actor"]})
    )
    clock = _FakeClock()
    guard = _guard(clock=clock)
    rule = _transfer_rule(guard=guard)
    state = rule.new_state()
    for recipient, claimed in (
        (landed, "landed-actor"),
        (still_draining, "draining-actor"),
    ):
        state.in_flight[recipient] = TransferInFlight(
            recipient=recipient,
            donors=frozenset({donor}),
            started_t=clock.now,
            claimed_ids=(claimed,),
        )
    guard.block(donor, f"donating to {landed.name}")

    refilled = {donor: _signal(1)}
    rule.advance(_ctx(now=clock.now, productive_tasks_signal=refilled), state)
    assert set(state.in_flight) == {still_draining}  # only the landed one closed
    assert not guard.can_scale(donor), "donor freed while a transfer still drains it"

    # Once the last transfer lands, the cooldown starts and then ends when the
    # donor's window refills.
    still_draining._pool.pending_ids = {}
    rule.advance(_ctx(now=clock.now), state)
    assert not state.in_flight
    assert not guard.can_scale(donor)
    rule.advance(_ctx(now=clock.now, productive_tasks_signal=refilled), state)
    assert guard.can_scale(donor)


def test_plan_node_skips_victims_that_free_nothing_the_recipient_lacks():
    # The node has a CPU to spare but no free GPU. Six idle CPU-only actors
    # sort ahead of the one busy GPU actor, yet evicting them frees nothing the
    # GPU recipient is short of: only the GPU actor is taken.
    cpu_donor = DonorCapacity(
        op=_op("cpu_donor"),
        view=_view(),
        per_actor=ExecutionResources(cpu=1),
        removable=6,
    )
    gpu_donor = DonorCapacity(
        op=_op("gpu_donor"),
        view=_view(),
        per_actor=ExecutionResources(cpu=1, gpu=1),
        removable=1,
    )
    candidates = sorted(
        [
            VictimCandidate.build(
                cpu_donor,
                f"c{i}",
                pending=False,
                tasks_in_flight=0,
                unconsumed_outputs=0,
                recipient_constrained=True,
            )
            for i in range(6)
        ]
        + [
            VictimCandidate.build(
                gpu_donor,
                "g0",
                pending=False,
                tasks_in_flight=1,
                unconsumed_outputs=0,
                recipient_constrained=True,
            )
        ]
    )

    plan = _plan_node(
        "node",
        max_actors=1,
        free={"node": ExecutionResources(cpu=1, gpu=0)},
        recipient_per_actor=ExecutionResources(cpu=1, gpu=1),
        candidates=candidates,
        num_removable_actors_by_op={cpu_donor.op: 6, gpu_donor.op: 1},
    )

    assert plan.num_recipient_actors == 1
    assert [victim.actor_id for victim in plan.victims] == ["g0"]


@pytest.mark.parametrize("pending,expected", [(0, 2), (2, None)])
def test_donor_protects_starting_actors(pending, expected):
    # 9 actors, 7 doing work. Without pending actors the 2 idle ones are
    # surplus; if those 2 are still starting, the window can't judge them.
    op = _op("op", pool=_Pool(current=9, pending=pending))
    donor = assess_donor(op, _ctx(productive_tasks_signal={op: _signal(7)}))
    assert (donor.removable if donor else None) == expected


def _idle_donor(name, node, actor_id, per_actor):
    return DonorCapacity(
        op=_op(name, pool=_Pool(gpu=per_actor.gpu)),
        view=_view(
            victim_ids_by_node={node: [actor_id]},
            victim_drain_cost_by_node={node: [(0, 0)]},
        ),
        per_actor=per_actor,
        removable=1,
    )


@pytest.mark.parametrize("cpu_donor_available", [True, False])
def test_cpu_recipient_takes_gpu_actors_only_as_last_resort(cpu_donor_available):
    # The GPU donor sits on the node the recipient ranks first, yet a CPU-only
    # donor elsewhere is preferred; without one, the GPU actor is taken.
    gpu_donor = _idle_donor("gpu_donor", "gpu_node", "g0", ExecutionResources(1, 1))
    donors = [gpu_donor]
    if cpu_donor_available:
        donors.append(_idle_donor("cpu_donor", "cpu_node", "c0", ExecutionResources(1)))
    plan, _ = plan_transfer(
        _op("recipient"),
        ExecutionResources(cpu=1),
        want=1,
        nodes=["gpu_node", "cpu_node"],
        free={
            "gpu_node": ExecutionResources(cpu=0, gpu=0),
            "cpu_node": ExecutionResources(cpu=0),
        },
        donors=donors,
        node_rank={"gpu_node": (0,), "cpu_node": (1,)},
    )
    assert plan is not None
    victims = [victim.actor_id for node_plan in plan for victim in node_plan.victims]
    assert victims == (["c0"] if cpu_donor_available else ["g0"])


@pytest.mark.parametrize(
    "lost,expected", [(False, "landed"), (True, "lost")], ids=["started", "cancelled"]
)
def test_transfer_lands_only_if_its_claimed_actors_are_running(lost, expected):
    # Leaving the pending set is not landing: a claimed actor that was
    # cancelled (or died) instead of starting closes the transfer as lost.
    recipient = _op("recipient", pool=_Pool(lost_ids=["claimed-actor"] if lost else ()))
    metrics = _Metrics()
    rule = _transfer_rule(metrics=metrics)
    state = rule.new_state()
    state.in_flight[recipient] = TransferInFlight(
        recipient=recipient,
        donors=frozenset({_donor_op()}),
        started_t=1000.0,
        claimed_ids=("claimed-actor",),
    )

    rule.advance(_ctx(now=1000.0), state)

    assert not state.in_flight
    assert metrics.actions[-1] == (rule.name, "recipient", expected)


def _transfer_tick(rule, state, *, free_cpu, **ctx_kwargs):
    """One optimizer tick for ``rule``: advance, then apply."""
    ctx = _ctx(free={"node": ExecutionResources(cpu=free_cpu)}, **ctx_kwargs)
    rule.advance(ctx, state)
    return ctx, rule.apply(ctx, state)


def test_capacity_transfer_places_the_recipient_once_donor_capacity_frees():
    """Firing downscales the donor only. The recipient is placed on a later
    tick, once the donor's slot is actually free, so the dataset never asks
    for more than its slice; until then the node's partial free capacity is
    held for it, so nothing else in the sizer spends it."""
    donor = _donor_op()
    recipient = _op("recipient")
    metrics = _Metrics()
    rule = _transfer_rule(metrics=metrics)
    state = rule.new_state()
    short = dict(
        non_pg_ops=[donor, recipient],
        shortfall_streak={recipient: _bool_streak(True)},
        shortfalls={recipient: _shortfall(1)},
        productive_tasks_signal={donor: _signal(1)},
    )

    # Fire: 0.5 CPU free, the donor's 1 CPU actor makes room for one actor.
    ctx, requests = _transfer_tick(rule, state, free_cpu=0.5, **short)
    assert _donors(requests) == {donor} and not _granted(requests)
    assert _owed(state) == {recipient: 1}
    assert ctx.free["node"].cpu == 0  # the partial 0.5 CPU is held

    # The donor is still draining: still nothing to place, and the 0.5 CPU
    # stays held rather than handed to another rule or the per-op passes.
    ctx, requests = _transfer_tick(rule, state, free_cpu=0.5)
    assert requests == [] and ctx.free["node"].cpu == 0

    # The donor died: its slot is free, so the recipient is placed there.
    ctx, requests = _transfer_tick(rule, state, free_cpu=1.5)
    assert _granted(requests) == [recipient]
    (placed,) = requests
    assert placed.request[recipient].nodes == ("node",)
    assert _owed(state) == {recipient: 0}
    assert ctx.free["node"].cpu == 0.5  # only what it took is held

    # Its pending is recorded as the transfer's; once it is running, landed.
    recipient._pool.pending_ids = {"node": ["new-actor"]}
    _transfer_tick(rule, state, free_cpu=0.5)
    assert state.in_flight[recipient].claimed_ids == ("new-actor",)
    recipient._pool.pending_ids = {}
    _transfer_tick(rule, state, free_cpu=0.5)
    assert not state.in_flight
    assert metrics.actions[-1] == (rule.name, "recipient", "landed")


def test_capacity_transfer_times_out_while_still_owed():
    # A donor that never frees its slot leaves the recipient owed: after the
    # timeout the transfer closes as timed out and stops holding capacity. Its
    # victim may never finish draining (its outputs can need the recipient to
    # be consumed), so the timeout gives it back to the donor; otherwise each
    # retry would strand one more donor actor.
    donor = _donor_op()
    recipient = _op("recipient")
    metrics = _Metrics()
    rule = _transfer_rule(metrics=metrics)
    state = rule.new_state()
    rule.apply(
        _ctx(
            free={"node": ExecutionResources(cpu=0.5)},
            non_pg_ops=[donor, recipient],
            shortfall_streak={recipient: _bool_streak(True)},
            shortfalls={recipient: _shortfall(1)},
            productive_tasks_signal={donor: _signal(1)},
        ),
        state,
    )

    late = _ctx(
        free={"node": ExecutionResources(cpu=0.5)},
        now=1000.0 + SIZER_TRANSFER_TIMEOUT_S + 1,
    )
    rule.advance(late, state)

    assert not state.in_flight
    assert metrics.actions[-1] == (rule.name, "recipient", "timeout")
    assert late.free["node"].cpu == 0.5  # no longer held
    assert donor._pool.reclaims == [(1, "node")]


def test_capacity_transfer_timeout_restarts_when_actors_are_placed():
    """A donor that drains slowly delays placement. The timeout runs from the
    last placement, so an actor placed late still gets the full window to
    start, and only a transfer that stops making progress times out."""
    donor = _donor_op()
    recipient = _op("recipient")
    metrics = _Metrics()
    rule = _transfer_rule(metrics=metrics)
    state = rule.new_state()
    _transfer_tick(
        rule,
        state,
        free_cpu=0.5,
        now=1000.0,
        non_pg_ops=[donor, recipient],
        shortfall_streak={recipient: _bool_streak(True)},
        shortfalls={recipient: _shortfall(1)},
        productive_tasks_signal={donor: _signal(1)},
    )

    # The donor frees its slot just before the timeout: the actor is placed.
    placed_t = 1000.0 + SIZER_TRANSFER_TIMEOUT_S - 1
    _, requests = _transfer_tick(rule, state, free_cpu=1.5, now=placed_t)
    assert _granted(requests) == [recipient]
    recipient._pool.pending_ids = {"node": ["new-actor"]}
    cancelled = []
    recipient._pool.scale = cancelled.append

    # Past the original deadline, but not past the placement's: still open.
    _transfer_tick(rule, state, free_cpu=0.5, now=1000.0 + SIZER_TRANSFER_TIMEOUT_S + 1)
    assert recipient in state.in_flight

    # The placed actor never starts: the transfer times out from placement.
    _transfer_tick(
        rule, state, free_cpu=0.5, now=placed_t + SIZER_TRANSFER_TIMEOUT_S + 1
    )
    assert not state.in_flight
    assert metrics.actions[-1] == (rule.name, "recipient", "timeout")
    (cancel,) = cancelled
    assert cancel.nodes == ("node",)
    # The victim's node was placed on, so its victim drained and is not
    # reclaimed.
    assert donor._pool.reclaims == []


def test_transfer_whose_recipient_never_grew_closes_as_lost():
    # The recipient's upscale was refused, so no actor appeared after the
    # transfer fired: the donors gave up actors for nothing, which must not be
    # recorded as landed. Its pre-existing actors are not the transfer's.
    recipient = _op("recipient", pool=_Pool(running_ids=["old-actor"]))
    metrics = _Metrics()
    rule = _transfer_rule(metrics=metrics)
    state = rule.new_state()
    state.in_flight[recipient] = TransferInFlight(
        recipient=recipient,
        donors=frozenset({_donor_op()}),
        started_t=1000.0,
        preexisting_ids=frozenset({"old-actor"}),
    )

    rule.advance(_ctx(now=1000.0), state)

    assert not state.in_flight
    assert metrics.actions[-1] == (rule.name, "recipient", "lost")


@pytest.mark.parametrize(
    "closed_t, now, refused",
    [
        # Closed just now: moving the capacity straight back is thrashing.
        (1000.0, 1001.0, True),
        # The window has passed: the earlier transfer may have been wrong, so
        # it can be undone.
        (1000.0, 1000.0 + THRASH_WINDOW_S + 1, False),
        # Still in flight: nothing can be judged yet, however long ago it fired.
        (None, 1000.0 + THRASH_WINDOW_S + 1, True),
    ],
)
def test_capacity_transfer_refuses_undoing_a_transfer_only_within_the_window(
    closed_t, now, refused
):
    # An earlier transfer moved capacity from ``op`` to ``donor``; now ``op`` is
    # short and ``donor`` has surplus, so this transfer would move it back.
    op = _op("op")
    donor = _donor_op()
    metrics = _Metrics()
    rule = _transfer_rule(metrics=metrics)
    state = rule.new_state()
    state.recent_transfers.append(
        RecentTransfer(recipient=donor, donors=frozenset({op}), closed_t=closed_t)
    )

    requests = rule.apply(
        _ctx(
            now=now,
            non_pg_ops=[donor, op],
            shortfall_streak={op: _bool_streak(True)},
            shortfalls={op: _shortfall(1)},
            productive_tasks_signal={donor: _signal(1)},
        ),
        state,
    )

    if refused:
        assert requests == []
        assert (rule.name, "thrashing") in metrics.refusals
    else:
        assert _donors(requests) == {donor}
        assert _owed(state) == {op: 1}


def test_closing_a_transfer_starts_its_thrash_window():
    donor = _donor_op()
    recipient = _op("recipient")
    rule = _transfer_rule()
    state = rule.new_state()
    rule.apply(
        _ctx(
            non_pg_ops=[donor, recipient],
            shortfall_streak={recipient: _bool_streak(True)},
            shortfalls={recipient: _shortfall(1)},
            productive_tasks_signal={donor: _signal(1)},
        ),
        state,
    )
    (record,) = state.recent_transfers
    assert record.closed_t is None

    # Nothing owed or pending any more: the transfer closes (as lost, since
    # no actor was claimed) and stamps the close time.
    state.in_flight[recipient].owed_by_node = {}
    rule.advance(_ctx(now=1500.0), state)

    assert not state.in_flight
    assert record.closed_t == 1500.0


# --- SilentBottleneckGrant -----------------------------------------------------


def test_silent_bottleneck_requires_downstream_output_unblocked():
    # Non-terminal consumer (it has its own downstream): its unblocked
    # streak is meaningful and required.
    consumer = _op("consumer", pool=_Pool(current=2), output_dependencies=[_op("sink")])
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2),
        output_dependencies=[consumer],
    )
    rule = _grant_rule()

    blocked_ctx = _bottleneck_ctx(
        bottleneck, consumer, consumer_busy=1, consumer_unblocked=False
    )
    assert rule.apply(blocked_ctx, rule.new_state()) == []

    unblocked_ctx = _bottleneck_ctx(
        bottleneck, consumer, consumer_busy=1, consumer_unblocked=True
    )
    requests = rule.apply(unblocked_ctx, rule.new_state())
    assert len(requests) == 1
    assert requests[0].request[bottleneck].delta == 2
    # The grant's capacity is debited, so later rules and passes can't reuse it.
    assert unblocked_ctx.free["node"].cpu == 8


def test_silent_bottleneck_busy_gate_needs_one_task_per_actor_not_full_slots():
    # A flow-controlled op's dispatch breathes: it cannot sustain every task
    # SLOT full at every window sample. Busy means every actor holds at least
    # one task, not cur x max_actor_concurrency.
    consumer = _op("consumer", pool=_Pool(current=2))  # terminal
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2, concurrency=2),
        output_dependencies=[consumer],
    )
    rule = _grant_rule()
    requests = rule.apply(
        _bottleneck_ctx(bottleneck, consumer, consumer_busy=1), rule.new_state()
    )
    assert len(requests) == 1
    assert requests[0].request[bottleneck].delta == 2


@pytest.mark.parametrize("fewest_tasks,granted", [(6, True), (5, False)])
def test_silent_bottleneck_busy_gate_is_a_fraction_of_actors(fewest_tasks, granted):
    # Some actors are always between tasks, waiting for a full input bundle,
    # even when the op is the bottleneck: 6 of 10 busy at every sample is
    # enough.
    consumer = _op("consumer", pool=_Pool(current=2))  # terminal
    bottleneck = _op(
        "bottleneck", pool=_Pool(current=10), output_dependencies=[consumer]
    )
    rule = _grant_rule()
    ctx = _bottleneck_ctx(bottleneck, consumer, bottleneck_busy=fewest_tasks)
    assert bool(rule.apply(ctx, rule.new_state())) == granted


@pytest.mark.parametrize(
    "consumer_pool,block_bottleneck",
    [
        # Another decision owns the op (e.g. it just donated).
        pytest.param(dict(current=2), True, id="guarded-op"),
        # The consumer is mid-resize: its absorption verdict is stale, so no
        # grant until its pendings land.
        pytest.param(dict(current=2, pending=3), False, id="consumer-resizing"),
    ],
)
def test_silent_bottleneck_holds(consumer_pool, block_bottleneck):
    consumer = _op("consumer", pool=_Pool(**consumer_pool))
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2),
        output_dependencies=[consumer],
    )
    guard = _guard()
    rule = _grant_rule(guard=guard)
    ctx = _bottleneck_ctx(bottleneck, consumer, consumer_unblocked=True)
    if block_bottleneck:
        guard.block(bottleneck, "donating to consumer")

    assert rule.apply(ctx, rule.new_state()) == []


def test_silent_bottleneck_records_shortfall_when_no_free_capacity():
    # A silent bottleneck never records its own shortfall. When free capacity
    # can't cover the grant, the rule records the rest, so CapacityTransfer
    # can pick the op as a recipient.
    consumer = _op("consumer", pool=_Pool(current=2))
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2),
        output_dependencies=[consumer],
    )
    rule = _grant_rule()
    ctx = _bottleneck_ctx(bottleneck, consumer, free_cpu=0, consumer_busy=1)
    assert rule.apply(ctx, rule.new_state()) == []
    assert bottleneck in ctx.silent_shortfalls
    assert ctx.silent_shortfalls[bottleneck].num_actors == 2


def test_silent_bottleneck_places_by_locality_rank_not_free_map_order():
    # The free map lists "z" first, but the placement strategy ranks "a"
    # better (the test strategy ranks nodes by id): the grant follows the
    # strategy, as place_upscale and CapacityTransfer do.
    consumer = _op("consumer", pool=_Pool(current=2))
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2),
        output_dependencies=[consumer],
    )
    rule = _grant_rule()
    ctx = _bottleneck_ctx(bottleneck, consumer, consumer_busy=1)
    ctx.free = {"z": ExecutionResources(cpu=4), "a": ExecutionResources(cpu=4)}
    ctx.schedulable_nodes[bottleneck] = frozenset(ctx.free)

    (placed,) = rule.apply(ctx, rule.new_state())

    assert placed.request[bottleneck].nodes == ("a", "a")


def test_silent_bottleneck_revives_draining_actors_before_free_nodes():
    # Draining actors still hold their node's resources: reviving them costs no
    # free capacity, so they go first, and only what neither covers is a
    # shortfall (which could otherwise pull a needless capacity transfer).
    consumer = _op("consumer", pool=_Pool(current=2))
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2),
        output_dependencies=[consumer],
        reclaimable=1,
    )
    rule = _grant_rule()
    ctx = _bottleneck_ctx(bottleneck, consumer, free_cpu=0, consumer_busy=1)

    requests = rule.apply(ctx, rule.new_state())

    req = requests[0].request[bottleneck]
    assert isinstance(req, ScaleUpRequest)
    assert req.delta == 1
    assert req.num_to_reclaim == 1
    assert req.nodes == ()
    assert ctx.silent_shortfalls[bottleneck].num_actors == 1


@pytest.mark.parametrize("kind", ["bundles", "pg-per-actor"])
def test_silent_bottleneck_pg_op_grows_into_its_pg(kind):
    # A PG-managed op is placed by its placement group: the grant is untargeted,
    # reclaims draining actors first, and is capped by room in the PG (its
    # bundles, or its headroom for a PG-per-actor op) -- never by the free
    # map, and never recorded as a shortfall for transfers.
    consumer = _op("consumer", pool=_Pool(current=2))
    bottleneck = _op(
        "bottleneck",
        pool=_Pool(current=2, in_flight=2),
        output_dependencies=[consumer],
        reclaimable=1,
    )
    rule = _grant_rule()
    ctx = _bottleneck_ctx(bottleneck, consumer, free_cpu=0, consumer_busy=1)
    ctx.growth_delta[bottleneck] = 5
    ctx.non_pg_ops = []
    bundle = ("pg1", 0)
    if kind == "bundles":
        ctx.schedulable_bundles[bottleneck] = (bundle,)
        ctx.pg_free[bundle] = ExecutionResources(cpu=2)  # room for 2 new actors
    else:
        ctx.unknown_pg_headroom[bottleneck] = 2

    requests = rule.apply(ctx, rule.new_state())

    req = requests[0].request[bottleneck]
    assert isinstance(req, ScaleUpRequest)
    assert req.delta == 3  # 1 reclaimed + 2 new
    assert req.num_to_reclaim == 1
    assert req.nodes == ()
    assert not ctx.silent_shortfalls
    # Only the new actors spend room; reclaimed ones hold their bundles.
    if kind == "bundles":
        assert ctx.pg_free[bundle].cpu == 0
    else:
        assert ctx.unknown_pg_headroom[bottleneck] == 0


# --- signal bookkeeping --------------------------------------------------------


def test_optimizer_merges_silent_shortfalls_into_sizing_results():
    # Silent shortfalls must flow through the SIZER's per-pass recording
    # (streak semantics), not be recorded directly -- a direct record would
    # interleave with the sizer's not-short record and never form a streak.
    op = _op("silent")
    signals = _signal_observer()
    sf = OpShortfall(num_actors=2)
    for _ in range(3):
        signals.merge_silent_shortfalls({op: sf})
        signals.record_sizing_results([op], set(), {})
    observed: Any = signals.signal_dict()
    assert observed["shortfall_streak"][op].is_streak()
    assert observed["shortfalls"][op].num_actors == 2
    # Consumed per pass: no carry-over without a fresh record.
    signals.record_sizing_results([op], set(), {})
    assert not observed["shortfall_streak"][op].is_streak()


def test_shortfall_streak_freezes_on_backpressure_hold_ticks():
    # An op that oscillates between "asked and refused" and "held for output
    # backpressure with demand" must still complete the streak: hold ticks
    # freeze it instead of resetting it. Satisfied ticks reset.
    op = _op("gpu_op")
    signals = _signal_observer()
    streaks: Any = signals.signal_dict()["shortfall_streak"]

    def tick(*, ungranted=False, neutral=False):
        signals.record_sizing_results(
            ordered_ops=[op],
            ungranted_ops={op} if ungranted else set(),
            shortfalls={},
            backpressured_with_demand={op} if neutral else set(),
        )

    # ask, ask, hold, ask -> streak completes (hold froze it)
    tick(ungranted=True)
    tick(ungranted=True)
    tick(neutral=True)
    tick(ungranted=True)
    assert streaks[op].is_streak()

    # a satisfied tick still resets
    tick()
    assert not streaks[op].is_streak()

    # holds alone can never BUILD a streak
    tick(neutral=True)
    tick(neutral=True)
    tick(neutral=True)
    assert not streaks[op].is_streak()


# --- OpScalingGuard ------------------------------------------------------------


def _guard_with_clock(start: float = 100.0):
    """A guard on a fake clock. Mutate ``clock.now`` to advance time."""
    clock = _FakeClock(start)
    return clock, _guard(clock=clock)


def test_guard_block_release_and_overwrite():
    clock, guard = _guard_with_clock()
    op = _op("a")
    assert guard.can_scale(op)
    assert guard.reason(op) is None

    guard.block(op, "resizing")
    assert not guard.can_scale(op)
    assert guard.reason(op) == "resizing"

    guard.release(op)
    assert guard.can_scale(op)
    assert guard.reason(op) is None
    guard.release(op)  # releasing an unblocked op is a no-op

    # A permanent block replaces a timed one: the old expiry is gone.
    guard.block(op, "first", until=clock.now + 5)
    guard.block(op, "second")
    clock.now += 100
    assert not guard.can_scale(op)
    assert guard.reason(op) == "second"

    # And a timed block replaces a permanent one: it now expires.
    guard.block(op, "third", until=clock.now + 5)
    assert guard.reason(op) == "third"
    clock.now += 5
    assert guard.can_scale(op)


def test_guard_blocks_expire_on_the_clock():
    clock, guard = _guard_with_clock()
    forever, timed, later = (_op(n) for n in ("forever", "timed", "later"))
    guard.block(forever, "no expiry")
    guard.block(timed, "short", until=clock.now + 5)
    guard.block(later, "long", until=clock.now + 50)
    assert guard.blocked_ops() == frozenset({forever, timed, later})

    clock.now += 4.9  # just before the short block's expiry
    assert not guard.can_scale(timed)
    assert guard.reason(timed) == "short"

    clock.now += 5.1  # only the short block has lapsed
    assert guard.can_scale(timed)
    assert guard.reason(timed) is None
    assert guard.blocked_ops() == frozenset({forever, later})


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))


# --- BottleneckTransfer (RAY_DATA_SIZER_BOTTLENECK_MODEL) ----------------------


def _bottleneck_rule(metrics=None, guard=None) -> BottleneckTransfer:
    return BottleneckTransfer(
        cast(Any, metrics or _Metrics()),
        cast(Any, _Placement()),
        guard or _guard(),
        reset_signals=lambda _op: None,
    )


def _tick(
    *,
    recipient=None,
    named=(),
    stable=(),
    eligibility=None,
    shares=None,
    slack=None,
    ttf=None,
    target=None,
) -> Any:
    """A BottleneckTick double: ``named`` [(op, severity)] this tick,
    ``stable`` the stable set, ``slack`` donor -> slack at any b', ``ttf``
    donor -> each victim's time-to-free."""
    return SimpleNamespace(
        snapshot=SimpleNamespace(
            bottlenecks=tuple(named),
            shares=shares if shares is not None else {},
            edges=(),
        ),
        stable=frozenset(stable),
        assessment=SimpleNamespace(
            recipient=recipient,
            eligibility=eligibility or {},
            stable=tuple((op, sev) for op, sev in named if op in set(stable)),
        ),
        victim_ttf=lambda donor, tasks, unconsumed: (ttf or {}).get(donor, 0.0),
        slack_for=lambda r, b_prime: dict(slack or {}),
        # op -> b'; unset ops are unbounded, so the step cap decides.
        growth_target=lambda op: (target or {}).get(op, 10**6),
    )


def _idle_victims_op(name, node, ids, *, current, cpu=1):
    """An op with idle, zero-cost actors ``ids`` on ``node``."""
    return _op(
        name,
        pool=_Pool(current=current, cpu=cpu),
        view=_view(
            victim_ids_by_node={node: list(ids)},
            victim_drain_cost_by_node={node: [(0, 0)] * len(ids)},
        ),
    )


def _apply_bottleneck(
    rule, tick, ops, *, free_cpu=0.0, growth=None, state=None, settle=999.0
):
    state = state or rule.new_state()
    ctx = _ctx(
        free={"node": ExecutionResources(cpu=free_cpu)},
        non_pg_ops=list(ops),
        growth_delta=dict(growth or {}),
        bottleneck=tick,
        seconds_since_allocation_change=settle,
    )
    return ctx, state, rule.apply(ctx, state)


def test_bottleneck_transfer_funds_labeled_stage_from_genstage_slack():
    """autoscale_contention (the design doc's example): LabeledStage (4 CPU)
    is the stable bottleneck at 2 actors with no free room; GenStage's idle
    1-CPU actors are slack. b' = 3 (step cap 1.5 x 2) takes 4 of them."""
    labeled = _op("Labeled", pool=_Pool(current=2, cpu=4))
    gen = _idle_victims_op("Gen", "node", [f"g{i}" for i in range(6)], current=15)
    tick = _tick(
        recipient=labeled,
        named=[(labeled, 1.0)],
        stable=[labeled],
        eligibility={labeled: "yes: victims"},
        shares={labeled: None, gen: None},
        slack={gen: 11},
        ttf={gen: 30.0},
    )
    rule = _bottleneck_rule()
    _, state, requests = _apply_bottleneck(rule, tick, [labeled, gen])

    assert _donors(requests) == {gen}
    (placed,) = requests
    assert placed.request[gen].nodes == ("node",) * 4
    assert _owed(state) == {labeled: 1}
    # 1.5 x 30s is under the 60s floor.
    assert state.in_flight[labeled].timeout_s == 60.0


def test_bottleneck_transfer_never_moves_predictor_capacity():
    """image_classification: Predictor, the bottleneck, is at max size, so
    there is no recipient; ReadFiles and Preprocess are never recipients and
    nothing moves."""
    predictor = _op("Predictor", pool=_Pool(current=10, gpu=1))
    preprocess = _idle_victims_op("Preprocess", "node", ["p0", "p1"], current=31)
    tick = _tick(
        recipient=None,
        named=[(predictor, 0.85)],
        stable=[predictor],
        eligibility={predictor: "no: at max size"},
        shares={predictor: None, preprocess: None},
        slack={preprocess: 20},
    )
    _, state, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [predictor, preprocess]
    )
    assert requests == [] and not state.in_flight


def test_stable_bottleneck_is_never_a_donor():
    # Predictor is stable (a bottleneck within the last window), so its idle
    # actor is not taken even though Preprocess, the recipient, needs one.
    preprocess = _op("Preprocess", pool=_Pool(current=21))
    predictor = _idle_victims_op("Predictor", "node", ["d0"], current=10)
    tick = _tick(
        recipient=preprocess,
        named=[(preprocess, 0.4), (predictor, 0.3)],
        stable=[preprocess, predictor],
        eligibility={preprocess: "yes: victims"},
        shares={preprocess: None, predictor: None},
        slack={predictor: 5},
    )
    metrics = _Metrics()
    _, _, requests = _apply_bottleneck(
        _bottleneck_rule(metrics), tick, [preprocess, predictor]
    )
    assert requests == []


def test_victims_are_taken_fastest_to_free_and_timeout_follows_them():
    """video_object_detection: Write needs one 1-CPU slot. ReadVideo's victim
    would take 900s to drain, ExtractImageFeatures' 50s: the faster one is
    taken, and the timeout is 1.5 x 50s."""
    write = _op("Write", pool=_Pool(current=1))
    read = _idle_victims_op("ReadVideo", "node", ["r0"], current=7)
    features = _idle_victims_op("ExtractImageFeatures", "node", ["f0"], current=5)
    tick = _tick(
        recipient=write,
        named=[(write, 1.0)],
        stable=[write],
        eligibility={write: "yes: victims"},
        shares={write: None, read: None, features: None},
        slack={read: 5, features: 4},
        ttf={read: 900.0, features: 50.0},
    )
    _, state, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [write, read, features]
    )
    assert _donors(requests) == {features}
    assert state.in_flight[write].timeout_s == pytest.approx(75.0)


def _busy_victims_op(name, node, ids, *, current):
    """An op whose actors on ``node`` each have a task in flight and three
    unconsumed outputs: they free only after a drain."""
    return _op(
        name,
        pool=_Pool(current=current),
        view=_view(
            victim_ids_by_node={node: list(ids)},
            victim_drain_cost_by_node={node: [(1, 3)] * len(ids)},
        ),
    )


def _surplus_tick(write, read, *, recipient=True):
    return _tick(
        recipient=write if recipient else None,
        named=[(write, 1.0)],
        stable=[write],
        eligibility={write: "yes: victims"},
        shares={write: None, read: None},
        slack={read: 2},
        ttf={read: 265.0},
    )


def test_busy_surplus_drains_passively_without_a_transfer():
    """video at 0:13:31: ReadVideo has slack 2 toward Write, but its actors
    are busy (outputs behind resize), so freeing one takes minutes. No
    transfer is opened (it would block every other one); after the surplus
    holds for a streak, one ReadVideo actor drains on a node where a Write
    actor fits once it frees."""
    write = _op("Write", pool=_Pool(current=1))
    read = _busy_victims_op("ReadVideo", "node", ["r0", "r1"], current=3)
    rule = _bottleneck_rule()
    state = rule.new_state()
    drained = []
    for _ in range(3):
        _, state, requests = _apply_bottleneck(
            rule, _surplus_tick(write, read), [write, read], state=state
        )
        assert not state.in_flight and _granted(requests) == []
        drained.append(_donors(requests))
    assert drained == [set(), set(), {read}]
    (placed,) = requests
    assert placed.request[read].nodes == ("node",)


def _blocked_tick(read, resize, *, blocked, self_limited=0.01, n=10):
    """No recipient (the GPU stage is named but cannot grow); ReadVideo's
    output edge to resize has ``blocked`` / ``self_limited`` shares."""
    tick = _tick(
        named=[],
        shares={
            read: SimpleNamespace(busy=0.01, starved=0.0, actors=n),
            resize: SimpleNamespace(busy=0.23, starved=0.0, actors=4),
        },
    )
    tick.snapshot.edges = (
        bottleneck_observer.Edge(
            producer=read,
            consumer=resize,
            producer_blocked=blocked,
            producer_self_limited=self_limited,
            consumer_starved=0.0,
            arrow=bottleneck_observer.Arrow.DOWNSTREAM,
        ),
    )
    return tick


@pytest.mark.parametrize(
    "blocked, self_limited, n, drains",
    [
        # video on g4dn.2xlarge: 10 ReadVideo actors, blocked 0.98 behind
        # resize, 1% busy. Keep is max(2, ceil(10 x 0.02 x 1.2)) = 2.
        (0.98, 0.01, 10, True),
        # Not blocked enough: idle time may be its own (or starved).
        (0.5, 0.01, 10, False),
        # Already at the floor.
        (0.98, 0.01, 2, False),
        # Self-limited: its budget is the limit, keep = ceil(10 x 0.99 x 1.2).
        (0.0, 0.98, 10, False),
    ],
    ids=["blocked-surplus", "partly-blocked", "at-floor", "self-limited"],
)
def test_blocked_surplus_drains_without_a_recipient(blocked, self_limited, n, drains):
    read = _busy_victims_op("ReadVideo", "node", ["r0"], current=n)
    resize = _op("resize", pool=_Pool(current=4))
    rule = _bottleneck_rule()
    state = rule.new_state()
    seen = []
    for _ in range(3):
        _, state, requests = _apply_bottleneck(
            rule,
            _blocked_tick(
                read, resize, blocked=blocked, self_limited=self_limited, n=n
            ),
            [read, resize],
            state=state,
        )
        assert not state.in_flight and _granted(requests) == []
        seen.append(_donors(requests))
    assert seen == [set(), set(), {read} if drains else set()]


@pytest.mark.parametrize("starved, drains", [(0.04, True), (0.07, False)])
@pytest.mark.parametrize("rule_kind", ["blocked-surplus", "surplus-to-recipient"])
def test_shrink_stops_while_the_op_it_feeds_starves(rule_kind, starved, drains):
    """video_object_detection: resize_frame (busy 0.04, blocked behind the
    GPU op) had a keep of 2, but its output budgets are the GPU op's supply:
    at 4 actors the GPU op starved past 0.1. Neither shrink drains an op
    while an op it feeds starves at least 0.05 of the time."""
    consumer = _op("Gpu", pool=_Pool(current=8))
    donor = _busy_victims_op("Resize", "node", ["r0"], current=10)
    donor.output_dependencies = [consumer]
    consumer_shares = SimpleNamespace(busy=0.6, starved=starved, actors=8)
    if rule_kind == "blocked-surplus":
        tick = _blocked_tick(donor, consumer, blocked=0.98)
        tick.snapshot.shares[consumer] = consumer_shares
    else:
        tick = _surplus_tick(consumer, donor)
        tick.snapshot.shares[consumer] = consumer_shares
    rule = _bottleneck_rule()
    state = rule.new_state()
    seen = []
    for _ in range(3):
        _, state, requests = _apply_bottleneck(
            rule, tick, [consumer, donor], state=state
        )
        seen.append(_donors(requests))
    assert seen == [set(), set(), {donor} if drains else set()]


@pytest.mark.parametrize("case", ["no-recipient", "already-draining"])
def test_surplus_is_not_drained(case):
    """No shrink without a recipient that wants the capacity, and one drain at
    a time per donor."""
    write = _op("Write", pool=_Pool(current=1))
    read = _busy_victims_op("ReadVideo", "node", ["r0", "r1"], current=3)
    if case == "already-draining":
        read.actor_pool.draining = 1
    rule = _bottleneck_rule()
    state = rule.new_state()
    for _ in range(4):
        _, state, requests = _apply_bottleneck(
            rule,
            _surplus_tick(write, read, recipient=case != "no-recipient"),
            [write, read],
            state=state,
        )
        assert _donors(requests) == set()


@pytest.mark.parametrize(
    "victims, expected_b_prime",
    [(4, 6), (1, 5)],
    ids=["step-cap", "slack-limited"],
)
def test_b_prime_is_the_largest_feasible_step(victims, expected_b_prime):
    # b = 4: the step cap allows b' up to 6. With 4 idle victims b' = 6 (2
    # actors); with 1, only b' = 5 is feasible.
    recipient = _op("B", pool=_Pool(current=4))
    donor = _idle_victims_op("A", "node", [f"a{i}" for i in range(victims)], current=10)
    tick = _tick(
        recipient=recipient,
        named=[(recipient, 1.0)],
        stable=[recipient],
        eligibility={recipient: "yes: victims"},
        shares={recipient: None, donor: None},
        slack={donor: 8},
    )
    _, state, _ = _apply_bottleneck(_bottleneck_rule(), tick, [recipient, donor])
    assert _owed(state) == {recipient: expected_b_prime - 4}


def test_free_growth_runs_while_the_cluster_resizes_but_transfers_wait():
    # Unsaturated and still resizing (allocation changed 2s ago): the named
    # bottleneck grows into free room, but no transfer fires until settled.
    write = _op("Write", pool=_Pool(current=2))
    read = _idle_victims_op("ReadVideo", "node", ["r0"], current=7)
    free_tick = _tick(
        named=[(write, 0.6)], eligibility={write: "yes: free"}, shares={write: None}
    )
    _, _, grown = _apply_bottleneck(
        _bottleneck_rule(),
        free_tick,
        [write],
        free_cpu=3.0,
        settle=2.0,
    )
    assert _granted(grown) == [write]

    full_tick = _tick(
        recipient=write,
        named=[(write, 1.0)],
        stable=[write],
        eligibility={write: "yes: victims"},
        shares={write: None, read: None},
        slack={read: 5},
    )
    _, state, moved = _apply_bottleneck(
        _bottleneck_rule(), full_tick, [write, read], settle=2.0
    )
    assert moved == [] and not state.in_flight


def test_named_bottleneck_grows_into_free_capacity_before_it_is_stable():
    # Growing into free capacity drains nothing, so it needs no stability:
    # Write is named this tick only, with room on its node, and grows toward
    # its growth target (2 -> 4) without a transfer.
    write = _op("Write", pool=_Pool(current=2))
    tick = _tick(
        recipient=None,
        named=[(write, 0.6)],
        eligibility={write: "yes: free"},
        shares={write: None},
        target={write: 4},
    )
    _, state, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [write], free_cpu=3.0
    )
    assert _granted(requests) == [write]
    (placed,) = requests
    assert placed.request[write].nodes == ("node", "node")
    assert not state.in_flight


def test_balanced_stages_each_grow_one_step_into_free_capacity():
    # No bottleneck named and every stage nearly always busy: each busy op
    # grows one actor into free capacity. An idle op, an op at its max size
    # and an op with pending actors stay put.
    read = _op("Read", pool=_Pool(current=2))
    infer = _op("Infer", pool=_Pool(current=4))
    idle = _op("Idle", pool=_Pool(current=2))
    full = _op("Full", pool=_Pool(current=3))
    full.actor_pool._max_size = 3
    pending = _op("Pending", pool=_Pool(current=2, pending=1))
    busy = SimpleNamespace(busy=0.95)
    tick = _tick(
        named=[],
        shares={
            read: busy,
            infer: busy,
            idle: SimpleNamespace(busy=0.5),
            full: busy,
            pending: busy,
        },
    )
    ctx, state, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [read, infer, idle, full, pending], free_cpu=5.0
    )
    assert sorted(op.name for op in _granted(requests)) == ["Infer", "Read"]
    for placed in requests:
        (op,) = placed.request
        assert len(placed.request[op].nodes) == 1
    assert ctx.free["node"].cpu == pytest.approx(3.0)
    assert not state.in_flight


def test_balanced_growth_waits_while_a_bottleneck_is_named():
    # A named bottleneck (even one that cannot grow) means the stages are
    # not balanced, so busy non-bottlenecks are not grown.
    read = _op("Read", pool=_Pool(current=2))
    write = _op("Write", pool=_Pool(current=2))
    tick = _tick(
        named=[(write, 0.05)],
        eligibility={write: "no: below severity floor"},
        shares={read: SimpleNamespace(busy=0.95), write: SimpleNamespace(busy=1.0)},
    )
    _, _, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [read, write], free_cpu=5.0
    )
    assert _granted(requests) == []


def test_refused_bottleneck_transfer_is_logged_as_refused(monkeypatch, caplog):
    """A transfer the thrash guard refuses is logged as not fired, never as a
    decision with donors."""
    write = _op("Write", pool=_Pool(current=1))
    read = _idle_victims_op("ReadVideo", "node", ["r0", "r1"], current=7)
    tick = _tick(
        recipient=write,
        named=[(write, 1.0)],
        stable=[write],
        eligibility={write: "yes: victims"},
        shares={write: None, read: None},
        slack={read: 5},
    )
    rule = _bottleneck_rule()
    monkeypatch.setattr(rule, "_fire", lambda *args, **kwargs: [])
    with caplog.at_level(logging.DEBUG):
        _, state, requests = _apply_bottleneck(rule, tick, [write, read])
    assert requests == [] and not state.in_flight
    assert "not fired" in caplog.text
    assert " from ReadVideo" not in caplog.text


def test_one_bottleneck_transfer_at_a_time():
    write = _op("Write", pool=_Pool(current=1))
    read = _idle_victims_op("ReadVideo", "node", ["r0", "r1"], current=7)
    tick = _tick(
        recipient=write,
        named=[(write, 1.0)],
        stable=[write],
        eligibility={write: "yes: victims"},
        shares={write: None, read: None},
        slack={read: 5},
    )
    rule = _bottleneck_rule()
    _, state, first = _apply_bottleneck(rule, tick, [write, read])
    assert _donors(first) == {read}
    _, _, second = _apply_bottleneck(rule, tick, [write, read], state=state)
    assert _donors(second) == set()


@pytest.mark.parametrize(
    "ttfs, expected",
    [
        ([], 60.0),
        ([10.0, 30.0], 60.0),
        ([100.0], 150.0),
        ([500.0], 300.0),
        ([math.inf], 300.0),
    ],
)
def test_transfer_timeout_from_victim_ttf(ttfs, expected):
    assert transfer_timeout_s(ttfs) == pytest.approx(expected)


def _apply_pg_bottleneck(rule, tick, pg_op, *, bundle_free_cpu, others=()):
    """Apply ``rule`` with ``pg_op`` placed by a one-bundle placement group
    and no free node capacity."""
    bundle = ("pg", 0)
    ctx = _ctx(
        non_pg_ops=list(others),
        actor_pool_ops=[pg_op, *others],
        schedulable_bundles={pg_op: (bundle,)},
        pg_free={bundle: ExecutionResources(cpu=bundle_free_cpu)},
        bottleneck=tick,
    )
    state = rule.new_state()
    return ctx, state, rule.apply(ctx, state), bundle


@pytest.mark.parametrize(
    "bundle_free_cpu, reclaimable, expected_delta",
    [(2, 0, 2), (2, 1, 3), (0, 0, None)],
    ids=["new-actors-capped-by-bundles", "reclaims-first", "pg-full"],
)
def test_named_pg_bottleneck_grows_into_its_placement_group(
    bundle_free_cpu, reclaimable, expected_delta
):
    """dataset_constraint_pg_scheduling: FakeGPUInfer (PG-scheduled) is the
    named bottleneck at 4 actors with no free node room. It grows toward its
    growth target (8), but only as far as its PG has room: untargeted (the PG
    picks the bundle), draining actors reclaimed first, and no transfer."""
    infer = _op("Infer", pool=_Pool(current=4), reclaimable=reclaimable)
    gen = _idle_victims_op("Gen", "node", ["g0", "g1"], current=11)
    tick = _tick(
        named=[(infer, 1.7)],
        stable=[infer],
        eligibility={infer: PG_ELIGIBLE},
        shares={infer: None, gen: None},
        target={infer: 8},
    )
    ctx, state, requests, bundle = _apply_pg_bottleneck(
        _bottleneck_rule(), tick, infer, bundle_free_cpu=bundle_free_cpu, others=[gen]
    )

    assert not state.in_flight
    if expected_delta is None:
        assert requests == []
        return
    (placed,) = requests
    req = placed.request[infer]
    assert isinstance(req, ScaleUpRequest)
    assert req.delta == expected_delta
    assert req.num_to_reclaim == reclaimable
    assert req.nodes == ()
    assert ctx.pg_free[bundle].cpu == 0


def test_balanced_pg_op_grows_one_step_into_its_placement_group():
    # No bottleneck named and the PG op nearly always busy: it grows one
    # actor into its PG, not into (absent) free node capacity.
    infer = _op("Infer", pool=_Pool(current=4))
    tick = _tick(named=[], shares={infer: SimpleNamespace(busy=0.95)})
    ctx, _, requests, bundle = _apply_pg_bottleneck(
        _bottleneck_rule(), tick, infer, bundle_free_cpu=2
    )
    (placed,) = requests
    assert placed.request[infer].delta == 1
    assert placed.request[infer].nodes == ()
    assert ctx.pg_free[bundle].cpu == 1


@pytest.mark.parametrize(
    "free_cpu, expected", [(54, 12), (42, 14), (28, 9), (2, 1), (0, 0)]
)
def test_bottleneck_free_growth_is_capped_by_competing_share(free_cpu, expected):
    current = {54: 24, 42: 36, 28: 50}.get(free_cpu, 24)
    download = _op("Download", pool=_Pool(current=current))
    peers = [_op("Decode"), _op("Plan")]
    tick = _tick(
        named=[(download, 0.43)],
        eligibility={download: "yes: free"},
        target={download: int(current * 1.5)},
    )
    ctx, _, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [download, *peers], free_cpu=free_cpu
    )
    added = sum(p.request[download].delta for p in requests)
    assert added == expected
    assert ctx.free["node"].cpu == free_cpu - expected


@pytest.mark.parametrize("excluded", ["completed", "at-cap", "gpu", "other-node", "pg"])
def test_bottleneck_free_share_excludes_noncompeting_ops(excluded):
    recipient = _op("Download", pool=_Pool(current=24))
    peer = _op("Peer")
    ctx = _ctx(non_pg_ops=[recipient, peer], free={"node": ExecutionResources(cpu=12)})
    if excluded == "completed":
        peer.has_completed = lambda: True
    elif excluded == "at-cap":
        peer.actor_pool._max_size = peer.actor_pool.serving_size()
    elif excluded == "gpu":
        peer.actor_pool._per_actor = ExecutionResources(cpu=1, gpu=1)
    elif excluded == "other-node":
        ctx.schedulable_nodes[peer] = frozenset({"other"})
    else:
        ctx.non_pg_ops = [recipient]
    assert BottleneckTransfer._free_growth_share(recipient, ctx) == 12


def test_bottleneck_free_share_counts_pending_peers_and_node_fragmentation():
    recipient = _op("B", pool=_Pool(current=24, cpu=2, gpu=1))
    recipient.actor_pool._per_actor = ExecutionResources(cpu=2, gpu=1, memory=4)
    peer = _op("Pending", pool=_Pool(pending=2, cpu=2, gpu=1))
    ctx = _ctx(
        non_pg_ops=[recipient, peer],
        free={
            "a": ExecutionResources(cpu=20, gpu=4, memory=8),  # 2 actors
            "b": ExecutionResources(cpu=1, gpu=8, memory=100),  # 0 actors
            "c": ExecutionResources(cpu=8, gpu=8, memory=8),  # 2 actors
        },
    )
    assert BottleneckTransfer._free_growth_share(recipient, ctx) == 2


def test_bottleneck_free_share_does_not_charge_reclaimed_actors():
    recipient = _op("B", pool=_Pool(current=24), reclaimable=4)
    peer = _op("Peer")
    tick = _tick(
        named=[(recipient, 0.5)],
        eligibility={recipient: "yes: free"},
        target={recipient: 36},
    )
    ctx, _, requests = _apply_bottleneck(
        _bottleneck_rule(), tick, [recipient, peer], free_cpu=6
    )
    request = requests[0].request[recipient]
    assert request.num_to_reclaim == 4
    assert len(request.nodes) == 3
    assert request.delta == 7
    assert ctx.free["node"].cpu == 3


def test_bottleneck_pg_free_share_counts_only_shared_bundles():
    recipient, shared, separate, ordinary = [
        _op(n) for n in ["B", "Shared", "Separate", "Ordinary"]
    ]
    bundle, other = ("pg", 0), ("other", 0)
    ctx = _ctx(
        actor_pool_ops=[recipient, shared, separate, ordinary],
        non_pg_ops=[ordinary],
        schedulable_bundles={
            recipient: (bundle,),
            shared: (bundle,),
            separate: (other,),
        },
        pg_free={
            bundle: ExecutionResources(cpu=12),
            other: ExecutionResources(cpu=100),
        },
    )
    assert BottleneckTransfer._free_growth_share(recipient, ctx) == 6
    requests = _bottleneck_rule()._grow_into_pg(recipient, 20, ctx, "bottleneck")
    assert requests is not None
    assert requests.request[recipient].delta == 6
    assert ctx.pg_free[bundle].cpu == 6
    assert ctx.pg_free[other].cpu == 100
