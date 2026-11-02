"""Unit tests for the pipeline optimizer's rules (``CapacityTransfer``,
``SilentBottleneckGrant``), the signal bookkeeping they decide on, and the
``OpScalingGuard`` they share. Duck-typed doubles; no cluster."""
import sys
from typing import Any, Dict, cast

import pytest

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.operators.placement_constraints import (
    PlacementConstraint,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    OpPlacementView,
)
from ray.data._internal.experimental.execution.sizer.models import OpShortfall
from ray.data._internal.experimental.execution.sizer.optimizer.capacity_transfer import (  # noqa: E501
    CapacityTransfer,
    DonorCapacity,
    TransferInFlight,
    VictimCandidate,
    _plan_node,
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
        pending_ids=None,
        lost_ids=(),
        running_ids=(),
    ):
        self._current = current
        self._min_size = min_size
        self._pending = pending
        self._in_flight = in_flight
        self._concurrency = concurrency
        self._per_actor = ExecutionResources(cpu=cpu)
        # Actors the pool is waiting on, per target node; a transfer stays in
        # flight while its claimed ids are still in here.
        self.pending_ids = dict(pending_ids or {})
        # Draining actors an upscale could revive; set by the owning _Op.
        self._reclaimable = 0
        # Claimed actors that left the pool without starting (cancelled/died).
        self.lost_ids = set(lost_ids)
        self.running_ids = list(running_ids)

    def current_size(self):
        return self._current

    def serving_size(self):
        # The fake has no draining actors, so serving == current.
        return self._current

    def min_size(self):
        return self._min_size

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

    def has_actor(self, actor_id):
        return actor_id not in self.lost_ids

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
) -> OptimizationContext:
    """The SilentBottleneckGrant scenario: a busy, backpressured ``bottleneck``
    feeding an idle-queued ``consumer``, with a growth delta of 2."""
    busy_tasks_signal = {bottleneck: _signal(2)}
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

    requests = rule.apply(ctx, rule.new_state())

    donor_downscales = sum(
        -placed.request[donor].delta
        for placed in requests
        if donor in placed.request and placed.request[donor].delta < 0
    )
    recipient_upscales = sum(
        placed.request[op].delta
        for placed in requests
        for op in (r1, r2)
        if op in placed.request
    )
    assert donor_downscales == 1
    assert recipient_upscales == 1


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
    assert _granted(rule.apply(ctx, rule.new_state())) == [recipient]
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

    assert _granted(rule.apply(ctx, rule.new_state())) == [real_op]


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

    requests = rule.apply(ctx, rule.new_state())

    assert _granted(requests) == [consumer]
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

    assert placed.request[bottleneck].target_nodes_to_scale_actors_on == ("a", "a")


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
    assert req.delta == 1
    assert req.num_to_reclaim == 1
    assert req.target_nodes_to_scale_actors_on == ()
    assert ctx.silent_shortfalls[bottleneck].num_actors == 1


@pytest.mark.parametrize("kind", ["bundles", "pg-per-actor"])
def test_silent_bottleneck_pg_op_grows_into_its_pg(kind):
    # A PG-managed op is placed by its placement group: the grant is untargeted,
    # revives draining actors first, and is capped by room in the PG (its
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
    assert req.delta == 3  # 1 revived + 2 new
    assert req.num_to_reclaim == 1
    assert req.target_nodes_to_scale_actors_on == ()
    assert not ctx.silent_shortfalls
    # Only the new actors spend room; revived ones hold their bundles.
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
