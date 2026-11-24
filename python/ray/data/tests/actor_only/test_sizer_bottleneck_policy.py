"""Unit tests for the sizer's bottleneck policy: the phase and decision
classifiers, each decision's handler (``BottleneckPolicy``), the transfer
mechanics (``TransferBook``), free-capacity growth, and the ``OpLifecycles``
they share. Duck-typed doubles; no cluster."""
import logging
import math
import sys
from types import SimpleNamespace
from typing import Any, Dict, cast

import pytest

from ray.data._internal.actor_autoscaler import AutoscalingActorPool
from ray.data._internal.execution.execution_flags import (
    SIZER_CLUSTER_SETTLE_S,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    BundleKey,
)
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
from ray.data._internal.experimental.execution.sizer.lifecycle import (
    OpLifecycle,
    OpLifecycles,
)
from ray.data._internal.experimental.execution.sizer.policy import growth
from ray.data._internal.experimental.execution.sizer.policy.bottleneck_policy import (  # noqa: E501
    BottleneckPolicy,
    transfer_timeout_s,
)
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.decision import (
    HOLD_CLUSTER_SETTLING,
    HOLD_NO_MODEL,
    HOLD_WARMUP,
    classify,
    classify_phase,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    DecisionKind,
    SizerPhase,
    TransferPhase,
)
from ray.data._internal.experimental.execution.sizer.policy.transfer import (
    THRASH_WINDOW_S,
    DonorCapacity,
    RecentTransfer,
    TransferBook,
    TransferInFlight,
    VictimCandidate,
    _plan_node,
    plan_transfer,
)


@pytest.fixture(autouse=True)
def _views_from_fake_ops(monkeypatch):
    """The doubles serve canned placement views from ``build_placement_view``;
    route the policy's view builder there."""
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

    def total_size(self):
        return self._current

    def serving_size(self):
        # The fake has no draining actors, so serving == current.
        return self._current

    def min_size(self):
        return self._min_size

    def max_size(self):
        return getattr(self, "_max_size", 100)

    bounded_max_size = AutoscalingActorPool.bounded_max_size

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

    def get_logical_ids(self):
        pending = [a for ids in self.pending_ids.values() for a in ids]
        return self.running_ids + pending


class _Op:
    def __init__(
        self, name, pool=None, view=None, output_dependencies=(), reclaimable=0
    ):
        self.name = name
        self._pool = pool or _Pool()
        self._view = view or _view()
        self.output_dependencies = list(output_dependencies)
        self._pool._reclaimable = reclaimable
        self.pending_first_input = False

    def placement_constraint(self):
        return PlacementConstraint()

    def has_completed(self):
        return False

    def awaiting_first_input(self):
        return self.pending_first_input

    @property
    def actor_pool(self):
        return self._pool

    def get_autoscaling_actor_pools(self):
        return [self._pool]

    def build_placement_view(self, rank_victims=True):
        return self._view


def _op(*args, **kwargs) -> Any:
    """An ``_Op``, typed Any once here: the policy takes real operators, and
    the double is duck-typed."""
    return _Op(*args, **kwargs)


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


def _ctx(**overrides) -> DecisionContext:
    """A context over the given ops, in STEADY. By default every op can run
    on every free node, every op is an actor-pool op, and an op's next
    actor-pool ops are its direct output dependencies."""
    base: Dict[str, Any] = dict(
        topology={},
        phase=SizerPhase.STEADY,
        free={"node": ExecutionResources(cpu=0)},
        non_pg_ops=[],
        seconds_since_allocation_change=999.0,
        now=1000.0,
    )
    base.update(overrides)
    base.setdefault("actor_pool_ops", list(base["non_pg_ops"]))
    ops = set(base["actor_pool_ops"]) | set(base["non_pg_ops"])
    base.setdefault("schedulable_nodes", {op: frozenset(base["free"]) for op in ops})
    base.setdefault("next_ops", {op: tuple(op.output_dependencies) for op in ops})
    return DecisionContext(**base)


def _lifecycles(*ops, cooldown_steps=20) -> OpLifecycles:
    return OpLifecycles(ops, cooldown_steps=cooldown_steps)


def _policy(metrics=None, lifecycles=None) -> BottleneckPolicy:
    return BottleneckPolicy(
        cast(Any, metrics or _Metrics()),
        cast(Any, _Placement()),
        lifecycles or _lifecycles(),
    )


def _book(metrics=None, lifecycles=None) -> TransferBook:
    return TransferBook(
        "bottleneck_transfer",
        cast(Any, metrics or _Metrics()),
        cast(Any, _Placement()),
        lifecycles or _lifecycles(),
    )


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
    policy, tick, ops, *, free_cpu=0.0, settle=999.0, growth_ceiling=None
):
    """One tick of ``policy`` over ``ops``; returns (ctx, transfer state,
    requests)."""
    ctx = _ctx(
        free={"node": ExecutionResources(cpu=free_cpu)},
        non_pg_ops=list(ops),
        bottleneck=tick,
        seconds_since_allocation_change=settle,
        growth_ceiling=dict(growth_ceiling or {}),
    )
    _, requests = policy.step(ctx)
    return ctx, policy.transfers.state, requests


# --- phase and decision classification -----------------------------------------


@pytest.mark.parametrize(
    "warmup, unmodeled, expected",
    [
        (True, {"op"}, SizerPhase.WARMUP),
        (True, set(), SizerPhase.WARMUP),
        (False, {"op"}, SizerPhase.MODEL_WARMING),
        (False, set(), SizerPhase.STEADY),
    ],
)
def test_classify_phase(warmup, unmodeled, expected):
    assert classify_phase(warmup, unmodeled) is expected


def _classify(tick, ops, *, free_cpu=0.0, settle=999.0, phase=SizerPhase.STEADY):
    ctx = _ctx(
        free={"node": ExecutionResources(cpu=free_cpu)},
        non_pg_ops=list(ops),
        bottleneck=tick,
        seconds_since_allocation_change=settle,
        phase=phase,
    )
    return classify(ctx, _lifecycles())


def test_classify_holds_in_warmup_and_without_a_model():
    write = _op("Write", pool=_Pool(current=2))
    tick = _tick(named=[(write, 0.6)], eligibility={write: "yes: free"})
    warm = _classify(tick, [write], free_cpu=3, phase=SizerPhase.WARMUP)
    assert (warm.kind, warm.reason) == (DecisionKind.HOLD, HOLD_WARMUP)
    no_model = _classify(None, [write], free_cpu=3)
    assert (no_model.kind, no_model.reason) == (DecisionKind.HOLD, HOLD_NO_MODEL)


@pytest.mark.parametrize(
    "scenario, expected",
    [
        ("named-with-room", DecisionKind.GROW_FREE),
        ("named-without-room-stable", DecisionKind.FUND_RECIPIENT),
        ("named-without-room-settling", DecisionKind.HOLD),
        ("nothing-named-busy", DecisionKind.GROW_BALANCED),
        ("nothing-named-idle", DecisionKind.SHRINK_SURPLUS),
    ],
)
def test_classify_picks_one_decision_by_priority(scenario, expected):
    write = _op("Write", pool=_Pool(current=2))
    read = _op("Read", pool=_Pool(current=2))
    settle = 999.0
    free_cpu = 3.0
    if scenario == "named-with-room":
        tick = _tick(
            named=[(write, 0.6)], eligibility={write: "yes: free"}, target={write: 4}
        )
    elif scenario.startswith("named-without-room"):
        free_cpu = 0.0
        tick = _tick(
            recipient=write,
            named=[(write, 1.0)],
            stable=[write],
            eligibility={write: "yes: victims"},
        )
        if scenario.endswith("settling"):
            settle = SIZER_CLUSTER_SETTLE_S - 1
    else:
        busy = 0.95 if scenario.endswith("busy") else 0.5
        tick = _tick(shares={read: SimpleNamespace(busy=busy)})
    decision = _classify(tick, [write, read], free_cpu=free_cpu, settle=settle)
    assert decision.kind is expected
    if expected is DecisionKind.HOLD:
        assert decision.reason == HOLD_CLUSTER_SETTLING
    if expected is DecisionKind.GROW_FREE:
        assert dict(decision.targets) == {write: 4}


def test_classify_skips_a_named_bottleneck_held_by_a_transfer():
    # Receiving a transfer: not grown into free capacity this tick.
    write = _op("Write", pool=_Pool(current=2))
    tick = _tick(named=[(write, 0.6)], eligibility={write: "yes: free"})
    lifecycles = _lifecycles(write)
    lifecycles.start_receiving(write)
    ctx = _ctx(
        free={"node": ExecutionResources(cpu=3)}, non_pg_ops=[write], bottleneck=tick
    )
    assert classify(ctx, lifecycles).kind is not DecisionKind.GROW_FREE


# --- OpLifecycles ----------------------------------------------------------------


def test_lifecycle_releases_an_op_once_its_first_input_arrives():
    write = _op("Write")
    write.pending_first_input = True
    lifecycles = _lifecycles(write)
    assert lifecycles.state(write) is OpLifecycle.AWAITING_FIRST_INPUT
    assert not lifecycles.can_scale(write)

    lifecycles.on_step()
    assert not lifecycles.can_scale(write)  # still waiting

    write.pending_first_input = False
    lifecycles.on_step()
    assert lifecycles.state(write) is OpLifecycle.ACTIVE
    assert lifecycles.can_scale(write)


def test_lifecycle_transfer_states_block_scaling_until_activated():
    recipient, donor = _op("recipient"), _op("donor")
    lifecycles = _lifecycles(recipient, donor)
    lifecycles.start_receiving(recipient)
    lifecycles.start_donating(donor, recipient)
    assert lifecycles.state(recipient) is OpLifecycle.RECEIVING
    assert lifecycles.reason(donor) == "donating (to recipient)"
    assert lifecycles.ops_in(OpLifecycle.DONATING) == {donor}
    assert not lifecycles.can_scale(recipient) and not lifecycles.can_scale(donor)

    lifecycles.activate(recipient)
    assert lifecycles.can_scale(recipient)
    assert lifecycles.reason(recipient) is None


def test_lifecycle_cooldown_counts_scheduling_steps():
    donor = _op("donor")
    lifecycles = _lifecycles(donor, cooldown_steps=3)
    lifecycles.start_cooling(donor)
    for _ in range(2):
        lifecycles.on_step()
        assert lifecycles.state(donor) is OpLifecycle.COOLING
    lifecycles.on_step()
    assert lifecycles.can_scale(donor)


def test_lifecycle_reports_completed_ops():
    op = _op("op")
    op.has_completed = lambda: True
    assert _lifecycles(op).state(op) is OpLifecycle.COMPLETED


# --- TransferBook: transfer mechanics ------------------------------------------


def _fire(book, recipient, donor, ctx, removable=1):
    """Plan and fire a transfer to ``recipient`` from ``donor``'s idle
    actors, as the policy's FUND_RECIPIENT does."""
    donor_capacity = DonorCapacity(
        op=donor,
        view=donor.build_placement_view(),
        per_actor=donor.actor_pool.per_actor_resource_usage(),
        removable=removable,
    )
    nodes = book.transfer_nodes(recipient, ctx)
    plan, refusal = book.plan(recipient, 1, nodes, [donor_capacity], ctx)
    assert plan is not None, refusal
    return book.fire(recipient, plan, {donor: donor_capacity}, ctx)


def _transfer_tick(book, *, free_cpu, **ctx_kwargs):
    """One tick of the book: advance, then bring up held actors."""
    ctx = _ctx(free={"node": ExecutionResources(cpu=free_cpu)}, **ctx_kwargs)
    book.advance(ctx)
    return ctx, book.bring_up_held_actors()


def test_transfer_places_the_recipient_once_donor_capacity_frees():
    """Firing downscales the donor only. The recipient is placed on a later
    tick, once the donor's slot is actually free, so the dataset never asks
    for more than its slice; until then the node's partial free capacity is
    held for it, so nothing else in the sizer spends it."""
    donor = _donor_op()
    recipient = _op("recipient")
    metrics = _Metrics()
    book = _book(metrics=metrics)

    # Fire: 0.5 CPU free, the donor's 1 CPU actor makes room for one actor.
    ctx = _ctx(
        free={"node": ExecutionResources(cpu=0.5)}, non_pg_ops=[donor, recipient]
    )
    requests = _fire(book, recipient, donor, ctx)
    assert _donors(requests) == {donor} and not _granted(requests)
    assert _owed(book.state) == {recipient: 1}
    assert ctx.free["node"].cpu == 0  # the partial 0.5 CPU is held
    assert book.phase is TransferPhase.IN_FLIGHT

    # The donor is still draining: still nothing to place, and the 0.5 CPU
    # stays held rather than handed to another decision or how_many.
    ctx, requests = _transfer_tick(book, free_cpu=0.5)
    assert requests == [] and ctx.free["node"].cpu == 0

    # The donor died: its slot is free, so the recipient is placed there.
    ctx, requests = _transfer_tick(book, free_cpu=1.5)
    assert _granted(requests) == [recipient]
    (placed,) = requests
    assert placed.request[recipient].nodes == ("node",)
    assert _owed(book.state) == {recipient: 0}
    assert ctx.free["node"].cpu == 0.5  # only what it took is held

    # Its pending is recorded as the transfer's; once it is running, landed.
    recipient._pool.pending_ids = {"node": ["new-actor"]}
    _transfer_tick(book, free_cpu=0.5)
    assert book.state.in_flight[recipient].claimed_ids == ("new-actor",)
    recipient._pool.pending_ids = {}
    _transfer_tick(book, free_cpu=0.5)
    assert not book.state.in_flight
    # The ticks above close the transfer; pyrefly keeps the earlier narrowing.
    assert book.phase is TransferPhase.IDLE  # pyrefly: ignore[unnecessary-comparison]
    assert metrics.actions[-1] == ("bottleneck_transfer", "recipient", "landed")


def test_transfer_opens_one_transfer_per_node():
    # A node an open transfer still owes on is not a node for another one.
    donor = _donor_op()
    first, second = _op("first"), _op("second")
    book = _book()
    ctx = _ctx(free={"node": ExecutionResources(cpu=0)}, non_pg_ops=[donor, first])
    _fire(book, first, donor, ctx)
    assert book.transfer_nodes(second, _ctx(non_pg_ops=[second])) == []


def test_transfer_times_out_while_still_owed():
    # A donor that never frees its slot leaves the recipient owed: after the
    # timeout the transfer closes as timed out and stops holding capacity.
    donor = _donor_op()
    recipient = _op("recipient")
    metrics = _Metrics()
    book = _book(metrics=metrics)
    _fire(
        book,
        recipient,
        donor,
        _ctx(free={"node": ExecutionResources(cpu=0.5)}, non_pg_ops=[donor, recipient]),
    )

    late, _ = _transfer_tick(
        book, free_cpu=0.5, now=1000.0 + SIZER_TRANSFER_TIMEOUT_S + 1
    )

    assert not book.state.in_flight
    assert metrics.actions[-1] == ("bottleneck_transfer", "recipient", "timeout")
    assert late.free["node"].cpu == 0.5  # no longer held


def test_transfer_timeout_restarts_when_actors_are_placed():
    """A donor that drains slowly delays placement. The timeout runs from the
    last placement, so an actor placed late still gets the full window to
    start, and only a transfer that stops making progress times out."""
    donor = _donor_op()
    recipient = _op("recipient")
    metrics = _Metrics()
    book = _book(metrics=metrics)
    _fire(
        book,
        recipient,
        donor,
        _ctx(free={"node": ExecutionResources(cpu=0.5)}, non_pg_ops=[donor, recipient]),
    )

    # The donor frees its slot just before the timeout: the actor is placed.
    placed_t = 1000.0 + SIZER_TRANSFER_TIMEOUT_S - 1
    _, requests = _transfer_tick(book, free_cpu=1.5, now=placed_t)
    assert _granted(requests) == [recipient]
    recipient._pool.pending_ids = {"node": ["new-actor"]}
    cancelled = []
    recipient._pool.scale = cancelled.append

    # Past the original deadline, but not past the placement's: still open.
    _transfer_tick(book, free_cpu=0.5, now=1000.0 + SIZER_TRANSFER_TIMEOUT_S + 1)
    assert recipient in book.state.in_flight

    # The placed actor never starts: the transfer times out from placement.
    _transfer_tick(book, free_cpu=0.5, now=placed_t + SIZER_TRANSFER_TIMEOUT_S + 1)
    assert not book.state.in_flight
    assert metrics.actions[-1] == ("bottleneck_transfer", "recipient", "timeout")
    (cancel,) = cancelled
    assert cancel.nodes == ("node",)


@pytest.mark.parametrize(
    "lost,expected", [(False, "landed"), (True, "lost")], ids=["started", "cancelled"]
)
def test_transfer_lands_only_if_its_claimed_actors_are_running(lost, expected):
    # Leaving the pending set is not landing: a claimed actor that was
    # cancelled (or died) instead of starting closes the transfer as lost.
    recipient = _op("recipient", pool=_Pool(lost_ids=["claimed-actor"] if lost else ()))
    metrics = _Metrics()
    book = _book(metrics=metrics)
    book.state.in_flight[recipient] = TransferInFlight(
        recipient=recipient,
        donors=frozenset({_donor_op()}),
        started_t=1000.0,
        claimed_ids=("claimed-actor",),
    )

    book.advance(_ctx(now=1000.0))

    assert not book.state.in_flight
    assert metrics.actions[-1] == ("bottleneck_transfer", "recipient", expected)


def test_transfer_whose_recipient_never_grew_closes_as_lost():
    # The recipient's upscale was refused, so no actor appeared after the
    # transfer fired: the donors gave up actors for nothing, which must not be
    # recorded as landed. Its pre-existing actors are not the transfer's.
    recipient = _op("recipient", pool=_Pool(running_ids=["old-actor"]))
    metrics = _Metrics()
    book = _book(metrics=metrics)
    book.state.in_flight[recipient] = TransferInFlight(
        recipient=recipient,
        donors=frozenset({_donor_op()}),
        started_t=1000.0,
        preexisting_ids=frozenset({"old-actor"}),
    )

    book.advance(_ctx(now=1000.0))

    assert not book.state.in_flight
    assert metrics.actions[-1] == ("bottleneck_transfer", "recipient", "lost")


def test_donor_cools_down_after_a_transfer_closes():
    # A donor must not be regrown -- by a decision or how_many -- until its
    # signals reflect the smaller pool: it stays held for the cooldown's
    # scheduling-loop steps after the transfer closes.
    donor = _op("donor", pool=_Pool(current=4))
    recipient = _op("recipient")
    lifecycles = _lifecycles(donor, recipient, cooldown_steps=2)
    book = _book(lifecycles=lifecycles)
    book.state.in_flight[recipient] = TransferInFlight(
        recipient=recipient,
        donors=frozenset({donor}),
        started_t=1000.0,
        claimed_ids=("claimed",),
    )
    # What firing a transfer does.
    lifecycles.start_receiving(recipient)
    lifecycles.start_donating(donor, recipient)

    # Recipient has no pendings -> lands; the donor starts cooling down.
    book.advance(_ctx(now=1000.0))
    assert not book.state.in_flight
    assert lifecycles.can_scale(recipient)
    assert lifecycles.state(donor) is OpLifecycle.COOLING

    lifecycles.on_step()
    assert not lifecycles.can_scale(donor)
    lifecycles.on_step()
    assert lifecycles.can_scale(donor)


def test_donor_funding_two_transfers_stays_donating_until_the_last_closes():
    # A donor can fund several recipients. The first transfer closing must
    # NOT start its cooldown: that is far shorter than the transfer timeout,
    # so the donor could be regrown while the other transfer still drains it.
    donor = _donor_op()
    landed = _op("landed")
    still_draining = _op(
        "still_draining", pool=_Pool(pending_ids={"node": ["draining-actor"]})
    )
    lifecycles = _lifecycles(donor, landed, still_draining, cooldown_steps=1)
    book = _book(lifecycles=lifecycles)
    for recipient, claimed in (
        (landed, "landed-actor"),
        (still_draining, "draining-actor"),
    ):
        book.state.in_flight[recipient] = TransferInFlight(
            recipient=recipient,
            donors=frozenset({donor}),
            started_t=1000.0,
            claimed_ids=(claimed,),
        )
    lifecycles.start_donating(donor, landed)

    book.advance(_ctx(now=1000.0))
    lifecycles.on_step()
    assert set(book.state.in_flight) == {still_draining}  # only one closed
    assert lifecycles.state(donor) is OpLifecycle.DONATING

    # Once the last transfer lands, the cooldown starts, then ends.
    still_draining._pool.pending_ids = {}
    book.advance(_ctx(now=1000.0))
    assert lifecycles.state(donor) is OpLifecycle.COOLING
    lifecycles.on_step()
    assert lifecycles.can_scale(donor)


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
def test_transfer_refuses_undoing_a_transfer_only_within_the_window(
    closed_t, now, refused
):
    # An earlier transfer moved capacity from ``op`` to ``donor``; this one
    # would move it back.
    op = _op("op")
    donor = _donor_op()
    metrics = _Metrics()
    book = _book(metrics=metrics)
    book.state.recent_transfers.append(
        RecentTransfer(recipient=donor, donors=frozenset({op}), closed_t=closed_t)
    )

    requests = _fire(book, op, donor, _ctx(now=now, non_pg_ops=[donor, op]))

    if refused:
        assert requests == []
        assert ("bottleneck_transfer", "thrashing") in metrics.refusals
    else:
        assert _donors(requests) == {donor}
        assert _owed(book.state) == {op: 1}


def test_closing_a_transfer_starts_its_thrash_window():
    donor = _donor_op()
    recipient = _op("recipient")
    book = _book()
    _fire(book, recipient, donor, _ctx(non_pg_ops=[donor, recipient]))
    (record,) = book.state.recent_transfers
    assert record.closed_t is None

    # Nothing owed or pending any more: the transfer closes (as lost, since
    # no actor was claimed) and stamps the close time.
    book.state.in_flight[recipient].owed_by_node = {}
    book.advance(_ctx(now=1500.0))

    assert not book.state.in_flight
    assert record.closed_t == 1500.0


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


# --- FUND_RECIPIENT ----------------------------------------------------------------


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
    _, state, requests = _apply_bottleneck(_policy(), tick, [labeled, gen])

    assert _donors(requests) == {gen}
    (placed,) = requests
    assert placed.request[gen].nodes == ("node",) * 4
    assert _owed(state) == {labeled: 1}
    # 1.5 x 30s is under the 60s floor.
    assert state.in_flight[labeled].timeout_s == 60.0


def test_bottleneck_transfer_holds_recipient_and_donors_until_it_closes():
    labeled = _op("Labeled", pool=_Pool(current=2, cpu=4))
    gen = _idle_victims_op("Gen", "node", [f"g{i}" for i in range(6)], current=15)
    tick = _tick(
        recipient=labeled,
        named=[(labeled, 1.0)],
        stable=[labeled],
        eligibility={labeled: "yes: victims"},
        shares={labeled: None, gen: None},
        slack={gen: 11},
    )
    lifecycles = _lifecycles(labeled, gen)
    policy = _policy(lifecycles=lifecycles)
    _apply_bottleneck(policy, tick, [labeled, gen])
    assert lifecycles.state(labeled) is OpLifecycle.RECEIVING
    assert lifecycles.state(gen) is OpLifecycle.DONATING
    assert policy.transfer_phase is TransferPhase.IN_FLIGHT


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
    _, state, requests = _apply_bottleneck(_policy(), tick, [predictor, preprocess])
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
    _, _, requests = _apply_bottleneck(_policy(), tick, [preprocess, predictor])
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
    _, state, requests = _apply_bottleneck(_policy(), tick, [write, read, features])
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
    policy = _policy()
    drained = []
    for _ in range(3):
        _, state, requests = _apply_bottleneck(
            policy, _surplus_tick(write, read), [write, read]
        )
        assert not state.in_flight and _granted(requests) == []
        drained.append(_donors(requests))
    assert drained == [set(), set(), {read}]
    (placed,) = requests
    assert placed.request[read].nodes == ("node",)


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
    _, state, _ = _apply_bottleneck(_policy(), tick, [recipient, donor])
    assert _owed(state) == {recipient: expected_b_prime - 4}


@pytest.mark.parametrize("ceiling, owed", [(5, 1), (4, None)])
def test_transfer_stays_within_the_recipients_growth_ceiling(ceiling, owed):
    # Upstream is done and B's remaining work needs ``ceiling`` actors: a
    # transfer funds it no further, and not at all once it is there.
    recipient = _op("B", pool=_Pool(current=4))
    donor = _idle_victims_op("A", "node", [f"a{i}" for i in range(4)], current=10)
    tick = _tick(
        recipient=recipient,
        named=[(recipient, 1.0)],
        stable=[recipient],
        eligibility={recipient: "yes: victims"},
        shares={recipient: None, donor: None},
        slack={donor: 8},
    )
    _, state, _ = _apply_bottleneck(
        _policy(), tick, [recipient, donor], growth_ceiling={recipient: ceiling}
    )
    assert _owed(state) == ({recipient: owed} if owed else {})


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
    policy = _policy()
    monkeypatch.setattr(policy.transfers, "fire", lambda *args, **kwargs: [])
    with caplog.at_level(logging.DEBUG):
        _, state, requests = _apply_bottleneck(policy, tick, [write, read])
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
    policy = _policy()
    _, _, first = _apply_bottleneck(policy, tick, [write, read])
    assert _donors(first) == {read}
    _, _, second = _apply_bottleneck(policy, tick, [write, read])
    assert _donors(second) == set()


# --- SHRINK_SURPLUS --------------------------------------------------------------


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
    policy = _policy()
    seen = []
    for _ in range(3):
        _, state, requests = _apply_bottleneck(
            policy,
            _blocked_tick(
                read, resize, blocked=blocked, self_limited=self_limited, n=n
            ),
            [read, resize],
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
    policy = _policy()
    seen = []
    for _ in range(3):
        _, _, requests = _apply_bottleneck(policy, tick, [consumer, donor])
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
    policy = _policy()
    for _ in range(4):
        _, _, requests = _apply_bottleneck(
            policy,
            _surplus_tick(write, read, recipient=case != "no-recipient"),
            [write, read],
        )
        assert _donors(requests) == set()


# --- GROW_FREE and GROW_BALANCED -------------------------------------------------


def test_free_growth_runs_while_the_cluster_resizes_but_transfers_wait():
    # Unsaturated and still resizing (allocation changed 2s ago): the named
    # bottleneck grows into free room, but no transfer fires until settled.
    write = _op("Write", pool=_Pool(current=2))
    read = _idle_victims_op("ReadVideo", "node", ["r0"], current=7)
    free_tick = _tick(
        named=[(write, 0.6)], eligibility={write: "yes: free"}, shares={write: None}
    )
    _, _, grown = _apply_bottleneck(
        _policy(), free_tick, [write], free_cpu=3.0, settle=2.0
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
    metrics = _Metrics()
    _, state, moved = _apply_bottleneck(
        _policy(metrics), full_tick, [write, read], settle=2.0
    )
    assert moved == [] and not state.in_flight
    assert ("bottleneck_transfer", HOLD_CLUSTER_SETTLING) in metrics.refusals


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
    _, state, requests = _apply_bottleneck(_policy(), tick, [write], free_cpu=3.0)
    assert _granted(requests) == [write]
    (placed,) = requests
    assert placed.request[write].nodes == ("node", "node")
    assert not state.in_flight


@pytest.mark.parametrize("ceiling, grown", [(3, 1), (2, 0)])
def test_free_growth_stays_within_the_growth_ceiling(ceiling, grown):
    # Upstream is done and Write's remaining work needs ``ceiling`` actors:
    # it grows no further, even with its growth target (4) and room for it.
    write = _op("Write", pool=_Pool(current=2))
    tick = _tick(
        named=[(write, 0.6)],
        eligibility={write: "yes: free"},
        shares={write: None},
        target={write: 4},
    )
    _, _, requests = _apply_bottleneck(
        _policy(), tick, [write], free_cpu=3.0, growth_ceiling={write: ceiling}
    )
    assert sum(p.request[write].delta for p in requests) == grown


def test_balanced_stages_each_grow_into_free_capacity():
    # No bottleneck named and every stage nearly always busy: each busy op
    # grows into free capacity. An idle op, an op at its max size and an op
    # with pending actors stay put. Little room: one actor each.
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
        _policy(), tick, [read, infer, idle, full, pending], free_cpu=5.0
    )
    assert sorted(op.name for op in _granted(requests)) == ["Infer", "Read"]
    for placed in requests:
        (op,) = placed.request
        assert len(placed.request[op].nodes) == 1
    assert ctx.free["node"].cpu == pytest.approx(3.0)
    assert not state.in_flight


@pytest.mark.parametrize(
    "free_cpu, idle_ops, expected_each",
    [
        # Half of 40 free CPUs over 2 unfinished ops: 10 each.
        (40, 0, 10),
        # An idle unfinished op counts in the split: half of 40 over 4 ops.
        (40, 2, 5),
        # Less room than ops: still one actor each.
        (3, 0, 1),
    ],
)
def test_balanced_growth_takes_half_the_free_capacity_split_over_unfinished_ops(
    free_cpu, idle_ops, expected_each
):
    read = _op("Read", pool=_Pool(current=2))
    infer = _op("Infer", pool=_Pool(current=4))
    idle = [_op(f"Idle{i}", pool=_Pool(current=1)) for i in range(idle_ops)]
    busy = SimpleNamespace(busy=0.95)
    tick = _tick(
        named=[],
        shares={
            read: busy,
            infer: busy,
            **{op: SimpleNamespace(busy=0.1) for op in idle},
        },
    )
    _, _, requests = _apply_bottleneck(
        _policy(), tick, [read, infer, *idle], free_cpu=free_cpu
    )
    grown = {
        op.name: placed.request[op].delta
        for placed in requests
        for op in placed.request
    }
    assert grown == {"Read": expected_each, "Infer": expected_each}


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
    _, _, requests = _apply_bottleneck(_policy(), tick, [read, write], free_cpu=5.0)
    assert _granted(requests) == []


def _apply_pg_bottleneck(policy, tick, pg_op, *, bundle_free_cpu, others=()):
    """Run ``policy`` with ``pg_op`` placed by a one-bundle placement group
    and no free node capacity."""
    bundle = BundleKey("pg", 0)
    ctx = _ctx(
        non_pg_ops=list(others),
        actor_pool_ops=[pg_op, *others],
        schedulable_bundles={pg_op: (bundle,)},
        pg_free={bundle: ExecutionResources(cpu=bundle_free_cpu)},
        bottleneck=tick,
    )
    _, requests = policy.step(ctx)
    return ctx, policy.transfers.state, requests, bundle


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
        _policy(), tick, infer, bundle_free_cpu=bundle_free_cpu, others=[gen]
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


def test_balanced_pg_op_grows_into_its_placement_group():
    # No bottleneck named and the PG op nearly always busy: it grows into its
    # PG, not into (absent) free node capacity: half of its 2 free slots.
    infer = _op("Infer", pool=_Pool(current=4))
    tick = _tick(named=[], shares={infer: SimpleNamespace(busy=0.95)})
    ctx, _, requests, bundle = _apply_pg_bottleneck(
        _policy(), tick, infer, bundle_free_cpu=2
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
        _policy(), tick, [download, *peers], free_cpu=free_cpu
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
    assert growth.free_growth_share(recipient, ctx) == 12


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
    assert growth.free_growth_share(recipient, ctx) == 2


def test_bottleneck_free_share_does_not_charge_reclaimed_actors():
    recipient = _op("B", pool=_Pool(current=24), reclaimable=4)
    peer = _op("Peer")
    tick = _tick(
        named=[(recipient, 0.5)],
        eligibility={recipient: "yes: free"},
        target={recipient: 36},
    )
    ctx, _, requests = _apply_bottleneck(_policy(), tick, [recipient, peer], free_cpu=6)
    request = requests[0].request[recipient]
    assert request.num_to_reclaim == 4
    assert len(request.nodes) == 3
    assert request.delta == 7
    assert ctx.free["node"].cpu == 3


def test_bottleneck_pg_free_share_counts_only_shared_bundles():
    recipient, shared, separate, ordinary = [
        _op(n) for n in ["B", "Shared", "Separate", "Ordinary"]
    ]
    bundle, other = BundleKey("pg", 0), BundleKey("other", 0)
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
    assert growth.free_growth_share(recipient, ctx) == 6
    planned = growth.plan_free_growth(recipient, 20, ctx)
    assert planned is not None
    grown = growth.apply_free_growth(
        recipient, planned, ctx, cast(Any, _Placement()), "bottleneck"
    )
    assert grown is not None
    assert grown.request[recipient].delta == 6
    assert ctx.pg_free[bundle].cpu == 6
    assert ctx.pg_free[other].cpu == 100


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
