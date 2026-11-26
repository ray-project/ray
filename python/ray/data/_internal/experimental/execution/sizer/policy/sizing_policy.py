"""The sizer's one policy: each tick, one rule per op.

``SizingPolicy.decide_op`` is the one place that decides how an op is
resized. It matches on the op's status (``op_status``), then reads the op's
own signals (the sizer's phase, the bottleneck snapshot's window) and picks one
rule (``OpRule``):

    status      signals                                   rule
    Completed   actors left                               RELEASE_POOL
    Held        -                                         NONE (room reserved)
    Cooling     below min size                            UPSCALE_TO_MIN
    Active      below min size                            UPSCALE_TO_MIN
    Active      fixed-size pool                           NONE
    Active      warmup                                    UPSCALE_WARMUP
    Active      a named bottleneck that can use actors    UPSCALE_BOTTLENECK
    Active      no bottleneck named, nearly always busy   UPSCALE_BUSY
    Active      blocked by its consumer some of the time,
                and a short upscaler fits where it frees  DOWNSCALE_BLOCKED
    Active      idle some of the time                     DOWNSCALE_IDLE
    otherwise                                             NONE

After warmup, the policy does not predict an op's right size. It steps the
op toward it (``steps.StepSizes``) and lets the bottleneck snapshot say, a
window later, whether to keep going. An op the policy stepped cools down
until its window
describes its new size (``Cooling``). A downscale keeps enough actors that
the op stays below the busy share that upscales it, so it does not turn
straight back into an upscale.

``SizingPolicy.decide`` runs ``decide_op`` for every op, then grants upscales
free capacity in priority order: min sizes first, then bottlenecks, most
severe first, then busy ops, then warmup shares. So the op that limits the
pipeline gets free capacity before any other op, including the room a
downscale frees.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import (
    SIZER_BOTTLENECK_ARROW_MARGIN,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    FIT_EPS,
    PLACEMENT_RESOURCE_DIMS,
)
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    Snapshot,
    blocked_share,
    why_should_not_upscale_blocker,
)
from ray.data._internal.experimental.execution.sizer.common import (
    CountByNode,
    ExecutionResourcesByNode,
    LandedUpscales,
)
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles
from ray.data._internal.experimental.execution.sizer.op_status import (
    Active,
    Completed,
    Cooling,
    Held,
    OpStatusMachine,
)
from ray.data._internal.experimental.execution.sizer.policy.ceiling import (
    upscale_ceiling,
)
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    STEP_RULE_DIRECTION,
    OpDecision,
    OpRule,
    SizerPhase,
)
from ray.data._internal.experimental.execution.sizer.policy.steps import (
    Direction,
    StepSizes,
)
from ray.data._internal.experimental.execution.sizer.policy.warmup import (
    WarmupShares,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)

OpDecisions = Dict["ExperimentalActorPoolMapOperator", OpDecision]

# With no bottleneck named, an op busy at least this share of its slot time
# upscales: the stages are balanced, so each of them limits the pipeline.
_UPSCALE_BUSY_SHARE = 1.0 - SIZER_BOTTLENECK_ARROW_MARGIN

# An op that wastes at least this share of its slot time downscales. Two
# kinds of time count as waste: time its consumer blocks it (its actors hold
# full output budgets and add no throughput) and time it is starved (idle
# with nothing queued). The margin is the same noise margin as for busy.
# ``_downscale_floor`` keeps a downscale from overshooting into a busy op.
_WASTED_SHARE_MIN = SIZER_BOTTLENECK_ARROW_MARGIN

# A blocked op does not downscale while an op it feeds starves at least this
# share of its slot time: its actors' output budgets are that op's supply.
_CONSUMER_STARVED_MAX = 0.05

# The order upscales take free capacity in (lower first). Bottlenecks are
# further ordered by severity, and every rank by the tick's op order.
_UPSCALE_RANK = {
    OpRule.UPSCALE_TO_MIN: 0,
    OpRule.UPSCALE_BOTTLENECK: 1,
    OpRule.UPSCALE_BUSY: 2,
    OpRule.UPSCALE_WARMUP: 3,
}


class SizingPolicy:
    """Decides one rule per op each tick. Owns what carries across ticks:
    each op's step sizes, and the allocation change it last saw."""

    def __init__(
        self,
        *,
        statuses: OpStatusMachine,
        profiles: OpProfiles,
        steps: Optional[StepSizes] = None,
    ):
        self._statuses = statuses
        self._profiles = profiles
        self.warmup = WarmupShares(profiles=profiles)
        self.steps = steps if steps is not None else StepSizes()
        # ``seconds_since_allocation_change`` at the last tick: when it goes
        # down, the allocation changed in between.
        self._last_allocation_age_s: Optional[float] = None

    def decide(self, *, ctx: DecisionContext) -> OpDecisions:
        """One tick: one ``OpDecision`` per op, in the order WHERE should
        place them.

        1. ``decide_op`` picks each op's rule and request.
        2. Upscales take free capacity in priority order (see the module
           docstring), each cut to what the ledger still has.
        3. A blocked op downscales only to make room for upscalers that
           are short (``unmet``), and by no more than that room
           (``_RoomToMake``). Its actors have queued work, so removing
           them when no op needs the room only costs drains, and they are
           needed again once the consumer speeds up. Downscales sort last,
           so every upscale's ``unmet`` part is known by then. Blocked ops
           share the room still needed, so two of them do not both make the
           same room. A blocked op that is kept may still downscale as
           idle.
        4. Each op the policy stepped starts cooling. A downscale records
           its step now; an upscale records it once its actors land
           (``record_landed_upscales``).
        """
        self._reset_steps_on_allocation_change(ctx=ctx)
        order = {op: i for i, op in enumerate(ctx.snapshot.ordered_ops)}
        wanted = {op: self.decide_op(op=op, ctx=ctx) for op in ctx.snapshot.ordered_ops}
        severity = (
            dict(ctx.bottleneck_snapshot.bottlenecks)
            if ctx.bottleneck_snapshot is not None
            else {}
        )

        def priority(op: "ExperimentalActorPoolMapOperator") -> Tuple[int, float, int]:
            """During each tick, an operator has to compete with other operators
            for resources. But how do you rank across operators? This function returns
            the priority rank for which operator gets higher scheduling priority. A lower
            value is a higher priority. Here's the rank tuple:

                1. Type of action (See `_UPSCALE_RANK`)
                2. How severe of a bottleneck it is
                3. Index in topological order.

            """
            decision = wanted[op]
            rank = _UPSCALE_RANK.get(decision.rule, len(_UPSCALE_RANK))
            return (rank, -severity.get(op, 0.0), order[op])

        decisions: OpDecisions = {}
        # The room short upscalers still need; made when the first blocked
        # downscale comes up, after every upscale is granted.
        room: Optional[_RoomToMake] = None
        for op in sorted(wanted, key=priority):
            decision = _within_capacity(op=op, decision=wanted[op], ctx=ctx)
            request = decision.request
            if decision.rule is OpRule.DOWNSCALE_BLOCKED and request is not None:
                if room is None:
                    # Lazily create until we have a downscale blocked rule. It basically
                    # takes a snapshot of the free resources, and then delegates them out
                    # similar to CapacityLedger, except the decisions are not set in stone,
                    # and does more custom logic.
                    room = _RoomToMake(decisions=decisions, ctx=ctx)
                assert (
                    request.delta < 0
                ), f"A {decision.rule} means we should downscale with <= -1, but got {request.delta}"
                remove = room.make(op=op, most=-request.delta)
                if remove == 0:
                    logger.debug(
                        "SizingPolicy: %s not downscaled as blocked: no short "
                        "upscaler would fit where it frees actors",
                        op.name,
                    )
                    decision = self._downscale_idle(op=op, ctx=ctx)
                elif remove < -request.delta:
                    decision = OpDecision(
                        decision.rule,
                        ActorPoolScalingRequest.downscale(
                            delta=-remove, force=True, reason=request.reason
                        ),
                    )
            decisions[op] = decision
            if decision.rule in STEP_RULE_DIRECTION and decision.request is not None:
                size = op.actor_pool.serving_size()
                delta = decision.request.delta
                if delta > 0:
                    self._statuses.start_cooling(op=op, now=ctx.now, upscale_from=size)
                else:
                    self.steps.record(
                        op=op,
                        direction=Direction.DOWN,
                        before=size,
                        after=size + delta,
                    )
                    self._statuses.start_cooling(op=op, now=ctx.now)
        return decisions

    def record_landed_upscales(
        self,
        *,
        landed: LandedUpscales,
    ) -> None:
        """Adapt each op's upscale step to the actors that actually started,
        as op -> ``SizeChange`` from ``OpStatusMachine.on_tick``."""
        for op, change in landed.items():
            self.steps.record(
                op=op,
                direction=Direction.UP,
                before=change.before,
                after=change.after,
            )

    def decide_op(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        ctx: DecisionContext,
    ) -> OpDecision:
        """The rule for ``op`` this tick and its request, before capacity
        (see the module docstring)."""
        pool = op.actor_pool
        match self._statuses.status(op):
            case Completed():
                if pool.serving_size() == 0:
                    return OpDecision(OpRule.NONE)
                return OpDecision(
                    OpRule.RELEASE_POOL,
                    ActorPoolScalingRequest.downscale(
                        delta=-pool.serving_size(),
                        # Release pending and running actors alike. Running
                        # actors drain first, so in-flight work still finishes.
                        force=True,
                        reason="operator completed",
                    ),
                )
            case Held():
                return OpDecision(OpRule.NONE)
            case Cooling():
                return self._upscale_to_min(op) or OpDecision(OpRule.NONE)
            case Active():
                return self._decide_active(op=op, ctx=ctx)
        raise AssertionError(f"unknown status for {op.name}")

    def _decide_active(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        ctx: DecisionContext,
    ) -> OpDecision:
        pool = op.actor_pool
        to_min = self._upscale_to_min(op)
        if to_min is not None:
            return to_min
        if pool.min_size() == pool.max_size():
            return OpDecision(OpRule.NONE)
        if ctx.phase is SizerPhase.WARMUP:
            # We special case the warmup phase to go
            # to the warmup target size.
            want = self.warmup.upscale(
                op=op,
                cluster_capacity=ctx.snapshot.get_global_limits(),
                pg_slots=ctx.snapshot.pg_slots,
            )
            want = _cap_at_ceiling(op=op, want=want, ctx=ctx)
            if want is None:
                return OpDecision(OpRule.NONE)
            return OpDecision(OpRule.UPSCALE_WARMUP, want)

        bottleneck_snapshot = ctx.bottleneck_snapshot
        shares = (
            bottleneck_snapshot.shares.get(op)
            if bottleneck_snapshot is not None
            else None
        )
        if bottleneck_snapshot is None or shares is None:
            return OpDecision(OpRule.NONE)
        next_ops = self._profiles.next_ops
        named = dict(bottleneck_snapshot.bottlenecks)
        if op in named:
            reason = why_should_not_upscale_blocker(
                op=op,
                snapshot=bottleneck_snapshot,
                next_ops=next_ops,
                margin=SIZER_BOTTLENECK_ARROW_MARGIN,
            )
            if reason is not None:
                logger.debug("SizingPolicy: %s not upscaled: %s", op.name, reason)
                return OpDecision(OpRule.NONE)
            return self._step(
                op=op,
                rule=OpRule.UPSCALE_BOTTLENECK,
                reason=f"bottleneck (severity {named[op]:.2f})",
                ctx=ctx,
            )
        if not named and shares.busy >= _UPSCALE_BUSY_SHARE:
            return self._step(
                op=op,
                rule=OpRule.UPSCALE_BUSY,
                reason=f"busy {shares.busy:.2f} with no bottleneck named",
                ctx=ctx,
            )
        blocked = blocked_share(
            op=op,
            shares=bottleneck_snapshot.shares,
            next_ops=next_ops,
            edge_shares=bottleneck_snapshot.edge_shares,
        )
        if blocked >= _WASTED_SHARE_MIN and not self._feeds_starving_op(
            op=op, bottleneck_snapshot=bottleneck_snapshot
        ):
            decision = self._step(
                op=op,
                rule=OpRule.DOWNSCALE_BLOCKED,
                reason=f"blocked by its consumer {blocked:.2f}",
                ctx=ctx,
                floor=_downscale_floor(op=op, wasted=blocked),
            )
            if decision.rule is not OpRule.NONE:
                return decision
        return self._downscale_idle(op=op, ctx=ctx)

    def _downscale_idle(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        ctx: DecisionContext,
    ) -> OpDecision:
        """A downscale of ``op`` if it is starved at least
        ``_WASTED_SHARE_MIN`` of its slot time, else no change."""
        snapshot = ctx.bottleneck_snapshot
        shares = snapshot.shares.get(op) if snapshot is not None else None
        if shares is None or shares.starved < _WASTED_SHARE_MIN:
            return OpDecision(OpRule.NONE)
        return self._step(
            op=op,
            rule=OpRule.DOWNSCALE_IDLE,
            reason=f"idle (starved {shares.starved:.2f})",
            ctx=ctx,
            floor=_downscale_floor(op=op, wasted=shares.starved),
        )

    # --- requests ---------------------------------------------------------------

    def _step(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        rule: OpRule,
        reason: str,
        ctx: DecisionContext,
        floor: int = 0,
    ) -> OpDecision:
        """One step of ``op`` in ``rule``'s direction, kept within [min size,
        upscale ceiling]. A downscale also keeps at least ``floor`` actors."""
        assert rule in STEP_RULE_DIRECTION, (
            "Invalid Rule passed into the step function: {rule}."
            "Must be a one of {STEP_RULE_DIRECTION.keys()}"
        )
        direction = STEP_RULE_DIRECTION[rule]
        pool = op.actor_pool
        cur_size = pool.serving_size()
        delta = self.steps.delta(op=op, direction=direction, size=cur_size)
        match direction:
            case Direction.UP:
                target = cur_size + delta
                delta = target - op.actor_pool.serving_size()
                if delta <= 0:
                    return OpDecision(OpRule.NONE)
                want = _cap_at_ceiling(
                    op=op,
                    want=ActorPoolScalingRequest.upscale(delta=delta, reason=reason),
                    ctx=ctx,
                )
                return (
                    OpDecision(rule, want)
                    if want is not None
                    else OpDecision(OpRule.NONE)
                )
            case Direction.DOWN:
                delta = max(delta, pool.min_size() - cur_size, floor - cur_size)
                if delta >= 0:
                    return OpDecision(OpRule.NONE)
                return OpDecision(
                    rule,
                    ActorPoolScalingRequest.downscale(
                        delta=delta, force=True, reason=reason
                    ),
                )
        raise AssertionError(f"unknown direction for {direction} to scale")

    @staticmethod
    def _upscale_to_min(
        op: "ExperimentalActorPoolMapOperator",
    ) -> Optional[OpDecision]:
        """An upscale to min size when ``op`` is below it (min size is a floor
        the user set, so the upscale ceiling does not cut it)."""
        pool = op.actor_pool
        below_min = pool.min_size() - pool.serving_size()
        if below_min <= 0:
            return None
        return OpDecision(
            OpRule.UPSCALE_TO_MIN,
            ActorPoolScalingRequest.upscale(delta=below_min, reason="below min size"),
        )

    # --- signals ----------------------------------------------------------------

    def _feeds_starving_op(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        bottleneck_snapshot: Snapshot["ExperimentalActorPoolMapOperator"],
    ) -> bool:
        """Whether an op that ``op`` feeds starves at least
        ``_CONSUMER_STARVED_MAX`` of its slot time."""
        return any(
            bottleneck_snapshot.shares[consumer].starved >= _CONSUMER_STARVED_MAX
            for consumer in self._profiles.next_ops.get(op, ())
            if consumer in bottleneck_snapshot.shares
        )

    def _reset_steps_on_allocation_change(self, *, ctx: DecisionContext) -> None:
        """Start every op's steps over when the dataset's allocation changed
        since the last tick: the steps learned before it no longer fit."""
        age = ctx.snapshot.seconds_since_allocation_change
        if (
            self._last_allocation_age_s is not None
            and age < self._last_allocation_age_s
        ):
            logger.debug("SizingPolicy: allocation changed; step sizes reset")
            self.steps.reset()
        self._last_allocation_age_s = age


class _RoomToMake:
    """The room short upscalers still need this tick, and the blocked ops'
    actors that make it.

    A short upscaler is one whose upscale found no room in the ledger
    (``OpDecision.unmet``). Its actors can only land on nodes where free
    capacity, plus what the blocked ops free there, holds one of them: an
    actor needing a GPU and a CPU gains nothing from CPUs freed next to a
    GPU already taken. So the room is counted per node: on each node a short
    actor may use, the blocked ops remove only enough actors to cover what
    that actor lacks there. WHERE removes actors on those nodes first
    (``scale_where.compute_favored_nodes``).

    Each node starts from its free capacity once its draining actors finish
    (``ClusterStateSnapshot.free_after_drains_by_node``), not its free capacity
    now. A drain an earlier tick started to make room still holds that room
    until it ends, and the short op stays short meanwhile. Counting only what
    is free now would remove more actors for the same room on every tick
    until the drain ends.
    """

    def __init__(self, *, decisions: OpDecisions, ctx: DecisionContext):
        self._ctx = ctx
        # Short op -> actors it still needs room for.
        self._needed: Dict["ExperimentalActorPoolMapOperator", int] = {
            op: d.unmet
            for op, d in decisions.items()
            if d.unmet > 0 and not ctx.apmo_profiles.is_pg_managed(op)
        }
        # Per node: free capacity once draining actors finish, plus what
        # blocked ops removed this tick, minus what the short actors placed on
        # it this tick take.
        free_after_drains = ctx.snapshot.free_after_drains_by_node
        self._free_by_node: ExecutionResourcesByNode = {
            node_id: free_after_drains.get(node_id, res).copy()
            for node_id, res in ctx.snapshot.free_by_node.items()
        }

    def make(self, *, op: "ExperimentalActorPoolMapOperator", most: int) -> int:
        """How many of ``op``'s actors to remove, at most ``most``, to make
        room for the short actors still waiting. Short actors are placed in
        the plan one at a time, in the order their ops were granted."""
        per_actor = op.actor_pool.per_actor_resource_usage()
        num_serving_actors_left_by_node = self._num_serving_actors_by_node(op)
        num_to_remove = 0
        for short_op in self._needed:
            short_per_actor = short_op.actor_pool.per_actor_resource_usage()
            nodes = sorted(self._ctx.snapshot.schedulable_nodes_by_op.get(short_op, ()))
            for node_id in nodes:
                while self._needed[short_op] > 0:
                    zero = ExecutionResources.zero()
                    room = self._free_by_node.get(node_id, zero)
                    cost = _actors_to_fit(
                        room=room,
                        per_actor=per_actor,
                        short_per_actor=short_per_actor,
                    )
                    if cost is None or cost > num_serving_actors_left_by_node.get(
                        node_id, 0
                    ):
                        break
                    if num_to_remove + cost > most:
                        return num_to_remove
                    num_to_remove += cost
                    num_serving_actors_left_by_node[node_id] = (
                        num_serving_actors_left_by_node.get(node_id, 0) - cost
                    )
                    self._free_by_node[node_id] = room.add(
                        per_actor.scale(cost)
                    ).subtract(short_per_actor)
                    self._needed[short_op] -= 1
        return num_to_remove

    @staticmethod
    def _num_serving_actors_by_node(
        op: "ExperimentalActorPoolMapOperator",
    ) -> CountByNode:
        """Num actors per node that a downscale can still remove.
        Basically excludes draining actors"""
        serving_by_node: CountByNode = {}
        for node_id, counts in op.actor_counts_by_node().items():
            serving = sum(
                n for status, n in counts.items() if status is not ActorStatus.DRAINING
            )
            if serving > 0:
                serving_by_node[node_id] = serving
        return serving_by_node


def _actors_to_fit(
    *,
    room: ExecutionResources,
    per_actor: ExecutionResources,
    short_per_actor: ExecutionResources,
) -> int | None:
    """How many actors of shape ``per_actor`` to remove from a node with
    ``room`` left so that one actor of shape ``short_per_actor`` fits there,
    or None if removing them cannot make it fit (it lacks a resource they do
    not hold)."""
    count = 0
    for dim in PLACEMENT_RESOURCE_DIMS:
        room_left: float = getattr(room, dim)
        need: float = getattr(short_per_actor, dim)
        lacking = need - room_left
        if lacking <= FIT_EPS:
            # Here, the short actor can already fit along resource `dim`
            continue
        to_remove = getattr(per_actor, dim)
        if to_remove <= 0:
            return None
        count = max(count, math.ceil(lacking / to_remove - FIT_EPS))
    return count


def _downscale_floor(*, op: "ExperimentalActorPoolMapOperator", wasted: float) -> int:
    """The smallest size ``op`` can downscale to without becoming busy
    enough to upscale again.

    ``wasted`` is the share of slot time the op spent not doing useful work
    (starved, or output-backpressured). The op's useful work is then
    ``(1 - wasted) * size`` actors' worth. Spread over ``target`` actors, the
    busy share becomes ``(1 - wasted) * size / target``. That share must stay
    at or under ``_UPSCALE_BUSY_SHARE``, or the next full window upscales the
    op back. Solving for ``target``:

        target >= size * (1 - wasted) / _UPSCALE_BUSY_SHARE

    For example, 10 actors with ``wasted = 0.5`` floor at 6 actors, each
    about 83% busy. At 5, they would be fully busy and upscale again.
    """
    size = op.actor_pool.serving_size()
    return math.ceil(size * (1.0 - wasted) / _UPSCALE_BUSY_SHARE)


def _cap_at_ceiling(
    *,
    op: "ExperimentalActorPoolMapOperator",
    want: Optional[ActorPoolScalingRequest],
    ctx: DecisionContext,
) -> Optional[ActorPoolScalingRequest]:
    """Cut an upscale to ``op``'s upscale ceiling, or None if it is there."""
    if want is None or want.delta <= 0:
        return want
    allowed = (
        upscale_ceiling(op=op, op_state=ctx.topology.get(op))
        - op.actor_pool.serving_size()
    )
    if allowed <= 0:
        return None
    if allowed >= want.delta:
        return want
    return ActorPoolScalingRequest.upscale(delta=allowed, reason=want.reason)


def _within_capacity(
    *,
    op: "ExperimentalActorPoolMapOperator",
    decision: OpDecision,
    ctx: DecisionContext,
) -> OpDecision:
    """``decision`` with its upscale cut to what still fits, granted in the
    ledger, and the part that did not fit kept as ``unmet``. Downscales pass
    through. An upscale with no room left keeps its rule but drops its
    request."""
    want = decision.request
    if want is None or want.delta <= 0:
        return decision
    delta = min(want.delta, ctx.capacity.max_upscale_delta(op))
    if delta <= 0:
        return OpDecision(decision.rule, unmet=want.delta)
    ctx.capacity.grant(op, delta)
    if delta == want.delta:
        return decision
    return OpDecision(
        decision.rule,
        request=ActorPoolScalingRequest.upscale(delta=delta, reason=want.reason),
        unmet=want.delta - delta,
    )
