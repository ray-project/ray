"""The sizer's one policy: each tick, one action per op.

``SizingPolicy.decide`` returns an ``OpDecision`` for every op. It first acts
on the bottleneck model, then sizes every op the model left alone with the
per-op rules (``PerOpRules``: release a completed pool, grow to held room,
grow toward the warmup share, shed idle actors, refill to min size).

The bottleneck model (``bottleneck_observer``) names, each tick, the
bottlenecks and the most severe stable one that can use capacity (the
recipient). ``decision.classify`` turns that into one ``DecisionKind``, and
the policy carries it out:

- GROW_FREE: the bottleneck grows into free capacity toward its growth
  target b' = ceil(b / (1 - w)), w the largest share of time a neighbour
  waits on it. New actors are capped at a share of free placeable capacity
  among unfinished, growable operators competing for the same resource
  class and placement domain (at least one actor when any fits). A
  PG-managed op grows into its own placement group.
- GROW_BALANCED: with no bottleneck named, every op that is nearly always
  busy grows by ``growth.balanced_step`` into free capacity.
- FUND_RECIPIENT: with no transfer in flight, pick b', the largest size up
  to the growth target that donors can fund, every non-bottleneck op giving
  at most its slack at b' (b'/b more work on the recipient's path, the same
  off it). Victims are only pending or idle actors, fastest to free first;
  busy surplus drains passively instead (``_shrink_toward_keep``) and
  reaches the recipient through GROW_FREE on a later tick.
- SHRINK_SURPLUS: with no recipient, drain ops mostly blocked behind their
  consumer down toward what they need.

Every growth stays within the op's growth ceiling (its pool's max size, or
the actors its remaining work can use once upstream is done). The
mechanics of a transfer (owed capacity held for the recipient, bring-up,
timeout, giving stranded victims back, thrash guard) are ``TransferBook``'s.
"""

from __future__ import annotations

import dataclasses
import logging
import math
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.execution.execution_flags import (
    SIZER_BOTTLENECK_MAX_STEP_RATIO,
    SIZER_BOTTLENECK_MAX_TIMEOUT_S,
    SIZER_BOTTLENECK_SLACK_MARGIN,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleDownRequest,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.metrics import (
    OptimizerMetrics,
)
from ray.data._internal.experimental.execution.sizer.models import (
    PlacedSizingRequest,
)
from ray.data._internal.experimental.execution.sizer.op_profile import OpProfiles
from ray.data._internal.experimental.execution.sizer.op_status import OpStatusMachine
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.decision import (
    HOLD_CLUSTER_SETTLING,
    classify,
    why_cannot_grow,
)
from ray.data._internal.experimental.execution.sizer.policy.growth import (
    apply_free_growth,
    growth_ceiling,
    plan_free_growth,
)
from ray.data._internal.experimental.execution.sizer.policy.per_op import PerOpRules
from ray.data._internal.experimental.execution.sizer.policy.states import (
    Decision,
    DecisionKind,
    OpDecision,
    OpRule,
    TransferPhase,
)
from ray.data._internal.experimental.execution.sizer.policy.transfer import (
    DonorCapacity,
    NodePlan,
    TransferBook,
    VictimCandidate,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_models import (  # noqa: E501
        PlacedScalingRequest,
    )
    from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
        ActorPlacementStrategy,
    )
    from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
        BottleneckTick,
    )

logger = logging.getLogger(__name__)

# A transfer's timeout is this many times its slowest victim's estimated
# time-to-free: the estimate is a lower bound.
_TIMEOUT_PER_TTF = 1.5


def transfer_timeout_s(victim_ttfs: List[float]) -> float:
    """Seconds a transfer may go without progress: 1.5x its slowest victim's
    estimated time-to-free, within [SIZER_TRANSFER_TIMEOUT_S,
    SIZER_BOTTLENECK_MAX_TIMEOUT_S]."""
    slowest = max(victim_ttfs, default=0.0)
    if math.isinf(slowest):
        return SIZER_BOTTLENECK_MAX_TIMEOUT_S
    return min(
        max(slowest * _TIMEOUT_PER_TTF, SIZER_TRANSFER_TIMEOUT_S),
        SIZER_BOTTLENECK_MAX_TIMEOUT_S,
    )


# Ticks a donor's surplus must persist before one of its actors is drained,
# so one noisy window does not shrink an op.
_SURPLUS_STREAK_TICKS = 3

# With no recipient, an op whose output is blocked (its budget full while the
# consumer has no idle slot) at least this share of its slot time is shrunk
# toward its keep size, but never below the floor: its actors pin full
# output budgets in the object store and add no throughput.
_BLOCKED_SHARE_MIN = 0.8
_BLOCKED_KEEP_FLOOR = 2

# Neither shrink drains an op while an op it feeds starves at least this
# share of its slot time. An op mostly blocked behind its consumer does
# little work, but its actors' output budgets are the consumer's supply:
# in video_object_detection, resize_frame (busy 0.04) kept the GPU op fed
# at 8 actors (starved 0.01-0.05) and not at 4 (starved climbing past 0.1),
# while its keep from busy and self-limited time was 2.
_CONSUMER_STARVED_MAX = 0.05


def max_step_size(b: int, max_size: int) -> int:
    """The largest b' one step may grow a recipient of size b to."""
    return max(
        b + 1, min(max_size, math.floor(max(b, 1) * SIZER_BOTTLENECK_MAX_STEP_RATIO))
    )


# The rule each bottleneck decision gives the ops it touches. FUND_RECIPIENT
# gives FUND to the recipient and DONATE to everyone else it touches.
_RULE_BY_KIND: Dict[DecisionKind, OpRule] = {
    DecisionKind.GROW_FREE: OpRule.GROW_FREE,
    DecisionKind.GROW_BALANCED: OpRule.GROW_BALANCED,
    DecisionKind.FUND_RECIPIENT: OpRule.DONATE,
    DecisionKind.SHRINK_SURPLUS: OpRule.SHRINK_SURPLUS,
}


def _within_capacity(
    op: "ExperimentalActorPoolMapOperator", decision: OpDecision, ctx: DecisionContext
) -> OpDecision:
    """``decision`` with its upscale cut to what still fits, granted in the
    ledger. Downscales pass through. Warmup shares are already bounded, but
    cutting them too keeps a constrained op from getting an all-cluster share
    it cannot place. An upscale with no room left keeps its rule but drops
    its request."""
    want = decision.request
    if want is None or want.delta <= 0:
        return decision
    delta = min(want.delta, ctx.capacity.max_upscale_delta(op))
    if delta <= 0:
        return OpDecision(decision.rule)
    ctx.capacity.grant(op, delta)
    if delta == want.delta:
        return decision
    return OpDecision(
        decision.rule,
        request=ActorPoolScalingRequest.upscale(delta=delta, reason=want.reason),
    )


class SizingPolicy:
    """Decides one action per op each tick. Owns what persists across ticks:
    open transfers (``TransferBook``), the surplus streaks, and the per-op
    rules' idle streaks."""

    # The metrics label for this policy's actions and refusals.
    name = "bottleneck_transfer"

    def __init__(
        self,
        metrics: OptimizerMetrics,
        placement: "ActorPlacementStrategy",
        statuses: OpStatusMachine,
        profiles: OpProfiles,
    ):
        self._metrics = metrics
        self.per_op = PerOpRules(profiles=profiles)
        # The bottleneck decision of the last tick, for logs and tests.
        self.last_decision: Optional[Decision] = None
        self._placement = placement
        self._statuses = statuses
        self._book = TransferBook(self.name, metrics, placement, statuses)
        # Per donor: consecutive ticks it has had surplus toward the recipient.
        self._surplus_streak: Dict["ExperimentalActorPoolMapOperator", int] = {}
        # Per op: consecutive ticks it has been blocked with surplus while
        # there was no recipient.
        self._blocked_streak: Dict["ExperimentalActorPoolMapOperator", int] = {}
        self._handlers: Dict[
            DecisionKind,
            Callable[[Decision, DecisionContext], List[PlacedSizingRequest]],
        ] = {
            DecisionKind.GROW_FREE: self._grow_free,
            DecisionKind.GROW_BALANCED: self._grow_balanced,
            DecisionKind.FUND_RECIPIENT: self._fund_recipient,
            DecisionKind.SHRINK_SURPLUS: self._shrink_surplus,
            DecisionKind.HOLD: self._hold,
        }

    @property
    def transfers(self) -> TransferBook:
        return self._book

    @property
    def transfer_phase(self) -> TransferPhase:
        return self._book.phase

    def held_capacity_by_node(self) -> Dict[NodeIdStr, ExecutionResources]:
        """Capacity open transfers hold for their recipients' owed actors."""
        return self._book.held_capacity_by_node()

    def decide(
        self, ctx: DecisionContext
    ) -> Dict["ExperimentalActorPoolMapOperator", OpDecision]:
        """One tick: one ``OpDecision`` per op.

        1. Advance open transfers, and bring up the actors they owe their
           recipients (FUND).
        2. Classify the bottleneck decision and carry it out (GROW_FREE,
           GROW_BALANCED, FUND / DONATE, SHRINK_SURPLUS). These requests
           already name their nodes.
        3. Run the per-op rules for every op that steps 1 and 2 did not
           touch, in the tick's op order, and grant each upscale only what
           is still free. A completed op always gets its per-op action.

        Every step checks and spends capacity in ``ctx.capacity``, so a
        later step never hands out capacity an earlier one took.
        """
        placed: Dict[
            "ExperimentalActorPoolMapOperator",
            Tuple[OpRule, List["PlacedScalingRequest"]],
        ] = {}

        def add(requests: List[PlacedSizingRequest], rule_for) -> None:
            for request in requests:
                for op, req in request.request.items():
                    _, reqs = placed.setdefault(op, (rule_for(op), []))
                    reqs.append(req)

        self._book.advance(ctx)
        # Open transfers place first, into the capacity ``advance`` held.
        add(self._book.bring_up_held_actors(), lambda op: OpRule.FUND)

        decision = classify(ctx, self._statuses)
        self.last_decision = decision
        logger.debug(
            "SizingPolicy: phase=%s transfer=%s decision=%s%s (%s)",
            ctx.phase.value,
            self._book.phase.value,
            decision.kind.value,
            "".join(f" {op.name}->{target}" for op, target in decision.targets.items()),
            decision.reason,
        )
        default_rule = _RULE_BY_KIND.get(decision.kind)
        if default_rule is not None:
            add(
                self._handlers[decision.kind](decision, ctx),
                lambda op: (
                    OpRule.FUND
                    if decision.kind is DecisionKind.FUND_RECIPIENT
                    and op in decision.targets
                    else default_rule
                ),
            )
        else:
            self._handlers[decision.kind](decision, ctx)

        decisions: Dict["ExperimentalActorPoolMapOperator", OpDecision] = {
            op: OpDecision(rule, placed=tuple(reqs))
            for op, (rule, reqs) in placed.items()
        }
        if ctx.snapshot is not None:
            for op in ctx.snapshot.ordered_ops:
                if op in decisions and not op.has_completed():
                    continue
                wanted = self.per_op.decide(
                    op,
                    status=self._statuses.status(op),
                    phase=ctx.phase,
                    modeled=op in ctx.modeled_ops,
                    snapshot=ctx.snapshot,
                    op_state=ctx.topology[op],
                )
                decisions[op] = _within_capacity(op, wanted, ctx)
        return decisions

    # --- decision handlers ----------------------------------------------------

    def _hold(
        self, decision: Decision, ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        if decision.reason == HOLD_CLUSTER_SETTLING:
            self._metrics.record_refusal(self.name, decision.reason)
        return []

    def _grow_free(
        self, decision: Decision, ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        return self._grow_each(decision, ctx)

    def _grow_balanced(
        self, decision: Decision, ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        return self._grow_each(decision, ctx)

    def _grow_each(
        self, decision: Decision, ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        """Grow every op in the decision toward its target, in order, each
        planned against the free capacity the ones before it left."""
        requests: List[PlacedSizingRequest] = []
        for op, target in decision.targets.items():
            growth = plan_free_growth(op, target, ctx)
            if growth is None:
                continue
            grown = apply_free_growth(
                op, growth, ctx, self._placement, reason=decision.reason
            )
            if grown is not None:
                requests.append(grown)
        return requests

    def _fund_recipient(
        self, decision: Decision, ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        (recipient,) = decision.targets
        tick = ctx.bottleneck
        assert tick is not None
        self._blocked_streak.clear()
        requests: List[PlacedSizingRequest] = []
        if (
            self._book.phase is TransferPhase.IDLE
            and why_cannot_grow(recipient, self._statuses) is None
        ):
            # One transfer at a time: let the window refill before the next.
            requests.extend(self._transfer_to(recipient, tick, ctx))
        requests.extend(self._shrink_toward_keep(recipient, tick, ctx))
        return requests

    def _shrink_surplus(
        self, decision: Decision, ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        tick = ctx.bottleneck
        assert tick is not None
        self._surplus_streak.clear()
        return self._shrink_blocked_surplus(tick, ctx)

    def _transfer_to(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        tick: "BottleneckTick",
        ctx: DecisionContext,
    ) -> List[PlacedSizingRequest]:
        nodes = self._book.transfer_nodes(recipient, ctx)
        if not nodes:
            return []
        pool = recipient.actor_pool
        b = pool.serving_size()
        ceiling = growth_ceiling(recipient, ctx)
        if b >= ceiling:
            logger.debug(
                "SizingPolicy: bottleneck %s (b=%d) not funded: at its "
                "growth ceiling",
                recipient.name,
                b,
            )
            return []
        views = self._donor_views(recipient, tick, ctx)
        if not views:
            self._metrics.record_refusal(self.name, "no_donor")
            return []

        def victim_key(candidate: VictimCandidate) -> Optional[Tuple]:
            # Only victims that free right away: pending, or idle with every
            # output consumed. Anything else drains through
            # ``_shrink_toward_keep``, outside a transfer, so a slow drain
            # never blocks the next transfer.
            pending = candidate.eviction_rank % 3 == 0
            idle = candidate.tasks_in_flight == 0 and candidate.unconsumed_outputs == 0
            if not (pending or idle):
                return None
            ttf = tick.victim_ttf(
                candidate.donor.op,
                candidate.tasks_in_flight,
                candidate.unconsumed_outputs,
            )
            return (not pending, ttf, candidate.eviction_rank)

        best: Optional[Tuple[int, List[NodePlan], Dict]] = None
        top = min(
            max_step_size(b, pool.bounded_max_size()),
            tick.growth_target(recipient),
            ceiling,
        )
        for b_prime in range(b + 1, max(top, b + 1) + 1):
            slack_by_op = tick.slack_for(recipient, b_prime)
            donors: List[DonorCapacity] = []
            for donor in views:
                donor_pool = donor.op.actor_pool
                removable = min(
                    slack_by_op.get(donor.op, 0),
                    donor_pool.serving_size() - donor_pool.min_size(),
                )
                if removable > 0:
                    donors.append(dataclasses.replace(donor, removable=removable))
            if not donors:
                break
            plan, _ = self._book.plan(
                recipient, b_prime - b, nodes, donors, ctx, victim_key=victim_key
            )
            placed = sum(p.num_recipient_actors for p in plan or ())
            if plan is None or placed < b_prime - b:
                # Feasibility is monotone in b': a larger step needs more
                # victims from donors with less slack.
                break
            best = (b_prime, plan, {donor.op: donor for donor in donors})
        if best is None:
            self._metrics.record_refusal(self.name, "victims_insufficient")
            logger.debug(
                "SizingPolicy: bottleneck %s (b=%d) cannot be funded now: "
                "no idle or pending donor actor frees a node it may use",
                recipient.name,
                b,
            )
            return []
        b_prime, plan, donors_by_op = best
        ttfs = [
            tick.victim_ttf(v.donor.op, v.tasks_in_flight, v.unconsumed_outputs)
            for node_plan in plan
            for v in node_plan.victims
        ]
        timeout_s = transfer_timeout_s(ttfs)
        # Before firing: ``_fire`` consumes the donors' removable counts.
        donors_summary = ", ".join(
            f"{op.name} x{count} (slack {donors_by_op[op].removable})"
            for op, count in _victims_by_donor(plan).items()
        )
        fired = self._book.fire(recipient, plan, donors_by_op, ctx, timeout_s=timeout_s)
        if not fired:
            logger.debug(
                "SizingPolicy: bottleneck %s %d -> %d not fired: it would "
                "reverse a recent transfer (thrash guard)",
                recipient.name,
                b,
                b_prime,
            )
            return fired
        severity = dict(tick.assessment.stable).get(recipient, float("nan"))
        logger.debug(
            "SizingPolicy: bottleneck %s (sev=%.2f) %d -> %d from %s; "
            "victim ttf max=%.0fs, timeout=%.0fs",
            recipient.name,
            severity,
            b,
            b_prime,
            donors_summary,
            max(ttfs, default=0.0),
            timeout_s,
        )
        return fired

    def _shrink_toward_keep(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        tick: "BottleneckTick",
        ctx: DecisionContext,
    ) -> List[PlacedSizingRequest]:
        """Drain one actor from each donor that has had surplus toward the
        recipient for a streak of ticks: its keep size is its actors less its
        slack at the recipient's next step.

        No transfer is opened and nothing is held: the actor drains normally
        (until its tasks finish and its outputs are consumed), and the freed
        capacity reaches the recipient through growth into free capacity.
        If the donor is needed again first, growing it reclaims its draining
        actor, which cancels the drain. One drain at a time per donor, so
        surplus measured over a lagging window is not over-taken. A donor
        feeding an op that starves (``_CONSUMER_STARVED_MAX``) is not drained.
        """
        pool = recipient.actor_pool
        b = pool.serving_size()
        b_prime = max(
            b + 1,
            min(
                max_step_size(b, pool.bounded_max_size()),
                tick.growth_target(recipient),
            ),
        )
        slack_by_op = tick.slack_for(recipient, b_prime)
        recipient_per_actor = pool.per_actor_resource_usage()
        recipient_nodes = ctx.schedulable_nodes.get(recipient, frozenset())
        requests: List[PlacedSizingRequest] = []
        seen = set()
        for donor in self._donor_views(recipient, tick, ctx):
            op = donor.op
            seen.add(op)
            donor_pool = op.actor_pool
            surplus = min(
                slack_by_op.get(op, 0),
                donor_pool.serving_size() - donor_pool.min_size(),
            )
            if surplus < 1 or self._keeps_consumer_fed(op, tick, ctx):
                self._surplus_streak.pop(op, None)
                continue
            streak = self._surplus_streak.get(op, 0) + 1
            self._surplus_streak[op] = streak
            if (
                streak < _SURPLUS_STREAK_TICKS
                or donor_pool.num_draining_actors() > 0
                or donor_pool.num_pending_actors() > 0
            ):
                continue
            node, ttf = self._surplus_victim_node(
                donor, recipient_per_actor, recipient_nodes, tick, ctx
            )
            if node is None:
                continue
            self._surplus_streak[op] = 0
            logger.debug(
                "SizingPolicy: surplus %s -1 on %s (slack %d toward %s at "
                "%d -> %d; victim ttf %.0fs)",
                op.name,
                node,
                surplus,
                recipient.name,
                b,
                b_prime,
                ttf,
            )
            requests.append(
                PlacedSizingRequest(
                    request={
                        op: ScaleDownRequest.on_nodes(
                            (node,),
                            force=True,
                            reason=f"optimizer: surplus for {recipient.name}",
                        )
                    }
                )
            )
        for op in [op for op in self._surplus_streak if op not in seen]:
            del self._surplus_streak[op]
        return requests

    def _shrink_blocked_surplus(
        self, tick: "BottleneckTick", ctx: DecisionContext
    ) -> List[PlacedSizingRequest]:
        """With no recipient: drain one actor from each op whose output edge
        is blocked (budget full while the consumer had no idle slot) at least
        ``_BLOCKED_SHARE_MIN`` of the time, down toward its keep size (actors
        needed for its busy and self-limited time, at least
        ``_BLOCKED_KEEP_FLOOR``). Such actors hold full output budgets in the
        object store and add no throughput while the consumer is the limit.
        Same streak, one-drain-at-a-time and starving-consumer check as
        ``_shrink_toward_keep``."""
        blocked_by_op: Dict["ExperimentalActorPoolMapOperator", float] = {}
        self_by_op: Dict["ExperimentalActorPoolMapOperator", float] = {}
        for edge in tick.snapshot.edges:
            blocked_by_op[edge.producer] = max(
                blocked_by_op.get(edge.producer, 0.0), edge.producer_blocked
            )
            self_by_op[edge.producer] = max(
                self_by_op.get(edge.producer, 0.0), edge.producer_self_limited
            )
        named = {op for op, _ in tick.snapshot.bottlenecks} | set(tick.stable)
        requests: List[PlacedSizingRequest] = []
        seen = set()
        for op in ctx.non_pg_ops:
            shares = tick.snapshot.shares.get(op)
            if (
                shares is None
                or op in named
                or op.has_completed()
                or not self._statuses.can_scale(op)
                or blocked_by_op.get(op, 0.0) < _BLOCKED_SHARE_MIN
            ):
                continue
            pool = op.actor_pool
            needed = min(1.0, shares.busy + self_by_op.get(op, 0.0))
            keep = max(
                _BLOCKED_KEEP_FLOOR,
                pool.min_size(),
                math.ceil(
                    pool.serving_size() * needed * (1 + SIZER_BOTTLENECK_SLACK_MARGIN)
                ),
            )
            if pool.serving_size() <= keep or self._keeps_consumer_fed(op, tick, ctx):
                continue
            seen.add(op)
            streak = self._blocked_streak.get(op, 0) + 1
            self._blocked_streak[op] = streak
            if (
                streak < _SURPLUS_STREAK_TICKS
                or pool.num_draining_actors() > 0
                or pool.num_pending_actors() > 0
            ):
                continue
            donor = DonorCapacity(
                op=op,
                view=placement_view.build_op_placement_view(op, rank_victims=True),
                per_actor=pool.per_actor_resource_usage(),
                removable=0,
            )
            node, ttf = self._surplus_victim_node(donor, None, None, tick, ctx)
            if node is None:
                continue
            self._blocked_streak[op] = 0
            logger.debug(
                "SizingPolicy: blocked surplus %s -1 on %s (blocked %.2f, "
                "keep %d of %d; victim ttf %.0fs)",
                op.name,
                node,
                blocked_by_op[op],
                keep,
                pool.serving_size(),
                ttf,
            )
            requests.append(
                PlacedSizingRequest(
                    request={
                        op: ScaleDownRequest.on_nodes(
                            (node,),
                            force=True,
                            reason="optimizer: blocked surplus",
                        )
                    }
                )
            )
        for op in [op for op in self._blocked_streak if op not in seen]:
            del self._blocked_streak[op]
        return requests

    @staticmethod
    def _starved_consumer(
        op: "ExperimentalActorPoolMapOperator",
        tick: "BottleneckTick",
        ctx: DecisionContext,
    ) -> Optional[Tuple["ExperimentalActorPoolMapOperator", float]]:
        """An op ``op`` feeds that starves at least ``_CONSUMER_STARVED_MAX``
        of the time, with its starved share, or None."""
        for consumer in ctx.next_ops.get(op, ()):
            shares = tick.snapshot.shares.get(consumer)
            if shares is not None and shares.starved >= _CONSUMER_STARVED_MAX:
                return consumer, shares.starved
        return None

    def _keeps_consumer_fed(
        self,
        op: "ExperimentalActorPoolMapOperator",
        tick: "BottleneckTick",
        ctx: DecisionContext,
    ) -> bool:
        """Whether ``op`` must not shrink because an op it feeds is starving.
        Logs the skip."""
        starved = self._starved_consumer(op, tick, ctx)
        if starved is None:
            return False
        consumer, share = starved
        logger.debug(
            "SizingPolicy: %s not shrunk: %s it feeds is starved %.2f",
            op.name,
            consumer.name,
            share,
        )
        return True

    @staticmethod
    def _surplus_victim_node(
        donor: DonorCapacity,
        recipient_per_actor: Optional[ExecutionResources],
        recipient_nodes: Optional[AbstractSet[NodeIdStr]],
        tick: "BottleneckTick",
        ctx: DecisionContext,
    ) -> Tuple[Optional[NodeIdStr], float]:
        """The node to drain a donor actor on: the one whose cheapest victim
        frees soonest, and, given a recipient, one where it may run and one of
        its actors fits once that victim frees. (None, 0) if none."""
        best: Optional[Tuple[NodeIdStr, float]] = None
        for node, costs in donor.view.victim_drain_cost_by_node.items():
            if not costs:
                continue
            if recipient_per_actor is not None:
                if recipient_nodes is None or node not in recipient_nodes:
                    continue
                room = ctx.capacity.free.get(node, ExecutionResources.zero()).add(
                    donor.per_actor
                )
                if max_placeable_actors(room, recipient_per_actor) < 1:
                    continue
            ttf = min(
                tick.victim_ttf(donor.op, in_flight, unconsumed)
                for in_flight, unconsumed in costs
            )
            if best is None or ttf < best[1]:
                best = (node, ttf)
        return best if best is not None else (None, 0.0)

    def _donor_views(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        tick: "BottleneckTick",
        ctx: DecisionContext,
    ) -> List[DonorCapacity]:
        """Candidate donors: non-PG ops the model has shares for, other than
        the recipient and every stable bottleneck (never take from an op that
        was a bottleneck within the last window), and ACTIVE. Their
        removable count is set per b' from slack."""
        donors: List[DonorCapacity] = []
        for op in ctx.non_pg_ops:
            if (
                op is recipient
                or op in tick.stable
                or op not in tick.snapshot.shares
                or op.has_completed()
                or not self._statuses.can_scale(op)
            ):
                continue
            donors.append(
                DonorCapacity(
                    op=op,
                    view=placement_view.build_op_placement_view(op, rank_victims=True),
                    per_actor=op.actor_pool.per_actor_resource_usage(),
                    removable=0,
                )
            )
        return donors


def _victims_by_donor(
    plan: List[NodePlan],
) -> Dict["ExperimentalActorPoolMapOperator", int]:
    counts: Dict["ExperimentalActorPoolMapOperator", int] = {}
    for node_plan in plan:
        for op, count in node_plan.victims_by_donor().items():
            counts[op] = counts.get(op, 0) + count
    return counts
