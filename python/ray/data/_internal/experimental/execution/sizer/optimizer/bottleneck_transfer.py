"""Grow the pipeline's bottleneck, funding it from donors' slack.

The bottleneck model (``bottleneck_observer``) names, each tick, the most
severe stable bottleneck that can use capacity: the recipient. This rule
grows it into free capacity when it fits, and otherwise moves capacity to it
from operators whose actors add no throughput, choosing the largest step the
donors' slack can fund. It replaces CapacityTransfer's recipient and donor
policy and SilentBottleneckGrant under ``RAY_DATA_SIZER_BOTTLENECK_MODEL``,
and keeps all of CapacityTransfer's transfer mechanics (owed capacity held
for the recipient, bring-up, timeout, giving stranded victims back, donor
cooldown, thrash guard).
"""

from __future__ import annotations

import dataclasses
import logging
import math
from typing import TYPE_CHECKING, AbstractSet, Dict, List, Optional, Tuple

from typing_extensions import override

from ray.data._internal.execution.execution_flags import (
    SIZER_BOTTLENECK_ARROW_MARGIN,
    SIZER_BOTTLENECK_MAX_STEP_RATIO,
    SIZER_BOTTLENECK_MAX_TIMEOUT_S,
    SIZER_BOTTLENECK_SLACK_MARGIN,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.common import NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleDownRequest,
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
    max_placeable_actors,
)
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    PG_ELIGIBLE,
)
from ray.data._internal.experimental.execution.sizer.models import (
    PlacedSizingRequest,
)
from ray.data._internal.experimental.execution.sizer.op_profile import (
    bounded_max_size,
)
from ray.data._internal.experimental.execution.sizer.optimizer.capacity_transfer import (  # noqa: E501
    CapacityTransfer,
    DonorCapacity,
    NodePlan,
    TransferState,
    VictimCandidate,
)
from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)
from ray.data._internal.experimental.execution.sizer.optimizer.utils import (
    fill_free_nodes,
    free_schedulable_nodes,
    grow_pg_op,
    upscale_request,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
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


def max_step_size(b: int, max_size: int) -> int:
    """The largest b' one step may grow a recipient of size b to."""
    return max(
        b + 1, min(max_size, math.floor(max(b, 1) * SIZER_BOTTLENECK_MAX_STEP_RATIO))
    )


class BottleneckTransfer(CapacityTransfer):
    """Grow the model's recipient: into free capacity, else from donors.

    Per tick:

    1. Free capacity: the most severe bottleneck named this tick (severity
       at or above the floor) with room on a node it may use grows there
       toward its growth target b' = ceil(b / (1 - w)), w the largest share
       of time a neighbour waits on it. Growing into free capacity drains nothing and is cheap to
       undo, so it does not wait for the bottleneck to be stable.
       With no bottleneck named, every op that is nearly always busy grows
       one actor into free capacity instead (balanced stages).
    2. Otherwise, with no transfer in flight, the assessment's recipient
       (most severe stable bottleneck at or above the floor that can use
       capacity) is funded from donors: pick b', the largest size up to the
       model's growth target (``growth_target``) that donors can fund, every non-bottleneck op giving at most
       its slack at b' (b'/b more work on the recipient's path, the same off
       it). Victims are taken pending first, then fastest to free. The
       transfer's timeout follows its slowest victim's estimated
       time-to-free.

    An op that is guard-blocked or has pending actors is skipped. The model
    marks an op ineligible ("no: ...") when it is not named this tick, a more
    severe named op feeds it, or a producer feeding it is self-limited, so
    neither step grows an op that would wait for input. A decision is logged
    once it fires; a refusal is logged with its reason.
    """

    name = "bottleneck_transfer"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Per donor: consecutive ticks it has had surplus toward the recipient.
        self._surplus_streak: Dict["ExperimentalActorPoolMapOperator", int] = {}
        # Per op: consecutive ticks it has been blocked with surplus while
        # there was no recipient.
        self._blocked_streak: Dict["ExperimentalActorPoolMapOperator", int] = {}

    @override
    def apply(
        self, ctx: OptimizationContext, state: TransferState
    ) -> List[PlacedSizingRequest]:
        requests = self._bring_up_held_actors(state)
        tick: Optional["BottleneckTick"] = ctx.bottleneck
        if ctx.warmup or tick is None:
            # Warmup sizes from equal shares; the model has no data yet.
            return requests
        # Growing into free capacity drains nothing, so it also runs while
        # the cluster is still resizing (when it is most needed); only
        # transfers wait for it to settle.
        for op, _ in tick.snapshot.bottlenecks:
            if tick.assessment.eligibility.get(op) not in ("yes: free", PG_ELIGIBLE):
                continue
            blocked = self._why_cannot_grow(op, ctx)
            if blocked is not None:
                logger.debug(
                    "PipelineOptimizer: bottleneck %s not grown into free "
                    "capacity: %s",
                    op.name,
                    blocked,
                )
                continue
            grown = self._grow_into_free(op, tick.growth_target(op), ctx)
            if grown is not None:
                requests.append(grown)
                return requests
        if not tick.snapshot.bottlenecks:
            balanced = self._grow_balanced(tick, ctx)
            if balanced:
                requests.extend(balanced)
                return requests
        refusal = self._precondition_refusal(ctx)
        if refusal is not None:
            self._metrics.record_refusal(self.name, refusal)
            return requests
        recipient = tick.assessment.recipient
        if recipient is None:
            # Surplus is shrunk toward a recipient that wants the capacity;
            # with none, only blocked surplus is, for the memory it pins.
            self._surplus_streak.clear()
            requests.extend(self._shrink_blocked_surplus(tick, ctx))
            return requests
        self._blocked_streak.clear()
        if not state.in_flight and self._can_grow(recipient, ctx):
            # One transfer at a time: let the window refill before the next.
            requests.extend(self._transfer_to(recipient, tick, ctx, state))
        requests.extend(self._shrink_toward_keep(recipient, tick, ctx))
        return requests

    def _can_grow(
        self, op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> bool:
        return self._why_cannot_grow(op, ctx) is None

    def _why_cannot_grow(
        self, op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> Optional[str]:
        if not self._guard.can_scale(op):
            return f"guard ({self._guard.reason(op)})"
        if op.actor_pool.num_pending_actors() > 0:
            return "actors pending"
        return None

    def _grow_balanced(
        self, tick: "BottleneckTick", ctx: OptimizationContext
    ) -> List[PlacedSizingRequest]:
        """With no bottleneck named, grow every op that is nearly always
        busy (busy share at least 1 - arrow margin) one step into free
        capacity: the stages are balanced and all of them are the limit."""
        requests: List[PlacedSizingRequest] = []
        for op, shares in tick.snapshot.shares.items():
            pool = op.actor_pool
            if (
                shares.busy < 1.0 - SIZER_BOTTLENECK_ARROW_MARGIN
                or op.has_completed()
                or pool.serving_size() >= bounded_max_size(pool)
                or not self._can_grow(op, ctx)
            ):
                continue
            grown = self._grow_into_free(
                op, pool.serving_size() + 1, ctx, reason="balanced"
            )
            if grown is not None:
                requests.append(grown)
        return requests

    def _grow_into_free(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        b_prime: int,
        ctx: OptimizationContext,
        reason: str = "bottleneck",
    ) -> Optional[PlacedSizingRequest]:
        """Grow toward ``b_prime`` (the model's growth target) as far as free
        capacity allows, or None if no actor fits. A PG-managed op grows into
        its own placement group's free bundles instead."""
        if ctx.is_pg_managed(recipient):
            return self._grow_into_pg(recipient, b_prime, ctx, reason)
        per_actor = recipient.actor_pool.per_actor_resource_usage()
        nodes = free_schedulable_nodes(ctx, recipient)
        if not any(max_placeable_actors(ctx.free[n], per_actor) >= 1 for n in nodes):
            logger.debug(
                "PipelineOptimizer: %s %s not grown into free capacity: no "
                "allowed node has room after capacity held for transfers",
                reason,
                recipient.name,
            )
            return None
        step = max(1, b_prime - recipient.actor_pool.serving_size())
        num_to_reclaim = min(
            step, recipient.actor_pool.num_reclaimable_draining_actors()
        )
        view = placement_view.build_op_placement_view(recipient, rank_victims=False)
        ranked = sorted(nodes, key=lambda node: self._placement.rank_node(view, node))
        targets = fill_free_nodes(ctx, recipient, step - num_to_reclaim, ranked)
        if not targets and not num_to_reclaim:
            return None
        logger.debug(
            "PipelineOptimizer: %s %s grows +%d into free capacity",
            reason,
            recipient.name,
            len(targets) + num_to_reclaim,
        )
        return upscale_request(
            recipient,
            ScaleUpRequest.on_nodes(
                targets,
                num_to_reclaim=num_to_reclaim,
                reason=f"optimizer: {reason} grows into free capacity",
            ),
        )

    @staticmethod
    def _grow_into_pg(
        op: "ExperimentalActorPoolMapOperator",
        b_prime: int,
        ctx: OptimizationContext,
        reason: str,
    ) -> Optional[PlacedSizingRequest]:
        step = max(1, b_prime - op.actor_pool.serving_size())
        grown = grow_pg_op(
            op, step, ctx, reason=f"optimizer: {reason} grows into placement group"
        )
        if grown is None:
            logger.debug(
                "PipelineOptimizer: %s %s not grown: placement group full",
                reason,
                op.name,
            )
            return None
        logger.debug(
            "PipelineOptimizer: %s %s grows +%d into its placement group",
            reason,
            op.name,
            grown.request[op].delta,
        )
        return grown

    def _transfer_to(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        tick: "BottleneckTick",
        ctx: OptimizationContext,
        state: TransferState,
    ) -> List[PlacedSizingRequest]:
        nodes = self._transfer_nodes(recipient, ctx, state)
        if not nodes:
            return []
        pool = recipient.actor_pool
        b = pool.serving_size()
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
            max_step_size(b, bounded_max_size(pool)), tick.growth_target(recipient)
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
            plan, _ = self._plan(
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
                "PipelineOptimizer: bottleneck %s (b=%d) cannot be funded now: "
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
        fired = self._fire(
            recipient, plan, donors_by_op, ctx, state, timeout_s=timeout_s
        )
        if not fired:
            logger.debug(
                "PipelineOptimizer: bottleneck %s %d -> %d not fired: it would "
                "reverse a recent transfer (thrash guard)",
                recipient.name,
                b,
                b_prime,
            )
            return fired
        severity = dict(tick.assessment.stable).get(recipient, float("nan"))
        logger.debug(
            "PipelineOptimizer: bottleneck %s (sev=%.2f) %d -> %d from %s; "
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
        ctx: OptimizationContext,
    ) -> List[PlacedSizingRequest]:
        """Drain one actor from each donor that has had surplus toward the
        recipient for a streak of ticks: its keep size is its actors less its
        slack at the recipient's next step.

        No transfer is opened and nothing is held: the actor drains normally
        (until its tasks finish and its outputs are consumed), and the freed
        capacity reaches the recipient through growth into free capacity.
        If the donor is needed again first, growing it reclaims its draining
        actor, which cancels the drain. One drain at a time per donor, so
        surplus measured over a lagging window is not over-taken.
        """
        pool = recipient.actor_pool
        b = pool.serving_size()
        b_prime = max(
            b + 1,
            min(
                max_step_size(b, bounded_max_size(pool)),
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
            if surplus < 1:
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
                "PipelineOptimizer: surplus %s -1 on %s (slack %d toward %s at "
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
        self, tick: "BottleneckTick", ctx: OptimizationContext
    ) -> List[PlacedSizingRequest]:
        """With no recipient: drain one actor from each op whose output edge
        is blocked (budget full while the consumer had no idle slot) at least
        ``_BLOCKED_SHARE_MIN`` of the time, down toward its keep size (actors
        needed for its busy and self-limited time, at least
        ``_BLOCKED_KEEP_FLOOR``). Such actors hold full output budgets in the
        object store and add no throughput while the consumer is the limit.
        Same streak and one-drain-at-a-time as ``_shrink_toward_keep``."""
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
                or not self._guard.can_scale(op)
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
            if pool.serving_size() <= keep:
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
                "PipelineOptimizer: blocked surplus %s -1 on %s (blocked %.2f, "
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
    def _surplus_victim_node(
        donor: DonorCapacity,
        recipient_per_actor: Optional[ExecutionResources],
        recipient_nodes: Optional[AbstractSet[NodeIdStr]],
        tick: "BottleneckTick",
        ctx: OptimizationContext,
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
                room = ctx.free.get(node, ExecutionResources.zero()).add(
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
        ctx: OptimizationContext,
    ) -> List[DonorCapacity]:
        """Candidate donors: non-PG ops the model has shares for, other than
        the recipient and every stable bottleneck (never take from an op that
        was a bottleneck within the last window), not guard-blocked. Their
        removable count is set per b' from slack."""
        donors: List[DonorCapacity] = []
        for op in ctx.non_pg_ops:
            if (
                op is recipient
                or op in tick.stable
                or op not in tick.snapshot.shares
                or op.has_completed()
                or not self._guard.can_scale(op)
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
