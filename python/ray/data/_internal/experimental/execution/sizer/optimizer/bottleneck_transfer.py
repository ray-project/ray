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
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from typing_extensions import override

from ray.data._internal.execution.execution_flags import (
    SIZER_BOTTLENECK_MAX_STEP_RATIO,
    SIZER_BOTTLENECK_MAX_TIMEOUT_S,
    SIZER_TRANSFER_TIMEOUT_S,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    ScaleUpRequest,
)
from ray.data._internal.experimental.execution.sizer import placement_view
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (  # noqa: E501
    max_placeable_actors,
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
    2. Otherwise, with no transfer in flight, the assessment's recipient
       (most severe stable bottleneck at or above the floor that can use
       capacity) is funded from donors: pick b', the largest size up to the
       model's growth target (``growth_target``) that donors can fund, every non-bottleneck op giving at most
       its slack at b' (b'/b more work on the recipient's path, the same off
       it). Victims are taken pending first, then fastest to free. The
       transfer's timeout follows its slowest victim's estimated
       time-to-free.

    An op that is guard-blocked or has pending actors is skipped.
    """

    name = "bottleneck_transfer"

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
            if tick.assessment.eligibility.get(op) == "yes: free" and self._can_grow(
                op, ctx
            ):
                grown = self._grow_into_free(op, tick.growth_target(op), ctx)
                if grown is not None:
                    requests.append(grown)
                    return requests
        refusal = self._precondition_refusal(ctx)
        if refusal is not None:
            self._metrics.record_refusal(self.name, refusal)
            return requests
        recipient = tick.assessment.recipient
        if recipient is None or state.in_flight or not self._can_grow(recipient, ctx):
            # One transfer at a time: let the window refill before the next.
            return requests
        requests.extend(self._transfer_to(recipient, tick, ctx, state))
        return requests

    def _can_grow(
        self, op: "ExperimentalActorPoolMapOperator", ctx: OptimizationContext
    ) -> bool:
        return (
            not ctx.is_pg_managed(op)
            and self._guard.can_scale(op)
            and op.actor_pool.num_pending_actors() == 0
        )

    def _grow_into_free(
        self,
        recipient: "ExperimentalActorPoolMapOperator",
        b_prime: int,
        ctx: OptimizationContext,
    ) -> Optional[PlacedSizingRequest]:
        """Grow toward ``b_prime`` (the model's growth target) as far as free
        capacity allows, or None if no actor fits."""
        per_actor = recipient.actor_pool.per_actor_resource_usage()
        nodes = free_schedulable_nodes(ctx, recipient)
        if not any(max_placeable_actors(ctx.free[n], per_actor) >= 1 for n in nodes):
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
            "PipelineOptimizer: bottleneck %s grows +%d into free capacity",
            recipient.name,
            len(targets) + num_to_reclaim,
        )
        return upscale_request(
            recipient,
            ScaleUpRequest.on_nodes(
                targets,
                num_to_reclaim=num_to_reclaim,
                reason="optimizer: bottleneck grows into free capacity",
            ),
        )

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

        def victim_key(candidate: VictimCandidate) -> Tuple:
            # Pending first; then whatever frees soonest.
            pending = candidate.eviction_rank % 3 == 0
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
                "PipelineOptimizer: bottleneck %s (b=%d) cannot be funded: no "
                "donor slack frees a node it may use",
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
        severity = dict(tick.assessment.stable).get(recipient, float("nan"))
        logger.debug(
            "PipelineOptimizer: bottleneck %s (sev=%.2f) %d -> %d from %s; "
            "victim ttf max=%.0fs, timeout=%.0fs",
            recipient.name,
            severity,
            b,
            b_prime,
            ", ".join(
                f"{op.name} x{count} (slack {donors_by_op[op].removable})"
                for op, count in _victims_by_donor(plan).items()
            ),
            max(ttfs, default=0.0),
            timeout_s,
        )
        return self._fire(
            recipient, plan, donors_by_op, ctx, state, timeout_s=timeout_s
        )

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
