"""Classify the sizer's phase and each tick's decision.

Both functions only read: they never claim capacity or resize a pool, so
what the sizer will do this tick can be tested (and logged) apart from how
it does it (``sizing_policy``).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, AbstractSet, Dict, Optional

from ray.data._internal.execution.execution_flags import (
    SIZER_BOTTLENECK_ARROW_MARGIN,
    SIZER_CLUSTER_SETTLE_S,
)
from ray.data._internal.experimental.execution.sizer.bottleneck_observer import (
    PG_ELIGIBLE,
)
from ray.data._internal.experimental.execution.sizer.policy.context import (
    DecisionContext,
)
from ray.data._internal.experimental.execution.sizer.policy.growth import (
    balanced_step,
    growth_ceiling,
    plan_free_growth,
)
from ray.data._internal.experimental.execution.sizer.policy.states import (
    Decision,
    DecisionKind,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)

# Eligibility verdicts (``Assessment.eligibility``) under which a named
# bottleneck may grow into free capacity.
_FREE_ELIGIBLE = ("yes: free", PG_ELIGIBLE)

# HOLD reasons.
HOLD_WARMUP = "warmup"
HOLD_NO_MODEL = "no_model"
HOLD_CLUSTER_SETTLING = "cluster_settling"


def why_cannot_grow(
    op: "ExperimentalActorPoolMapOperator",
    active: AbstractSet["ExperimentalActorPoolMapOperator"],
) -> Optional[str]:
    """Why ``op`` may not grow this tick, or None. ``active``: the ops the
    bottleneck move may resize this tick (``SizingPolicy.decide``)."""
    if op not in active:
        return "not active"
    if op.actor_pool.num_pending_actors() > 0:
        return "actors pending"
    return None


def classify_decision(
    ctx: DecisionContext, active: AbstractSet["ExperimentalActorPoolMapOperator"]
) -> Decision:
    """This tick's primary action, first match wins:

    | phase / model                       | condition                     | decision       |
    |-------------------------------------|-------------------------------|----------------|
    | WARMUP                              |                               | HOLD           |
    | no model yet                        |                               | HOLD           |
    | a named bottleneck, eligible free   | it can grow, room fits        | GROW_FREE      |
    | no bottleneck named                 | a nearly-always-busy op fits  | GROW_BALANCED  |
    | allocation changed recently         |                               | HOLD           |
    | a stable recipient                  |                               | FUND_RECIPIENT |
    | no recipient                        |                               | SHRINK_SURPLUS |

    Growing into free capacity drains nothing and is cheap to undo, so it
    runs before the cluster settles (when it is most needed) and does not
    wait for a bottleneck to be stable; only moving capacity waits for both.
    """
    if ctx.warmup:
        # Warmup sizes from equal shares; the model has no data yet.
        return Decision(DecisionKind.HOLD, reason=HOLD_WARMUP)
    tick = ctx.bottleneck
    if tick is None:
        return Decision(DecisionKind.HOLD, reason=HOLD_NO_MODEL)

    for op, _ in tick.snapshot.bottlenecks:
        if tick.assessment.eligibility.get(op) not in _FREE_ELIGIBLE:
            continue
        blocked = why_cannot_grow(op, active)
        if blocked is not None:
            logger.debug(
                "SizingPolicy: bottleneck %s not grown into free capacity: %s",
                op.name,
                blocked,
            )
            continue
        target = tick.growth_target(op)
        if plan_free_growth(op, target, ctx) is not None:
            return Decision(
                kind=DecisionKind.GROW_FREE,
                targets={op: target},
                reason="bottleneck",
            )

    if not tick.snapshot.bottlenecks:
        targets = _balanced_targets(ctx, active)
        if targets:
            return Decision(
                kind=DecisionKind.GROW_BALANCED,
                targets=targets,
                reason="balanced",
            )

    # Moving capacity corrects a settled allocation; while the cluster is
    # still resizing, growth is the autoscaler's move.
    if ctx.seconds_since_allocation_change < SIZER_CLUSTER_SETTLE_S:
        return Decision(DecisionKind.HOLD, reason=HOLD_CLUSTER_SETTLING)

    recipient = tick.assessment.recipient
    if recipient is None:
        # Surplus is shrunk toward a recipient that wants the capacity; with
        # none, only blocked surplus is, for the memory it pins.
        return Decision(
            kind=DecisionKind.SHRINK_SURPLUS,
            reason="no recipient",
        )
    return Decision(
        kind=DecisionKind.FUND_RECIPIENT,
        targets={recipient: tick.growth_target(recipient)},
        reason="stable recipient",
    )


def _balanced_targets(
    ctx: DecisionContext, active: AbstractSet["ExperimentalActorPoolMapOperator"]
) -> Dict["ExperimentalActorPoolMapOperator", int]:
    """With no bottleneck named, every op that is nearly always busy (busy
    share at least 1 - arrow margin) and can grow: the stages are balanced
    and all of them are the limit. Each grows by its ``balanced_step``,
    computed for all of them before any grows, so they split the free
    capacity evenly."""
    tick = ctx.bottleneck
    assert tick is not None
    targets: Dict["ExperimentalActorPoolMapOperator", int] = {}
    for op, shares in tick.snapshot.shares.items():
        pool = op.actor_pool
        if (
            shares.busy < 1.0 - SIZER_BOTTLENECK_ARROW_MARGIN
            or op.has_completed()
            or pool.serving_size() >= growth_ceiling(op, ctx.topology.get(op))
            or why_cannot_grow(op, active) is not None
        ):
            continue
        target = pool.serving_size() + balanced_step(op, ctx)
        if plan_free_growth(op, target, ctx) is not None:
            targets[op] = target
    return targets
