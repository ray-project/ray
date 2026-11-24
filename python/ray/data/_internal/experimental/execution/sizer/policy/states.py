"""The sizer's phase, and the enums and records of each tick's decision.

State that carries across ticks:

- ``SizerPhase``: the whole sizer, only ever moving forward.
- ``TransferPhase``: whether a transfer is open. A tick may grow an op into
  free capacity while a transfer is in flight, so this is not a decision.
- Per operator, ``op_status.OpStatus``: whether the op may be resized.

Each tick's decision, recomputed every tick with no memory:

- ``BottleneckMove``: the one move the bottleneck model makes this tick
  (``choose_bottleneck_move`` in ``decision``): ``GrowFree``,
  ``GrowBalanced``, ``FundRecipient``, ``ShrinkSurplus`` or ``Hold``. Each
  carries only what it needs.
- ``OpRule``: what happened to one op this tick. The move names the rule for
  each op it resizes; the per-op rules (``per_op``) name it for the rest.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Mapping, Optional, Tuple, Union

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_models import (  # noqa: E501
        PlacedScalingRequest,
    )


class SizerPhase(enum.Enum):
    # Until every critical op has produced output there is no feedback, so
    # every op grows toward an equal share of the allocation.
    WARMUP = "warmup"
    # Warmup is over, but the bottleneck model has not yet seen a full window
    # of some op. Those ops keep growing toward their equal share; the model
    # decides for the rest.
    MODEL_WARMING = "model warming"
    # The model has seen every unfinished op; it drives all growth.
    STEADY = "steady"


class TransferPhase(enum.Enum):
    IDLE = "idle"
    IN_FLIGHT = "in flight"


# --- the bottleneck move: one per tick, chosen by ``choose_bottleneck_move`` --
#
# Each move resizes only Active ops, and names the ``OpRule`` of each op it
# resizes. ``name`` is what the logs show; ``reason`` is why it was chosen.


@dataclass(frozen=True)
class GrowFree:
    """A named bottleneck has room in free capacity (or its placement
    group): grow ``op`` toward ``target`` (GROW_FREE)."""

    op: "ExperimentalActorPoolMapOperator"
    target: int
    name: ClassVar[str] = "grow bottleneck into free capacity"
    reason: ClassVar[str] = "bottleneck"


@dataclass(frozen=True)
class GrowBalanced:
    """Nothing is named and some ops are nearly always busy: the stages are
    balanced, so each op in ``targets`` grows toward its size by a share of
    the free capacity (GROW_BALANCED)."""

    targets: Mapping["ExperimentalActorPoolMapOperator", int]
    name: ClassVar[str] = "grow balanced stages"
    reason: ClassVar[str] = "balanced"


@dataclass(frozen=True)
class FundRecipient:
    """A stable recipient with no free room: fund ``recipient`` from donors'
    slack (one transfer at a time), and drain donors' surplus toward it.
    Only donors shrink this tick (DONATE); the recipient's actors come up on
    later ticks (FUND)."""

    recipient: "ExperimentalActorPoolMapOperator"
    name: ClassVar[str] = "fund recipient"
    reason: ClassVar[str] = "stable recipient"


@dataclass(frozen=True)
class ShrinkSurplus:
    """No recipient: drain ops mostly blocked behind their consumer
    (SHRINK_SURPLUS)."""

    name: ClassVar[str] = "shrink blocked surplus"
    reason: ClassVar[str] = "no recipient"


@dataclass(frozen=True)
class Hold:
    """Resize nothing: warmup, no model yet, or the cluster still settling
    (``reason``)."""

    reason: str
    name: ClassVar[str] = "hold"


BottleneckMove = Union[GrowFree, GrowBalanced, FundRecipient, ShrinkSurplus, Hold]


class OpRule(enum.Enum):
    """The action that sized one op this tick (``SizingPolicy.decide``).

    The first five come from the bottleneck model and place their actors
    themselves; the rest are per-op actions (``PerOpRules.decide``). FUND
    brings up the actors an open transfer owes its recipient.
    """

    GROW_FREE = "grow a bottleneck into free capacity"
    GROW_BALANCED = "grow a balanced stage into free capacity"
    FUND = "receive actors from a capacity transfer"
    DONATE = "give actors to a bottleneck"
    SHRINK_SURPLUS = "shed blocked surplus"
    RELEASE_POOL = "release the pool of a completed op"
    GRANT_HELD = "grow to the room held for it"
    WARMUP_SHARE = "grow toward its warmup share"
    SHED_IDLE = "shed idle actors"
    REFILL_TO_MIN = "refill to min size"
    NONE = "no change"


@dataclass(frozen=True)
class OpDecision:
    """One op's action this tick. It sets at most one of the two:
    ``request``, a count that already fits and that WHERE places; or
    ``placed``, requests that already name their nodes, in the order to apply
    them (a bottleneck action, whose outcome depends on the node)."""

    rule: OpRule
    request: Optional["ActorPoolScalingRequest"] = None
    placed: Tuple["PlacedScalingRequest", ...] = ()
