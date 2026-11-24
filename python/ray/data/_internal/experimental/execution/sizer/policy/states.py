"""The sizer's state machine: its phase, and each tick's decision.

Three machines run side by side, each with its own enum:

- ``SizerPhase``: the whole sizer, only ever moving forward.
- ``DecisionKind``: in a phase that decides from the bottleneck model, the
  one primary action a tick takes (``classify`` in ``decision``).
- ``TransferPhase``: whether a transfer is open. A tick may grow an op into
  free capacity while a transfer is in flight, so this is not a decision.

Per operator, ``op_status.OpStatus`` says whether the op may be resized, and
``OpRule`` says which per-op action sized it this tick (``per_op``).
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping, Optional, Tuple

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


class DecisionKind(enum.Enum):
    """One tick's primary action, in priority order."""

    # A named bottleneck has room in free capacity (or its placement group):
    # grow it there toward its growth target.
    GROW_FREE = "grow bottleneck into free capacity"
    # Nothing is named and some ops are nearly always busy: the stages are
    # balanced, so each grows by a share of the free capacity.
    GROW_BALANCED = "grow balanced stages"
    # A stable recipient with no free room: fund it from donors' slack
    # (one transfer at a time), and drain donors' surplus toward it.
    FUND_RECIPIENT = "fund recipient"
    # No recipient: drain ops mostly blocked behind their consumer.
    SHRINK_SURPLUS = "shrink blocked surplus"
    # Nothing to do (warmup, no model yet, the cluster still settling).
    HOLD = "hold"


class TransferPhase(enum.Enum):
    IDLE = "idle"
    IN_FLIGHT = "in flight"


@dataclass(frozen=True)
class Decision:
    kind: DecisionKind
    # GROW_FREE / GROW_BALANCED: each op to grow and the size to grow it
    # toward. FUND_RECIPIENT: the recipient and its growth target.
    targets: Mapping["ExperimentalActorPoolMapOperator", int] = field(
        default_factory=dict
    )
    reason: str = ""


class OpRule(enum.Enum):
    """The action that sized one op this tick (``SizingPolicy.decide``).

    The first five come from the bottleneck model and place their actors
    themselves; the rest are per-op actions (``PerOpRules.decide``).
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
    """One op's action this tick. A per-op action sets ``request``, which the
    sizer still clamps and places. A bottleneck action sets ``placed``: its
    requests already name their nodes, in the order to apply them."""

    rule: OpRule
    request: Optional["ActorPoolScalingRequest"] = None
    placed: Tuple["PlacedScalingRequest", ...] = ()
