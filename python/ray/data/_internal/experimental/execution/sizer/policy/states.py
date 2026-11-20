"""The sizer's state machine: its phase, and each tick's decision.

Three machines run side by side, each with its own enum:

- ``SizerPhase``: the whole sizer, only ever moving forward.
- ``DecisionKind``: in a phase that decides from the bottleneck model, the
  one primary action a tick takes (``classify`` in ``decision``).
- ``TransferPhase``: whether a transfer is open. A tick may grow an op into
  free capacity while a transfer is in flight, so this is not a decision.

Per operator, ``lifecycle.OpLifecycle`` says whether the op may be resized.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
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
