"""The sizer's phase, and the records of each tick's decision.

State that carries across ticks:

- ``SizerPhase``: the whole sizer, only ever moving forward.
- Per operator, ``op_status.OpStatus``: which rules may resize the op.
- Per operator, its step sizes (``steps.StepSizes``).

Each tick's decision, recomputed every tick:

- ``OpRule``: the rule that sized one op this tick (``SizingPolicy.decide_op``).
- ``OpDecision``: that rule and the op's request, if any.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional

from ray.data._internal.experimental.execution.sizer.policy.steps import Direction

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest


class SizerPhase(enum.Enum):
    # Every op upscales toward an equal share of the allocation until the
    # pipeline flows and the bottleneck observer has then measured one full
    # window. Before that there is no feedback to step from.
    WARMUP = "warmup"
    # The bottleneck snapshot decides every resize.
    STEADY = "steady"


class OpRule(enum.Enum):
    """The rule that sized one op this tick (``SizingPolicy.decide_op``)."""

    # Any phase.
    RELEASE_POOL = "release the pool of a completed op"
    UPSCALE_TO_MIN = "upscale to min size"
    UPSCALE_TO_HELD_ROOM = "upscale into the room held until its first input"
    # Warmup.
    UPSCALE_WARMUP = "upscale toward its warmup share"
    # From the bottleneck snapshot.
    UPSCALE_BOTTLENECK = "upscale a bottleneck"
    UPSCALE_BUSY = "upscale a busy op while no bottleneck is named"
    DOWNSCALE_BLOCKED = "downscale an op blocked by its consumer"
    DOWNSCALE_IDLE = "downscale an idle op"
    NONE = "no change"


# Rules that take one step in a fixed direction. The op's step sizes adapt
# after each one, and the op cools down before its next one.
STEP_RULE_DIRECTION: Dict[OpRule, Direction] = {
    OpRule.UPSCALE_BOTTLENECK: Direction.UP,
    OpRule.UPSCALE_BUSY: Direction.UP,
    OpRule.DOWNSCALE_BLOCKED: Direction.DOWN,
    OpRule.DOWNSCALE_IDLE: Direction.DOWN,
}


@dataclass(frozen=True)
class OpDecision:
    """One op's rule this tick, and its request: a count that WHERE places,
    or None for no change."""

    rule: OpRule
    request: Optional["ActorPoolScalingRequest"] = None
    # Actors an upscale wanted that free capacity could not hold this tick.
    # WHERE uses it to pick downscale victims on nodes where the capacity
    # they free would fit this op's actors.
    unmet: int = 0
