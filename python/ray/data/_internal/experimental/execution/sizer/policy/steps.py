"""How far one upscale or downscale moves an op after warmup.

The sizer does not predict an op's right size. It steps toward it and lets
the bottleneck snapshot say, a window later, whether to keep going: an upscale
multiplies the op's size by ``1 + upscale step``, a downscale by
``1 - downscale step``. Each op adapts its own steps from the fraction its
last resize in each direction actually moved:

- a step in the same direction as the op's last one doubles, up to x2 for an
  upscale and x0.5 for a downscale;
- a step in the other direction halves, so an op that overshoots and comes
  back moves less each time and settles.

A downscale moves at the decision, since draining actors serve no more
tasks. An upscale moves once its pending actors start, so its fraction is
recorded then.

Every step moves at least one actor. A change in the dataset's allocation
resets every op to the starting steps, since the steps learned before it no
longer fit.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional

from ray.data._internal.execution.execution_flags import (
    SIZER_DOWNSCALE_STEP,
    SIZER_UPSCALE_STEP,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

# The largest steps: an upscale at most doubles an op, a downscale at most
# halves it.
UPSCALE_STEP_MAX = 1.0
DOWNSCALE_STEP_MAX = 0.5


class Direction(enum.Enum):
    UP = "up"
    DOWN = "down"


@dataclass
class _OpSteps:
    upscale_fraction: float
    downscale_fraction: float
    last: Optional[Direction] = None


class StepSizes:
    """Each op's current upscale and downscale steps."""

    def __init__(
        self,
        *,
        upscale_step: float = SIZER_UPSCALE_STEP,
        downscale_step: float = SIZER_DOWNSCALE_STEP,
    ):
        assert 0 < upscale_step <= UPSCALE_STEP_MAX, upscale_step
        assert 0 < downscale_step <= DOWNSCALE_STEP_MAX, downscale_step
        self._initial_upscale_step = upscale_step
        self._initial_downscale_step = downscale_step
        self._by_op: Dict["ExperimentalActorPoolMapOperator", _OpSteps] = {}

    def delta(
        self, *, op: "ExperimentalActorPoolMapOperator", direction: Direction, size: int
    ) -> int:
        """Actors one step in ``direction`` moves ``op`` from ``size``: at
        least one, positive for an upscale and negative for a downscale.
        Only reads; ``record`` adapts the step once the resize is made."""
        step = self._next_step_fraction(op=op, direction=direction)
        # Round to the nearest actor, so a step near one actor moves exactly
        # one; ``max`` keeps a smaller step from moving none.
        actors = max(1, round(size * step))
        match direction:
            case Direction.UP:
                return actors
            case Direction.DOWN:
                return -actors
        raise AssertionError(f"Invalid direction {direction} to scale")

    def record(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        direction: Direction,
        before: int,
        after: int,
    ) -> None:
        """``op`` moved from ``before`` to ``after`` actors in ``direction``:
        keep the fraction it actually moved, so the next step doubles or
        halves from it.

        The fraction can differ from the step asked for: the upscale ceiling,
        free capacity, min size or the downscale floor can cut a resize, and
        pending actors can expire without starting. A move of zero actors
        changes nothing, so the next try uses the same step.
        """
        moved = abs(after - before)
        if moved == 0:
            return
        steps = self._step_fractions(op)
        if direction is Direction.UP:
            fraction = moved / before if before > 0 else UPSCALE_STEP_MAX
            steps.upscale_fraction = min(fraction, UPSCALE_STEP_MAX)
        else:
            steps.downscale_fraction = min(moved / before, DOWNSCALE_STEP_MAX)
        steps.last = direction

    def reset(self) -> None:
        """Start every op over from the starting steps."""
        self._by_op.clear()

    def _next_step_fraction(
        self, *, op: "ExperimentalActorPoolMapOperator", direction: Direction
    ) -> float:
        steps = self._step_fractions(op)
        match direction:
            case Direction.UP:
                step, most = steps.upscale_fraction, UPSCALE_STEP_MAX
            case Direction.DOWN:
                step, most = steps.downscale_fraction, DOWNSCALE_STEP_MAX
            case _:
                raise AssertionError(f"Invalid direction {direction}")
        if steps.last is None:
            return step
        if steps.last is direction:
            return min(step * 2, most)
        return step / 2

    def _step_fractions(self, op: "ExperimentalActorPoolMapOperator") -> _OpSteps:
        if op not in self._by_op:
            self._by_op[op] = _OpSteps(
                upscale_fraction=self._initial_upscale_step,
                downscale_fraction=self._initial_downscale_step,
            )
        return self._by_op[op]
