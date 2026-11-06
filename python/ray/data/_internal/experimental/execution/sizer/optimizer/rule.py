"""The interface every cross-operator optimization rule implements."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Generic, List, TypeVar

from ray.data._internal.experimental.execution.sizer.optimizer.context import (
    OptimizationContext,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.sizer.models import (
        PlacedSizingRequest,
    )

# The cross-tick state a rule declares via ``new_state``.
StateT = TypeVar("StateT")


class OptimizationRule(abc.ABC, Generic[StateT]):
    """One cross-operator optimization.

    Rules are stateless: whatever a rule must remember between ticks it
    declares via ``new_state``, and the optimizer owns that object and hands
    it back on every call. A rule that needs an operator to itself blocks it
    on the shared OpScalingGuard instead of tracking it privately.
    """

    name: str

    def new_state(self) -> StateT:
        """The cross-tick state this rule wants the optimizer to carry."""
        return None  # type: ignore[return-value]

    def advance(self, ctx: OptimizationContext, state: StateT) -> None:
        """Settle work started on an earlier tick (landings, timeouts).

        Runs every tick before ``apply``, whether or not the rule proposes
        anything.
        """

    @abc.abstractmethod
    def apply(
        self, ctx: OptimizationContext, state: StateT
    ) -> List["PlacedSizingRequest"]:
        """Propose fully-placed scaling requests for this tick."""
