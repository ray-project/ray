"""A lock over operators, so two sizing rules never resize the same pool.

Three things resize pools: the per-operator warm loop and the two optimizer
rules. A capacity transfer, for instance, must hold both its donor and its
recipient until the move lands -- otherwise the warm loop regrows the donor and
undoes it.
"""

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, FrozenSet, Optional

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Block:
    reason: str
    # Monotonic-clock expiry; None means blocked until release() is called.
    until: Optional[float]


class OpScalingGuard:
    """Operators currently held by one sizing rule."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._blocked: Dict["ExperimentalActorPoolMapOperator", _Block] = {}

    def block(
        self,
        op: "ExperimentalActorPoolMapOperator",
        reason: str,
        until: Optional[float] = None,
    ) -> None:
        """Block ``op`` from scaling, until ``until`` or an explicit release."""
        self._blocked[op] = _Block(reason=reason, until=until)
        expiry = "" if until is None else f"; for {until - self._clock():.0f}s"
        logger.debug("OpScalingGuard: %s blocked (%s%s)", op.name, reason, expiry)

    def release(self, op: "ExperimentalActorPoolMapOperator") -> None:
        if self._blocked.pop(op, None) is not None:
            logger.debug("OpScalingGuard: %s released", op.name)

    def can_scale(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        return self._entry(op) is None

    def reason(self, op: "ExperimentalActorPoolMapOperator") -> Optional[str]:
        entry = self._entry(op)
        return entry.reason if entry is not None else None

    def blocked_ops(self) -> FrozenSet["ExperimentalActorPoolMapOperator"]:
        return frozenset(op for op in list(self._blocked) if not self.can_scale(op))

    def _entry(self, op: "ExperimentalActorPoolMapOperator") -> Optional[_Block]:
        """The live block on ``op``, dropping it if its expiry has passed."""
        entry = self._blocked.get(op)
        if entry is None:
            return None
        if entry.until is not None and self._clock() >= entry.until:
            del self._blocked[op]
            logger.debug(
                "OpScalingGuard: %s released (%s expired)", op.name, entry.reason
            )
            return None
        return entry
