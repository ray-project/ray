"""Each operator's sizing lifecycle: whether the sizer may resize it now.

Two things resize pools: the bottleneck policy and the per-op how-many pass. A
transfer must hold both its donors and its recipient until the move lands,
or the how-many pass (or the next decision) would undo it; an op that has not
received its first input must stay at zero actors. Both are states here, and
only an ``ACTIVE`` op may be resized.

    AWAITING_FIRST_INPUT --(first bundle queued)--> ACTIVE
    ACTIVE --(transfer fires)--> RECEIVING (recipient) / DONATING (donors)
    RECEIVING --(transfer closes)--> ACTIVE
    DONATING --(its last transfer closes)--> COOLING --(cooldown steps)--> ACTIVE
    any --(op completes)--> COMPLETED
"""

from __future__ import annotations

import enum
import logging
from typing import TYPE_CHECKING, Dict, FrozenSet, Iterable, Optional

from ray.data._internal.execution.execution_flags import SIZER_DONOR_COOLDOWN_STEPS

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


class OpLifecycle(enum.Enum):
    # It has not queued its first input bundle, so it stays at zero actors:
    # there is nothing for them to run yet, and an ``on_start`` (e.g.
    # ``on_write_start``) runs on that bundle and must run before the first
    # actor serializes the MapTransformer with the state it sets.
    AWAITING_FIRST_INPUT = "awaiting first input"
    ACTIVE = "active"
    RECEIVING = "receiving a capacity transfer"
    DONATING = "donating"
    # A donor whose last transfer closed: its windows still describe the
    # larger pool, so it sits out until they have refilled.
    COOLING = "post-donation cooldown"
    COMPLETED = "completed"


class OpLifecycles:
    """The lifecycle of every actor-pool op. ``COMPLETED`` is derived from
    the op itself; every other state is set by a transition here."""

    def __init__(
        self,
        ops: Iterable["ExperimentalActorPoolMapOperator"],
        *,
        cooldown_steps: int = SIZER_DONOR_COOLDOWN_STEPS,
    ):
        self._cooldown_steps = cooldown_steps
        self._state: Dict["ExperimentalActorPoolMapOperator", OpLifecycle] = {}
        # Why the op is in its state, beyond the state itself (the recipient
        # a donor gives to), for logs.
        self._detail: Dict["ExperimentalActorPoolMapOperator", str] = {}
        # Scheduling-loop steps a cooling donor has left.
        self._cooldown_left: Dict["ExperimentalActorPoolMapOperator", int] = {}
        for op in ops:
            if op.awaiting_first_input():
                self._set(op, OpLifecycle.AWAITING_FIRST_INPUT)

    # --- queries ----------------------------------------------------------

    def state(self, op: "ExperimentalActorPoolMapOperator") -> OpLifecycle:
        if op.has_completed():
            return OpLifecycle.COMPLETED
        return self._state.get(op, OpLifecycle.ACTIVE)

    def can_scale(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        """Whether a sizing decision may resize ``op`` now. A completed op
        is released by the how-many pass, which checks completion first."""
        return self._state.get(op, OpLifecycle.ACTIVE) is OpLifecycle.ACTIVE

    def reason(self, op: "ExperimentalActorPoolMapOperator") -> Optional[str]:
        """Why ``op`` cannot be resized, or None if it can."""
        state = self._state.get(op, OpLifecycle.ACTIVE)
        if state is OpLifecycle.ACTIVE:
            return None
        detail = self._detail.get(op)
        return f"{state.value} ({detail})" if detail else state.value

    def ops_in(
        self, state: OpLifecycle
    ) -> FrozenSet["ExperimentalActorPoolMapOperator"]:
        return frozenset(op for op, s in self._state.items() if s is state)

    # --- transitions ------------------------------------------------------

    def on_step(self) -> None:
        """Advance the time-driven transitions; called once per
        scheduling-loop step: release ops that have received their first input, and
        count down cooling donors."""
        for op, state in list(self._state.items()):
            if (
                state is OpLifecycle.AWAITING_FIRST_INPUT
                and not op.awaiting_first_input()
            ):
                self.activate(op)
            elif state is OpLifecycle.COOLING:
                left = self._cooldown_left.get(op, 0) - 1
                if left <= 0:
                    self.activate(op)
                else:
                    self._cooldown_left[op] = left

    def activate(self, op: "ExperimentalActorPoolMapOperator") -> None:
        if self._state.pop(op, None) is not None:
            self._detail.pop(op, None)
            self._cooldown_left.pop(op, None)
            logger.debug("OpLifecycle: %s -> active", op.name)

    def start_receiving(self, op: "ExperimentalActorPoolMapOperator") -> None:
        self._set(op, OpLifecycle.RECEIVING)

    def start_donating(
        self,
        op: "ExperimentalActorPoolMapOperator",
        recipient: "ExperimentalActorPoolMapOperator",
    ) -> None:
        self._set(op, OpLifecycle.DONATING, f"to {recipient.name}")

    def start_cooling(self, op: "ExperimentalActorPoolMapOperator") -> None:
        """Hold a donor out for ``cooldown_steps`` scheduling-loop steps."""
        self._set(op, OpLifecycle.COOLING)
        self._cooldown_left[op] = self._cooldown_steps

    def _set(
        self,
        op: "ExperimentalActorPoolMapOperator",
        state: OpLifecycle,
        detail: Optional[str] = None,
    ) -> None:
        self._state[op] = state
        self._cooldown_left.pop(op, None)
        if detail:
            self._detail[op] = detail
        else:
            self._detail.pop(op, None)
        logger.debug(
            "OpLifecycle: %s -> %s%s",
            op.name,
            state.value,
            f" ({detail})" if detail else "",
        )
