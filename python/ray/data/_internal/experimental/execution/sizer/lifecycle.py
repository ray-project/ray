"""Each operator's sizing state: what the sizer may do to it now.

Two things resize pools: the bottleneck policy and the per-op how-many pass.
An op that has not received its first input must stay at zero actors, with
room held for it; a transfer must hold its recipient and donors until the
move lands, or the how-many pass (or the next decision) would undo it. Each
is a state here, and each state carries what it needs:

    HELD(reserved) --(first input arrives)--> RELEASED(grow_to) --(next tick)--> ACTIVE
    ACTIVE --(transfer fires)--> RECEIVING (recipient) / DONATING(recipient) (donors)
    RECEIVING --(transfer closes)--> ACTIVE
    DONATING --(its last transfer closes)--> COOLING(steps_left) --(steps)--> ACTIVE
    any --(op completes)--> COMPLETED

What each state allows:

    state      policy may resize   how-many wants   refill to min   room held
    HELD       no                  no               no (stays at 0) yes
    RELEASED   no                  yes + held grant yes             no
    ACTIVE     yes                 yes              yes             no
    RECEIVING  no                  no               no              no
    DONATING   no                  no               no              no
    COOLING    no                  no               no              no
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Dict, FrozenSet, Iterable, Optional, Union

from ray.data._internal.execution.execution_flags import SIZER_DONOR_COOLDOWN_STEPS

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


class OpLifecycle(enum.Enum):
    """Which state an op is in; each ``OpState`` variant carries one."""

    HELD = "held until its first input"
    RELEASED = "released from its hold"
    ACTIVE = "active"
    RECEIVING = "receiving a capacity transfer"
    DONATING = "donating"
    COOLING = "post-donation cooldown"
    COMPLETED = "completed"


@dataclass(frozen=True)
class Held:
    """No input bundle queued yet, so no actors: there is nothing for them to
    run, and an ``on_start`` (e.g. ``on_write_start``) runs on that bundle and
    must run before the first actor serializes the MapTransformer with the
    state it sets. Room for ``reserved`` actors (its warmup share) is held
    out of free capacity meanwhile, so ops that started earlier cannot take
    every node's space."""

    reserved: int = 0
    kind: ClassVar[OpLifecycle] = OpLifecycle.HELD


@dataclass(frozen=True)
class Released:
    """The tick its first input arrived: grow straight to ``grow_to`` (the
    room it had reserved), once."""

    grow_to: int
    kind: ClassVar[OpLifecycle] = OpLifecycle.RELEASED


@dataclass(frozen=True)
class Active:
    kind: ClassVar[OpLifecycle] = OpLifecycle.ACTIVE


@dataclass(frozen=True)
class Receiving:
    kind: ClassVar[OpLifecycle] = OpLifecycle.RECEIVING


@dataclass(frozen=True)
class Donating:
    """Donating to ``recipient`` (the latest, if it funds several)."""

    recipient: "ExperimentalActorPoolMapOperator"
    kind: ClassVar[OpLifecycle] = OpLifecycle.DONATING


@dataclass(frozen=True)
class Cooling:
    """A donor whose last transfer closed: its windows still describe the
    larger pool, so it sits out ``steps_left`` more scheduling-loop steps."""

    steps_left: int
    kind: ClassVar[OpLifecycle] = OpLifecycle.COOLING


@dataclass(frozen=True)
class Completed:
    kind: ClassVar[OpLifecycle] = OpLifecycle.COMPLETED


OpState = Union[Held, Released, Active, Receiving, Donating, Cooling, Completed]

_ACTIVE = Active()
_COMPLETED = Completed()


class OpLifecycles:
    """The state of every actor-pool op. ``COMPLETED`` is derived from the op
    itself; every other state is set by a transition here."""

    def __init__(
        self,
        ops: Iterable["ExperimentalActorPoolMapOperator"],
        *,
        cooldown_steps: int = SIZER_DONOR_COOLDOWN_STEPS,
    ):
        self._cooldown_steps = cooldown_steps
        # Ops not in ``ACTIVE``; a missing op is active.
        self._state: Dict["ExperimentalActorPoolMapOperator", OpState] = {}
        for op in ops:
            if op.awaiting_first_input():
                self._set(op, Held())

    # --- queries ----------------------------------------------------------

    def state(self, op: "ExperimentalActorPoolMapOperator") -> OpState:
        if op.has_completed():
            return _COMPLETED
        return self._state.get(op, _ACTIVE)

    def kind(self, op: "ExperimentalActorPoolMapOperator") -> OpLifecycle:
        return self.state(op).kind

    def can_scale(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        """Whether the policy may resize ``op`` now (grow it, transfer to or
        from it, shed its surplus). A completed op is released by the
        how-many pass, which checks completion first."""
        return self._state.get(op, _ACTIVE).kind is OpLifecycle.ACTIVE

    def may_refill_to_min(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        """Whether the how-many pass may size ``op`` (its wants, and topping
        it up to min size): only once it has input and no transfer holds
        it."""
        return self._state.get(op, _ACTIVE).kind in (
            OpLifecycle.ACTIVE,
            OpLifecycle.RELEASED,
        )

    def reason(self, op: "ExperimentalActorPoolMapOperator") -> Optional[str]:
        """Why the policy may not resize ``op``, or None if it may."""
        state = self._state.get(op, _ACTIVE)
        if isinstance(state, Active):
            return None
        if isinstance(state, Donating):
            return f"{state.kind.value} (to {state.recipient.name})"
        return state.kind.value

    def ops_in(
        self, kind: OpLifecycle
    ) -> FrozenSet["ExperimentalActorPoolMapOperator"]:
        return frozenset(op for op, s in self._state.items() if s.kind is kind)

    # --- transitions ------------------------------------------------------

    def on_step(self) -> None:
        """Count down cooling donors; called once per scheduling-loop step,
        so the cooldown is measured in steps."""
        for op, state in list(self._state.items()):
            if isinstance(state, Cooling):
                if state.steps_left <= 1:
                    self.activate(op)
                else:
                    self._state[op] = Cooling(state.steps_left - 1)

    def on_tick(self) -> None:
        """The hold transitions; called once per sizing tick, before held
        ops' room is reserved. A released op becomes active, and a held op
        whose first input has arrived is released for this one tick."""
        for op, state in list(self._state.items()):
            if isinstance(state, Released):
                self.activate(op)
            elif isinstance(state, Held) and not op.awaiting_first_input():
                self._set(op, Released(grow_to=state.reserved))

    def set_reserved(self, op: "ExperimentalActorPoolMapOperator", n: int) -> None:
        """Record the room held for a held op this tick."""
        if isinstance(self._state.get(op), Held):
            self._state[op] = Held(reserved=n)

    def activate(self, op: "ExperimentalActorPoolMapOperator") -> None:
        if self._state.pop(op, None) is not None:
            logger.debug("OpLifecycle: %s -> active", op.name)

    def start_receiving(self, op: "ExperimentalActorPoolMapOperator") -> None:
        self._set(op, Receiving())

    def start_donating(
        self,
        op: "ExperimentalActorPoolMapOperator",
        recipient: "ExperimentalActorPoolMapOperator",
    ) -> None:
        self._set(op, Donating(recipient))

    def start_cooling(self, op: "ExperimentalActorPoolMapOperator") -> None:
        """Hold a donor out for ``cooldown_steps`` scheduling-loop steps."""
        self._set(op, Cooling(self._cooldown_steps))

    def _set(self, op: "ExperimentalActorPoolMapOperator", state: OpState) -> None:
        self._state[op] = state
        logger.debug("OpLifecycle: %s -> %s", op.name, state)
