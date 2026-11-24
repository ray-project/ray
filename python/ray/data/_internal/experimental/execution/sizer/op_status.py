"""Each operator's sizing status: what the sizer may do to it now.

A status is a permission, not a size. How big an op grows is decided by the
actions below and their guards; the status only says which actions may touch
the op this tick. The sizer-wide ``SizerPhase`` (WARMUP, MODEL_WARMING,
STEADY) is separate: it is one value for the whole pipeline, and some
actions' guards read it.

Statuses (``OpStatus``; each carries an ``OpStatusKind`` tag)
    Held(reserved)        No input bundle queued yet, so no actors; room for
                          ``reserved`` actors (its warmup share) is kept out
                          of free capacity for it.
    Released(grow_to)     The one tick after its first input arrives: grow
                          straight to ``grow_to``, the room it had held. Needed
                          because that same tick often ends WARMUP and gives
                          the op its first model window, after which
                          WARMUP_SHARE no longer applies to it.
    Active                Nothing outside the op constrains it: any action
                          whose guard passes may resize it.
    Receiving             The recipient of an open capacity transfer.
    Donating(recipient)   A donor to an open transfer.
    Cooling(steps_left)   A donor whose last transfer closed; its windows
                          still describe the larger pool.
    Completed             Done. Derived from ``op.has_completed()``.

Initial status: ``Held(0)`` for an op awaiting its first input when the sizer
is built, else ``Active``.

Terminal status: ``Completed``, unless lineage reconstruction reopens the op
(an external event); it then resumes as ``Active``, with its pool released.

Events
    STEP               One scheduling-loop step.
    TICK               One sizing tick (a step on which the sizer decides).
    TRANSFER_FIRED     The policy starts a transfer (``TransferBook``).
    TRANSFER_CLOSED    A transfer lands, is lost or times out.
    COMPLETED          The op completes.
    REOPENED           Lineage reconstruction reopens a completed op.

Transitions (anything else raises)
    from        event            guard                               to
    Held(r)     TICK             its first input has arrived          Released(r)
    Held        TICK             still waiting                        Held(reserved)
    Released    TICK             -                                    Active
    Active      TRANSFER_FIRED   it is the recipient                  Receiving
    Active      TRANSFER_FIRED   it is a donor                        Donating
    Donating    TRANSFER_FIRED   it donates to another transfer too   Donating
    Receiving   TRANSFER_CLOSED  -                                    Active
    Donating    TRANSFER_CLOSED  it donates to another open transfer  Donating
    Donating    TRANSFER_CLOSED  that was its last transfer           Cooling(N)
    Cooling(n)  STEP             n > 1                                Cooling(n - 1)
    Cooling(1)  STEP             -                                    Active
    any         COMPLETED        -                                    Completed
    Completed   REOPENED         -                                    Active

Actions, and the status each needs (guards beyond the status in brackets)
    GROW_FREE, GROW_BALANCED,   Active  [phase is not WARMUP; the bottleneck
    FUND / DONATE,                       model picks one per tick]
    SHRINK_SURPLUS
    RESERVE                     Held    [not PG-managed: its PG is its room]
    GRANT_HELD                  Released [grow_to above its current size]
    WARMUP_SHARE                Active, Released  [phase is WARMUP, or the op
                                         is not yet modeled; every upstream
                                         has produced]
    SHED_IDLE                   Active  [modeled; idle for the hysteresis]
    REFILL_TO_MIN               Active, Released  [fewer actors than min_size]
    RELEASE_POOL                Completed
Every upscale is also capped at the actors the op's remaining work can use.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    Optional,
    Set,
    Union,
)

from ray.data._internal.execution.execution_flags import SIZER_DONOR_COOLDOWN_STEPS

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


class OpStatusKind(enum.Enum):
    """Which status an op is in; each ``OpStatus`` variant carries one."""

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
    state it sets. ``reserved``: actors' worth of room held for it this tick,
    so ops that started earlier cannot take every node's space."""

    reserved: int = 0
    kind: ClassVar[OpStatusKind] = OpStatusKind.HELD


@dataclass(frozen=True)
class Released:
    """The tick its first input arrived: grow straight to ``grow_to`` (the
    room it had held), once."""

    grow_to: int
    kind: ClassVar[OpStatusKind] = OpStatusKind.RELEASED


@dataclass(frozen=True)
class Active:
    kind: ClassVar[OpStatusKind] = OpStatusKind.ACTIVE


@dataclass(frozen=True)
class Receiving:
    kind: ClassVar[OpStatusKind] = OpStatusKind.RECEIVING


@dataclass(frozen=True)
class Donating:
    """Donating to ``recipient`` (the latest, if it funds several)."""

    recipient: "ExperimentalActorPoolMapOperator"
    kind: ClassVar[OpStatusKind] = OpStatusKind.DONATING


@dataclass(frozen=True)
class Cooling:
    """Sits out ``steps_left`` more scheduling-loop steps."""

    steps_left: int
    kind: ClassVar[OpStatusKind] = OpStatusKind.COOLING


@dataclass(frozen=True)
class Completed:
    kind: ClassVar[OpStatusKind] = OpStatusKind.COMPLETED


OpStatus = Union[Held, Released, Active, Receiving, Donating, Cooling, Completed]

_ACTIVE = Active()
_COMPLETED = Completed()

# Legal moves between stored statuses. ``Completed`` is derived, not stored,
# so it is not here.
_ALLOWED: Dict[OpStatusKind, Set[OpStatusKind]] = {
    OpStatusKind.HELD: {OpStatusKind.HELD, OpStatusKind.RELEASED},
    OpStatusKind.RELEASED: {OpStatusKind.ACTIVE},
    OpStatusKind.ACTIVE: {OpStatusKind.RECEIVING, OpStatusKind.DONATING},
    OpStatusKind.RECEIVING: {OpStatusKind.ACTIVE},
    OpStatusKind.DONATING: {OpStatusKind.DONATING, OpStatusKind.COOLING},
    OpStatusKind.COOLING: {OpStatusKind.COOLING, OpStatusKind.ACTIVE},
}


class IllegalStatusTransition(ValueError):
    pass


class OpStatusMachine:
    """The status of every actor-pool op, and every transition between
    statuses (see the module docstring for the full table)."""

    def __init__(
        self,
        ops: Iterable["ExperimentalActorPoolMapOperator"],
        *,
        cooldown_steps: int = SIZER_DONOR_COOLDOWN_STEPS,
    ):
        self._cooldown_steps = cooldown_steps
        # Ops not ``Active``; a missing op is active.
        self._status: Dict["ExperimentalActorPoolMapOperator", OpStatus] = {}
        # Initial status: held until its first input, else active.
        for op in ops:
            if op.awaiting_first_input():
                self._status[op] = Held()

    # --- queries ----------------------------------------------------------

    def status(self, op: "ExperimentalActorPoolMapOperator") -> OpStatus:
        if op.has_completed():
            return _COMPLETED
        return self._status.get(op, _ACTIVE)

    def kind(self, op: "ExperimentalActorPoolMapOperator") -> OpStatusKind:
        return self.status(op).kind

    def can_scale(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        """Whether the policy may resize ``op`` now (grow it, transfer to or
        from it, shed its surplus): only an ``Active`` op."""
        return self._status.get(op, _ACTIVE).kind is OpStatusKind.ACTIVE

    def can_size_per_op(self, op: "ExperimentalActorPoolMapOperator") -> bool:
        """Whether the per-op actions (WARMUP_SHARE, GRANT_HELD, SHED_IDLE,
        REFILL_TO_MIN) may resize ``op`` now: an ``Active`` or ``Released``
        op. A completed op is released by the how-many pass, which checks
        completion first."""
        return self._status.get(op, _ACTIVE).kind in (
            OpStatusKind.ACTIVE,
            OpStatusKind.RELEASED,
        )

    def reason(self, op: "ExperimentalActorPoolMapOperator") -> Optional[str]:
        """Why ``op`` may not be resized, or None if it may."""
        status = self._status.get(op, _ACTIVE)
        if isinstance(status, Active):
            return None
        if isinstance(status, Donating):
            return f"{status.kind.value} (to {status.recipient.name})"
        return status.kind.value

    def ops_in(
        self, kind: OpStatusKind
    ) -> FrozenSet["ExperimentalActorPoolMapOperator"]:
        return frozenset(
            op
            for op, s in self._status.items()
            if s.kind is kind and not op.has_completed()
        )

    # --- events -----------------------------------------------------------

    def on_step(self) -> None:
        """STEP: count down cooling donors, so the cooldown is measured in
        scheduling-loop steps."""
        for op, status in list(self._status.items()):
            if isinstance(status, Cooling):
                if status.steps_left <= 1:
                    self._transition(op, _ACTIVE)
                else:
                    self._transition(op, Cooling(status.steps_left - 1))

    def on_tick(self) -> None:
        """TICK, before held ops' room is reserved: an op released last tick
        becomes active, and a held op whose first input has arrived is
        released for this one tick. A completed op's stored status is
        dropped, so if lineage reconstruction reopens it, it resumes as
        active."""
        for op, status in list(self._status.items()):
            if op.has_completed():
                del self._status[op]
            elif isinstance(status, Released):
                self._transition(op, _ACTIVE)
            elif isinstance(status, Held) and not op.awaiting_first_input():
                self._transition(op, Released(grow_to=status.reserved))

    def set_reserved(self, op: "ExperimentalActorPoolMapOperator", n: int) -> None:
        """Record the room held for a held op this tick."""
        self._transition(op, Held(reserved=n))

    def start_receiving(self, op: "ExperimentalActorPoolMapOperator") -> None:
        """TRANSFER_FIRED, for the recipient."""
        self._transition(op, Receiving())

    def start_donating(
        self,
        op: "ExperimentalActorPoolMapOperator",
        recipient: "ExperimentalActorPoolMapOperator",
    ) -> None:
        """TRANSFER_FIRED for a donor, or TRANSFER_CLOSED for a donor still
        funding another open transfer."""
        self._transition(op, Donating(recipient))

    def start_cooling(self, op: "ExperimentalActorPoolMapOperator") -> None:
        """TRANSFER_CLOSED for a donor's last transfer: sit out
        ``cooldown_steps`` scheduling-loop steps."""
        self._transition(op, Cooling(self._cooldown_steps))

    def activate(self, op: "ExperimentalActorPoolMapOperator") -> None:
        """TRANSFER_CLOSED, for the recipient."""
        self._transition(op, _ACTIVE)

    def _transition(self, op: "ExperimentalActorPoolMapOperator", to: OpStatus) -> None:
        """Move ``op`` to ``to``, or raise if the table does not allow it. A
        completed op is left alone: its status is derived from the op."""
        if op.has_completed():
            return
        current = self._status.get(op, _ACTIVE)
        if current == to:
            return
        if to.kind not in _ALLOWED[current.kind]:
            raise IllegalStatusTransition(
                f"{op.name}: {current.kind.name} -> {to.kind.name} is not a legal "
                "status transition"
            )
        if isinstance(to, Active):
            self._status.pop(op, None)
        else:
            self._status[op] = to
        if current.kind is not to.kind:
            logger.debug("OpStatus: %s %s -> %s", op.name, current, to)
