"""Each operator's sizing status: what the sizer may do to it now.

A status is a permission, not a size. ``SizingPolicy.decide_op`` matches on
it to pick the op's rule; the status itself only says which rules may touch
the op this tick. The sizer-wide ``SizerPhase`` (WARMUP, STEADY) is
separate: it is one value for the whole pipeline, and some rules
read it.

Statuses (``OpStatus``; each carries an ``OpStatusKind`` tag)
    Held(reserved)        No input bundle queued yet, so no actors; room for
                          ``reserved`` actors (its warmup share) is kept out
                          of free capacity for it.
    Active                Any rule may resize it.
    Cooling(until, upscale_from)
                          The policy resized it after warmup. ``until`` is
                          when its window has seen only the new size. After
                          an upscale, ``until`` is None while actors are
                          still pending, and ``upscale_from`` is the size
                          before the upscale. A downscale cools from the
                          decision: draining actors are not measured, so the
                          window describes the new size at once.
    Completed             Done. Derived from ``op.has_completed()``.

Initial status: ``Held(0)`` for an op awaiting its first input when the sizer
is built, else ``Active``.

Terminal status: ``Completed``, unless lineage reconstruction reopens the op
(an external event); it then resumes as ``Active``, with its pool released.

Events
    TICK         One sizing tick, before the policy decides.
    UPSCALED     The policy upscaled the op after warmup.
    DOWNSCALED   The policy downscaled the op after warmup.
    COMPLETED    The op completes.
    REOPENED     Lineage reconstruction reopens a completed op.

Transitions (anything else raises)
    from           event       guard                         to
    Held           TICK        its first input has arrived   Active
    Held           TICK        still waiting                 Held(reserved)
    Active         UPSCALED    -                             Cooling(None, size)
    Active         DOWNSCALED  -                             Cooling(now + cooldown)
    Cooling(None)  TICK        no actor pending              Cooling(now + cooldown)
    Cooling(t)     TICK        now >= t                      Active
    any            COMPLETED  -                                    Completed
    Completed      REOPENED   -                                    Active
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

from ray.data._internal.execution.execution_flags import SIZER_BOTTLENECK_WINDOW_S
from ray.data._internal.experimental.execution.sizer.common import (
    LandedUpscales,
    SizeChange,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)


class OpStatusKind(enum.Enum):
    """Which status an op is in; each ``OpStatus`` variant carries one."""

    HELD = "held until its first input"
    ACTIVE = "active"
    COOLING = "cooling after a resize"
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
class Active:
    kind: ClassVar[OpStatusKind] = OpStatusKind.ACTIVE


@dataclass(frozen=True)
class Cooling:
    """Resized after warmup; waits until its window describes the new size.
    ``until`` is None while an upscale's actors are pending, and
    ``upscale_from`` is the size before that upscale."""

    until: Optional[float] = None
    upscale_from: Optional[int] = None
    kind: ClassVar[OpStatusKind] = OpStatusKind.COOLING


@dataclass(frozen=True)
class Completed:
    kind: ClassVar[OpStatusKind] = OpStatusKind.COMPLETED


OpStatus = Union[Held, Active, Cooling, Completed]

_ACTIVE = Active()
_COMPLETED = Completed()

# Legal moves between stored statuses. ``Completed`` is derived, not stored,
# so it is not here.
_ALLOWED: Dict[OpStatusKind, Set[OpStatusKind]] = {
    OpStatusKind.HELD: {OpStatusKind.HELD, OpStatusKind.ACTIVE},
    OpStatusKind.ACTIVE: {OpStatusKind.COOLING},
    OpStatusKind.COOLING: {OpStatusKind.COOLING, OpStatusKind.ACTIVE},
}


class IllegalStatusTransition(ValueError):
    pass


class OpStatusMachine:
    """The status of every actor-pool op, and every transition between
    statuses (see the module docstring for the full table)."""

    def __init__(
        self,
        *,
        ops: Iterable["ExperimentalActorPoolMapOperator"],
        cooldown_s: float = SIZER_BOTTLENECK_WINDOW_S,
    ):
        self._cooldown_s = cooldown_s
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

    def ops_in(
        self, *, kind: OpStatusKind
    ) -> FrozenSet["ExperimentalActorPoolMapOperator"]:
        return frozenset(
            op
            for op, s in self._status.items()
            if s.kind is kind and not op.has_completed()
        )

    # --- events -----------------------------------------------------------

    def on_tick(self, *, now: float) -> LandedUpscales:
        """TICK, before held ops' room is reserved and before the policy
        decides:

        - a held op whose first input has arrived becomes active;
        - a cooling op whose upscale has landed (no actor pending) starts its
          cooldown, and one whose cooldown has passed becomes active.

        Returns each upscale that landed this tick, as op -> ``SizeChange``.
        Pending actors can expire without starting, so ``after`` can be
        smaller than the upscale asked for.

        A completed op's stored status is dropped, so if lineage
        reconstruction reopens it, it resumes as active."""
        landed: LandedUpscales = {}
        for op, status in list(self._status.items()):
            if op.has_completed():
                del self._status[op]
            elif isinstance(status, Held) and not op.awaiting_first_input():
                self._transition(op=op, to=_ACTIVE)
            elif isinstance(status, Cooling):
                if status.until is None:
                    pool = op.actor_pool
                    if pool.num_pending_actors() == 0:
                        assert status.upscale_from is not None
                        landed[op] = SizeChange(
                            before=status.upscale_from, after=pool.serving_size()
                        )
                        self._transition(
                            op=op, to=Cooling(until=now + self._cooldown_s)
                        )
                elif now >= status.until:
                    self._transition(op=op, to=_ACTIVE)
        return landed

    def set_reserved(self, *, op: "ExperimentalActorPoolMapOperator", n: int) -> None:
        """Record the room held for a held op this tick."""
        self._transition(op=op, to=Held(reserved=n))

    def start_cooling(
        self,
        *,
        op: "ExperimentalActorPoolMapOperator",
        now: float,
        upscale_from: Optional[int] = None,
    ) -> None:
        """UPSCALED when ``upscale_from`` is set: wait until no actor is
        pending, then for one cooldown. DOWNSCALED otherwise: one cooldown
        from ``now``."""
        if upscale_from is None:
            self._transition(op=op, to=Cooling(until=now + self._cooldown_s))
        else:
            self._transition(op=op, to=Cooling(upscale_from=upscale_from))

    def _transition(
        self, *, op: "ExperimentalActorPoolMapOperator", to: OpStatus
    ) -> None:
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
