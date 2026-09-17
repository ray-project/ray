"""The output of the Decide stage: one instruction for the controller."""
import logging
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from typing import TYPE_CHECKING, ClassVar, List, Optional, Sequence

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.health.probe import OnDemandProbe

logger = logging.getLogger(__name__)


@DeveloperAPI
class Action(Enum):
    """Ordered by severity: the most severe action among all signals wins."""

    NOOP = auto()  # healthy, do nothing
    DIAGNOSE = auto()  # run an active check (may need workers stopped)
    REATTEMPT = auto()  # restart from the last checkpoint
    EVICT = auto()  # remove node(s) and restart without them


SEVERITY = {
    Action.NOOP: 0,
    Action.DIAGNOSE: 1,
    Action.REATTEMPT: 2,
    Action.EVICT: 3,
}


@DeveloperAPI
class Cause(Enum):
    """Structured classification, alongside the free-text reason.

    Downstream consumers key off this: only ``HARDWARE`` authorizes tainting a
    node, and a cross-run "lemon node" tracker aggregates on it.
    """

    UNKNOWN = auto()
    HARDWARE = auto()  # GPU, NIC, or host fault attributable to a node
    INFRASTRUCTURE = auto()  # remote storage, preemption, control plane
    APPLICATION = auto()  # NaN loss, user code, data pipeline
    NO_PROGRESS = auto()  # hang or straggler, not yet attributed


@DeveloperAPI
@dataclass
class HealthDecision:
    action: ClassVar[Action]
    cause: Cause = Cause.UNKNOWN
    reason: str = ""


@DeveloperAPI
@dataclass
class Noop(HealthDecision):
    action: ClassVar[Action] = Action.NOOP


@DeveloperAPI
@dataclass
class Reattempt(HealthDecision):
    action: ClassVar[Action] = Action.REATTEMPT


@DeveloperAPI
@dataclass
class Evict(HealthDecision):
    action: ClassVar[Action] = Action.EVICT
    target_nodes: List[str] = field(default_factory=list)


@DeveloperAPI
@dataclass
class Diagnose(HealthDecision):
    action: ClassVar[Action] = Action.DIAGNOSE
    on_demand_probes: List["OnDemandProbe"] = field(default_factory=list)
    target_nodes: List[str] = field(default_factory=list)
    target_ranks: List[int] = field(default_factory=list)


def _merge_causes(causes: Sequence[Cause]) -> Cause:
    """One cause for the merged decision.

    Distinct causes agreeing on a node is the normal case for a real hardware
    fault (a NIC probe and a straggler check both firing). Keep the specific
    one rather than collapsing to ``UNKNOWN``, preferring the most actionable:
    ``HARDWARE`` authorizes quarantine, so it wins over an unattributed
    ``NO_PROGRESS``.
    """
    ordered = [
        Cause.HARDWARE,
        Cause.INFRASTRUCTURE,
        Cause.APPLICATION,
        Cause.NO_PROGRESS,
        Cause.UNKNOWN,
    ]
    present = set(causes)
    for cause in ordered:
        if cause in present:
            return cause
    return Cause.UNKNOWN


def _dedup(values: Sequence) -> List:
    """Order-preserving dedup; ``target_nodes`` order shows up in events."""
    seen, out = set(), []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


@DeveloperAPI
def merge_decisions(
    decisions: Sequence[HealthDecision],
    *,
    handled_nodes: Optional[Sequence[str]] = None,
) -> Optional[HealthDecision]:
    """Merge every evaluator's decisions into the single one the controller acts on.

    1. Drop ``NOOP`` and anything already being handled.
    2. Rank by severity; the most severe action wins. Position in
       ``HealthConfig.policies`` carries no priority -- policies are peers.
    3. Merge *at* the winning severity rather than picking one arbitrarily, so
       the emitted event names every evaluator that fired.

    Args:
        decisions: What the evaluators returned this poll, concatenated.
        handled_nodes: Nodes with an action already in flight. An ``Evict`` for
            one of these is suppressed, so a signal that persists across polls
            does not retrigger every poll.

    Returns:
        The merged decision, or ``None`` if nothing survives.
    """
    handled = set(handled_nodes or ())

    live: List[HealthDecision] = []
    for d in decisions:
        if d is None or d.action is Action.NOOP:
            continue
        if isinstance(d, Evict):
            remaining = [n for n in d.target_nodes if n not in handled]
            if not remaining:
                # Every node it names is already being dealt with.
                continue
            d = replace(d, target_nodes=remaining)
        elif isinstance(d, Diagnose) and d.target_nodes:
            remaining = [n for n in d.target_nodes if n not in handled]
            if not remaining:
                # Diagnosing a node the run has already condemned is work that
                # cannot change the outcome, and it costs a py-spy attach on
                # every rank there.
                continue
            d = replace(d, target_nodes=remaining)
        live.append(d)

    if not live:
        return None

    winner = max(SEVERITY[d.action] for d in live)
    at_winner = [d for d in live if SEVERITY[d.action] == winner]

    cause = _merge_causes([d.cause for d in at_winner])
    reason = "; ".join(_dedup([d.reason for d in at_winner if d.reason]))

    first = at_winner[0]
    if isinstance(first, Evict):
        return Evict(
            cause=cause,
            reason=reason,
            target_nodes=_dedup([n for d in at_winner for n in d.target_nodes]),
        )
    if isinstance(first, Diagnose):
        return Diagnose(
            cause=cause,
            reason=reason,
            on_demand_probes=_dedup([p for d in at_winner for p in d.on_demand_probes]),
            target_nodes=_dedup([n for d in at_winner for n in d.target_nodes]),
            target_ranks=_dedup([r for d in at_winner for r in d.target_ranks]),
        )
    return Reattempt(cause=cause, reason=reason)
