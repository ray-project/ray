from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Callable, ClassVar, List

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train.health.probe import NodeIdStr, Probe


@PublicAPI(stability="alpha")
class Action(Enum):
    """The action a ``HealthDecision`` requests."""

    NOOP = auto()
    DIAGNOSE = auto()
    REATTEMPT = auto()
    EVICT = auto()


@PublicAPI(stability="alpha")
@dataclass
class HealthDecision:
    """Base class for the decisions an ``Evaluator`` returns. Use a subclass.

    Attributes:
        reason: A human-readable explanation of why the decision was made.
    """

    action: ClassVar[Action]
    reason: str = ""


@PublicAPI(stability="alpha")
@dataclass
class Noop(HealthDecision):
    """Take no action."""

    action: ClassVar[Action] = Action.NOOP


@PublicAPI(stability="alpha")
@dataclass
class Reattempt(HealthDecision):
    """Restart the workers."""

    action: ClassVar[Action] = Action.REATTEMPT


@PublicAPI(stability="alpha")
@dataclass
class Evict(HealthDecision):
    """Stop using some nodes for the rest of the run.

    Attributes:
        target_nodes: IDs of the nodes to evict.
    """

    action: ClassVar[Action] = Action.EVICT
    target_nodes: List["NodeIdStr"] = field(default_factory=list)


@PublicAPI(stability="alpha")
@dataclass(kw_only=True)
class Diagnose(HealthDecision):
    """Poll some probes once.

    Attributes:
        probe_creator: Creates the probes to poll.
        target_ranks: World ranks to poll ``WorkerProbe``\\ s on. Empty means
            all ranks.
        target_nodes: IDs of the nodes to poll ``NodeProbe``\\ s on. Empty means
            all nodes with workers.
    """

    action: ClassVar[Action] = Action.DIAGNOSE
    probe_creator: Callable[[], List["Probe"]]
    target_ranks: List[int] = field(default_factory=list)
    target_nodes: List["NodeIdStr"] = field(default_factory=list)
