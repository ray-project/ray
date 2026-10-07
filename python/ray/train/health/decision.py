from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train.health.check import HealthCheck
    from ray.train.health.probe import NodeIdStr


@PublicAPI(stability="alpha")
@dataclass
class HealthDecision:
    """Base class for the decisions an ``Evaluator`` returns.

    Abstract: return a ``Noop``, ``Reattempt``, ``Evict`` or ``Diagnose``.

    Attributes:
        reason: A human-readable explanation of why the decision was made.
    """

    reason: str = ""

    def __new__(cls, *args, **kwargs):
        if cls is HealthDecision:
            raise TypeError(
                "HealthDecision is abstract; use Noop, Reattempt, Evict or Diagnose."
            )
        return super().__new__(cls)


@PublicAPI(stability="alpha")
@dataclass
class Noop(HealthDecision):
    """Take no action."""


@PublicAPI(stability="alpha")
@dataclass
class Reattempt(HealthDecision):
    """End the current attempt of the training run and start a new one."""


@PublicAPI(stability="alpha")
@dataclass(kw_only=True)
class Evict(HealthDecision):
    """Stop using some nodes for the rest of the run.

    Attributes:
        target_nodes: IDs of the nodes to evict.
    """

    target_nodes: List["NodeIdStr"]


@PublicAPI(stability="alpha")
@dataclass(kw_only=True)
class Diagnose(HealthDecision):
    """Run some health checks once: poll their probes once, and have their
    evaluators decide on the readings.

    Attributes:
        checks: The checks to run.
        target_ranks: World ranks to poll ``WorkerProbe``\\ s on. Empty means
            all ranks.
        target_nodes: IDs of the nodes to poll ``NodeProbe``\\ s on. Empty means
            all nodes with workers.
    """

    checks: List["HealthCheck"]
    target_ranks: List[int] = field(default_factory=list)
    target_nodes: List["NodeIdStr"] = field(default_factory=list)
