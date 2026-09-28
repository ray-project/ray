import abc
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from ray.train.health.decision import HealthDecision
from ray.train.health.probe import Probe
from ray.train.health.state import HealthState
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class Evaluator(abc.ABC):
    """Decides what the run should do, based on the latest probe results.

    One instance is used for the whole run. If ``evaluate()`` raises, Ray Train
    logs the exception and does not call this evaluator again during the run.
    """

    @abc.abstractmethod
    def evaluate(self, state: HealthState) -> HealthDecision:
        """Decide what the run should do.

        Args:
            state: The latest probe results.

        Returns:
            A ``HealthDecision``. ``Noop()`` if no action is needed.
        """
        raise NotImplementedError

    def on_worker_group_start(self) -> None:
        """Called when a new set of workers starts, including after a restart."""


ProbeCreator = Callable[[], List[Probe]]
EvaluatorCreator = Callable[[], List[Evaluator]]


@PublicAPI(stability="alpha")
@dataclass
class HealthPolicy:
    """A set of probes and the evaluators that decide on their results.

    The policy's probes are polled periodically. The creators are called once
    per run.

    Attributes:
        probe_creator: Creates the policy's probes.
        evaluator_creator: Creates the policy's evaluators.
        preflight: Whether the policy runs before training starts, instead of
            while training runs.

    Raises:
        ValueError: If the policy has neither probes nor evaluators.
    """

    probe_creator: Optional[ProbeCreator] = None
    evaluator_creator: Optional[EvaluatorCreator] = None
    preflight: bool = False

    def __post_init__(self):
        if self.probe_creator is None and self.evaluator_creator is None:
            raise ValueError(
                "A HealthPolicy needs a probe_creator, an evaluator_creator, "
                "or both."
            )


@PublicAPI(stability="alpha")
@dataclass
class HealthConfig:
    """Health monitoring configuration for a run.

    Attributes:
        policies: The health policies. Empty means no health monitoring.
    """

    policies: List[HealthPolicy] = field(default_factory=list)
