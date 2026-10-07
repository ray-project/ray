import abc
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from ray.train.health.decision import HealthDecision
from ray.train.health.probe import Probe
from ray.train.health.state import HealthState
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class Evaluator(abc.ABC):
    """Decides what the run should do from probe readings. Implement
    ``evaluate()``."""

    @abc.abstractmethod
    def evaluate(self, state: HealthState) -> HealthDecision:
        """Decide what the run should do.

        Args:
            state: The latest probe readings.

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
class HealthCheck:
    """A set of probes that collect data, and the evaluators that decide on it.

    Attributes:
        probe_creator: Returns the check's probes.
        evaluator_creator: Returns the check's evaluators.

    Raises:
        ValueError: If the check has neither probes nor evaluators.
    """

    probe_creator: Optional[ProbeCreator] = None
    evaluator_creator: Optional[EvaluatorCreator] = None

    def __post_init__(self):
        if self.probe_creator is None and self.evaluator_creator is None:
            raise ValueError(
                "A HealthCheck needs a probe_creator, an evaluator_creator, or both."
            )


@PublicAPI(stability="alpha")
@dataclass
class HealthConfig:
    """Health monitoring configuration for a run.

    Attributes:
        mid_training_checks: Checks that run periodically during training.
        preflight_checks: Checks that run once on each node before training
            starts.
    """

    mid_training_checks: List[HealthCheck] = field(default_factory=list)
    preflight_checks: List[HealthCheck] = field(default_factory=list)
