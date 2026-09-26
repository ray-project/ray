"""What to measure and what to conclude from it."""
import abc
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from ray.train.health.decision import HealthDecision
from ray.train.health.probe import Probe
from ray.train.health.state import HealthState
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Evaluator(abc.ABC):
    """Judges a ``HealthState`` and returns zero or more decisions.

    Runs on the controller and lives for the whole run, so it may keep its own
    history across polls. An evaluator that raises is disabled for the run.
    """

    @abc.abstractmethod
    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        raise NotImplementedError

    def on_worker_group_start(self) -> None:
        """Clear per-attempt history. Called on every (re)start."""


ProbeCreator = Callable[[], List[Probe]]
EvaluatorCreator = Callable[[], List[Evaluator]]


@DeveloperAPI
@dataclass
class HealthPolicy:
    """A bundle of probes and the evaluators that judge them.

    Attributes:
        probe_creator: Builds the policy's probes, once per run.
        evaluator_creator: Builds the policy's evaluators, once per run.
        preflight: Also run the policy's ``OnDemandProbe``s on every candidate
            node before its first worker group is scheduled there.
    """

    probe_creator: Optional[ProbeCreator] = None
    evaluator_creator: Optional[EvaluatorCreator] = None
    preflight: bool = False


@DeveloperAPI
@dataclass
class HealthConfig:
    """Passed via ``RunConfig(health_config=...)``. Empty means off."""

    policies: List[HealthPolicy] = field(default_factory=list)
