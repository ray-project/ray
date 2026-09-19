"""The user-facing half of the health loop: what to measure, what to conclude."""
import abc
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from ray.train.v2._internal.execution.health.decision import HealthDecision
from ray.train.v2._internal.execution.health.probe import Probe
from ray.train.v2._internal.execution.health.state import HealthState
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Evaluator(abc.ABC):
    """Judges a ``HealthState`` and returns zero or more decisions.

    Runs on the controller, so the time series lives outside the failure domain
    of the thing being measured. An evaluator is long-lived for a run, so it may
    keep its own bounded history (e.g. a deque of the last N steps per rank) --
    that is what gives the temporal axis its teeth.

    An evaluator must not raise. The manager catches and disables a misbehaving
    evaluator rather than failing the run, but an evaluator that returns ``[]``
    when it is unsure is the contract.
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
    """(probes, evaluators) -- what to measure, and what to conclude from it.

    Two creators rather than two lists, so nothing is constructed at import
    time and the framework builds fresh probes and evaluators per run.

    Attributes:
        probe_creator: Builds the probes this policy needs.
        evaluator_creator: Builds the evaluators that judge them.
        preflight: Run this policy's probes as a gate before training starts,
            in addition to (or instead of) during the run.
        name: For logs and events. Defaults to nothing, filled by the manager.
    """

    probe_creator: Optional[ProbeCreator] = None
    evaluator_creator: Optional[EvaluatorCreator] = None
    preflight: bool = False
    name: str = ""


@DeveloperAPI
@dataclass
class HealthConfig:
    """Passed via ``RunConfig(health_config=...)``. Empty means off.

    With no policies, no ``NodeMonitor`` is started, no probes run, and the
    controller behaves exactly as it does today.
    """

    policies: List[HealthPolicy] = field(default_factory=list)
