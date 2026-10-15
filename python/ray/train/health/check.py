from dataclasses import dataclass, field
from typing import Callable, List

from ray.train.health.evaluator import Evaluator
from ray.train.health.probe import Probe
from ray.util.annotations import PublicAPI

ProbeCreator = Callable[[], List[Probe]]
EvaluatorCreator = Callable[[], List[Evaluator]]


@PublicAPI(stability="alpha")
@dataclass
class HealthCheck:
    """A set of probes that collect data, and the evaluators that decide on it.

    Attributes:
        probe_creator: Returns the check's probes. Defaults to none.
        evaluator_creator: Returns the check's evaluators. Defaults to none.
    """

    probe_creator: ProbeCreator = lambda: []
    evaluator_creator: EvaluatorCreator = lambda: []


@PublicAPI(stability="alpha")
@dataclass(kw_only=True)
class PeriodicHealthCheck(HealthCheck):
    """A health check whose probes are polled every ``poll_interval_s`` seconds.

    Attributes:
        poll_interval_s: Seconds between polls of the check's probes.

    Raises:
        ValueError: If ``poll_interval_s`` is not positive.
    """

    poll_interval_s: float

    def __post_init__(self):
        if self.poll_interval_s <= 0:
            raise ValueError(
                f"poll_interval_s must be positive, got {self.poll_interval_s}."
            )


@PublicAPI(stability="alpha")
@dataclass
class HealthConfig:
    """Health monitoring configuration for a run.

    Attributes:
        inflight_checks: Checks that run during training.
        preflight_checks: Checks that run once on each node before training
            starts.
    """

    inflight_checks: List[PeriodicHealthCheck] = field(default_factory=list)
    preflight_checks: List[HealthCheck] = field(default_factory=list)
