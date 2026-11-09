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
        stop_workers: Whether the probes need the workers not to be training,
            such as an all-reduce test. Such a check runs at the start of an
            attempt, and training waits for it to decide: as a pre-flight
            check, or after its ``Diagnose`` restarts the worker group.
    """

    probe_creator: ProbeCreator = lambda: []
    evaluator_creator: EvaluatorCreator = lambda: []
    stop_workers: bool = False


@PublicAPI(stability="alpha")
@dataclass(kw_only=True)
class PeriodicHealthCheck(HealthCheck):
    """A health check whose probes are polled every ``poll_interval_s`` seconds.

    Attributes:
        poll_interval_s: Seconds between polls of the check's probes.

    Raises:
        ValueError: If ``poll_interval_s`` is not positive, or ``stop_workers``
            is set.
    """

    poll_interval_s: float

    def __post_init__(self):
        if self.poll_interval_s <= 0:
            raise ValueError(
                f"poll_interval_s must be positive, got {self.poll_interval_s}."
            )
        if self.stop_workers:
            raise ValueError("A PeriodicHealthCheck runs during training.")


@PublicAPI(stability="alpha")
@dataclass
class HealthConfig:
    """Health monitoring configuration for a run.

    Attributes:
        inflight_checks: Checks that run during training.
        preflight_checks: Checks that run once on each attempt's nodes as its
            workers start. See ``HealthCheck.stop_workers``.
    """

    inflight_checks: List[PeriodicHealthCheck] = field(default_factory=list)
    preflight_checks: List[HealthCheck] = field(default_factory=list)
