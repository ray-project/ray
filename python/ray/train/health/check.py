from dataclasses import dataclass, field
from typing import Callable, List, Union

from ray.train.health.evaluator import Evaluator
from ray.train.health.probe import OnDemandProbe, PeriodicProbe
from ray.util.annotations import PublicAPI

ProbeCreator = Callable[[], List[Union[PeriodicProbe, OnDemandProbe]]]
EvaluatorCreator = Callable[[], List[Evaluator]]


@PublicAPI(stability="alpha")
@dataclass
class HealthCheck:
    """A set of probes that collect data, and the evaluators that decide on it.

    Attributes:
        probe_creator: Returns the check's probes, each wrapped in a
            ``PeriodicProbe`` or an ``OnDemandProbe``. Defaults to none.
        evaluator_creator: Returns the check's evaluators. Defaults to none.
    """

    probe_creator: ProbeCreator = lambda: []
    evaluator_creator: EvaluatorCreator = lambda: []


@PublicAPI(stability="alpha")
@dataclass
class HealthConfig:
    """Health monitoring configuration for a run.

    Attributes:
        inflight_checks: Checks that run during training. Their probes must be
            wrapped in ``PeriodicProbe``.
        preflight_checks: Checks that run once on each node before training
            starts. Their probes must be wrapped in ``OnDemandProbe``.
    """

    inflight_checks: List[HealthCheck] = field(default_factory=list)
    preflight_checks: List[HealthCheck] = field(default_factory=list)
