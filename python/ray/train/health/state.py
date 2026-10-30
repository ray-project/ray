from dataclasses import dataclass, field
from typing import Any, Mapping

from ray.train.health.probe import ProbeResult
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class HealthState:
    """The probe readings an ``Evaluator`` decides on.

    Attributes:
        probe_results: ``{probe name: {world rank, node ID or key:
            ProbeResult}}``, the latest reading of each probe on each worker or
            node, or for each key of a ``ControllerProbe``. Probe names come
            from ``Probe.probe_name()``.
    """

    probe_results: Mapping[str, Mapping[Any, ProbeResult]] = field(default_factory=dict)
