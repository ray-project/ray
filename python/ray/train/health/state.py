from dataclasses import dataclass, field
from typing import Any, Mapping

from ray.train.health.probe import ProbeResult
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class HealthState:
    """The probe readings an ``Evaluator`` decides on.

    Attributes:
        probe_results: ``{probe name: {world rank or node ID: ProbeResult}}``,
            the latest reading of each probe on each worker or node. Probe names
            come from ``Probe.probe_name()``.
        controller_probe_results: ``{probe name: {key: ProbeResult}}``, the
            latest readings of each ``ControllerProbe`` whose latest poll
            returned.
        controller_probe_errors: ``{probe name: exception}``, the error of each
            ``ControllerProbe`` whose latest poll failed.
    """

    probe_results: Mapping[str, Mapping[Any, ProbeResult]] = field(default_factory=dict)
    controller_probe_results: Mapping[str, Mapping[Any, ProbeResult]] = field(
        default_factory=dict
    )
    controller_probe_errors: Mapping[str, Exception] = field(default_factory=dict)
