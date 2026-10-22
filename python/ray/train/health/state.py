from dataclasses import dataclass, field
from typing import Any, Mapping, Union

from ray.train.health.probe import ProbeResult
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class HealthState:
    """The probe readings an ``Evaluator`` decides on.

    Attributes:
        probe_results: ``{probe name: {world rank or node ID: ProbeResult}}``,
            the latest reading of each ``WorkerProbe`` on each worker, and of
            each ``NodeProbe`` on each node. Probe names come from
            ``Probe.probe_name()``.
        controller_probe_results: ``{probe name: {key: ProbeResult}}``, the
            latest readings of each ``ControllerProbe``, or a single
            ``ProbeResult`` with the ``error`` if its latest poll failed.
    """

    probe_results: Mapping[str, Mapping[Any, ProbeResult]] = field(default_factory=dict)
    controller_probe_results: Mapping[
        str, Union[ProbeResult, Mapping[Any, ProbeResult]]
    ] = field(default_factory=dict)
