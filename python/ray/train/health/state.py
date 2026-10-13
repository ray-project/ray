from dataclasses import dataclass, field
from typing import Mapping, Union

from ray.train.health.probe import NodeIdStr, ProbeResult
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class HealthState:
    """The probe readings an ``Evaluator`` decides on.

    Attributes:
        probe_results: ``{probe name: {world rank or node ID: ProbeResult}}``,
            the latest reading of each probe on each worker or node. Probe names
            come from ``Probe.probe_name()``.
    """

    probe_results: Mapping[str, Mapping[Union[int, NodeIdStr], ProbeResult]] = field(
        default_factory=dict
    )
