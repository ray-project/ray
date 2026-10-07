from dataclasses import dataclass, field
from typing import Mapping, Union

from ray.train.health.probe import ProbeResult
from ray.util.annotations import PublicAPI

# A world rank, a node ID, or a key a ControllerProbe chose.
ResultKey = Union[int, str]


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

    probe_results: Mapping[str, Mapping[ResultKey, ProbeResult]] = field(
        default_factory=dict
    )
