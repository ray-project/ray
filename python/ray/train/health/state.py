from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ray.train.health.probe import ProbeResult
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class HealthState:
    """The probe readings an ``Evaluator`` decides on.

    Attributes:
        probe_results: ``{probe name: {world rank, node ID or key:
            [ProbeResult, ...]}}``, the latest readings of each probe on each
            worker or node, or for each key of a ``ControllerProbe``: oldest
            first, at most the probe's ``window_size``. Probe names come from
            ``Probe.probe_name()``.
    """

    probe_results: Mapping[str, Mapping[Any, Sequence[ProbeResult]]] = field(
        default_factory=dict
    )
