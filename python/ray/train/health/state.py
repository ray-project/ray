from dataclasses import dataclass, field
from typing import Dict, Mapping, Type, Union

from ray.train.health.probe import NodeIdStr, Probe, ProbeResult
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class HealthState:
    """The latest result of each probe, from each worker, node and
    ``ControllerProbe`` key.

    Attributes:
        probe_results: ``{probe name: {world rank, node ID or key: ProbeResult}}``.
    """

    probe_results: Mapping[str, Mapping[Union[int, NodeIdStr], ProbeResult]] = field(
        default_factory=dict
    )

    def results(self, probe: Type[Probe]) -> Dict[Union[int, NodeIdStr], ProbeResult]:
        """Get the latest results of a probe.

        Args:
            probe: The probe class.

        Returns:
            ``{world rank: ProbeResult}`` for a ``WorkerProbe``,
            ``{node ID: ProbeResult}`` for a ``NodeProbe``, or
            ``{key: ProbeResult}`` for a ``ControllerProbe``.
        """
        return dict(self.probe_results.get(probe.probe_name(), {}))
