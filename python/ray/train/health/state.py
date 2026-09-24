"""The aggregated health view evaluators read on each poll."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Type

from ray.train.health.probe import Probe, ProbeResult
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class WorkerHealth:
    """One worker's snapshot, carried on ``WorkerStatus.health``."""

    worker_rank: int
    node_id: str
    snapshot_at: float
    step: Optional[int] = None
    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)
    reported: Dict[str, Any] = field(default_factory=dict)


@DeveloperAPI
@dataclass
class NodeHealth:
    node_id: str
    snapshot_at: float
    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)


@DeveloperAPI
@dataclass
class HealthState:
    """One poll's worth of evidence: the latest snapshot per source.

    Attributes:
        workers: ``{world_rank: WorkerHealth}``.
        nodes: ``{node_id: NodeHealth}``.
        entities: ``{probe_name: {entity_id: ProbeResult}}`` for cluster
            probes whose entity is not a node.
        on_demand_probes: ``{probe_name: {entity_id: ProbeResult}}``.
    """

    workers: Mapping[int, WorkerHealth] = field(default_factory=dict)
    nodes: Mapping[str, NodeHealth] = field(default_factory=dict)
    entities: Mapping[str, Dict[str, ProbeResult]] = field(default_factory=dict)
    on_demand_probes: Mapping[str, Dict[str, ProbeResult]] = field(default_factory=dict)

    def results(self, probe: Type[Probe]) -> Dict[str, ProbeResult]:
        """Latest results of ``probe`` as ``{entity_id: ProbeResult}``."""
        name = probe.probe_name()
        if name in self.entities:
            return dict(self.entities[name])
        return {
            node_id: node.probe_results[name]
            for node_id, node in self.nodes.items()
            if name in node.probe_results
        }

    def worker_results(self, probe: Type[Probe]) -> Dict[int, ProbeResult]:
        name = probe.probe_name()
        return {
            rank: worker.probe_results[name]
            for rank, worker in self.workers.items()
            if name in worker.probe_results
        }

    def on_demand_probe_results(self, probe: Type[Probe]) -> Dict[str, ProbeResult]:
        return dict(self.on_demand_probes.get(probe.probe_name(), {}))

    @property
    def reported(self) -> Dict[int, Dict[str, Any]]:
        """``{world_rank: metrics}`` from ``ray.train.health.report()``."""
        return {
            rank: worker.reported
            for rank, worker in self.workers.items()
            if worker.reported
        }

    def ranks_on(self, node_id: str) -> List[int]:
        return sorted(r for r, w in self.workers.items() if w.node_id == node_id)

    def node_of(self, rank: int) -> Optional[str]:
        worker = self.workers.get(rank)
        return worker.node_id if worker else None
