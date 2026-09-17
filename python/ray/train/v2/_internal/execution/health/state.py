"""The aggregated health view evaluators read each poll."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Type

from ray.train.v2._internal.execution.health.probe import Probe, ProbeResult
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class WorkerHealth:
    """One worker's health snapshot, carried on ``WorkerStatus.health``."""

    worker_rank: int
    node_id: str
    snapshot_at: float
    step: Optional[int] = None
    # probe_name -> ProbeResult
    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)
    # whatever ray.train.health.report() accumulated since the last poll
    reported: Dict[str, Any] = field(default_factory=dict)


@DeveloperAPI
@dataclass
class NodeHealth:
    """One node's health snapshot, from its ``NodeMonitor`` or a cluster probe."""

    node_id: str
    snapshot_at: float
    # probe_name -> ProbeResult
    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)


@DeveloperAPI
@dataclass
class HealthState:
    """The input contract for evaluators: one poll's worth of evidence.

    The manager holds the latest snapshot from each source, not a history.
    History lives on the evaluator, which is long-lived across a run.
    """

    workers: Mapping[int, WorkerHealth] = field(default_factory=dict)
    nodes: Mapping[str, NodeHealth] = field(default_factory=dict)
    # probe_name -> entity_id -> result, for cluster probes whose entity is not
    # a node. Node-keyed results live in `nodes` instead, so they merge with
    # every NodeProbe sample for the same host.
    entities: Mapping[str, Dict[str, ProbeResult]] = field(default_factory=dict)
    on_demand_probes: Mapping[str, Dict[str, ProbeResult]] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Typed reads. Evaluators look results up by probe class, not by string,
    # so a renamed probe is a type error rather than a silently empty dict.
    # ------------------------------------------------------------------
    def results(self, probe: Type[Probe]) -> Dict[str, ProbeResult]:
        """Latest slice for ``probe``, as ``{entity_id: ProbeResult}``.

        The entity is whatever that probe measures: a node for a ``NodeProbe``
        or a node-keyed ``ClusterProbe``, a communicator for NCCL RAS. One read
        shape regardless, so an evaluator never has to know which kind of probe
        produced the signal it is judging.
        """
        name = probe.probe_name()
        if name in self.entities:
            return dict(self.entities[name])
        return {
            node_id: node.probe_results[name]
            for node_id, node in self.nodes.items()
            if name in node.probe_results
        }

    def worker_results(self, probe: Type[Probe]) -> Dict[int, ProbeResult]:
        """Latest per-rank slice for ``probe``, as ``{world_rank: ProbeResult}``."""
        name = probe.probe_name()
        return {
            rank: worker.probe_results[name]
            for rank, worker in self.workers.items()
            if name in worker.probe_results
        }

    def on_demand_probe_results(self, probe: Type[Probe]) -> Dict[str, ProbeResult]:
        """Results of the last push of ``probe``, as ``{node_id: ProbeResult}``."""
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
        """The world ranks this run has on ``node_id``."""
        return sorted(rank for rank, w in self.workers.items() if w.node_id == node_id)

    def node_of(self, rank: int) -> Optional[str]:
        worker = self.workers.get(rank)
        return worker.node_id if worker else None
