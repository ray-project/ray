"""The Act side of DIAGNOSE: carry out a push, feed the evidence back.

The controller decides it needs an active check, pushes on-demand probes at the
entities a ``Diagnose`` names, and files what comes back into the next
``HealthState``. That last step is what makes the loop a loop: an action
produces fresh evidence the next Decide step reads, rather than a folder of
files a human opens afterwards.

Nothing here touches Ray directly. The two dispatch callables and the uploader
are injected, so the whole path is exercisable without a cluster and the
controller supplies the real ones.
"""
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from ray.train.v2._internal.execution.health.decision import Diagnose
from ray.train.v2._internal.execution.health.probe import (
    NODE_SCOPE,
    WORKER_SCOPE,
    NodeInfo,
    OnDemandProbe,
    OnDemandProbeContext,
    ProbeResult,
)

logger = logging.getLogger(__name__)

#: ``(rank, node_id)`` for every worker in the group.
Topology = Sequence[Tuple[int, str]]

#: ``dispatch(entity, probe, ctx) -> ProbeResult``
Dispatch = Callable[[str, OnDemandProbe, OnDemandProbeContext], ProbeResult]


@dataclass
class DiagnosticReport:
    """What one ``Diagnose`` produced, ready to ingest."""

    # probe_name -> entity_id -> result
    results: Dict[str, Dict[str, ProbeResult]] = field(default_factory=dict)
    artifacts: List[str] = field(default_factory=list)

    @property
    def probe_names(self) -> List[str]:
        return sorted(self.results)


class DiagnosticRunner:
    """Runs the on-demand probes a ``Diagnose`` decision asks for.

    Args:
        run_on_worker: Runs a probe inside a training worker. Production wires
            this to ``worker.execute_async``.
        run_on_node: Runs a probe in a node's ``NodeMonitor``, which is the
            whole reason node-scoped diagnostics exist: a wedged driver or a
            hung training process cannot be asked to describe itself, and the
            monitor is outside that process.
        upload: ``upload(name, {filename: contents}) -> path``, passed through
            to each probe so bulk output lands in run storage rather than in
            the health loop.
        pause_workers / resume_workers: Called around any probe that declares
            ``stop_workers``, because it needs the accelerator free.
    """

    def __init__(
        self,
        run_on_worker: Dispatch,
        run_on_node: Dispatch,
        upload: Optional[Callable[[str, Dict[str, str]], str]] = None,
        pause_workers: Optional[Callable[[], None]] = None,
        resume_workers: Optional[Callable[[], None]] = None,
    ):
        self._run_on_worker = run_on_worker
        self._run_on_node = run_on_node
        self._upload = upload
        self._pause_workers = pause_workers
        self._resume_workers = resume_workers

    def run(self, decision: Diagnose, topology: Topology) -> DiagnosticReport:
        """Run every probe the decision names, against the entities it names."""
        report = DiagnosticReport()
        if not decision.on_demand_probes:
            return report

        node_infos = self._node_infos(topology)
        paused = False
        try:
            for probe in decision.on_demand_probes:
                if probe.stop_workers and not paused:
                    # Pause once for the whole batch, not per probe: stopping
                    # and restarting workers repeatedly costs more than the
                    # checks do.
                    paused = self._pause()
                results = self._run_probe(probe, decision, topology, node_infos)
                if results:
                    report.results[probe.probe_name()] = results
                    for result in results.values():
                        report.artifacts.extend(result.artifacts)
        finally:
            if paused:
                self._resume()
        return report

    # ------------------------------------------------------------------
    def _run_probe(
        self,
        probe: OnDemandProbe,
        decision: Diagnose,
        topology: Topology,
        node_infos: List[NodeInfo],
    ) -> Dict[str, ProbeResult]:
        results: Dict[str, ProbeResult] = {}
        for entity_id, node_id, rank in self._targets(probe, decision, topology):
            ctx = OnDemandProbeContext(
                entity_id=entity_id,
                node_id=node_id,
                rank=rank,
                nodes=node_infos,
                timeout_s=probe.timeout_s,
                upload=self._upload,
            )
            dispatch = (
                self._run_on_node if probe.scope == NODE_SCOPE else self._run_on_worker
            )
            try:
                results[entity_id] = dispatch(entity_id, probe, ctx)
            except Exception as e:  # noqa: BLE001
                # A diagnostic that fails is itself a finding -- `nvidia-smi`
                # blocking in a wedged driver is the answer, not an error to
                # swallow -- so it is recorded rather than dropped.
                logger.warning(
                    "Diagnostic %s failed on %s: %s", probe.probe_name(), entity_id, e
                )
                results[entity_id] = ProbeResult(
                    passed=False, detail=f"diagnostic did not complete: {e}"
                )
        return results

    @staticmethod
    def _targets(
        probe: OnDemandProbe, decision: Diagnose, topology: Topology
    ) -> List[Tuple[str, str, Optional[int]]]:
        """Resolve ``(entity_id, node_id, rank)`` for one probe.

        An empty target list on the decision means "everywhere": a hang that
        could not be localized still wants a full sweep.
        """
        if probe.scope == WORKER_SCOPE:
            ranks = decision.target_ranks or [rank for rank, _ in topology]
            node_of = dict(topology)
            return [(str(r), node_of.get(r, ""), r) for r in ranks if r in node_of]

        nodes = list(decision.target_nodes)
        if not nodes and decision.target_ranks:
            # Ranks localize to their hosts, so a rank-level finding can still
            # drive a node-level check.
            node_of = dict(topology)
            nodes = [node_of[r] for r in decision.target_ranks if r in node_of]
        if not nodes:
            nodes = [node for _, node in topology]
        seen, unique = set(), []
        for node in nodes:
            if node and node not in seen:
                seen.add(node)
                unique.append(node)
        return [(node, node, None) for node in unique]

    @staticmethod
    def _node_infos(topology: Topology) -> List[NodeInfo]:
        ranks_by_node: Dict[str, List[int]] = {}
        for rank, node_id in topology:
            ranks_by_node.setdefault(node_id, []).append(rank)
        return [
            NodeInfo(node_id=node, ranks=sorted(ranks))
            for node, ranks in sorted(ranks_by_node.items())
        ]

    def _pause(self) -> bool:
        if self._pause_workers is None:
            logger.warning(
                "A diagnostic asked for the accelerator but no pause hook is "
                "wired; running it against live workers."
            )
            return False
        self._pause_workers()
        return True

    def _resume(self) -> None:
        if self._resume_workers is not None:
            try:
                self._resume_workers()
            except Exception:
                logger.exception("Failed to resume workers after a diagnostic.")
