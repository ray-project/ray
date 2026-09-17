"""The Decide stage: fuse every health stream into one decision per poll."""
import logging
import time
from typing import Dict, List, Optional, Sequence, Set

from ray.train.v2._internal.execution.health.decision import (
    Evict,
    HealthDecision,
    merge_decisions,
)
from ray.train.v2._internal.execution.health.policy import Evaluator, HealthPolicy
from ray.train.v2._internal.execution.health.probe import (
    ClusterContext,
    ClusterProbe,
    NodeProbe,
    Probe,
    ProbeDegraded,
    ProbeResult,
    WorkerProbe,
)
from ray.train.v2._internal.execution.health.state import (
    HealthState,
    NodeHealth,
    WorkerHealth,
)

logger = logging.getLogger(__name__)


class HealthManager:
    """One per run, living inside the ``TrainController``.

    Every health signal flows in here; its job is to fuse those streams into
    one view the decision logic can reason about, and to emit a single
    instruction for the controller to carry out.

    It holds the *latest* snapshot from each source, not a history. History is
    the evaluator's business (see :class:`Evaluator`).
    """

    def __init__(self, policies: Sequence[HealthPolicy]):
        self._policies: List[HealthPolicy] = list(policies)

        # Built once per run, not at import time.
        self._probes: List[Probe] = []
        self._evaluators: List[Evaluator] = []
        for i, policy in enumerate(self._policies):
            if not policy.name:
                policy.name = f"policy_{i}"
            if policy.probe_creator:
                self._probes.extend(policy.probe_creator())
            if policy.evaluator_creator:
                self._evaluators.extend(policy.evaluator_creator())

        # Latest snapshot per source.
        self._workers: Dict[int, WorkerHealth] = {}
        self._nodes: Dict[str, NodeHealth] = {}
        self._on_demand: Dict[str, Dict[str, ProbeResult]] = {}

        # Nodes with an action already in flight, so a persistent signal does
        # not retrigger every poll. Cleared when the worker group restarts.
        self._handled_nodes: Set[str] = set()

        # probe_name -> entity_id -> result, for non-node cluster probes.
        self._entities: Dict[str, Dict[str, ProbeResult]] = {}

        # An evaluator that raises is disabled rather than failing the run.
        self._disabled: Set[int] = set()
        # Probes that declared themselves permanently degraded (missing binary,
        # unsupported vendor version). Dropped rather than retried every poll.
        self._degraded_probes: Set[str] = set()

    # ------------------------------------------------------------------
    # Probe registry -- who runs where.
    # ------------------------------------------------------------------
    def worker_probes(self) -> List[WorkerProbe]:
        return [p for p in self._probes if isinstance(p, WorkerProbe)]

    def node_probes(self) -> List[NodeProbe]:
        return [p for p in self._probes if isinstance(p, NodeProbe)]

    def cluster_probes(self) -> List[ClusterProbe]:
        return [p for p in self._probes if isinstance(p, ClusterProbe)]

    @property
    def enabled(self) -> bool:
        return bool(self._probes or self._evaluators)

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------
    def ingest_worker_health(self, health: WorkerHealth) -> None:
        self._workers[health.worker_rank] = health

    def ingest_node_health(self, health: NodeHealth) -> None:
        """Merge a node snapshot, keeping results from other probes on that node.

        Node-level evidence arrives from two places -- the node's own
        ``NodeMonitor`` and any cluster probe -- on independent schedules. A
        blind overwrite would make the two sources flap over each other.
        """
        existing = self._nodes.get(health.node_id)
        if existing is None:
            self._nodes[health.node_id] = health
            return
        merged = dict(existing.probe_results)
        merged.update(health.probe_results)
        self._nodes[health.node_id] = NodeHealth(
            node_id=health.node_id,
            snapshot_at=max(existing.snapshot_at, health.snapshot_at),
            probe_results=merged,
        )

    def ingest_probe_result(self, probe_name: str, entity_id: str, result: ProbeResult):
        """File the result of an on-demand probe the controller pushed."""
        self._on_demand.setdefault(probe_name, {})[entity_id] = result

    def ingest_diagnostic_report(self, report) -> None:
        """File everything one ``Diagnose`` produced.

        This is the step that closes the loop: the evidence an action produced
        is in the next ``HealthState``, so the next Decide pass reads it instead
        of a human reading files after the run has already failed.
        """
        for probe_name, results in report.results.items():
            for entity_id, result in results.items():
                self.ingest_probe_result(probe_name, entity_id, result)

    def run_cluster_probes(self, node_ids: Sequence[str]) -> None:
        """Poll every cluster probe and file its results under its entity.

        Runs on the controller's poll path, so a slow or broken source must
        never stall the loop: a transient failure is logged and the previous
        snapshot is left in place, and a probe that declares itself permanently
        degraded is dropped for the rest of the run.
        """
        now = time.monotonic()
        ctx = ClusterContext(node_ids=list(node_ids))
        for probe in self.cluster_probes():
            name = probe.probe_name()
            if name in self._degraded_probes:
                continue
            try:
                results = probe.poll(ctx)
            except ProbeDegraded as e:
                self._retire_probe(probe, e)
                continue
            except Exception:
                logger.warning(
                    "Cluster probe %s failed; keeping the previous snapshot.",
                    name,
                    exc_info=True,
                )
                continue

            if probe.entity != "node":
                # Opaque entities: only this probe's own evaluator knows what
                # the keys mean, so the framework just holds them.
                self._entities[name] = dict(results)
                continue

            for node_id, result in results.items():
                self.ingest_node_health(
                    NodeHealth(
                        node_id=node_id,
                        snapshot_at=now,
                        probe_results={name: result},
                    )
                )

    def _retire_probe(self, probe: Probe, reason: Exception) -> None:
        logger.warning(
            "Probe %s is degraded (%s). Disabling it for the rest of this run.",
            probe.probe_name(),
            reason,
        )
        self._degraded_probes.add(probe.probe_name())

    # ------------------------------------------------------------------
    # Decide
    # ------------------------------------------------------------------
    def build_state(self) -> HealthState:
        return HealthState(
            workers=dict(self._workers),
            nodes=dict(self._nodes),
            entities={k: dict(v) for k, v in self._entities.items()},
            on_demand_probes={k: dict(v) for k, v in self._on_demand.items()},
        )

    def poll_decision(self) -> Optional[HealthDecision]:
        """Assemble a ``HealthState``, run every evaluator, merge into ONE decision."""
        if not self._evaluators:
            return None

        state = self.build_state()
        candidates: List[HealthDecision] = []
        for i, evaluator in enumerate(self._evaluators):
            if i in self._disabled:
                continue
            try:
                candidates.extend(evaluator.evaluate(state) or [])
            except Exception:
                # A detector bug must not fail the training run.
                logger.exception(
                    "Evaluator %s raised; disabling it for the rest of this run.",
                    type(evaluator).__name__,
                )
                self._disabled.add(i)

        decision = merge_decisions(candidates, handled_nodes=self._handled_nodes)
        if decision is None:
            return None

        if isinstance(decision, Evict):
            # Mark before returning: the controller acts asynchronously, and
            # the same signal will still be present on the next poll.
            self._handled_nodes.update(decision.target_nodes)
        return decision

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def on_worker_group_start(self) -> None:
        """Reset per-attempt state. Evidence from the previous attempt is stale."""
        self._workers.clear()
        self._nodes.clear()
        self._entities.clear()
        self._on_demand.clear()
        for evaluator in self._evaluators:
            try:
                evaluator.on_worker_group_start()
            except Exception:
                logger.warning(
                    "Evaluator %s failed to reset.",
                    type(evaluator).__name__,
                    exc_info=True,
                )

    @property
    def evicted_nodes(self) -> List[str]:
        """Nodes this run has decided not to schedule onto again."""
        return sorted(self._handled_nodes)
