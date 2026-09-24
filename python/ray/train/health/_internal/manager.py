"""The Decide stage: fuse every health stream into one decision per poll."""
import logging
import time
from dataclasses import replace
from typing import Dict, Iterable, List, Optional, Sequence, Set

from ray.train.health.decision import (
    SEVERITY,
    Action,
    Cause,
    Diagnose,
    Evict,
    HealthDecision,
    Reattempt,
)
from ray.train.health.policy import Evaluator, HealthPolicy
from ray.train.health.probe import (
    ClusterContext,
    ClusterProbe,
    NodeProbe,
    OnDemandProbe,
    Probe,
    ProbeResult,
    WorkerProbe,
)
from ray.train.health.state import HealthState, NodeHealth, WorkerHealth

logger = logging.getLogger(__name__)

# Most actionable first: HARDWARE is the only cause that authorizes eviction.
_CAUSE_PRIORITY = [
    Cause.HARDWARE,
    Cause.INFRASTRUCTURE,
    Cause.APPLICATION,
    Cause.NO_PROGRESS,
    Cause.UNKNOWN,
]

# probe_name -> entity_id -> result
Results = Dict[str, Dict[str, ProbeResult]]


def _dedup(values: Iterable) -> List:
    return list(dict.fromkeys(values))


def merge_decisions(
    decisions: Sequence[HealthDecision],
    *,
    handled_nodes: Iterable[str] = (),
) -> Optional[HealthDecision]:
    """Merge every evaluator's decisions into the one the controller acts on.

    Drops ``NOOP`` and anything naming only already-handled nodes, keeps the
    most severe action, and unions everything at that severity.
    """
    handled = set(handled_nodes)
    live: List[HealthDecision] = []
    for d in decisions:
        if d is None or d.action is Action.NOOP:
            continue
        if isinstance(d, (Evict, Diagnose)) and d.target_nodes:
            remaining = [n for n in d.target_nodes if n not in handled]
            if not remaining:
                continue
            d = replace(d, target_nodes=remaining)
        live.append(d)
    if not live:
        return None

    top = max(SEVERITY[d.action] for d in live)
    winners = [d for d in live if SEVERITY[d.action] == top]
    present = {d.cause for d in winners}
    cause = next(c for c in _CAUSE_PRIORITY if c in present)
    reason = "; ".join(_dedup(d.reason for d in winners if d.reason))

    if isinstance(winners[0], Evict):
        return Evict(
            cause=cause,
            reason=reason,
            target_nodes=_dedup(n for d in winners for n in d.target_nodes),
        )
    if isinstance(winners[0], Diagnose):
        return Diagnose(
            cause=cause,
            reason=reason,
            on_demand_probes=_dedup(p for d in winners for p in d.on_demand_probes),
            target_nodes=_dedup(n for d in winners for n in d.target_nodes),
            target_ranks=_dedup(r for d in winners for r in d.target_ranks),
        )
    return Reattempt(cause=cause, reason=reason)


class HealthManager:
    """One per run, on the controller. Holds the probes, evaluators, and latest
    evidence, and emits one ``HealthDecision`` per poll."""

    def __init__(self, policies: Sequence[HealthPolicy]):
        self._probes: List[Probe] = []
        self._evaluators: List[Evaluator] = []
        self._preflight_probes: List[OnDemandProbe] = []
        self._preflight_evaluators: List[Evaluator] = []
        for policy in policies:
            probes = policy.probe_creator() if policy.probe_creator else []
            evaluators = policy.evaluator_creator() if policy.evaluator_creator else []
            self._probes.extend(probes)
            self._evaluators.extend(evaluators)
            if policy.preflight:
                self._preflight_probes.extend(
                    p for p in probes if isinstance(p, OnDemandProbe)
                )
                self._preflight_evaluators.extend(evaluators)

        self._workers: Dict[int, WorkerHealth] = {}
        self._nodes: Dict[str, NodeHealth] = {}
        self._entities: Results = {}
        self._on_demand: Results = {}

        # Kept across restarts: an evicted node stays out for the whole run.
        self._evicted: Set[str] = set()
        self._disabled: Set[int] = set()
        self._failing_probes: Set[str] = set()

    @property
    def enabled(self) -> bool:
        return bool(self._probes or self._evaluators)

    def worker_probes(self) -> List[WorkerProbe]:
        return [p for p in self._probes if isinstance(p, WorkerProbe)]

    def node_probes(self) -> List[NodeProbe]:
        return [p for p in self._probes if isinstance(p, NodeProbe)]

    def cluster_probes(self) -> List[ClusterProbe]:
        return [p for p in self._probes if isinstance(p, ClusterProbe)]

    def preflight_probes(self) -> List[OnDemandProbe]:
        return list(self._preflight_probes)

    @property
    def evicted_nodes(self) -> List[str]:
        return sorted(self._evicted)

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------
    def ingest_worker_health(self, health: WorkerHealth) -> None:
        self._workers[health.worker_rank] = health

    def ingest_node_health(self, health: NodeHealth) -> None:
        existing = self._nodes.get(health.node_id)
        if existing is None:
            self._nodes[health.node_id] = health
            return
        self._nodes[health.node_id] = NodeHealth(
            node_id=health.node_id,
            snapshot_at=max(existing.snapshot_at, health.snapshot_at),
            probe_results={**existing.probe_results, **health.probe_results},
        )

    def ingest_probe_results(self, results: Results) -> None:
        """File what a pushed on-demand probe produced."""
        for probe_name, per_entity in results.items():
            self._on_demand.setdefault(probe_name, {}).update(per_entity)

    def poll_cluster_probe(
        self, probe: ClusterProbe, ctx: ClusterContext
    ) -> Optional[Dict[str, ProbeResult]]:
        """Poll one cluster probe. Safe to call off the controller thread."""
        name = probe.probe_name()
        try:
            results = probe.poll(ctx)
        except Exception:
            if name not in self._failing_probes:
                self._failing_probes.add(name)
                logger.warning("Cluster probe %s failed.", name, exc_info=True)
            return None
        self._failing_probes.discard(name)
        return results

    def ingest_cluster_results(
        self,
        probe: ClusterProbe,
        results: Dict[str, ProbeResult],
        snapshot_at: Optional[float] = None,
    ) -> None:
        name = probe.probe_name()
        if probe.entity != "node":
            self._entities[name] = dict(results)
            return
        now = snapshot_at if snapshot_at is not None else time.time()
        for node_id, result in results.items():
            self.ingest_node_health(NodeHealth(node_id, now, {name: result}))

    def run_cluster_probes(self, ctx: ClusterContext) -> None:
        """Poll and ingest every cluster probe synchronously."""
        for probe in self.cluster_probes():
            results = self.poll_cluster_probe(probe, ctx)
            if results is not None:
                self.ingest_cluster_results(probe, results)

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
        """Run every evaluator over the current state and merge the results."""
        return self._decide(self._evaluate(self._evaluators, self.build_state()))

    def evaluate_preflight(self, results: Results) -> Optional[Evict]:
        """Judge pre-flight results. Any failed check, or an ``Evict`` from a
        pre-flight policy's evaluator, rejects the node."""
        failed = [
            Evict(
                reason=f"pre-flight {name} failed on {node}: {r.detail or 'failed'}",
                target_nodes=[node],
            )
            for name, per_node in results.items()
            for node, r in per_node.items()
            if r.passed is False
        ]
        judged = self._evaluate(
            self._preflight_evaluators, HealthState(on_demand_probes=results)
        )
        return self._decide(failed + [d for d in judged if isinstance(d, Evict)])

    def _evaluate(
        self, evaluators: Sequence[Evaluator], state: HealthState
    ) -> List[HealthDecision]:
        decisions: List[HealthDecision] = []
        for evaluator in evaluators:
            if id(evaluator) in self._disabled:
                continue
            try:
                decisions.extend(evaluator.evaluate(state) or [])
            except Exception:
                logger.exception(
                    "Evaluator %s raised; disabling it for this run.",
                    type(evaluator).__name__,
                )
                self._disabled.add(id(evaluator))
        return decisions

    def _decide(self, decisions: List[HealthDecision]) -> Optional[HealthDecision]:
        decision = merge_decisions(decisions, handled_nodes=self._evicted)
        if isinstance(decision, Evict):
            self._evicted.update(decision.target_nodes)
        return decision

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def on_worker_group_start(self) -> None:
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
