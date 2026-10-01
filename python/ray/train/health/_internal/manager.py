"""The Decide stage: turn the collected probe results into one decision."""
import logging
import time
from dataclasses import replace
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Union

from ray.train.health.decision import (
    Diagnose,
    Evict,
    HealthDecision,
    Noop,
    Reattempt,
)
from ray.train.health.policy import Evaluator, HealthPolicy
from ray.train.health.probe import (
    ControllerProbe,
    NodeIdStr,
    NodeProbe,
    Probe,
    ProbeResult,
    WorkerProbe,
)
from ray.train.health.state import HealthState

logger = logging.getLogger(__name__)

# A world rank, a node ID, or a key a ControllerProbe chose.
ResultKey = Union[int, NodeIdStr]

# Least to most severe. When evaluators disagree, the most severe decision wins.
_SEVERITY = (Noop, Diagnose, Reattempt, Evict)


def _severity(decision: HealthDecision) -> int:
    for level, kind in enumerate(_SEVERITY):
        if isinstance(decision, kind):
            return level
    raise TypeError(f"{type(decision).__name__} is not a known HealthDecision.")


def _dedup(values: Iterable) -> List:
    return list(dict.fromkeys(values))


def _union_targets(target_lists: Iterable[List]) -> List:
    """Union target lists in which an empty list means "all"."""
    target_lists = list(target_lists)
    if any(not targets for targets in target_lists):
        return []
    return _dedup(t for targets in target_lists for t in targets)


def merge_decisions(
    decisions: Sequence[HealthDecision],
    *,
    evicted_nodes: Iterable[NodeIdStr] = (),
) -> Optional[HealthDecision]:
    """Merge the evaluators' decisions into the one the controller acts on.

    ``Noop``\\ s are dropped, as are nodes in ``evicted_nodes`` and any decision
    left with no nodes to act on. The most severe remaining kind wins, and the
    decisions of that kind are combined into one.

    Args:
        decisions: One decision per evaluator.
        evicted_nodes: Nodes already evicted from the run.

    Returns:
        The merged decision, or ``None`` if nothing needs doing.
    """
    evicted = set(evicted_nodes)
    live: List[HealthDecision] = []
    for decision in decisions:
        if isinstance(decision, Noop):
            continue
        if isinstance(decision, (Evict, Diagnose)) and decision.target_nodes:
            remaining = [n for n in decision.target_nodes if n not in evicted]
            if not remaining:
                continue
            decision = replace(decision, target_nodes=remaining)
        if isinstance(decision, Evict) and not decision.target_nodes:
            continue
        live.append(decision)
    if not live:
        return None

    top = max(_severity(d) for d in live)
    winners = [d for d in live if _severity(d) == top]
    reason = "; ".join(_dedup(d.reason for d in winners if d.reason))

    if isinstance(winners[0], Evict):
        return Evict(
            reason=reason,
            target_nodes=_dedup(n for d in winners for n in d.target_nodes),
        )
    if isinstance(winners[0], Diagnose):
        creators = [d.probe_creator for d in winners]
        return Diagnose(
            reason=reason,
            probe_creator=lambda: [p for create in creators for p in create()],
            target_ranks=_union_targets(d.target_ranks for d in winners),
            target_nodes=_union_targets(d.target_nodes for d in winners),
        )
    return Reattempt(reason=reason)


class HealthManager:
    """Holds one run's probes, evaluators and collected results, on the
    controller, and turns the results into one decision per evaluation.

    Args:
        policies: The run's health policies. Their creators are called here,
            once.
    """

    def __init__(self, policies: Sequence[HealthPolicy]):
        self._probes: List[Probe] = []
        self._evaluators: List[Evaluator] = []
        self._preflight_probes: List[Probe] = []
        self._preflight_evaluators: List[Evaluator] = []
        for policy in policies:
            probes = policy.probe_creator() if policy.probe_creator else []
            evaluators = policy.evaluator_creator() if policy.evaluator_creator else []
            if policy.preflight:
                self._preflight_probes.extend(probes)
                self._preflight_evaluators.extend(evaluators)
            else:
                self._probes.extend(probes)
                self._evaluators.extend(evaluators)

        # {probe name: {key: latest result}}
        self._results: Dict[str, Dict[ResultKey, ProbeResult]] = {}
        # Kept across restarts: an evicted node stays out for the whole run.
        self._evicted: Set[NodeIdStr] = set()
        self._disabled: Set[int] = set()

    @property
    def enabled(self) -> bool:
        return bool(
            self._probes
            or self._evaluators
            or self._preflight_probes
            or self._preflight_evaluators
        )

    def worker_probes(self) -> List[WorkerProbe]:
        return [p for p in self._probes if isinstance(p, WorkerProbe)]

    def node_probes(self) -> List[NodeProbe]:
        return [p for p in self._probes if isinstance(p, NodeProbe)]

    def controller_probes(self) -> List[ControllerProbe]:
        return [p for p in self._probes if isinstance(p, ControllerProbe)]

    def preflight_probes(self) -> List[Probe]:
        return list(self._preflight_probes)

    @property
    def evicted_nodes(self) -> List[NodeIdStr]:
        return sorted(self._evicted)

    def ingest(self, probe_name: str, results: Mapping[ResultKey, ProbeResult]) -> None:
        """Store the latest results of one probe.

        Args:
            probe_name: The probe's ``probe_name()``.
            results: ``{world rank, node ID or key: ProbeResult}``. A result
                without a ``timestamp_s`` is stamped with the current time.
        """
        stored = self._results.setdefault(probe_name, {})
        now = time.time()
        for key, result in results.items():
            if result.timestamp_s is None:
                result = replace(result, timestamp_s=now)
            stored[key] = result

    def build_state(self) -> HealthState:
        return HealthState(
            probe_results={name: dict(r) for name, r in self._results.items()}
        )

    def poll_decision(self) -> Optional[HealthDecision]:
        """Run every evaluator on the current results and merge their decisions.

        Returns:
            The decision to act on, or ``None`` if nothing needs doing.
        """
        return self._decide(self._evaluate(self._evaluators, self.build_state()))

    def evaluate_preflight(
        self, results: Mapping[str, Mapping[NodeIdStr, ProbeResult]]
    ) -> Optional[Evict]:
        """Run the pre-flight evaluators on pre-flight results.

        Only ``Evict`` decisions apply before training; others are ignored.

        Args:
            results: ``{probe name: {node ID: ProbeResult}}``.

        Returns:
            The nodes to keep out of the run, or ``None``.
        """
        state = HealthState(
            probe_results={name: dict(r) for name, r in results.items()}
        )
        decisions = self._evaluate(self._preflight_evaluators, state)
        return self._decide([d for d in decisions if isinstance(d, Evict)])

    def on_worker_group_start(self) -> None:
        """Drop the previous workers' results and notify the evaluators."""
        self._results.clear()
        for evaluator in self._evaluators:
            try:
                evaluator.on_worker_group_start()
            except Exception:
                logger.warning(
                    "Evaluator %s raised in on_worker_group_start().",
                    type(evaluator).__name__,
                    exc_info=True,
                )

    def _evaluate(
        self, evaluators: Sequence[Evaluator], state: HealthState
    ) -> List[HealthDecision]:
        decisions: List[HealthDecision] = []
        for evaluator in evaluators:
            if id(evaluator) in self._disabled:
                continue
            try:
                decision = evaluator.evaluate(state)
                _severity(decision)
            except Exception:
                logger.exception(
                    "Evaluator %s failed; it will not be called again during "
                    "this run.",
                    type(evaluator).__name__,
                )
                self._disabled.add(id(evaluator))
                continue
            decisions.append(decision)
        return decisions

    def _decide(self, decisions: List[HealthDecision]) -> Optional[HealthDecision]:
        decision = merge_decisions(decisions, evicted_nodes=self._evicted)
        if isinstance(decision, Evict):
            self._evicted.update(decision.target_nodes)
        return decision
