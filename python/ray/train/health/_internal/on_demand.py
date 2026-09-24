"""Runs pushed on-demand probes: for a ``Diagnose`` decision, or as pre-flight."""
import logging
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from ray.train.health._internal.manager import Results
from ray.train.health.decision import Diagnose
from ray.train.health.probe import (
    NODE_SCOPE,
    NodeInfo,
    OnDemandProbe,
    OnDemandProbeContext,
    ProbeResult,
)

logger = logging.getLogger(__name__)

# dispatch(probe, contexts) -> {entity_id: result, or the exception it raised}
Dispatch = Callable[
    [OnDemandProbe, List[OnDemandProbeContext]],
    Mapping[str, Union[ProbeResult, BaseException]],
]


class OnDemandRunner:
    """Resolves where each probe runs, dispatches it, and collects results.

    Args:
        run_on_node: Dispatches node-scoped probes.
        run_on_worker: Dispatches worker-scoped probes. ``None`` before any
            worker exists, as in pre-flight.
        upload: Passed to probes as ``OnDemandProbeContext.upload``.
        pause_workers / resume_workers: Called around probes that set
            ``stop_workers``.
    """

    def __init__(
        self,
        run_on_node: Dispatch,
        run_on_worker: Optional[Dispatch] = None,
        upload: Optional[Callable[[str, Dict[str, str]], str]] = None,
        pause_workers: Optional[Callable[[], None]] = None,
        resume_workers: Optional[Callable[[], None]] = None,
    ):
        self._run_on_node = run_on_node
        self._run_on_worker = run_on_worker
        self._upload = upload
        self._pause_workers = pause_workers
        self._resume_workers = resume_workers

    def diagnose(self, decision: Diagnose, rank_to_node: Dict[int, str]) -> Results:
        """Run the decision's probes against the ranks and nodes it names.

        No targets means every rank and node of the worker group.
        """
        nodes = _node_infos(rank_to_node)
        results: Results = {}
        paused = False
        try:
            for probe in decision.on_demand_probes:
                if probe.stop_workers and not paused:
                    paused = self._pause()
                targets = _targets(probe, decision, rank_to_node)
                results[probe.probe_name()] = self._run(probe, targets, nodes)
        finally:
            if paused and self._resume_workers is not None:
                self._resume_workers()
        return results

    def preflight(
        self, probes: Sequence[OnDemandProbe], node_ids: Sequence[str]
    ) -> Results:
        """Run node-scoped probes once on each candidate node."""
        nodes = [NodeInfo(node_id=n) for n in node_ids]
        results: Results = {}
        for probe in probes:
            if probe.scope != NODE_SCOPE:
                logger.warning(
                    "Skipping pre-flight probe %s: worker-scoped probes need a "
                    "running worker.",
                    probe.probe_name(),
                )
                continue
            targets = [(n, n, None) for n in node_ids]
            results[probe.probe_name()] = self._run(probe, targets, nodes)
        return results

    def _run(
        self,
        probe: OnDemandProbe,
        targets: List[Tuple[str, str, Optional[int]]],
        nodes: List[NodeInfo],
    ) -> Dict[str, ProbeResult]:
        if not targets:
            return {}
        contexts = [
            OnDemandProbeContext(
                entity_id=entity_id,
                node_id=node_id,
                rank=rank,
                nodes=nodes,
                timeout_s=probe.timeout_s,
                upload=self._upload,
            )
            for entity_id, node_id, rank in targets
        ]
        dispatch = (
            self._run_on_node if probe.scope == NODE_SCOPE else self._run_on_worker
        )
        try:
            raw = dict(dispatch(probe, contexts)) if dispatch else {}
        except Exception as e:
            raw = {ctx.entity_id: e for ctx in contexts}

        # A check that did not complete is a finding, not a pass.
        results = {}
        for ctx in contexts:
            outcome = raw.get(ctx.entity_id, RuntimeError("no result returned"))
            if isinstance(outcome, BaseException):
                logger.warning(
                    "On-demand probe %s failed on %s: %s",
                    probe.probe_name(),
                    ctx.entity_id,
                    outcome,
                )
                outcome = ProbeResult(
                    passed=False, detail=f"did not complete: {outcome}"
                )
            results[ctx.entity_id] = outcome
        return results

    def _pause(self) -> bool:
        if self._pause_workers is None:
            logger.warning(
                "An on-demand probe set stop_workers, but pausing workers is not "
                "supported yet; running it against live workers."
            )
            return False
        self._pause_workers()
        return True


def _targets(
    probe: OnDemandProbe, decision: Diagnose, rank_to_node: Dict[int, str]
) -> List[Tuple[str, str, Optional[int]]]:
    """``(entity_id, node_id, rank)`` for each place ``probe`` runs."""
    if probe.scope != NODE_SCOPE:
        ranks = decision.target_ranks or sorted(rank_to_node)
        return [(str(r), rank_to_node[r], r) for r in ranks if r in rank_to_node]

    nodes = decision.target_nodes or [
        rank_to_node[r] for r in decision.target_ranks if r in rank_to_node
    ]
    nodes = nodes or list(rank_to_node.values())
    return [(n, n, None) for n in dict.fromkeys(nodes) if n]


def _node_infos(rank_to_node: Dict[int, str]) -> List[NodeInfo]:
    ranks: Dict[str, List[int]] = {}
    for rank, node in sorted(rank_to_node.items()):
        ranks.setdefault(node, []).append(rank)
    return [NodeInfo(node_id=n, ranks=r) for n, r in sorted(ranks.items())]
