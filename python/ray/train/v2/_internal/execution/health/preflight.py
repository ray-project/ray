"""Pre-flight: reject bad nodes before they become expensive.

The REP's first goal, and the cheapest of the three. A bad GPU found in the
first minute costs a minute; the same GPU found six hours into a 1,024-GPU run
costs six hours of everyone's time plus whatever the checkpoint interval lost.

A policy opts in by setting ``preflight=True``. Its probes then run once,
against the candidate nodes, *before* the worker group starts training, and its
evaluators judge the results. Nodes that fail are excluded and the run starts
without them.

Two properties worth stating, because they decide what a check may do:

- **Pre-flight runs before any worker exists.** So only node-scoped checks can
  run -- there is no training process to attach to, and no rank to ask. A check
  that needs the training process belongs in the mid-flight path instead.
- **Pre-flight may be destructive.** Nothing is training yet, so a check is
  free to take the whole GPU: an NCCL all-reduce across candidates, a DCGM
  diagnostic, a TFLOPS screen. This is the one place ``stop_workers`` is
  meaningless because there is nothing to stop.

What happens to the failures is the caller's decision, and it is a real one:
with a fixed world size, excluding nodes means the run cannot start unless
spares were provisioned. :class:`PreflightResult` reports rather than decides.
"""
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from ray.train.v2._internal.execution.health.decision import Action, Evict
from ray.train.v2._internal.execution.health.policy import HealthPolicy
from ray.train.v2._internal.execution.health.probe import (
    NODE_SCOPE,
    NodeInfo,
    OnDemandProbe,
    OnDemandProbeContext,
    ProbeResult,
)
from ray.train.v2._internal.execution.health.state import HealthState, NodeHealth

logger = logging.getLogger(__name__)


@dataclass
class PreflightResult:
    """What pre-flight found. Reports; does not decide.

    Attributes:
        healthy: Candidate nodes nothing objected to.
        rejected: ``{node_id: why}`` for nodes a check or evaluator failed.
        results: ``{probe_name: {node_id: ProbeResult}}``, the raw evidence.
        ran: Names of the probes that actually ran.
    """

    healthy: List[str] = field(default_factory=list)
    rejected: Dict[str, str] = field(default_factory=dict)
    results: Dict[str, Dict[str, ProbeResult]] = field(default_factory=dict)
    ran: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.rejected

    def enough_for(self, num_workers: int, per_node: int = 1) -> bool:
        """Whether the healthy set can still host the requested world size."""
        return len(self.healthy) * per_node >= num_workers

    def summary(self) -> str:
        if self.passed:
            return f"pre-flight passed on {len(self.healthy)} node(s): {self.ran}"
        lines = [f"pre-flight rejected {len(self.rejected)} node(s):"]
        lines += [f"  {node}: {why}" for node, why in sorted(self.rejected.items())]
        lines.append(f"  {len(self.healthy)} node(s) remain healthy")
        return "\n".join(lines)


class PreflightRunner:
    """Runs the probes of every ``preflight=True`` policy across candidates.

    Args:
        run_on_node: ``(node_id, probe, ctx) -> ProbeResult``. In production
            this reaches the node's ``NodeMonitor``; tests inject a fake.
        upload: Optional ``upload(name, {filename: contents}) -> path``, passed
            to probes that produce bulk output.
    """

    def __init__(
        self,
        run_on_node: Callable[[str, OnDemandProbe, OnDemandProbeContext], ProbeResult],
        upload: Optional[Callable[[str, Dict[str, str]], str]] = None,
    ):
        self._run_on_node = run_on_node
        self._upload = upload

    def run(
        self, policies: Sequence[HealthPolicy], node_ids: Sequence[str]
    ) -> PreflightResult:
        """Screen ``node_ids`` with every pre-flight policy."""
        result = PreflightResult(healthy=list(node_ids))
        preflight = [p for p in policies if p.preflight]
        if not preflight or not node_ids:
            return result

        nodes = [NodeInfo(node_id=n) for n in node_ids]

        for policy in preflight:
            probes = self._probes(policy)
            for probe in probes:
                if probe.scope != NODE_SCOPE:
                    # There is no training process yet, so nothing worker-scoped
                    # can run. Saying so beats silently skipping it.
                    logger.warning(
                        "Pre-flight probe %s is worker-scoped and cannot run "
                        "before the worker group exists. Skipping.",
                        probe.probe_name(),
                    )
                    continue
                result.ran.append(probe.probe_name())
                result.results[probe.probe_name()] = self._run_probe(
                    probe, node_ids, nodes, result
                )

            self._judge(policy, result)

        result.healthy = [n for n in node_ids if n not in result.rejected]
        return result

    # ------------------------------------------------------------------
    @staticmethod
    def _probes(policy: HealthPolicy) -> List[OnDemandProbe]:
        if not policy.probe_creator:
            return []
        return [p for p in policy.probe_creator() if isinstance(p, OnDemandProbe)]

    def _run_probe(self, probe, node_ids, nodes, result) -> Dict[str, ProbeResult]:
        out: Dict[str, ProbeResult] = {}
        for node_id in node_ids:
            ctx = OnDemandProbeContext(
                entity_id=node_id,
                node_id=node_id,
                nodes=nodes,
                timeout_s=probe.timeout_s,
                upload=self._upload,
            )
            try:
                probe_result = self._run_on_node(node_id, probe, ctx)
            except Exception as e:  # noqa: BLE001
                # A check that cannot complete is not a pass. Before training
                # starts, refusing a node costs almost nothing; accepting a bad
                # one costs the run.
                logger.warning(
                    "Pre-flight probe %s failed on %s: %s",
                    probe.probe_name(),
                    node_id,
                    e,
                )
                probe_result = ProbeResult(
                    passed=False, detail=f"pre-flight check did not complete: {e}"
                )
            out[node_id] = probe_result
            if probe_result.passed is False:
                result.rejected.setdefault(
                    node_id,
                    f"{probe.probe_name()}: {probe_result.detail or 'failed'}",
                )
        return out

    def _judge(self, policy: HealthPolicy, result: PreflightResult) -> None:
        """Let the policy's evaluators reject nodes the probes let through.

        A probe reports; an evaluator decides. A thermal reading is not a
        failure on its own, but an evaluator comparing it against its peers may
        well say the node is an outlier.
        """
        if not policy.evaluator_creator:
            return

        state = HealthState(
            nodes={
                node_id: NodeHealth(
                    node_id=node_id,
                    snapshot_at=0.0,
                    probe_results={
                        name: per_node[node_id]
                        for name, per_node in result.results.items()
                        if node_id in per_node
                    },
                )
                for node_id in result.healthy
            },
            # No workers yet, deliberately: an evaluator that needs ranks
            # cannot say anything useful here and will return nothing.
            workers={},
        )

        for evaluator in policy.evaluator_creator():
            try:
                decisions = evaluator.evaluate(state) or []
            except Exception:
                logger.exception(
                    "Pre-flight evaluator %s raised; ignoring it.",
                    type(evaluator).__name__,
                )
                continue
            for decision in decisions:
                if decision.action is not Action.EVICT:
                    continue
                for node_id in getattr(decision, "target_nodes", []):
                    result.rejected.setdefault(
                        node_id, decision.reason or "rejected by pre-flight policy"
                    )


def preflight_policy(
    probe_creator, evaluator_creator=None, name: str = "preflight"
) -> HealthPolicy:
    """Wrap checks as a policy that runs before training starts."""
    return HealthPolicy(
        name=name,
        probe_creator=probe_creator,
        evaluator_creator=evaluator_creator,
        preflight=True,
    )
