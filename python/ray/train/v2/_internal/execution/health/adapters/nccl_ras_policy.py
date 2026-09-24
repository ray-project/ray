"""NCCL RAS, re-expressed against the health contracts.

``callbacks/nccl_ras.py`` is already Collect -> Decide -> Act, built ad-hoc
before the contracts existed: ``RASPoller`` is a probe, the per-communicator
frozen-streak logic is an evaluator, the text report and the ``py-spy`` fan-out
are diagnostics, and ``NCCLHangError`` is a reattempt. Porting it is the
cheapest real test of whether the contracts are sufficient, because it is a
detector that already works and whose behavior must not change.

Everything hard -- the ``ncclras`` transport, the JSON schema repair, the
report parsing, the op-count diff -- is imported unchanged. What changes is
the shape around it.

The port keys results by **communicator**, which is what a RAS report is
actually about and what actually hangs. That needs no new probe kind: a
``ClusterProbe`` returns ``{entity_id: ProbeResult}`` and declares what its
entity is, so NVSentinel keying by node and RAS keying by communicator are the
same class with different ``entity`` values.

Two things the port buys that the callback cannot have:

1. **A hang becomes attributable.** RAS names the stalled ranks;
   ``state.node_of(rank)`` names their hosts; an NVSentinel probe result for
   that host says whether its NIC is down. That join upgrades the verdict from
   ``Reattempt`` (retry onto the same hardware, where a real NIC fault hangs
   again) to ``Evict(cause=HARDWARE)``.
2. **Confirmation runs on wall-clock, not poll counts.** The callback converts
   a confirm *duration* into a poll count by assuming each report it drains
   represents exactly one poll interval. That holds only while the controller
   drains at least as fast as the poller produces.
"""
import logging
import time
from typing import Dict, List, Optional

from ray.train.v2._internal.callbacks.nccl_ras import (
    RASReport,
    compute_report_op_diff,
)
from ray.train.v2._internal.execution.health.decision import (
    Cause,
    Diagnose,
    Evict,
    HealthDecision,
    Reattempt,
)
from ray.train.v2._internal.execution.health.policy import Evaluator, HealthPolicy
from ray.train.v2._internal.execution.health.probe import (
    ClusterContext,
    ClusterProbe,
    ProbeResult,
)
from ray.train.v2._internal.execution.health.state import HealthState

logger = logging.getLogger(__name__)

# The NVSentinel probe's name, looked up by string so this module does not
# depend on that adapter being installed.
_NVSENTINEL_PROBE = "NVSentinelProbe"


class NcclRasProbe(ClusterProbe):
    """One ``ncclras`` query per poll, reduced to one result per communicator.

    A RAS report is keyed by communicator and spans every rank in the mesh, so
    a communicator -- not a node and not a rank -- is the entity it measures.
    That is the whole difference from the NVSentinel probe; both are cluster
    probes reading an out-of-band source from the controller.

    The probe holds the previous report and emits the *delta* rather than
    shipping raw op counts onward. At 1,024 ranks x ~5 communicators x ~6 ops
    that is 30k counters per poll versus a handful of floats per communicator,
    and a delta against the last sample is a property of the measurement. The
    judgment -- has this persisted long enough to act on -- stays in the
    evaluator, where the REP puts history.

    Holding state here is safe only because a cluster probe runs on the
    controller. A probe inside the failure domain must stay stateless.
    """

    name = "NcclRasProbe"
    entity = "communicator"

    def __init__(self, query, interval_s: float = 15.0):
        """
        Args:
            query: Callable returning the current ``RASReport``, or ``None``
                when this poll produced nothing usable. In production this is
                ``RASPoller.next_result``; tests pass a fake. Raising
                ``ProbeDegraded`` retires the probe for the run, which is how
                the callback's ``_is_ras_degraded`` latch is expressed.
            interval_s: Seconds between queries.
        """
        self._query = query
        self.interval_s = interval_s
        self._prev: Optional[RASReport] = None

    def poll(self, ctx: ClusterContext) -> Dict[str, ProbeResult]:
        report = self._query()
        if report is None:
            return {}

        prev, self._prev = self._prev, report
        mismatched = report.mismatched_comms
        op_diff = compute_report_op_diff(prev, report) if prev else {}

        results: Dict[str, ProbeResult] = {}
        for comm_id, ranks in report.comm_op_counts.items():
            deltas = op_diff.get(comm_id, {})
            advanced = any(
                d != 0 for rank_deltas in deltas.values() for d in rank_deltas.values()
            )
            is_mismatched = comm_id in mismatched
            # A comparable measurement can be absent -- a new communicator, or
            # the very first report -- which is not the same as no progress.
            frozen = bool(deltas) and is_mismatched and not advanced

            results[comm_id] = ProbeResult(
                metrics={
                    "mismatched": float(is_mismatched),
                    "ops_advanced": float(advanced),
                    "ranks": float(len(ranks)),
                },
                # Per-rank slice within this communicator, so a decision can
                # name the ranks to diagnose rather than sweeping all 1,024.
                devices={
                    str(rank): {
                        "ops_advanced": float(
                            any(d != 0 for d in deltas.get(rank, {}).values())
                        )
                    }
                    for rank in ranks
                },
                events=["frozen"] if frozen else [],
                # No delta yet means no opinion, not a clean bill of health.
                passed=(not frozen) if deltas else None,
                detail=(
                    f"communicator {comm_id}: {len(ranks)} ranks mismatched and "
                    "making no progress"
                    if frozen
                    else ""
                ),
            )
        return results


class NcclHangEvaluator(Evaluator):
    """Confirms a hang, then attributes it.

    A communicator is a hang candidate only when it is mismatched *and* no rank
    advanced any op since the last sample: a real deadlock blocks every rank, so
    every op freezes. One frozen sample is a blip, so a candidate must stay
    frozen for ``confirm_duration_s`` of wall clock before it counts.

    On confirmation the evaluator looks for a hardware explanation among the
    hosts of the stalled ranks. Finding one changes both the action and who is
    accountable for it: ``Evict(HARDWARE)`` restarts without that host, where a
    bare ``Reattempt(NO_PROGRESS)`` would put the run straight back onto it.
    """

    def __init__(
        self,
        confirm_duration_s: float = 600.0,
        suspect_after_s: float = 60.0,
        observe_only: bool = False,
        diagnostics=None,
        clock=time.monotonic,
    ):
        """
        Args:
            confirm_duration_s: How long a communicator must stay frozen.
            suspect_after_s: When to start warning that something looks wrong.
            observe_only: Detect and log, take no action. The env-var switch
                the callback reads, as an argument.
            diagnostics: On-demand probes to push on a confirmed hang, from
                ``hang_diagnostics()``. With none, a confirmed hang acts on the
                passive evidence alone.
            clock: Injectable for tests.
        """
        self._confirm_s = confirm_duration_s
        self._suspect_s = suspect_after_s
        self._observe_only = observe_only
        self._diagnostics = list(diagnostics or [])
        self._clock = clock
        # comm_id -> monotonic time it was first seen frozen
        self._frozen_since: Dict[str, float] = {}
        self._suspected: set = set()
        # Communicators a diagnostic has already been pushed for, so a hang
        # that persists does not re-push py-spy at every rank every poll.
        self._diagnosed: set = set()

    def on_worker_group_start(self) -> None:
        self._frozen_since = {}
        self._suspected = set()
        self._diagnosed = set()

    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        comms = state.results(NcclRasProbe)
        if not comms:
            return []

        now = self._clock()
        frozen = {comm_id for comm_id, r in comms.items() if "frozen" in r.events}

        # Any communicator that resumed drops its streak.
        for comm_id in list(self._frozen_since):
            if comm_id not in frozen:
                if comm_id in self._suspected:
                    logger.info(
                        "NCCL communicator %s resumed after %.0fs stalled; no longer "
                        "suspected of hanging.",
                        comm_id,
                        now - self._frozen_since[comm_id],
                    )
                self._frozen_since.pop(comm_id)
                self._suspected.discard(comm_id)
                self._diagnosed.discard(comm_id)

        confirmed = []
        for comm_id in frozen:
            since = self._frozen_since.setdefault(comm_id, now)
            stalled_for = now - since
            if stalled_for >= self._confirm_s:
                confirmed.append(comm_id)
            elif stalled_for >= self._suspect_s and comm_id not in self._suspected:
                self._suspected.add(comm_id)
                logger.warning(
                    "Possible NCCL hang: communicator %s has made no progress for "
                    "%.0fs. Continuing to monitor.",
                    comm_id,
                    stalled_for,
                )

        if not confirmed:
            return []

        reason = (
            f"{len(confirmed)} of {len(comms)} NCCL communicators "
            f"({', '.join(sorted(confirmed))}) made no progress for "
            f"{self._confirm_s:.0f}s"
        )

        if self._observe_only:
            logger.warning("%s (observe mode: taking no action).", reason)
            return []

        stalled_ranks = self._stalled_ranks(comms, confirmed)

        # 1. Passive evidence already names faulty hardware under the stall:
        #    nothing to ask, the answer is in.
        bad_nodes = self._hosts_with_hardware_faults(state)
        if bad_nodes:
            return [
                Evict(
                    cause=Cause.HARDWARE,
                    reason=(
                        f"{reason}; the stalled ranks sit on {sorted(bad_nodes)}, "
                        "which independent probes report as faulty"
                    ),
                    target_nodes=sorted(bad_nodes),
                )
            ]

        # 2. Nothing passive explains it. Ask: push the diagnostics once, at the
        #    ranks and hosts actually involved.
        undiagnosed = [c for c in confirmed if c not in self._diagnosed]
        if self._diagnostics and undiagnosed:
            self._diagnosed.update(undiagnosed)
            nodes = sorted({n for n in (state.node_of(r) for r in stalled_ranks) if n})
            return [
                Diagnose(
                    cause=Cause.NO_PROGRESS,
                    reason=f"{reason}; no hardware evidence yet, running diagnostics",
                    on_demand_probes=list(self._diagnostics),
                    target_ranks=stalled_ranks,
                    target_nodes=nodes,
                )
            ]

        # 3. The diagnostics have come back. This is the loop closing: an action
        #    produced fresh evidence, and this pass reads it.
        faulty = self._hosts_failing_diagnostics(state)
        if faulty:
            return [
                Evict(
                    cause=Cause.HARDWARE,
                    reason=(
                        f"{reason}; on-demand diagnostics found "
                        + "; ".join(f"{node} {what}" for node, what in faulty.items())
                    ),
                    target_nodes=sorted(faulty),
                )
            ]
        # Nothing was attributed. Say *why* nothing was attributed, because
        # "the hardware was checked and is fine" and "nothing checked the
        # hardware" lead a reader to opposite conclusions.
        if not self._diagnostics:
            unexplained = (
                "no diagnostics are configured, so nothing has examined the "
                "hardware under these ranks"
            )
        elif not state.on_demand_probes:
            unexplained = "diagnostics were requested but have not reported back yet"
        else:
            unexplained = (
                "diagnostics found no hardware fault, so this is software or data"
            )
        return [Reattempt(cause=Cause.NO_PROGRESS, reason=f"{reason}; {unexplained}")]

    @staticmethod
    def _stalled_ranks(comms, confirmed) -> List[int]:
        """The ranks inside the confirmed communicators, for targeting."""
        ranks = set()
        for comm_id in confirmed:
            ranks.update(int(r) for r in comms[comm_id].devices)
        return sorted(ranks)

    def _hosts_failing_diagnostics(self, state: HealthState) -> Dict[str, str]:
        """Nodes whose on-demand GPU snapshot came back bad.

        A driver that would not answer counts: ``nvidia-smi`` blocking is the
        classic signature of a wedged GPU, and the probe records that rather
        than dropping it.
        """
        from ray.train.v2._internal.execution.health.adapters import (
            nccl_ras_diagnostics as diag,
        )

        faulty: Dict[str, str] = {}
        smi = state.on_demand_probe_results(diag.NvidiaSmiProbe)
        for node_id, result in smi.items():
            if result.passed is False and result.events:
                faulty[node_id] = ", ".join(result.events)
        return faulty

    @staticmethod
    def _hosts_with_hardware_faults(state: HealthState) -> List[str]:
        """Nodes hosting this run that another probe independently calls faulty.

        Deliberately keyed off a *different* probe's verdict. A hang alone is
        ``NO_PROGRESS`` -- it says nothing about whose fault it is, and evicting
        on a hang alone would let a bug in user code cordon healthy hardware.
        """
        bad = []
        for node_id, node in state.nodes.items():
            result = node.probe_results.get(_NVSENTINEL_PROBE)
            if result is None or not state.ranks_on(node_id):
                continue
            if result.metrics.get("fatal_conditions", 0) > 0:
                bad.append(node_id)
        return bad


def nccl_ras_policy(
    query,
    *,
    confirm_duration_s: float = 600.0,
    observe_only: bool = False,
    interval_s: float = 15.0,
    diagnostics=None,
) -> HealthPolicy:
    """The ported detector, as one policy.

    Compose it with ``nvsentinel_policy()`` to get the attributed path: RAS
    names the stalled ranks, NVSentinel names the broken hardware under them.
    """
    return HealthPolicy(
        name="nccl_ras",
        probe_creator=lambda: [NcclRasProbe(query, interval_s=interval_s)],
        evaluator_creator=lambda: [
            NcclHangEvaluator(
                confirm_duration_s=confirm_duration_s,
                observe_only=observe_only,
                diagnostics=diagnostics,
            )
        ],
    )
