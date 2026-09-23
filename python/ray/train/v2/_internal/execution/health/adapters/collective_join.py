"""Joining NCCL RAS against what the training loop reports.

RAS knows a communicator stopped advancing. It does not know *which*
communicator that is, or whether stopping was abnormal -- and both answers are
workload-specific, which is why they can only come from the training loop.

Two false positives a RAS-only detector cannot avoid:

- **Pipeline and expert parallelism idle by design.** In a PP schedule, a
  send/recv group is legitimately quiet during a bubble; in MoE, an all-to-all's
  participation varies with routing. Frozen op counts on those groups are
  normal. A detector that cannot tell a TP group from a PP group will fire on
  exactly the workloads that matter most.
- **"How long is too long" is a property of the job.** The shipped detector
  confirms after a fixed 10 minutes. For a job with 2-second steps that is 300
  steps of dead GPU; for one with 40-second steps it is barely over the noise
  floor. The right threshold is a multiple of *this* job's step time, which only
  the job knows.

The join needs no new plumbing and no comm-hash bookkeeping. RAS already
reports which ranks are in each communicator; the training loop reports each
rank's coordinates:

    health.report(
        {"step": step, "dp_rank": dp, "tp_rank": tp, "pp_rank": pp,
         "step_time_s": dt},
        step=step,
    )

Matching a frozen communicator's rank set against the groups those coordinates
imply names the communicator. That is the whole trick.
"""
import logging
import statistics
from collections import defaultdict
from typing import Dict, FrozenSet, List, Optional, Tuple

from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
    NcclRasProbe,
)
from ray.train.v2._internal.execution.health.adapters.udf_signals import (
    hardware_evidence_against,
)
from ray.train.v2._internal.execution.health.decision import (
    Cause,
    Diagnose,
    Evict,
    HealthDecision,
    Reattempt,
)
from ray.train.v2._internal.execution.health.policy import Evaluator, HealthPolicy
from ray.train.v2._internal.execution.health.state import HealthState

logger = logging.getLogger(__name__)

#: Reported keys naming a rank's position on each parallelism axis. A job that
#: reports none of them gets the RAS-only behavior, which is the status quo.
DEFAULT_AXES = ("dp_rank", "tp_rank", "pp_rank", "ep_rank")

#: Axes whose collectives are synchronous every step, so a freeze there is
#: unambiguous. The others idle by design and need progress evidence.
ALWAYS_SYNCHRONOUS = frozenset({"dp_rank", "tp_rank"})


def parallelism_groups(
    reported: Dict[int, dict], axes=DEFAULT_AXES
) -> Dict[FrozenSet[int], str]:
    """Map each parallelism group's rank set to the axis it varies along.

    The group for axis A containing rank r is every rank that agrees with r on
    every *other* axis. So with dp=2, tp=2 over 4 ranks, the TP groups are
    {0,1} and {2,3} and the DP groups are {0,2} and {1,3}.

    Args:
        reported: ``{rank: metrics}`` from ``health.report()``.
        axes: Reported keys to treat as parallelism coordinates.

    Returns:
        ``{frozenset_of_ranks: axis_name}``. Empty when the job reports no
        coordinates, or reports only one axis with a single group (nothing to
        distinguish).
    """
    present = [a for a in axes if any(a in m for m in reported.values())]
    if not present:
        return {}

    coords: Dict[int, Tuple] = {}
    for rank, metrics in reported.items():
        if all(a in metrics for a in present):
            coords[rank] = tuple(metrics[a] for a in present)
    if len(coords) < 2:
        return {}

    groups: Dict[FrozenSet[int], str] = {}
    for i, axis in enumerate(present):
        buckets: Dict[Tuple, set] = defaultdict(set)
        for rank, c in coords.items():
            # Everything except this axis identifies the group.
            buckets[c[:i] + c[i + 1 :]].add(rank)
        for ranks in buckets.values():
            if len(ranks) > 1:
                # A rank set that is a group on two axes is ambiguous; keep the
                # first, which is the more synchronous one by DEFAULT_AXES order.
                groups.setdefault(frozenset(ranks), axis)
    return groups


class CollectiveHangEvaluator(Evaluator):
    """Confirms a NCCL hang using the job's own notion of progress.

    Every input comes from one of two places, and neither is sufficient:

    ==========================  ==========================================
    from ``NcclRasProbe``       which communicators froze, and their ranks
    from ``health.report()``    each rank's step, step time, and
                                parallelism coordinates
    ==========================  ==========================================

    What the join buys:

    - **Naming the communicator.** Matching a frozen rank set against the
      parallelism groups says whether it is the DP all-reduce or a PP
      send/recv. Only the former is unambiguous.
    - **A threshold in the job's own units.** A stall is confirmed once no rank
      has advanced a step for ``stall_factor`` times the median step time,
      rather than after a fixed wall-clock timeout that is wrong for every job
      but one.
    - **Refusing to fire when the job is fine.** A frozen PP group while steps
      keep advancing is a pipeline bubble, and calling it a hang would take out
      a healthy run.
    """

    def __init__(
        self,
        stall_factor: float = 5.0,
        min_stall_s: float = 30.0,
        max_stall_s: float = 900.0,
        axes=DEFAULT_AXES,
        diagnostics=None,
    ):
        """
        Args:
            stall_factor: Multiples of the median step time with no progress
                before a frozen communicator is confirmed.
            min_stall_s: Floor, so a fast-stepping job does not fire on jitter.
            max_stall_s: Ceiling, so a job that never reports progress still
                eventually confirms.
            axes: Reported keys holding parallelism coordinates.
            diagnostics: On-demand probes to push before blaming hardware.
        """
        self._stall_factor = stall_factor
        self._min_stall_s = min_stall_s
        self._max_stall_s = max_stall_s
        self._axes = tuple(axes)
        self._diagnostics = list(diagnostics or [])
        self._frozen_since: Dict[str, float] = {}
        self._max_step: Optional[int] = None
        self._max_step_at: Optional[float] = None
        self._diagnosed: set = set()

    def on_worker_group_start(self) -> None:
        self._frozen_since.clear()
        self._max_step = None
        self._max_step_at = None
        self._diagnosed = set()

    # ------------------------------------------------------------------
    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        comms = state.results(NcclRasProbe)
        if not comms:
            return []

        now = self._observe_progress(state)
        frozen = {c: r for c, r in comms.items() if "frozen" in r.events}

        for comm_id in list(self._frozen_since):
            if comm_id not in frozen:
                self._frozen_since.pop(comm_id, None)
                self._diagnosed.discard(comm_id)
        if not frozen:
            return []

        groups = parallelism_groups(state.reported, self._axes)
        threshold = self._stall_threshold(state)
        stalled_for = (now - self._max_step_at) if self._max_step_at else None

        decisions: List[HealthDecision] = []
        for comm_id, result in sorted(frozen.items()):
            ranks = frozenset(int(r) for r in result.devices)
            axis = groups.get(ranks)
            self._frozen_since.setdefault(comm_id, now)

            # A group that idles by design needs progress evidence before it
            # counts. A synchronous one does not: if the DP all-reduce is
            # frozen, the job is stopped by definition.
            idles_by_design = axis is not None and axis not in ALWAYS_SYNCHRONOUS

            if idles_by_design and not self._no_progress(stalled_for, threshold):
                logger.info(
                    "Communicator %s (%s group over ranks %s) is frozen, but the "
                    "job is still stepping. That is a schedule bubble, not a hang.",
                    comm_id,
                    axis,
                    sorted(ranks),
                )
                continue

            held_for = now - self._frozen_since[comm_id]
            if held_for < threshold:
                continue

            what = self._describe(
                comm_id, axis, ranks, held_for, stalled_for, threshold
            )
            decisions.append(self._act(state, comm_id, ranks, what))
        return decisions

    # ------------------------------------------------------------------
    def _observe_progress(self, state: HealthState) -> float:
        """Track the furthest step any rank has reached, and when.

        Uses the *max* across ranks on purpose: during a hang the healthy ranks
        are blocked too, so every rank's step stops advancing together. The
        question is whether the job as a whole moved.
        """
        import time

        now = time.monotonic()
        steps = [w.step for w in state.workers.values() if w.step is not None]
        if not steps:
            return now
        top = max(steps)
        if self._max_step is None or top > self._max_step:
            self._max_step = top
            self._max_step_at = now
        elif self._max_step_at is None:
            self._max_step_at = now
        return now

    def _stall_threshold(self, state: HealthState) -> float:
        """How long without progress counts, in this job's own units."""
        times = [
            float(m["step_time_s"])
            for m in state.reported.values()
            if isinstance(m.get("step_time_s"), (int, float))
            and not isinstance(m.get("step_time_s"), bool)
        ]
        if not times:
            return self._max_stall_s
        median = statistics.median(times)
        return max(
            self._min_stall_s, min(self._max_stall_s, median * self._stall_factor)
        )

    @staticmethod
    def _no_progress(stalled_for: Optional[float], threshold: float) -> bool:
        return stalled_for is not None and stalled_for >= threshold

    def _describe(self, comm_id, axis, ranks, held_for, stalled_for, threshold) -> str:
        named = f"the {axis.replace('_rank', '').upper()} group" if axis else "a group"
        progress = (
            f"no rank has advanced a step for {stalled_for:.0f}s "
            f"(threshold {threshold:.0f}s, derived from this job's step time)"
            if stalled_for is not None
            else "the job reports no step progress at all"
        )
        return (
            f"NCCL communicator {comm_id} -- {named} over ranks {sorted(ranks)} -- "
            f"has made no collective progress for {held_for:.0f}s, and {progress}"
        )

    def _act(self, state, comm_id, ranks, what) -> HealthDecision:
        nodes = sorted({n for n in (state.node_of(r) for r in ranks) if n})

        faulty = [n for n in nodes if hardware_evidence_against(state, n)]
        if faulty:
            detail = "; ".join(
                f"{n}: {hardware_evidence_against(state, n)}" for n in faulty
            )
            return Evict(
                cause=Cause.HARDWARE,
                reason=f"{what}; hardware under it is faulty ({detail})",
                target_nodes=faulty,
            )

        if self._diagnostics and comm_id not in self._diagnosed:
            self._diagnosed.add(comm_id)
            return Diagnose(
                cause=Cause.NO_PROGRESS,
                reason=f"{what}; nothing yet says which host is at fault",
                on_demand_probes=list(self._diagnostics),
                target_ranks=sorted(ranks),
                target_nodes=nodes,
            )

        return Reattempt(cause=Cause.NO_PROGRESS, reason=what)


def collective_hang_policy(
    query,
    *,
    stall_factor: float = 5.0,
    min_stall_s: float = 30.0,
    interval_s: float = 15.0,
    diagnostics=None,
) -> HealthPolicy:
    """RAS plus the training loop's own progress, as one policy.

    The probe half is workload-agnostic; the evaluator half is not, and that is
    the point. Compose with ``nvsentinel_policy()`` to let a confirmed hang
    escalate from ``Reattempt`` to ``Evict`` when the hardware agrees.
    """
    return HealthPolicy(
        name="collective_hang",
        probe_creator=lambda: [NcclRasProbe(query, interval_s=interval_s)],
        evaluator_creator=lambda: [
            CollectiveHangEvaluator(
                stall_factor=stall_factor,
                min_stall_s=min_stall_s,
                diagnostics=diagnostics,
            )
        ],
    )
