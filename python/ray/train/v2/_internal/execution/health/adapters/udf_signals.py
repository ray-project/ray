"""Policies over signals the training loop reports, joined to hardware evidence.

These are the checks that answer "why does this belong in Ray Train rather than
in a node agent". Both of them need a number that exists only inside the
training process, and both of them are wrong without hardware evidence to join
it against:

- :class:`StragglerEvaluator` -- one rank is slow. This is the common case and
  the expensive one: 42.5% of production LLM training jobs are affected by
  stragglers, wasting 10.4% of GPU hours, and in a 55-day 504-GPU study 35.3%
  of all incidents were performance degradation with *no error code at all*.
  The hard part is not noticing, it is telling apart four causes that look
  identical from outside the process -- a degrading GPU, pipeline-stage skew, a
  GC pause, a slow data shard. Only one of them should evict a node, and the
  discriminators (is it the same rank every time, is the excess in compute or
  in data loading) exist only inside the training loop.

- :class:`NumericalEvaluator` -- the numbers go wrong. Loss spikes, gradient-norm
  spikes and NaN propagation are the observable signature of silent data
  corruption, which is by definition invisible to hardware telemetry: no XID,
  no ECC, clocks nominal, every node condition green, the collective
  progressing. The discrimination that matters is whether one rank is always
  the odd one out -- a fault under that rank -- or whether every rank spikes on
  the same step, which is a bad batch or a learning-rate event and must never
  evict anything.

The shape is the same in both: a number from the UDF localizes *which rank*,
the topology turns that into *which host*, and the discrimination decides
whether a node is at fault at all. Neither half reaches the decision alone.

All of it is injectable in a test without special hardware -- see
``examples/why_in_ray_demo.py`` and the tests. The symptom is what a detector
consumes, and the symptom is a number the training loop reports.
"""
import logging
import statistics
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional

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

# Looked up by name so neither adapter has to be installed for this to import.
_NVSENTINEL_PROBE = "NVSentinelProbe"
_NVIDIA_SMI_PROBE = "NvidiaSmiProbe"

#: nvidia-smi / NVSentinel events that mean "this host is degrading", as
#: opposed to "this host is broken". A straggler needs only the former to be
#: attributable; an outright fault would have been caught by the hardware path
#: long before a policy here looked at it.
_DEGRADATION_EVENTS = frozenset(
    {"thermal_throttle", "gpu_hot", "ecc_uncorrectable", "nvidia_smi_unresponsive"}
)


def hardware_evidence_against(state: HealthState, node_id: str) -> Optional[str]:
    """What, if anything, independently says ``node_id`` is degrading.

    Reads both the passive path (NVSentinel's node conditions) and the active
    one (an ``nvidia-smi`` snapshot a ``Diagnose`` pushed). A policy over UDF
    metrics calls this before attributing anything to hardware.
    """
    node = state.nodes.get(node_id)
    if node is not None:
        nvs = node.probe_results.get(_NVSENTINEL_PROBE)
        if nvs is not None and nvs.metrics.get("fatal_conditions", 0) > 0:
            return nvs.detail or "NVSentinel reports a fatal condition"

    smi = state.on_demand_probes.get(_NVIDIA_SMI_PROBE, {}).get(node_id)
    if smi is not None:
        hits = sorted(set(smi.events) & _DEGRADATION_EVENTS)
        if hits:
            return ", ".join(hits)
    return None


def _reported_floats(state: HealthState, key: str) -> Dict[int, float]:
    """``{rank: value}`` for one reported metric, skipping ranks without it."""
    out: Dict[int, float] = {}
    for rank, metrics in state.reported.items():
        value = metrics.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            out[rank] = float(value)
    return out


class StragglerEvaluator(Evaluator):
    """One rank is persistently slower than its peers -- and *why*.

    Holds a rolling window of per-rank step times, the temporal axis the REP
    puts on the evaluator because the controller outlives the rank it measures.

    Noticing a straggler is easy. The value is in the discrimination, because
    the four common causes look identical from outside the process and only one
    of them means a node is at fault:

    ===========================  =====================================  ========
    cause                        what distinguishes it                  action
    ===========================  =====================================  ========
    degrading GPU                same rank, every window, excess in     evict
                                 compute
    GC pause / host jitter       the slow rank *moves* between polls    none
    pipeline-stage skew          slow ranks share a parallelism stage   none
    slow data shard              excess is in data loading, not         none
                                 compute
    ===========================  =====================================  ========

    So the evaluator needs three things from the training loop: the step time,
    optionally a phase breakdown, and which rank reported them. A node agent
    has none of the three.
    """

    def __init__(
        self,
        metric: str = "step_time_s",
        window: int = 20,
        slow_ratio: float = 1.5,
        min_samples: int = 5,
        pinned_windows: int = 3,
        compute_metric: str = "compute_time_s",
        stage_metric: str = "pipeline_stage",
        diagnostics=None,
    ):
        """
        Args:
            metric: Reported key holding per-step wall time.
            window: Samples per rank to keep.
            slow_ratio: How much slower than the cluster median counts.
            min_samples: Samples needed before judging a rank at all.
            pinned_windows: How many consecutive evaluations the *same* rank
                must be the outlier. A slow rank that moves is jitter, and
                jitter is the single most common false positive.
            compute_metric: Optional reported key holding time spent in
                compute. When present, a rank whose excess is *not* in compute
                is not the node's fault.
            stage_metric: Optional reported key naming the rank's pipeline or
                parallelism stage. When every slow rank shares a stage, the
                schedule is imbalanced, not the hardware.
            diagnostics: On-demand probes to push when a straggler is pinned
                but nothing yet explains it.
        """
        self._metric = metric
        self._window = window
        self._slow_ratio = slow_ratio
        self._min_samples = min_samples
        self._pinned_windows = pinned_windows
        self._compute_metric = compute_metric
        self._stage_metric = stage_metric
        self._diagnostics = list(diagnostics or [])
        self._history: Dict[int, Deque[float]] = defaultdict(
            lambda: deque(maxlen=window)
        )
        self._last_step: Dict[int, int] = {}
        self._pinned: Dict[int, int] = defaultdict(int)
        self._diagnosed: set = set()

    def on_worker_group_start(self) -> None:
        self._history.clear()
        self._last_step.clear()
        self._pinned.clear()
        self._diagnosed = set()

    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        self._record(state)

        ready = {
            rank: times
            for rank, times in self._history.items()
            if len(times) >= self._min_samples
        }
        if len(ready) < 3:
            # With fewer than three ranks there is no majority to be slow
            # relative to, and "slower than one peer" is not evidence.
            return []

        medians = {rank: statistics.median(times) for rank, times in ready.items()}
        baseline = statistics.median(medians.values())
        if baseline <= 0:
            return []

        slow = {
            rank: m for rank, m in medians.items() if m >= baseline * self._slow_ratio
        }
        self._update_pinning(slow, medians)

        if not slow:
            return []
        if len(slow) == len(medians):
            # Everyone is slow, so the baseline moved: the data pipeline, a
            # checkpoint, or a global config change. Not a node.
            logger.info("Every rank slowed together; this is not a per-node fault.")
            return []
        if self._shares_a_stage(state, slow):
            logger.info(
                "Slow ranks %s all sit in the same parallelism stage; this is "
                "schedule imbalance, not hardware.",
                sorted(slow),
            )
            return []

        decisions: List[HealthDecision] = []
        for rank, median in sorted(slow.items()):
            if self._pinned[rank] < self._pinned_windows:
                # Still might be jitter moving between ranks.
                continue
            node_id = state.node_of(rank)
            if node_id is None:
                continue

            ratio = median / baseline
            what = (
                f"rank {rank} step time {median:.3f}s is {ratio:.1f}x the "
                f"cluster median {baseline:.3f}s, for "
                f"{self._pinned[rank]} consecutive windows"
            )

            elsewhere = self._excess_outside_compute(state, rank, median, baseline)
            if elsewhere is not None:
                logger.info(
                    "%s, but %s. Not attributable to the node.", what, elsewhere
                )
                continue

            evidence = hardware_evidence_against(state, node_id)
            if evidence:
                decisions.append(
                    Evict(
                        cause=Cause.HARDWARE,
                        reason=f"{what}, and its host reports {evidence}",
                        target_nodes=[node_id],
                    )
                )
            elif self._diagnostics and rank not in self._diagnosed:
                # Slow is ambiguous on its own. Go and find out.
                self._diagnosed.add(rank)
                decisions.append(
                    Diagnose(
                        cause=Cause.NO_PROGRESS,
                        reason=f"{what}; checking whether its host is degrading",
                        on_demand_probes=list(self._diagnostics),
                        target_ranks=[rank],
                        target_nodes=[node_id],
                    )
                )
        return decisions

    # ------------------------------------------------------------------
    def _update_pinning(self, slow: Dict[int, float], medians: Dict[int, float]):
        """Count consecutive evaluations each rank has been the outlier.

        A GC pause or host jitter produces a slow rank that is a *different*
        rank each time. Hardware degradation stays put. This is the cheapest
        discriminator there is and it costs one counter.
        """
        for rank in medians:
            if rank in slow:
                self._pinned[rank] += 1
            else:
                self._pinned[rank] = 0
                self._diagnosed.discard(rank)

    def _shares_a_stage(self, state: HealthState, slow: Dict[int, float]) -> bool:
        """Whether every slow rank sits in the same parallelism stage.

        Pipeline-stage skew makes a whole stage slow. Evicting one of its nodes
        moves the bubble somewhere else; it does not remove it.
        """
        stages = set()
        for rank in slow:
            metrics = state.reported.get(rank, {})
            if self._stage_metric not in metrics:
                return False
            stages.add(metrics[self._stage_metric])
        return len(slow) > 1 and len(stages) == 1

    def _excess_outside_compute(
        self, state: HealthState, rank: int, median: float, baseline: float
    ) -> Optional[str]:
        """Say where the extra time went, when it did not go into compute.

        A rank starved by its data shard is slow through no fault of its GPU.
        Returns ``None`` when compute really is where the time went, or when
        the training loop did not report a breakdown.
        """
        metrics = state.reported.get(rank, {})
        compute = metrics.get(self._compute_metric)
        if not isinstance(compute, (int, float)) or isinstance(compute, bool):
            return None
        excess = median - baseline
        if excess <= 0:
            return None
        compute_excess = float(compute) - baseline
        if compute_excess < excess * 0.5:
            return (
                f"only {max(compute_excess, 0.0):.3f}s of the {excess:.3f}s excess is "
                "in compute; the rest is outside the GPU"
            )
        return None

    def _record(self, state: HealthState) -> None:
        """Take one sample per rank per *step*, not per poll.

        The controller polls faster than the training loop steps, so sampling
        per poll would fill the window with repeats of the same step and make a
        stalled rank look stable.
        """
        values = _reported_floats(state, self._metric)
        for rank, value in values.items():
            step = state.workers[rank].step if rank in state.workers else None
            if step is not None and self._last_step.get(rank) == step:
                continue
            if step is not None:
                self._last_step[rank] = step
            self._history[rank].append(value)


class SdcEvaluator(Evaluator):
    """One rank disagrees with its data-parallel peers on the same step.

    After the gradient all-reduce, every data-parallel rank holds the same
    tensors, so a checksum over them must match. A rank whose checksum differs
    computed something different from everyone else -- silent data corruption,
    a bad NVLink path, or a GPU that is quietly wrong.

    No node-level agent can detect this. Silent corruption is silent precisely
    because the hardware raises nothing: DCGM is clean, no XID, no ECC, clocks
    nominal, and NCCL keeps advancing. The disagreement is only visible from
    inside the training loop, where the numbers are.

    A disagreement must be a *minority*. If half the ranks disagree, the
    collective is wrong rather than one GPU, and evicting a node would not fix
    it.
    """

    def __init__(
        self,
        metric: str = "weight_checksum",
        confirm_steps: int = 2,
        diagnostics=None,
    ):
        self._metric = metric
        self._confirm_steps = confirm_steps
        self._diagnostics = list(diagnostics or [])
        self._streaks: Dict[int, int] = defaultdict(int)
        self._seen_steps: Dict[int, int] = {}
        self._diagnosed: set = set()

    def on_worker_group_start(self) -> None:
        self._streaks.clear()
        self._seen_steps.clear()
        self._diagnosed = set()

    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        outliers = self._outliers(state)
        if outliers is None:
            return []

        for rank in list(self._streaks):
            if rank not in outliers:
                self._streaks.pop(rank, None)
                self._diagnosed.discard(rank)

        decisions: List[HealthDecision] = []
        for rank in sorted(outliers):
            self._streaks[rank] += 1
            if self._streaks[rank] < self._confirm_steps:
                continue

            node_id = state.node_of(rank)
            what = (
                f"rank {rank} disagrees with its data-parallel peers on "
                f"{self._metric} for {self._streaks[rank]} consecutive steps"
            )
            if node_id is None:
                decisions.append(Reattempt(cause=Cause.APPLICATION, reason=what))
                continue

            evidence = hardware_evidence_against(state, node_id)
            if evidence:
                reason = f"{what}, and its host reports {evidence}"
            else:
                # Corroboration is welcome but not required: a mismatched
                # checksum against healthy peers is already attributable to the
                # rank that produced it, and that rank sits on one host.
                reason = f"{what}; no other rank on healthy hardware disagrees"
            decisions.append(
                Evict(cause=Cause.HARDWARE, reason=reason, target_nodes=[node_id])
            )
        return decisions

    def _outliers(self, state: HealthState) -> Optional[set]:
        """Ranks whose value is a minority at the latest commonly-reported step.

        Returns ``None`` when there is nothing comparable to judge -- too few
        ranks, or ranks that have not reached the same step yet.
        """
        by_step: Dict[int, Dict[int, object]] = defaultdict(dict)
        for rank, metrics in state.reported.items():
            if self._metric not in metrics:
                continue
            worker = state.workers.get(rank)
            if worker is None or worker.step is None:
                continue
            by_step[worker.step][rank] = metrics[self._metric]

        # Ranks drift a step or two apart; compare only where enough of them
        # have actually reached the same step.
        for step in sorted(by_step, reverse=True):
            values = by_step[step]
            if len(values) < 3:
                continue
            if self._seen_steps.get(step) == len(values):
                return set()  # already judged this step, nothing new
            self._seen_steps[step] = len(values)

            counts: Dict[object, int] = defaultdict(int)
            for value in values.values():
                counts[value] += 1
            majority_value, majority_count = max(counts.items(), key=lambda kv: kv[1])
            if majority_count * 2 <= len(values):
                # No majority: the collective is wrong, not one rank.
                logger.warning(
                    "No majority %s at step %d; this is not a single-rank fault.",
                    self._metric,
                    step,
                )
                return set()
            return {r for r, v in values.items() if v != majority_value}
        return None


class NumericalEvaluator(Evaluator):
    """The numbers go wrong -- and whether one rank is to blame.

    Loss spikes, gradient-norm spikes and NaN propagation are the observable
    signature of silent data corruption, which hardware telemetry cannot see by
    construction. But they are also the signature of a bad batch, a learning-rate
    event, or a genuinely divergent model, and those must never evict anything.

    The discriminator is *shape*, and it is only visible with every rank's
    numbers side by side at the same step:

    - **one rank, repeatedly** -- the same rank is the outlier while its peers
      are fine. Something under that rank is computing differently. Hardware.
    - **every rank, one step** -- they all moved together on the same batch.
      Data or optimizer. Restart will not help and eviction is actively wrong.

    Those need two different comparisons, and a check with only one of them is
    blind to half the cases. Peer-relative ("is this rank unlike its peers")
    cannot see a global spike, because when every rank spikes the peer median
    spikes with them and nobody stands out. Self-relative ("is the cluster
    unlike its own recent past") cannot localize. The evaluator keeps both: a
    rolling baseline of the cluster median for the temporal axis, and the
    per-step peer comparison for the spatial one.

    Args:
        metrics: Reported keys to watch. Any of them going non-finite, or
            standing out from the peers, counts.
        confirm_steps: How many steps the same rank must be the outlier.
        outlier_ratio: How far from the peer median counts as standing out.
        diagnostics: Probes to push before attributing to hardware.
    """

    def __init__(
        self,
        metrics=("grad_norm", "loss"),
        confirm_steps: int = 3,
        outlier_ratio: float = 5.0,
        window: int = 20,
        diagnostics=None,
    ):
        window_ = window
        self._metrics = tuple(metrics)
        self._confirm_steps = confirm_steps
        self._outlier_ratio = outlier_ratio
        self._diagnostics = list(diagnostics or [])
        self._streaks: Dict[int, int] = defaultdict(int)
        self._judged_steps: set = set()
        # Rolling cluster-median history, one entry per judged step. This is
        # the temporal axis: without it a spike that hits every rank at once is
        # invisible, because the peer median spikes with them.
        self._baseline: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=window_)
        )

    def on_worker_group_start(self) -> None:
        self._streaks.clear()
        self._judged_steps = set()
        self._baseline.clear()

    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        step, values = self._aligned(state)
        if step is None or step in self._judged_steps:
            return []
        self._judged_steps.add(step)

        # Temporal axis first: did the whole cluster move relative to its own
        # recent past? If so no single rank is responsible, whatever the peer
        # comparison says.
        global_anomaly = self._global_anomaly(values)
        outliers = self._outliers(values)
        if global_anomaly or (outliers and len(outliers) == len(values)):
            logger.warning(
                "Every rank reported a numerical anomaly at step %d (%s). This is "
                "data or optimizer state, not hardware.",
                step,
                global_anomaly or "all ranks are outliers",
            )
            self._streaks.clear()
            return [
                Reattempt(
                    cause=Cause.APPLICATION,
                    reason=(
                        f"every rank reported a numerical anomaly at step {step} "
                        f"({global_anomaly or 'all ranks'}); this is data or "
                        "optimizer state, not a node"
                    ),
                )
            ]

        for rank in list(self._streaks):
            if not outliers or rank not in outliers:
                self._streaks.pop(rank, None)

        decisions: List[HealthDecision] = []
        for rank in sorted(outliers or ()):
            self._streaks[rank] += 1
            if self._streaks[rank] < self._confirm_steps:
                continue

            node_id = state.node_of(rank)
            what = (
                f"rank {rank} is the only rank with anomalous "
                f"{'/'.join(self._metrics)} for {self._streaks[rank]} steps, "
                f"while its peers are fine"
            )
            if node_id is None:
                decisions.append(Reattempt(cause=Cause.APPLICATION, reason=what))
                continue

            evidence = hardware_evidence_against(state, node_id)
            if evidence:
                decisions.append(
                    Evict(
                        cause=Cause.HARDWARE,
                        reason=f"{what}, and its host reports {evidence}",
                        target_nodes=[node_id],
                    )
                )
            elif self._diagnostics:
                decisions.append(
                    Diagnose(
                        cause=Cause.UNKNOWN,
                        reason=f"{what}; checking its host before attributing",
                        on_demand_probes=list(self._diagnostics),
                        target_ranks=[rank],
                        target_nodes=[node_id],
                    )
                )
            else:
                # One rank, consistently, with healthy peers on the same data.
                # That is attributable to what is under that rank.
                decisions.append(
                    Evict(
                        cause=Cause.HARDWARE,
                        reason=f"{what}; no peer on other hardware agrees",
                        target_nodes=[node_id],
                    )
                )
        return decisions

    def _aligned(self, state: HealthState):
        """The latest step at least three ranks have all reported."""
        by_step: Dict[int, Dict[int, Dict[str, float]]] = defaultdict(dict)
        for rank, metrics in state.reported.items():
            worker = state.workers.get(rank)
            if worker is None or worker.step is None:
                continue
            present = {
                key: float(metrics[key])
                for key in self._metrics
                if isinstance(metrics.get(key), (int, float))
                and not isinstance(metrics.get(key), bool)
            }
            if present:
                by_step[worker.step][rank] = present

        for step in sorted(by_step, reverse=True):
            if len(by_step[step]) >= 3:
                return step, by_step[step]
        return None, {}

    def _global_anomaly(self, values) -> Optional[str]:
        """Whether the cluster as a whole broke from its own recent history.

        Records the cluster median for each metric and compares it to the median
        of what came before. A step where every rank went non-finite counts
        immediately: there is no baseline that makes that normal.
        """
        import math

        for key in self._metrics:
            present = [v[key] for v in values.values() if key in v]
            if len(present) < 3:
                continue

            finite = [v for v in present if math.isfinite(v)]
            if not finite:
                return f"every rank reported a non-finite {key}"

            current = statistics.median(finite)
            history = self._baseline[key]
            baseline = statistics.median(history) if len(history) >= 3 else None
            history.append(current)

            if baseline is not None and baseline > 0:
                if current >= baseline * self._outlier_ratio:
                    return (
                        f"cluster median {key} jumped from {baseline:.3g} to "
                        f"{current:.3g}"
                    )
        return None

    def _outliers(self, values) -> Optional[set]:
        """Ranks whose numbers are non-finite, or far from the peer median."""
        import math

        outliers = set()
        for key in self._metrics:
            present = {r: v[key] for r, v in values.items() if key in v}
            if len(present) < 3:
                continue

            nonfinite = {r for r, v in present.items() if not math.isfinite(v)}
            outliers |= nonfinite

            finite = {r: v for r, v in present.items() if math.isfinite(v)}
            if len(finite) < 3:
                continue
            median = statistics.median(finite.values())
            if median <= 0:
                continue
            outliers |= {
                r for r, v in finite.items() if abs(v) >= median * self._outlier_ratio
            }
        return outliers


def numerical_policy(*, confirm_steps: int = 3, diagnostics=None) -> HealthPolicy:
    """Attribute a numerical anomaly to a rank, or refuse to."""
    return HealthPolicy(
        name="numerical",
        evaluator_creator=lambda: [
            NumericalEvaluator(confirm_steps=confirm_steps, diagnostics=diagnostics)
        ],
    )


def straggler_policy(
    *, metric: str = "step_time_s", slow_ratio: float = 1.5, diagnostics=None
) -> HealthPolicy:
    """Detect a degraded-but-alive rank, and attribute it before acting."""
    return HealthPolicy(
        name="straggler",
        evaluator_creator=lambda: [
            StragglerEvaluator(
                metric=metric, slow_ratio=slow_ratio, diagnostics=diagnostics
            )
        ],
    )


def sdc_policy(*, metric: str = "weight_checksum", diagnostics=None) -> HealthPolicy:
    """Detect a rank computing the wrong answer, which hardware cannot see."""
    return HealthPolicy(
        name="sdc",
        evaluator_creator=lambda: [
            SdcEvaluator(metric=metric, diagnostics=diagnostics)
        ],
    )
