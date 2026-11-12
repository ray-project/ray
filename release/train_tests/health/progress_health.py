"""Training progress checks on ``ray.train.health``, after GR00T 2's straggler
detection and recovery.

Each rank reports its progress from the training loop with
``report_progress()``: its step, its phase (init, train or checkpoint), its
loss, and a straggler score, such as NVRx's relative GPU performance score.

``stall_check()``
  ``ProgressEvaluator`` reattempts when a rank's loss isn't finite. When a
  rank's step and phase haven't changed for longer than its phase's timeout, it
  diagnoses: ``DecideOnStall`` evicts the stalled ranks' nodes whose GPUs show
  a fault, and otherwise reattempts. It writes the ranks' stack traces to
  ``artifacts_dir``.
``straggler_check()``
  ``DiagnoseStragglers`` diagnoses the nodes of the ranks whose straggler score
  falls below a threshold. A low score is evidence, not proof:
  ``EvictOnConfirmedGpuFault`` evicts a node only if its GPUs show a fault,
  and otherwise training continues.

A GPU shows a fault when its node has a fatal GPU health event, or its hottest
GPU is at ``temp_limit_c`` or more (see ``gpu_health``).

Scripts using this module call ``harness.ship_by_value(gpu_health,
progress_health)``.
"""
import json
import logging
import math
import os
import time
from typing import Callable, Dict, Iterable, List, Optional

from gpu_health import GpuHealthEventProbe, GpuTemperatureProbe

from ray.train.health import (
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthState,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
    Reattempt,
    WorkerProbe,
    report,
)
from ray.train.v2._internal.callbacks.nccl_ras import StackTraceProbe

logger = logging.getLogger("ray.train")

INIT, TRAIN, CHECKPOINT = 0, 1, 2
PHASE_NAMES = {INIT: "init", TRAIN: "train", CHECKPOINT: "checkpoint"}
DEFAULT_PHASE_TIMEOUTS_S = {INIT: 1800.0, TRAIN: 600.0, CHECKPOINT: 1800.0}


def report_progress(
    step: int, phase: int, loss: float, straggler_score: float = 1.0
) -> None:
    """Report this rank's progress. Call it from the training loop."""
    report(
        {
            "phase": float(phase),
            "loss": float(loss),
            "straggler_score": float(straggler_score),
        },
        step=step,
    )


class WorkerNodeProbe(WorkerProbe):
    """The node of this worker."""

    def poll(self) -> ProbeResult:
        import ray

        return ProbeResult(detail=ray.get_runtime_context().get_node_id())


def _nodes_of(state: HealthState, ranks: Iterable[int]) -> List[str]:
    nodes = state.probe_results.get("WorkerNodeProbe", {})
    return sorted({nodes[r].detail for r in ranks if r in nodes and nodes[r].detail})


def gpu_faults(
    state: HealthState, nodes: Iterable[str], temp_limit_c: float
) -> Dict[str, str]:
    """``{node: fault}`` for the ``nodes`` with a fatal GPU health event, or a
    GPU at ``temp_limit_c`` or more."""
    events = state.probe_results.get("GpuHealthEventProbe", {})
    temps = state.probe_results.get("GpuTemperatureProbe", {})
    faults = {}
    for node in nodes:
        event, temp = events.get(node), temps.get(node)
        if temp is not None and temp.metrics.get("temp_c", 0.0) >= temp_limit_c:
            faults[node] = f"{temp.metrics['temp_c']:.0f}C"
        if event is not None and event.metrics.get("is_fatal"):
            faults[node] = json.loads(event.detail).get("message", event.detail)
    return faults


def _short(faults: Dict[str, str]) -> Dict[str, str]:
    return {node[:8]: fault for node, fault in faults.items()}


# ----------------------------------------------------------------------
# Stalls and a loss that isn't finite
# ----------------------------------------------------------------------
class DecideOnStall(Evaluator):
    """Evicts the stalled ranks' nodes whose GPUs show a fault, and otherwise
    reattempts. Writes every rank's stack trace to ``artifacts_dir``."""

    def __init__(
        self,
        stalled: Dict[int, str],
        temp_limit_c: float,
        artifacts_dir: Optional[str],
        on_done: Callable[[], None],
    ):
        self._stalled = stalled
        self._temp_limit_c = temp_limit_c
        self._artifacts_dir = artifacts_dir
        self._on_done = on_done

    def evaluate(self, state: HealthState):
        self._on_done()
        where = ""
        if self._artifacts_dir:
            root = os.path.join(self._artifacts_dir, f"stall_{int(time.time())}")
            os.makedirs(root, exist_ok=True)
            for rank, r in state.probe_results.get("StackTraceProbe", {}).items():
                with open(os.path.join(root, f"rank_{rank}.log"), "w") as f:
                    f.write(r.detail if r.error is None else f"error: {r.error}")
            where = f" Stack traces: {root}."
        nodes = _nodes_of(state, self._stalled)
        faults = gpu_faults(state, nodes, self._temp_limit_c)
        if faults:
            return Evict(
                reason=f"stalled ranks {sorted(self._stalled)} run on nodes with "
                f"GPU faults: {_short(faults)}.{where}",
                target_nodes=sorted(faults),
            )
        return Reattempt(
            reason=f"stalled ranks {sorted(self._stalled)}: the GPUs of nodes "
            f"{[n[:8] for n in nodes]} show no fault, so a software or data "
            f"stall.{where}"
        )


class ProgressEvaluator(Evaluator):
    """Reattempts when a rank reports a loss that isn't finite. Diagnoses the
    ranks whose step and phase haven't changed for longer than their phase's
    timeout in ``phase_timeouts_s``, once per attempt."""

    def __init__(
        self,
        phase_timeouts_s: Dict[int, float],
        temp_limit_c: float,
        artifacts_dir: Optional[str],
    ):
        self._phase_timeouts_s = phase_timeouts_s
        self._temp_limit_c = temp_limit_c
        self._artifacts_dir = artifacts_dir
        self.on_reset()

    def on_reset(self) -> None:
        # {rank: ((step, phase), when it last changed)}
        self._progress = {}
        self._diagnosed = False

    def _mark_diagnosed(self) -> None:
        self._diagnosed = True

    def evaluate(self, state: HealthState):
        now = time.monotonic()
        stalled = {}
        for rank, r in sorted(state.health_reports.items()):
            loss = r.metrics.get("loss", 0.0)
            if not math.isfinite(loss):
                return Reattempt(
                    reason=f"rank {rank} reported a loss of {loss} at step {r.step}"
                )
            phase = int(r.metrics.get("phase", TRAIN))
            last = self._progress.get(rank)
            if last is None or last[0] != (r.step, phase):
                self._progress[rank] = ((r.step, phase), now)
                continue
            idle_s = now - last[1]
            if idle_s > self._phase_timeouts_s[phase]:
                stalled[
                    rank
                ] = f"step {r.step} in {PHASE_NAMES[phase]} for {idle_s:.0f}s"
        if not stalled or self._diagnosed:
            return Noop()
        artifacts_dir, temp_limit_c = self._artifacts_dir, self._temp_limit_c
        return Diagnose(
            reason=f"{len(stalled)} ranks made no progress: {stalled}",
            checks=[
                HealthCheck(
                    probe_creator=lambda: [
                        WorkerNodeProbe(),
                        StackTraceProbe(),
                        GpuHealthEventProbe(),
                        GpuTemperatureProbe(),
                    ],
                    evaluator_creator=lambda: [
                        DecideOnStall(
                            stalled, temp_limit_c, artifacts_dir, self._mark_diagnosed
                        )
                    ],
                )
            ],
        )


def stall_check(
    phase_timeouts_s: Dict[int, float] = DEFAULT_PHASE_TIMEOUTS_S,
    temp_limit_c: float = 85.0,
    artifacts_dir: Optional[str] = None,
) -> PeriodicHealthCheck:
    return PeriodicHealthCheck(
        evaluator_creator=lambda: [
            ProgressEvaluator(phase_timeouts_s, temp_limit_c, artifacts_dir)
        ],
        poll_interval_s=1.0,
    )


# ----------------------------------------------------------------------
# Stragglers
# ----------------------------------------------------------------------
class EvictOnConfirmedGpuFault(Evaluator):
    """Evicts the straggling ranks' nodes whose GPUs show a fault. Otherwise
    calls ``on_healthy`` with why, and training continues."""

    def __init__(
        self,
        ranks: List[int],
        temp_limit_c: float,
        on_done: Callable[[Iterable[int]], None],
        on_healthy: Callable[[str], None],
    ):
        self._ranks = ranks
        self._temp_limit_c = temp_limit_c
        self._on_done = on_done
        self._on_healthy = on_healthy

    def evaluate(self, state: HealthState):
        self._on_done(self._ranks)
        nodes = _nodes_of(state, self._ranks)
        faults = gpu_faults(state, nodes, self._temp_limit_c)
        if faults:
            return Evict(
                reason=f"straggling ranks {self._ranks} run on nodes with GPU "
                f"faults: {_short(faults)}",
                target_nodes=sorted(faults),
            )
        self._on_healthy(
            f"ranks {self._ranks} straggle, but the GPUs of nodes "
            f"{[n[:8] for n in nodes]} show no fault; training continues"
        )
        return Noop()


class DiagnoseStragglers(Evaluator):
    """Diagnoses the nodes of the ranks whose ``straggler_score`` is below
    ``threshold``, once per rank and attempt."""

    def __init__(
        self,
        threshold: float,
        temp_limit_c: float,
        on_healthy: Callable[[str], None],
    ):
        self._threshold = threshold
        self._temp_limit_c = temp_limit_c
        self._on_healthy = on_healthy
        self.on_reset()

    def on_reset(self) -> None:
        self._diagnosed = set()

    def evaluate(self, state: HealthState):
        scores = {
            rank: r.metrics["straggler_score"]
            for rank, r in state.health_reports.items()
            if r.metrics.get("straggler_score", 1.0) < self._threshold
            and rank not in self._diagnosed
        }
        if not scores:
            return Noop()
        ranks = sorted(scores)
        temp_limit_c, on_healthy = self._temp_limit_c, self._on_healthy
        return Diagnose(
            reason=f"ranks {ranks} report straggler scores "
            f"{[scores[r] for r in ranks]}, below {self._threshold}",
            checks=[
                HealthCheck(
                    probe_creator=lambda: [
                        WorkerNodeProbe(),
                        GpuHealthEventProbe(),
                        GpuTemperatureProbe(),
                    ],
                    evaluator_creator=lambda: [
                        EvictOnConfirmedGpuFault(
                            ranks, temp_limit_c, self._diagnosed.update, on_healthy
                        )
                    ],
                )
            ],
        )


def straggler_check(
    threshold: float = 0.7,
    temp_limit_c: float = 85.0,
    on_healthy: Callable[[str], None] = logger.warning,
) -> PeriodicHealthCheck:
    return PeriodicHealthCheck(
        evaluator_creator=lambda: [
            DiagnoseStragglers(threshold, temp_limit_c, on_healthy)
        ],
        poll_interval_s=1.0,
    )
