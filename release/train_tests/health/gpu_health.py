"""GPU health checks on ``ray.train.health``, with GPU health events that can
be mocked.

An event has the fields of NVSentinel's ``HealthEvent``
(``data-models/protobufs/health_event.proto``), as ``xid_event()`` builds it::

    {"checkName": "GpuXidError", "isFatal": True, "isHealthy": False,
     "errorCode": ["79"], "entitiesImpacted": [{"entityType": "GPU", "entityValue": "3"}],
     "message": "GPU has fallen off the bus", "recommendedAction": "RESTART_VM"}

As in NVSentinel, an event is fatal when its recommended action isn't
``NONE``, and a later event with ``isHealthy`` clears it.
``harness.inject_gpu_event(node_id, event)`` writes one on a node, and the
probes here read it there, so the tests need no broken GPU. NVSentinel injects
the same faults into a real DCGM with ``dcgmi test --inject --gpuid N -f 230
-v 79`` (field 230 is the XID). Without an injected event,
``GpuTemperatureProbe`` reads the real GPUs with ``nvidia-smi`` when it can.

Scripts using this module call ``harness.ship_by_value(gpu_health)``.
"""
import json
import logging
import os
import subprocess
from typing import Dict, List, Optional

from ray.train.health import (
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthState,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
    Reattempt,
    WorkerProbe,
)

logger = logging.getLogger("ray.train")

GPU_EVENTS_DIR = "/tmp/ray_train_health_gpu_events"

# The recommended action of a few XIDs, from NVIDIA's XID catalog as
# NVSentinel's syslog health monitor maps it. NONE means not fatal.
XID_ACTIONS = {
    13: ("NONE", "graphics engine exception"),
    31: ("NONE", "GPU memory page fault"),
    48: ("COMPONENT_RESET", "double-bit ECC error"),
    79: ("RESTART_VM", "GPU has fallen off the bus"),
    94: ("CONTACT_SUPPORT", "contained ECC error"),
    95: ("RESTART_VM", "uncontained ECC error"),
    119: ("COMPONENT_RESET", "GSP RPC timeout"),
}


def xid_event(xid: int, gpu: int) -> Dict:
    """An NVSentinel health event for ``xid`` on GPU ``gpu``."""
    action, message = XID_ACTIONS[xid]
    return {
        "checkName": "GpuXidError",
        "isFatal": action != "NONE",
        "isHealthy": False,
        "errorCode": [str(xid)],
        "entitiesImpacted": [{"entityType": "GPU", "entityValue": str(gpu)}],
        "message": message,
        "recommendedAction": action,
    }


def event_path(node_id: str) -> str:
    """Where a GPU health event injected on ``node_id`` waits, on that node."""
    return os.path.join(GPU_EVENTS_DIR, f"{node_id}.json")


def read_injected_event() -> Optional[Dict]:
    """The GPU health event injected on this node, if any."""
    import ray

    path = event_path(ray.get_runtime_context().get_node_id())
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _nvidia_smi_query(field: str) -> List[float]:
    proc = subprocess.run(
        ["nvidia-smi", f"--query-gpu={field}", "--format=csv,noheader,nounits"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    return [float(v) for v in proc.stdout.split() if v.strip()]


# ----------------------------------------------------------------------
# In-flight: health events
# ----------------------------------------------------------------------
class GpuHealthEventProbe(NodeProbe):
    """This node's latest GPU health event, as NVSentinel's health monitors
    would report it: here, only an injected one."""

    def poll(self) -> ProbeResult:
        event = read_injected_event()
        if event is None or "checkName" not in event or event.get("isHealthy"):
            return ProbeResult()
        codes = [int(c) for c in event.get("errorCode", []) if str(c).isdigit()]
        return ProbeResult(
            metrics={
                "is_fatal": float(bool(event.get("isFatal"))),
                "error_code": float(codes[0]) if codes else 0.0,
            },
            detail=json.dumps(event),
        )


class EvictOnFatalGpuEvent(Evaluator):
    """Evicts the nodes with a fatal GPU health event."""

    def evaluate(self, state: HealthState):
        fatal = {
            node: r.detail
            for node, r in state.probe_results.get("GpuHealthEventProbe", {}).items()
            if r.metrics.get("is_fatal")
        }
        if not fatal:
            return Noop()
        return Evict(
            reason=f"fatal GPU health events: {fatal}", target_nodes=sorted(fatal)
        )


def gpu_health_event_check(poll_interval_s: float = 1.0) -> PeriodicHealthCheck:
    return PeriodicHealthCheck(
        probe_creator=lambda: [GpuHealthEventProbe()],
        evaluator_creator=lambda: [EvictOnFatalGpuEvent()],
        poll_interval_s=poll_interval_s,
    )


# ----------------------------------------------------------------------
# In-flight: a slow node, diagnosed by its temperature
# ----------------------------------------------------------------------
class GpuSpeedProbe(NodeProbe):
    """This node's relative GPU speed, 1.0 being full speed: the injected
    ``relative_speed``, else 1.0."""

    def poll(self) -> ProbeResult:
        event = read_injected_event() or {}
        speed = float(event.get("relative_speed", 1.0))
        return ProbeResult(metrics={"relative_speed": speed})


class GpuTemperatureProbe(NodeProbe):
    """This node's hottest GPU: the injected ``temp_c``, or ``nvidia-smi``."""

    def poll(self) -> ProbeResult:
        event = read_injected_event() or {}
        if "temp_c" in event:
            return ProbeResult(metrics={"temp_c": float(event["temp_c"])})
        try:
            return ProbeResult(
                metrics={"temp_c": max(_nvidia_smi_query("temperature.gpu"))}
            )
        except Exception as e:  # noqa: BLE001
            return ProbeResult(error=e)


class ReattemptOnAHotNode(Evaluator):
    """Reattempts when a node's hottest GPU is at ``limit_c`` or more, or at
    least ``margin_c`` hotter than another node's."""

    def __init__(self, limit_c: float = 85.0, margin_c: float = 15.0):
        self._limit_c = limit_c
        self._margin_c = margin_c

    def evaluate(self, state: HealthState):
        temps = {
            node: r.metrics["temp_c"]
            for node, r in state.probe_results.get("GpuTemperatureProbe", {}).items()
            if "temp_c" in r.metrics
        }
        if not temps:
            return Noop()
        hottest = max(temps, key=temps.get)
        coolest = min(temps.values())
        if temps[hottest] < self._limit_c and temps[hottest] - coolest < self._margin_c:
            logger.info("No node is too hot: %s", temps)
            return Noop()
        return Reattempt(
            reason=f"node {hottest[:8]} is {temps[hottest]:.0f}C: "
            f"{({n[:8]: t for n, t in temps.items()})}"
        )


class DiagnoseASlowNode(Evaluator):
    """Diagnoses the nodes' temperatures when a node starts to run slower than
    ``threshold`` of full speed."""

    def __init__(self, threshold: float = 0.7):
        self._threshold = threshold
        self._had_slow_node = False

    def on_reset(self) -> None:
        self._had_slow_node = False

    def evaluate(self, state: HealthState):
        speeds = {
            node: r.metrics["relative_speed"]
            for node, r in state.probe_results.get("GpuSpeedProbe", {}).items()
            if "relative_speed" in r.metrics
        }
        slow = sorted(n for n, s in speeds.items() if s < self._threshold)
        started = bool(slow) and not self._had_slow_node
        self._had_slow_node = bool(slow)
        if not started:
            return Noop()
        return Diagnose(
            reason=f"nodes {[n[:8] for n in slow]} run slower than the others",
            checks=[
                HealthCheck(
                    probe_creator=lambda: [GpuTemperatureProbe()],
                    evaluator_creator=lambda: [ReattemptOnAHotNode()],
                )
            ],
        )


def slow_node_check(poll_interval_s: float = 1.0) -> PeriodicHealthCheck:
    return PeriodicHealthCheck(
        probe_creator=lambda: [GpuSpeedProbe()],
        evaluator_creator=lambda: [DiagnoseASlowNode()],
        poll_interval_s=poll_interval_s,
    )


# ----------------------------------------------------------------------
# Pre-flight: an all-reduce across every worker, with training stopped
# ----------------------------------------------------------------------
class AllReduceProbe(WorkerProbe):
    """All-reduces a one on every worker, over the run's process group, and
    reports the sum and this worker's node."""

    def poll(self) -> ProbeResult:
        import torch
        import torch.distributed as dist

        import ray

        device = "cuda" if torch.cuda.is_available() else "cpu"
        ones = torch.ones(1, device=device)
        dist.all_reduce(ones)
        return ProbeResult(
            metrics={"sum": ones.item(), "world_size": float(dist.get_world_size())},
            detail=ray.get_runtime_context().get_node_id(),
        )


class EvictNodesFailingAllReduce(Evaluator):
    """Evicts the nodes of the workers whose all-reduce failed or summed wrong."""

    def evaluate(self, state: HealthState):
        results = state.probe_results.get("AllReduceProbe", {})
        bad = sorted(
            {
                r.detail or f"rank {rank}"
                for rank, r in results.items()
                if r.error is not None
                or r.metrics.get("sum") != r.metrics.get("world_size")
            }
        )
        logger.info(
            "Pre-flight all-reduce: %s",
            {
                rank: r.metrics.get("sum", str(r.error))
                for rank, r in sorted(results.items())
            },
        )
        if not bad:
            return Noop()
        return Evict(
            reason=f"the pre-flight all-reduce failed on {bad}", target_nodes=bad
        )


def all_reduce_preflight_check() -> HealthCheck:
    return HealthCheck(
        probe_creator=lambda: [AllReduceProbe()],
        evaluator_creator=lambda: [EvictNodesFailingAllReduce()],
        stop_workers=True,
    )
