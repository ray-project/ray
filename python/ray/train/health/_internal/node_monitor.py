"""The per-node agent that polls ``NodeProbe``\\ s outside the training workers.

One ``NodeMonitor`` actor runs on each node, so it keeps reporting when a worker
on its node hangs or dies.

- Periodic: a background thread polls each probe every ``poll_interval_s``;
  ``latest_results()`` returns the latest result of each.
- Once: ``poll_once()`` polls a probe in a subprocess with a hard timeout. A
  probe that hangs is killed, and the monitor keeps running.
"""
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import replace
from typing import Dict, List, Set

import ray
import ray.cloudpickle as cloudpickle
from ray.train.health.probe import NodeIdStr, NodeProbe, ProbeResult
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

_MIN_POLL_INTERVAL_S = 0.1

_CHILD = (
    "from ray.train.health._internal.node_monitor import _child_main; _child_main()"
)


def _stamped(result: ProbeResult) -> ProbeResult:
    if result.timestamp_s is None:
        return replace(result, timestamp_s=time.time())
    return result


def _child_pythonpath() -> str:
    """This process's import path, with the ``ray`` it imported first, so the
    child cannot pick up a different ``ray`` from elsewhere on the path."""
    ray_root = os.path.dirname(os.path.dirname(os.path.abspath(ray.__file__)))
    return os.pathsep.join([ray_root] + [p for p in sys.path if p and p != ray_root])


def _child_main() -> None:
    """Subprocess entry point: read a probe from stdin, write its result."""
    result_path = sys.argv[1]
    probe = cloudpickle.loads(sys.stdin.buffer.read())
    try:
        outcome = ("ok", probe.poll())
    except BaseException as e:  # noqa: BLE001
        outcome = ("error", f"{type(e).__name__}: {e}")
    with open(result_path, "wb") as f:
        f.write(cloudpickle.dumps(outcome))


class NodeMonitor:
    """Polls ``NodeProbe``\\ s on the node it runs on.

    Args:
        probes: The probes to poll periodically.
    """

    def __init__(self, probes: List[NodeProbe]):
        self._probes = list(probes)
        self._latest: Dict[str, ProbeResult] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._failing: Set[str] = set()
        self._thread = threading.Thread(
            target=self._run, name="ray-train-node-monitor", daemon=True
        )
        if self._probes:
            self._thread.start()

    def latest_results(self) -> Dict[str, ProbeResult]:
        """The latest result of each periodic probe.

        Returns:
            ``{probe name: ProbeResult}``. A probe that has not returned a
            result yet is absent.
        """
        with self._lock:
            return dict(self._latest)

    def poll_once(self, probe: NodeProbe, timeout_s: float) -> ProbeResult:
        """Poll a probe once, in a subprocess that is killed after a timeout.

        Args:
            probe: The probe to poll.
            timeout_s: Seconds to wait before killing the subprocess.

        Returns:
            The probe's result.

        Raises:
            TimeoutError: If the probe did not finish in time.
            RuntimeError: If the probe raised, or its subprocess died.
        """
        env = dict(os.environ, PYTHONPATH=_child_pythonpath())
        name = probe.probe_name()
        with tempfile.TemporaryDirectory() as tmp:
            result_path = os.path.join(tmp, "result")
            proc = subprocess.Popen(
                [sys.executable, "-c", _CHILD, result_path],
                stdin=subprocess.PIPE,
                env=env,
            )
            try:
                proc.communicate(cloudpickle.dumps(probe), timeout=timeout_s)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise TimeoutError(f"{name} did not finish in {timeout_s:.0f}s")
            if not os.path.exists(result_path):
                raise RuntimeError(f"{name} exited {proc.returncode} without a result")
            with open(result_path, "rb") as f:
                status, value = cloudpickle.loads(f.read())
        if status == "error":
            raise RuntimeError(value)
        return _stamped(value)

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        next_due = {id(p): 0.0 for p in self._probes}
        while not self._stop.is_set():
            for probe in self._probes:
                if time.monotonic() < next_due[id(probe)]:
                    continue
                interval = max(probe.poll_interval_s, _MIN_POLL_INTERVAL_S)
                next_due[id(probe)] = time.monotonic() + interval
                self._poll(probe)
            wait = min(next_due.values()) - time.monotonic()
            self._stop.wait(max(0.05, wait))

    def _poll(self, probe: NodeProbe) -> None:
        name = probe.probe_name()
        try:
            result = probe.poll()
        except Exception:
            if name not in self._failing:
                self._failing.add(name)
                logger.warning("Node probe %s failed.", name, exc_info=True)
            return
        self._failing.discard(name)
        with self._lock:
            self._latest[name] = _stamped(result)


def start_node_monitor(
    node_id: NodeIdStr, probes: List[NodeProbe]
) -> "ray.actor.ActorHandle":
    """Start a ``NodeMonitor`` actor on a node.

    The actor takes no CPUs, so it can share a node with workers that use all
    of them, and serves several calls at once, so a slow ``poll_once()`` does
    not block ``latest_results()``.

    Args:
        node_id: The node to start it on.
        probes: The probes it polls periodically.

    Returns:
        The actor handle.
    """
    return (
        ray.remote(NodeMonitor)
        .options(
            num_cpus=0,
            max_concurrency=8,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=False
            ),
        )
        .remote(probes)
    )
