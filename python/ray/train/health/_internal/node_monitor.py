import threading
import time
from dataclasses import dataclass, field, replace
from typing import Dict, List, NamedTuple, Optional

from ray.train.health.probe import NodeProbe, ProbeResult


class ScheduledProbe(NamedTuple):
    """A ``NodeProbe`` and the seconds between its polls."""

    probe: NodeProbe
    poll_interval_s: float


@dataclass
class NodeMonitorStatus:
    """The latest results of ``NodeProbe``\\ s on one node.

    Attributes:
        probe_results: ``{probe name: ProbeResult}``, the latest result of each
            probe. A failed poll's result carries its ``error``.
        monitor_error: Set by ``NodeMonitorGroup`` when the monitor itself could
            not answer: Ray's ``RayActorError`` if it died, or a ``TimeoutError``
            if it has not answered for longer than the health-check timeout.
            Its probes' results are then unknown.
    """

    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)
    monitor_error: Optional[Exception] = None


class NodeMonitor:
    """Samples ``NodeProbe``\\ s on the node it runs on, and polls probes once
    when asked.

    Each probe is sampled in its own thread every ``poll_interval_s``, and
    ``poll_status()`` returns the latest sample of each.

    Args:
        probes: The ``NodeProbe``\\ s to sample, each with its poll interval.
    """

    def __init__(self, probes: List[ScheduledProbe]):
        self._lock = threading.Lock()
        self._latest: Dict[str, ProbeResult] = {}
        for scheduled_probe in probes:
            name = scheduled_probe.probe.probe_name()
            threading.Thread(
                target=self._sample,
                args=(scheduled_probe,),
                name=f"NodeMonitor-{name}-{scheduled_probe.poll_interval_s:g}s",
                daemon=True,
            ).start()

    def poll_status(self) -> NodeMonitorStatus:
        """Get the latest sample of every probe.

        Returns:
            The latest sample of every probe.
        """
        with self._lock:
            return NodeMonitorStatus(probe_results=dict(self._latest))

    def poll_probes(self, probes: List[NodeProbe]) -> NodeMonitorStatus:
        """Poll probes on demand.

        Args:
            probes: The probes to poll.

        Returns:
            The result of each probe.
        """
        return NodeMonitorStatus(
            probe_results={probe.probe_name(): self._poll(probe) for probe in probes}
        )

    def _sample(self, scheduled_probe: ScheduledProbe) -> None:
        """Poll the probe every ``poll_interval_s`` until the monitor is
        killed."""
        probe = scheduled_probe.probe
        while True:
            result = self._poll(probe)
            with self._lock:
                self._latest[probe.probe_name()] = result
            time.sleep(scheduled_probe.poll_interval_s)

    @staticmethod
    def _poll(probe: NodeProbe) -> ProbeResult:
        """Poll a probe. A result without a ``timestamp_s`` is stamped with the
        time the probe returned, and a poll that raises returns a result with
        the ``error``."""
        try:
            result = probe.poll()
        except Exception as e:
            return ProbeResult(timestamp_s=time.time(), error=e)
        if result.timestamp_s is None:
            result = replace(result, timestamp_s=time.time())
        return result
