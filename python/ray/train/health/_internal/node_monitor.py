import logging
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional

from ray.train.health.probe import PeriodicProbe, ProbeResult

logger = logging.getLogger(__name__)

# The shortest time between two samples of one probe.
_MIN_POLL_INTERVAL_S = 0.1


@dataclass
class NodeMonitorStatus:
    """The latest results of ``NodeProbe``\\ s on one node.

    Attributes:
        probe_results: ``{probe name: ProbeResult}``. A probe without a result
            yet is absent.
        probe_errors: ``{probe name: exception}`` for the probes whose last poll
            raised.
        error: An error reaching the monitor, if any.
    """

    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)
    probe_errors: Dict[str, Exception] = field(default_factory=dict)
    error: Optional[Exception] = None


class NodeMonitor:
    """Samples ``NodeProbe``\\ s on the node it runs on.

    Each probe is sampled in its own thread every ``poll_interval_s`` of its
    ``PeriodicProbe``, and ``poll_status()`` returns the latest sample of each.

    Args:
        probes: The ``NodeProbe``\\ s to sample, each wrapped in a
            ``PeriodicProbe``.
    """

    def __init__(self, probes: List[PeriodicProbe]):
        self._lock = threading.Lock()
        self._latest: Dict[str, ProbeResult] = {}
        self._errors: Dict[str, Exception] = {}
        for periodic_probe in probes:
            threading.Thread(
                target=self._sample,
                args=(periodic_probe,),
                name=f"NodeMonitor-{periodic_probe.probe.probe_name()}",
                daemon=True,
            ).start()

    def poll_status(self) -> NodeMonitorStatus:
        """Get the latest sample of every probe.

        Returns:
            The latest samples, and the probes whose last poll raised.
        """
        with self._lock:
            return NodeMonitorStatus(
                probe_results=dict(self._latest), probe_errors=dict(self._errors)
            )

    def _sample(self, periodic_probe: PeriodicProbe) -> None:
        """Poll the probe every ``poll_interval_s`` until the monitor is killed.
        A result without a ``timestamp_s`` is stamped with the time its poll
        finished."""
        probe = periodic_probe.probe
        name = probe.probe_name()
        interval_s = max(periodic_probe.poll_interval_s, _MIN_POLL_INTERVAL_S)
        while True:
            try:
                result = probe.poll()
            except Exception as e:
                with self._lock:
                    first_failure = name not in self._errors
                    self._errors[name] = e
                if first_failure:
                    logger.warning("Node probe %s failed.", name, exc_info=True)
            else:
                if result.timestamp_s is None:
                    result = replace(result, timestamp_s=time.time())
                with self._lock:
                    self._latest[name] = result
                    self._errors.pop(name, None)
            time.sleep(interval_s)
