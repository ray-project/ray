import logging
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Dict, List, NamedTuple, Optional, Union

from ray.train.health.probe import NodeProbe, ProbeResult

logger = logging.getLogger(__name__)


class ScheduledProbe(NamedTuple):
    """A ``NodeProbe`` and the seconds between its polls."""

    probe: NodeProbe
    poll_interval_s: float


@dataclass
class NodeMonitorStatus:
    """The latest results of ``NodeProbe``\\ s on one node.

    Attributes:
        probe_results: ``{probe name: ProbeResult}``.
        probe_errors: ``{probe name: exception}`` for the probes whose last poll
            raised. The monitor itself answered.
        error: Set by ``NodeMonitorGroup`` when the monitor itself could not
            answer: Ray's ``RayActorError`` if it died, or a ``TimeoutError`` if
            it has not answered for longer than the health-check timeout. Its
            probes' results are then unknown.
    """

    probe_results: Dict[str, ProbeResult] = field(default_factory=dict)
    probe_errors: Dict[str, Exception] = field(default_factory=dict)
    error: Optional[Exception] = None


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
        self._errors: Dict[str, Exception] = {}
        for scheduled_probe in probes:
            threading.Thread(
                target=self._sample,
                args=(scheduled_probe,),
                name=f"NodeMonitor-{scheduled_probe.probe.probe_name()}",
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

    def run_once(self, probes: List[NodeProbe], timeout_s: float) -> NodeMonitorStatus:
        """Poll each probe once, each in its own thread, and wait up to
        ``timeout_s`` for them.

        Args:
            probes: The probes to poll.
            timeout_s: Timeout in seconds to wait for them.

        Returns:
            The results of the probes that finished, and an error for each of
            the others.
        """
        outcomes: List[Optional[Union[ProbeResult, Exception]]] = [None] * len(probes)

        def run(i: int) -> None:
            try:
                outcomes[i] = self._poll(probes[i])
            except Exception as e:
                outcomes[i] = e

        threads = [
            threading.Thread(target=run, args=(i,), daemon=True)
            for i in range(len(probes))
        ]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + timeout_s
        for thread in threads:
            thread.join(max(deadline - time.monotonic(), 0))

        status = NodeMonitorStatus()
        for probe, outcome in zip(probes, list(outcomes)):
            name = probe.probe_name()
            if outcome is None:
                status.probe_errors[name] = TimeoutError(
                    f"Node probe {name} did not finish within {timeout_s} seconds."
                )
            elif isinstance(outcome, Exception):
                status.probe_errors[name] = outcome
            else:
                status.probe_results[name] = outcome
        return status

    def _sample(self, scheduled_probe: ScheduledProbe) -> None:
        """Poll the probe every ``poll_interval_s`` until the monitor is
        killed."""
        probe = scheduled_probe.probe
        name = probe.probe_name()
        while True:
            try:
                result = self._poll(probe)
            except Exception as e:
                with self._lock:
                    first_failure = name not in self._errors
                    self._errors[name] = e
                if first_failure:
                    logger.warning("Node probe %s failed.", name, exc_info=True)
            else:
                with self._lock:
                    self._latest[name] = result
                    self._errors.pop(name, None)
            time.sleep(scheduled_probe.poll_interval_s)

    @staticmethod
    def _poll(probe: NodeProbe) -> ProbeResult:
        """Poll a probe. A result without a ``timestamp_s`` is stamped with the
        time the probe returned."""
        result = probe.poll()
        if result.timestamp_s is None:
            result = replace(result, timestamp_s=time.time())
        return result
