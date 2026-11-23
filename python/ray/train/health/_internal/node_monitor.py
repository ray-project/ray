import threading
import time
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional, Union

import ray
from ray.train.health._internal.probe_utils import OnDemandPolls, ScheduledProbe
from ray.train.health.probe import Probe, ProbeResult


@dataclass
class NodeMonitorStatus:
    """The latest results of the probes on one node.

    Attributes:
        probe_results: ``{probe name: ProbeResult}``, the latest result of each
            probe, or ``{key: ProbeResult}`` for a ``ControllerProbe``. A failed
            poll's result is a ``ProbeResult`` that carries its ``error``.
        monitor_error: Set by ``NodeMonitorGroup`` when the monitor itself could
            not answer: Ray's ``RayActorError`` if it died, or a ``TimeoutError``
            if it has not answered in time. Its probes' results are then
            unknown.
    """

    probe_results: Dict[str, Union[ProbeResult, Dict[Any, ProbeResult]]] = field(
        default_factory=dict
    )
    monitor_error: Optional[Exception] = None


class NodeMonitor:
    """Samples probes on the node it runs on, and polls probes once when asked.

    Each probe is sampled in its own thread every ``poll_interval_s``, and
    ``poll_status()`` returns the latest sample of each.

    Args:
        probes: The probes to sample, each with its poll interval.
    """

    def __init__(self, probes: List[ScheduledProbe]):
        self._lock = threading.Lock()
        self._latest: Dict[str, Union[ProbeResult, Dict[Any, ProbeResult]]] = {}
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

    @ray.method(concurrency_group="on_demand")
    def poll_probes(self, probes: List[Probe], timeout_s: float) -> NodeMonitorStatus:
        """Poll probes on demand, one call at a time, so that ``poll_status()``
        still answers meanwhile. Each probe is polled in a thread of its own,
        and one still running after ``timeout_s`` gets a result with a
        ``TimeoutError``.

        Args:
            probes: The probes to poll.
            timeout_s: How long the probes have to return.

        Returns:
            The result of each probe.
        """
        polls = OnDemandPolls(probes, self._poll, timeout_s)
        return NodeMonitorStatus(probe_results=polls.results(wait=True))

    def _sample(self, scheduled_probe: ScheduledProbe) -> None:
        """Poll the probe every ``poll_interval_s``, from the start of one poll
        to the start of the next, until the monitor is killed.

        Args:
            scheduled_probe: The probe and its interval.
        """
        probe = scheduled_probe.probe
        while True:
            started = time.monotonic()
            result = self._poll(probe)
            with self._lock:
                self._latest[probe.probe_name()] = result
            elapsed_s = time.monotonic() - started
            time.sleep(max(0.0, scheduled_probe.poll_interval_s - elapsed_s))

    @staticmethod
    def _poll(probe: Probe) -> Union[ProbeResult, Dict[Any, ProbeResult]]:
        """Poll a probe. A result without a ``timestamp_s`` is stamped with the
        time the probe returned, and a poll that raises returns a result with
        the ``error``.

        Args:
            probe: The probe to poll.

        Returns:
            The probe's result.
        """
        try:
            result = probe.poll()
            if isinstance(result, ProbeResult):
                return _stamp(result)
            return {key: _stamp(r) for key, r in result.items()}
        except Exception as e:
            return ProbeResult(timestamp_s=time.time(), error=e)


def _stamp(result: ProbeResult) -> ProbeResult:
    if result.timestamp_s is None:
        return replace(result, timestamp_s=time.time())
    return result
