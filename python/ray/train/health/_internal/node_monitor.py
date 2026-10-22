import threading
import time
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional, Union

import ray
from ray.train.health._internal.probe_utils import ScheduledProbe
from ray.train.health.probe import Probe, ProbeResult


@dataclass
class NodeMonitorStatus:
    """The latest results of the probes on one ``NodeMonitor``.

    Attributes:
        probe_results: ``{probe name: ProbeResult}``, the latest result of each
            probe, or ``{probe name: {key: ProbeResult}}`` for a
            ``ControllerProbe``. A failed poll gives a ``ProbeResult`` with its
            ``error``.
        monitor_error: Set by ``NodeMonitorGroup`` when the monitor itself could
            not answer: Ray's ``RayActorError`` if it died, or a ``TimeoutError``
            if it has not answered for longer than the health-check timeout.
            Each probe's result then carries this error.
    """

    probe_results: Dict[str, Union[ProbeResult, Dict[Any, ProbeResult]]] = field(
        default_factory=dict
    )
    monitor_error: Optional[Exception] = None


class NodeMonitor:
    """Samples probes on the node it runs on, and polls probes once when asked.

    Each probe is sampled in its own thread every ``poll_interval_s``, and
    ``poll_status()`` returns the latest result of each.

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
        """Get the latest result of every sampled probe.

        Returns:
            The latest result of every sampled probe.
        """
        with self._lock:
            return NodeMonitorStatus(probe_results=dict(self._latest))

    @ray.method(concurrency_group="on_demand")
    def poll_probes(self, probes: List[Probe]) -> NodeMonitorStatus:
        """Poll probes on demand, one call at a time, in a thread of their own,
        so that ``poll_status()`` still answers meanwhile.

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
        killed.

        Args:
            scheduled_probe: The probe and its interval.
        """
        probe = scheduled_probe.probe
        while True:
            result = self._poll(probe)
            with self._lock:
                self._latest[probe.probe_name()] = result
            time.sleep(scheduled_probe.poll_interval_s)

    @staticmethod
    def _poll(probe: Probe) -> Union[ProbeResult, Dict[Any, ProbeResult]]:
        """Poll a probe. Each result without a ``timestamp_s`` is stamped with
        the time the probe returned, and a poll that raises returns a result
        with the ``error``.

        Args:
            probe: The probe to poll.

        Returns:
            The probe's result, or a ``ControllerProbe``'s ``{key: result}``.
        """
        try:
            results = probe.poll()
        except Exception as e:
            return ProbeResult(timestamp_s=time.time(), error=e)
        now = time.time()

        def stamp(result: ProbeResult) -> ProbeResult:
            if result.timestamp_s is None:
                return replace(result, timestamp_s=now)
            return result

        if isinstance(results, ProbeResult):
            return stamp(results)
        return {key: stamp(result) for key, result in results.items()}
