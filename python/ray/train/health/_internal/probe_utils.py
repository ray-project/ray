import threading
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)

from ray.train.health.check import PeriodicHealthCheck
from ray.train.health.probe import (
    ControllerProbe,
    NodeProbe,
    Probe,
    ProbeResult,
    WorkerProbe,
)


class ScheduledProbe(NamedTuple):
    """A probe and the seconds between its polls."""

    probe: Probe
    poll_interval_s: float


def merge_probes(checks: Sequence[PeriodicHealthCheck]) -> List[ScheduledProbe]:
    """Create the checks' probes, one per name, each polled at the shortest
    ``poll_interval_s`` among the checks that have it.

    Args:
        checks: The checks.

    Returns:
        One ``ScheduledProbe`` per probe name.

    Raises:
        ValueError: If two different probe classes have the same name.
    """
    probes: Dict[str, Probe] = {}
    intervals_s: Dict[str, float] = {}
    for check in checks:
        for probe in check.probe_creator():
            name = probe.probe_name()
            existing = probes.setdefault(name, probe)
            if type(existing) is not type(probe):
                raise ValueError(
                    f"Two probes have the same name ({name!r}) but different "
                    f"types ({type(existing).__name__} and {type(probe).__name__})."
                )
            intervals_s[name] = min(
                intervals_s.get(name, check.poll_interval_s), check.poll_interval_s
            )
    return [ScheduledProbe(probe, intervals_s[name]) for name, probe in probes.items()]


def split_probes_by_type(
    probes: Iterable[Probe],
) -> Tuple[List[WorkerProbe], List[NodeProbe], List[ControllerProbe]]:
    """Split probes by where they run.

    Args:
        probes: The probes.

    Returns:
        ``(worker probes, node probes, controller probes)``.
    """
    probes = list(probes)
    return (
        [p for p in probes if isinstance(p, WorkerProbe)],
        [p for p in probes if isinstance(p, NodeProbe)],
        [p for p in probes if isinstance(p, ControllerProbe)],
    )


class OnDemandPolls:
    """Polls probes once, each in a thread of its own, so that one slow probe
    holds up no other, and gives them ``timeout_s`` to return. A probe still
    running after that gets a result with a ``TimeoutError``. Its thread can't
    be stopped, so it's left to finish in the background.

    Args:
        probes: The probes to poll.
        poll: Polls one probe and returns its result. It must not raise.
        timeout_s: How long the probes have to return.
    """

    def __init__(
        self, probes: Iterable[Probe], poll: Callable[[Probe], Any], timeout_s: float
    ):
        self._timeout_s = timeout_s
        self._deadline = time.monotonic() + timeout_s
        self._polls = [_ProbePoll(probe, poll) for probe in probes]
        for probe_poll in self._polls:
            probe_poll.start()

    def results(self, wait: bool = False) -> Optional[Dict[str, Any]]:
        """Get each probe's result.

        Args:
            wait: Wait for the probes still running, until ``timeout_s`` passes.

        Returns:
            ``{probe name: result}``, with a ``TimeoutError`` result for each
            probe still running after ``timeout_s``. ``None`` if a probe is
            still running within ``timeout_s`` and ``wait`` is false.
        """
        for probe_poll in self._polls:
            remaining_s = self._deadline - time.monotonic()
            if wait:
                probe_poll.join(max(0.0, remaining_s))
            elif probe_poll.is_alive() and remaining_s > 0:
                return None
        return {p.probe.probe_name(): p.result(self._timeout_s) for p in self._polls}


class _ProbePoll(threading.Thread):
    """A daemon thread that polls one probe once."""

    def __init__(self, probe: Probe, poll: Callable[[Probe], Any]):
        super().__init__(name=f"OnDemandProbe-{probe.probe_name()}", daemon=True)
        self.probe = probe
        self._poll = poll
        self._result: Any = None

    def run(self) -> None:
        self._result = self._poll(self.probe)

    def result(self, timeout_s: float) -> Any:
        """The probe's result, or one with a ``TimeoutError`` if it's still
        running after ``timeout_s``."""
        if self.is_alive():
            return ProbeResult(
                timestamp_s=time.time(),
                error=TimeoutError(
                    f"{self.probe.probe_name()}.poll() did not return within "
                    f"{timeout_s:g}s."
                ),
            )
        return self._result
