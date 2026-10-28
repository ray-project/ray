from typing import Dict, List, NamedTuple, Sequence

from ray.train.health.check import PeriodicHealthCheck
from ray.train.health.probe import Probe


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
