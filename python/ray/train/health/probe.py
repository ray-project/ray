import abc
from dataclasses import dataclass, field
from typing import Dict, Optional

from ray.util.annotations import PublicAPI

NodeIdStr = str


@PublicAPI(stability="alpha")
@dataclass
class ProbeResult:
    """One reading returned by a probe's ``poll()``.

    Attributes:
        metrics: Readings by metric name, for example ``{"gpu0_temp_c": 71.0}``.
            The probe chooses the names; they only need to be unique within
            the probe. Each value is the reading at the time of the poll.
        timestamp_s: When the reading was taken, in seconds since the epoch.
            Leave it unset to use the time ``poll()`` returned.
    """

    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp_s: Optional[float] = None


@PublicAPI(stability="alpha")
class Probe(abc.ABC):
    """Takes a health reading. Subclass ``WorkerProbe`` or ``NodeProbe`` to
    choose where it runs.

    Attributes:
        name: The probe's name, which identifies its readings. Defaults to the
            class name. Set on the class.
        poll_interval_s: Seconds between polls when the probe is polled
            periodically. Not used when it is polled once.
    """

    name: Optional[str] = None
    poll_interval_s: float = 10.0

    @classmethod
    def probe_name(cls) -> str:
        return cls.name or cls.__name__


@PublicAPI(stability="alpha")
class WorkerProbe(Probe):
    """A probe that runs in each worker process."""

    @abc.abstractmethod
    def poll(self) -> ProbeResult:
        """Take one reading of this worker.

        Returns:
            The reading.
        """
        raise NotImplementedError


@PublicAPI(stability="alpha")
class NodeProbe(Probe):
    """A probe that runs on the node."""

    @abc.abstractmethod
    def poll(self) -> ProbeResult:
        """Take one reading of this node.

        Returns:
            The reading.
        """
        raise NotImplementedError
