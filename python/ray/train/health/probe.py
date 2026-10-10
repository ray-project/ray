import abc
from dataclasses import dataclass, field
from typing import Dict, Optional, Union

from ray.util.annotations import PublicAPI

NodeIdStr = str
# A world rank, a node ID, or another key a ControllerProbe chose.
ResultKey = Union[int, NodeIdStr]


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
    """Takes a health reading. Subclass ``WorkerProbe``, ``NodeProbe`` or
    ``ControllerProbe`` to choose where it runs.

    Attributes:
        name: The probe's name, which identifies its readings. Defaults to the
            class name. Set on the class.
    """

    name: Optional[str] = None

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


@PublicAPI(stability="alpha")
class ControllerProbe(Probe):
    """A probe that runs in the controller process."""

    @abc.abstractmethod
    def poll(self) -> Dict[ResultKey, ProbeResult]:
        """Take one reading of each node, worker or other item the probe
        observes.

        Returns:
            ``{node ID, world rank or key: ProbeResult}``.
        """
        raise NotImplementedError


def _check_probe(probe: Probe) -> None:
    if not isinstance(probe, Probe):
        raise TypeError(f"Expected a Probe instance, got {probe!r}.")


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class PeriodicProbe:
    """Wraps a probe to be polled every ``poll_interval_s`` seconds.

    Attributes:
        probe: The probe to poll.
        poll_interval_s: Seconds between polls.

    Raises:
        TypeError: If ``probe`` is not a ``Probe`` instance.
        ValueError: If ``poll_interval_s`` is not positive.
    """

    probe: Probe
    poll_interval_s: float = 10.0

    def __post_init__(self):
        _check_probe(self.probe)
        if self.poll_interval_s <= 0:
            raise ValueError(
                f"poll_interval_s must be positive, got {self.poll_interval_s}."
            )


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class OnDemandProbe:
    """Wraps a probe to be polled once, when requested.

    Attributes:
        probe: The probe to poll.

    Raises:
        TypeError: If ``probe`` is not a ``Probe`` instance.
    """

    probe: Probe

    def __post_init__(self):
        _check_probe(self.probe)
