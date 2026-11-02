import abc
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

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
        error: The exception that kept the probe from taking the reading, if
            any.
    """

    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp_s: Optional[float] = None
    error: Optional[Exception] = None


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
        """Return the name that identifies this probe's readings.

        Returns:
            The ``name`` class attribute, or the class name if ``name`` isn't set.
        """
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
    def poll(self) -> Dict[Any, ProbeResult]:
        """Take one reading of each node, worker or other item the probe
        observes.

        Returns:
            ``{node ID, world rank or key: ProbeResult}``.
        """
        raise NotImplementedError
