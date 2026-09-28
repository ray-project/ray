import abc
from dataclasses import dataclass, field
from typing import Dict, Optional

from ray.util.annotations import PublicAPI

NodeIdStr = str


@PublicAPI(stability="alpha")
@dataclass
class ProbeResult:
    """The result of polling a probe once.

    Attributes:
        metrics: Readings by name, for example ``{"gpu0_temp_c": 71.0}``. Each
            value is the reading at the time of the poll.
        timestamp_s: When Ray Train collected the result, in seconds since the
            epoch. Set by Ray Train.
    """

    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp_s: Optional[float] = None


@PublicAPI(stability="alpha")
class Probe(abc.ABC):
    """Base class for probes. Subclass ``WorkerProbe`` or ``NodeProbe``.

    Ray Train keeps the latest result of each probe from each worker and node,
    in memory. Results are dropped when a new set of workers starts.

    Attributes:
        name: The name results are stored under. Defaults to the class name.
            Set on the class.
    """

    name: Optional[str] = None

    @classmethod
    def probe_name(cls) -> str:
        return cls.name or cls.__name__


@PublicAPI(stability="alpha")
class WorkerProbe(Probe):
    """A probe that runs in each training worker process.

    When polled periodically, it is polled each time Ray Train polls the
    workers.
    """

    @abc.abstractmethod
    def poll(self) -> ProbeResult:
        """Poll this worker once.

        Returns:
            The result.
        """
        raise NotImplementedError


@PublicAPI(stability="alpha")
class NodeProbe(Probe):
    """A probe that runs on each node, outside the training worker processes.

    Attributes:
        poll_interval_s: Seconds between periodic polls.
    """

    poll_interval_s: float = 10.0

    @abc.abstractmethod
    def poll(self) -> ProbeResult:
        """Poll this node once.

        Returns:
            The result.
        """
        raise NotImplementedError
