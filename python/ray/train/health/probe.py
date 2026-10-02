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
        metrics: Readings by metric name, for example ``{"gpu0_temp_c": 71.0}``.
            The probe chooses the names, and Ray Train does not interpret them.
            Names only need to be unique within one probe, because each probe's
            results are stored separately. Each value is the reading at the
            time of the poll.
        timestamp_s: When Ray Train collected the result, in seconds since the
            epoch. Set by Ray Train.
    """

    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp_s: Optional[float] = None


@PublicAPI(stability="alpha")
class Probe(abc.ABC):
    """Base class for probes. Subclass ``WorkerProbe``, ``NodeProbe`` or
    ``ControllerProbe``.

    Ray Train keeps the latest result of each probe, per worker, node or
    ``ControllerProbe`` key, in memory. Results are dropped when a new set of
    workers starts.

    Attributes:
        name: The name results are stored under. Defaults to the class name.
            Set on the class.
        poll_interval_s: Seconds between periodic polls.
    """

    name: Optional[str] = None
    poll_interval_s: float = 10.0

    @classmethod
    def probe_name(cls) -> str:
        return cls.name or cls.__name__


@PublicAPI(stability="alpha")
class WorkerProbe(Probe):
    """A probe that runs in each training worker process.

    It is polled when Ray Train polls the workers, so it is not polled more
    often than that, even with a shorter ``poll_interval_s``.
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
    """A probe that runs on each node, outside the training worker processes."""

    @abc.abstractmethod
    def poll(self) -> ProbeResult:
        """Poll this node once.

        Returns:
            The result.
        """
        raise NotImplementedError


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class ControllerProbeContext:
    """What a ``ControllerProbe`` is told about the current workers.

    Attributes:
        rank_to_node: ``{world rank: node ID}`` for every worker.
    """

    rank_to_node: Dict[int, NodeIdStr] = field(default_factory=dict)


@PublicAPI(stability="alpha")
class ControllerProbe(Probe):
    """A probe that runs in the Ray Train controller process.

    One instance is used for the whole run, so it can keep state across polls.
    One poll returns results for several keys the probe chooses, such as one per
    NCCL communicator.
    """

    @abc.abstractmethod
    def poll(self, ctx: ControllerProbeContext) -> Dict[str, ProbeResult]:
        """Poll once.

        Args:
            ctx: The current workers.

        Returns:
            ``{key: ProbeResult}``.
        """
        raise NotImplementedError
