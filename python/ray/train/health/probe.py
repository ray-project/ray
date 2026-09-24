"""Probe contracts for the Collect stage of the health loop."""
import abc
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from ray.util.annotations import DeveloperAPI

# ----------------------------------------------------------------------
# Base
# ----------------------------------------------------------------------


@DeveloperAPI
@dataclass
class ProbeResult:
    """What every probe returns.

    Attributes:
        metrics: Scalar signals for the whole entity.
        devices: Per-device signals, keyed by device index or UUID.
        events: Discrete, named occurrences since the last sample.
        passed: Pass/fail verdict. ``None`` means the probe takes no position.
        detail: Free text for humans.
        artifacts: Paths to files the probe wrote, such as stack dumps.
    """

    metrics: Dict[str, float] = field(default_factory=dict)
    devices: Dict[str, Dict[str, float]] = field(default_factory=dict)
    events: List[str] = field(default_factory=list)
    passed: Optional[bool] = None
    detail: str = ""
    artifacts: List[str] = field(default_factory=list)


@DeveloperAPI
class Probe(abc.ABC):
    """Base class for every collector.

    ``name`` is the key results are filed under. Defaults to the class name.
    """

    name: str = ""

    @classmethod
    def probe_name(cls) -> str:
        return cls.name or cls.__name__


# ----------------------------------------------------------------------
# WorkerProbe
# ----------------------------------------------------------------------


@DeveloperAPI
class WorkerProbe(Probe):
    """Runs in each train worker, on every poll. Must return quickly."""

    @abc.abstractmethod
    def poll(self) -> Optional[ProbeResult]:
        raise NotImplementedError


# ----------------------------------------------------------------------
# NodeProbe
# ----------------------------------------------------------------------


@DeveloperAPI
@dataclass(frozen=True)
class NodeContext:
    node_id: str


@DeveloperAPI
class NodeProbe(Probe):
    """Runs in each node's ``NodeMonitor``, outside the worker process."""

    interval_s: float = 10.0

    @abc.abstractmethod
    def poll(self, ctx: NodeContext) -> Optional[ProbeResult]:
        raise NotImplementedError


# ----------------------------------------------------------------------
# ClusterProbe
# ----------------------------------------------------------------------


@DeveloperAPI
@dataclass(frozen=True)
class ClusterContext:
    """The run a cluster probe is watching.

    Attributes:
        node_ids: Nodes hosting at least one rank of the worker group.
        rank_to_node: ``{world_rank: node_id}`` for the worker group.
    """

    node_ids: List[str] = field(default_factory=list)
    rank_to_node: Dict[int, str] = field(default_factory=dict)


@DeveloperAPI
class ClusterProbe(Probe):
    """Runs on the controller and reports many entities in one read.

    For sources that already describe the whole run from one place, such as
    NCCL RAS. Returns ``{entity_id: ProbeResult}``.

    ``entity`` names what the keys are. With ``"node"``, results are merged
    into ``HealthState.nodes``; any other value is opaque to the framework.
    Polled on a background thread, so it may keep state between polls.
    """

    interval_s: float = 10.0
    entity: str = "node"

    @abc.abstractmethod
    def poll(self, ctx: ClusterContext) -> Dict[str, ProbeResult]:
        raise NotImplementedError


# ----------------------------------------------------------------------
# OnDemandProbe
# ----------------------------------------------------------------------

NODE_SCOPE = "NODE"
WORKER_SCOPE = "WORKER"


@DeveloperAPI
@dataclass(frozen=True)
class NodeInfo:
    node_id: str
    ranks: List[int] = field(default_factory=list)


@DeveloperAPI
@dataclass(frozen=True)
class OnDemandProbeContext:
    """Passed to an on-demand probe when it is pushed.

    Attributes:
        entity_id: The node id for a node-scoped probe, the rank otherwise.
        node_id: The node the probe runs on.
        rank: The world rank, for a worker-scoped probe.
        nodes: Every node taking part in this push.
        timeout_s: The budget for this invocation.
        upload: ``upload(name, {filename: contents}) -> path`` into run
            storage, or ``None`` when there is no storage yet (pre-flight).
    """

    entity_id: str = ""
    node_id: str = ""
    rank: Optional[int] = None
    nodes: List[NodeInfo] = field(default_factory=list)
    timeout_s: float = 60.0
    upload: Optional[Callable[[str, Dict[str, str]], str]] = None


@DeveloperAPI
class OnDemandProbe(Probe):
    """An active check the controller pushes: a diagnostic or pre-flight check.

    Attributes:
        stop_workers: The check needs the accelerator, so workers are paused.
        timeout_s: Hard timeout.
        scope: ``NODE_SCOPE`` runs once per node; ``WORKER_SCOPE`` runs inside
            each targeted training worker, for checks that must attach to it.
    """

    stop_workers: bool = False
    timeout_s: float = 60.0
    scope: str = NODE_SCOPE

    @abc.abstractmethod
    def poll(self, ctx: OnDemandProbeContext) -> ProbeResult:
        raise NotImplementedError
