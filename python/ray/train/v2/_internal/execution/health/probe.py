"""Probe contracts for the Collect stage of the health loop.

See the REP: reps/2026-08-11-ray-train-monitoring-and-diagnostics.

Every collector is a :class:`Probe` and returns a :class:`ProbeResult`. Probes
come in four flavors, distinguished by *where* they run and *when*:

- :class:`WorkerProbe` runs inside each train worker on every controller poll.
- :class:`NodeProbe` runs inside each node's ``NodeMonitor`` on a fixed interval.
- :class:`ClusterProbe` runs controller-side and reports for many nodes at once.
- :class:`OnDemandProbe` runs only when the controller pushes it (a diagnostic).

``ClusterProbe`` is an addition to the REP. Some health sources are keyed per
node but published in one place out-of-band -- an NVSentinel/K8s control plane,
a CSP maintenance API, a metrics backend. Polling those from every NodeMonitor
would fan one logical read out into N API calls, and would go blind on exactly
the nodes that have died. A cluster probe reads once and files its results into
the same per-node map, so evaluators cannot tell the difference.
"""
import abc
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class ProbeResult:
    """What every probe returns.

    Attributes:
        metrics: Scalar signals for the whole entity, e.g. ``{"ram_used_pct": 61.0}``.
        devices: Per-device signals, keyed by device index or UUID, e.g.
            ``{"0": {"temp_c": 64.0, "ecc_uncorrectable": 0.0}}``.
        events: Discrete, named occurrences since the last sample, e.g.
            ``["link_down:mlx5_1"]``.
        passed: Pass/fail verdict, for probes that have one. ``None`` means the
            probe only reports signals and takes no position.
        detail: Free text for humans, surfaced in logs and events.
        artifacts: Paths to files the probe wrote (stack dumps, traces, vendor
            diagnostic output). Diagnostics that produce files are the norm
            rather than the exception, and a path is the only part of a
            100 MB core dump that belongs in a health signal.
    """

    metrics: Dict[str, float] = field(default_factory=dict)
    devices: Dict[str, Dict[str, float]] = field(default_factory=dict)
    events: List[str] = field(default_factory=list)
    passed: Optional[bool] = None
    detail: str = ""
    artifacts: List[str] = field(default_factory=list)


#: An on-demand probe runs either on the node, in its ``NodeMonitor`` (so it
#: still answers when the training process is wedged), or inside the training
#: worker itself (for anything that must attach to that process).
NODE_SCOPE = "NODE"
WORKER_SCOPE = "WORKER"


@DeveloperAPI
class ProbeDegraded(Exception):
    """Raised by a probe that can never succeed for the rest of this run.

    A missing binary or an unsupported vendor version is not a transient
    failure, and retrying it every poll is pure noise. The manager drops a
    probe that raises this and keeps the run going. Transient failures should
    return ``None`` or raise anything else, which is retried next poll.
    """


@DeveloperAPI
class Probe(abc.ABC):
    """Base class for every collector.

    ``name`` is the key a result is filed under, and the key an evaluator looks
    it up by. It defaults to the class name.
    """

    name: str = ""

    @classmethod
    def probe_name(cls) -> str:
        return cls.name or cls.__name__


@DeveloperAPI
@dataclass(frozen=True)
class NodeContext:
    """What a node-scoped probe is told about where it is running."""

    node_id: str


@DeveloperAPI
@dataclass(frozen=True)
class NodeInfo:
    """One node participating in an on-demand probe."""

    node_id: str
    node_ip: str = ""
    ranks: List[int] = field(default_factory=list)


@DeveloperAPI
@dataclass(frozen=True)
class OnDemandProbeContext:
    """What an on-demand probe is told when the controller pushes it.

    Attributes:
        entity_id: What this invocation is about -- a node id for a node-scoped
            probe, a world rank for a worker-scoped one.
        node_id: The node the probe is running on, either way.
        rank: The world rank, for a worker-scoped probe; ``None`` otherwise.
        nodes: Every node taking part in this diagnostic. Collective checks
            build their world from this.
        timeout_s: The budget for this invocation.
        upload: ``upload(name, {filename: contents}) -> path``, writing files to
            the run's storage and returning where they landed. A diagnostic that
            produces bulk output (a stack dump, an ``nvidia-smi -q`` report)
            uploads it and returns the path in ``ProbeResult.artifacts``, rather
            than carrying megabytes back through the health loop.
    """

    entity_id: str = ""
    node_id: str = ""
    rank: Optional[int] = None
    nodes: List[NodeInfo] = field(default_factory=list)
    timeout_s: float = 60.0
    upload: Optional[Callable[[str, Dict[str, str]], str]] = None


@DeveloperAPI
class WorkerProbe(Probe):
    """Runs in each train worker, on every poll.

    For signals readable from inside the worker process without touching the
    training loop. Must return quickly: it runs on the worker's poll path.
    """

    @abc.abstractmethod
    def poll(self) -> Optional[ProbeResult]:
        raise NotImplementedError


@DeveloperAPI
class NodeProbe(Probe):
    """Runs in each ``NodeMonitor``'s monitor loop, outside the worker process.

    Keeps reporting when the worker on that node hangs or dies.
    """

    interval_s: float = 10.0

    @abc.abstractmethod
    def poll(self, ctx: NodeContext) -> Optional[ProbeResult]:
        raise NotImplementedError


@DeveloperAPI
@dataclass(frozen=True)
class ClusterContext:
    """What a controller-side probe is told about the run it is watching."""

    node_ids: List[str] = field(default_factory=list)


@DeveloperAPI
class ClusterProbe(Probe):
    """Runs on the controller and reports many entities in one read.

    For out-of-band health sources that are already aggregated somewhere else.
    Returns ``{entity_id: ProbeResult}``; entities the source says nothing about
    are simply absent, which is not the same as a clean bill of health.

    ``entity`` names what those keys are. ``"node"`` is the common case and gets
    one piece of special treatment: results land in ``HealthState.nodes``
    alongside every ``NodeProbe`` sample for the same host, so an evaluator can
    join a vendor control plane against an on-host probe with one lookup. Any
    other value is opaque to the framework -- only the evaluator reading that
    probe knows what its keys mean. NCCL RAS keys by communicator, because a
    communicator is the thing that hangs.

    A cluster probe may hold sample-to-sample state (a previous reading, to emit
    a delta) because it runs on the controller, outside the failure domain of
    what it measures. A ``WorkerProbe`` or ``NodeProbe`` must not: its history
    dies with the thing it is watching.
    """

    interval_s: float = 10.0
    entity: str = "node"

    @abc.abstractmethod
    def poll(self, ctx: ClusterContext) -> Dict[str, ProbeResult]:
        raise NotImplementedError


@DeveloperAPI
class OnDemandProbe(Probe):
    """An active check the controller pushes, a.k.a. a diagnostic.

    Attributes:
        stop_workers: True if the check needs the accelerator, so the
            controller pauses workers before running it.
        timeout_s: Hard timeout. The probe runs in a subprocess so a hang
            cannot take the ``NodeMonitor`` down with it.
        scope: Where it runs. ``NODE`` runs it in the target node's
            ``NodeMonitor``; ``WORKER`` runs it inside the training worker
            process itself. Some diagnostics have no node-level equivalent --
            a ``py-spy`` dump has to attach to the training process, and only
            the worker knows which process that is.
    """

    stop_workers: bool = False
    timeout_s: float = 60.0
    scope: str = NODE_SCOPE  # NODE_SCOPE | WORKER_SCOPE

    @abc.abstractmethod
    def poll(self, ctx: OnDemandProbeContext) -> ProbeResult:
        raise NotImplementedError
