"""NVSentinel adapter: NVIDIA's node-level fault detection, joined to Ray Train.

NVSentinel (https://github.com/NVIDIA/NVSentinel) detects GPU, NVSwitch, NIC,
syslog and cloud-maintenance faults on Kubernetes nodes and publishes them as
**node conditions**, **taints** and **cordons** on the ``Node`` object, then
cordons -> drains -> remediates. It owns everything below the node.

Ray Train owns everything above it: collective liveness, step progress,
numerics. Neither side alone can localize a silent hang -- NVSentinel sees a
NIC flap but not that the job stalled, Ray Train sees a stalled collective but
not which of 128 hosts caused it. Joining the two is the whole point.

The adapter runs in both directions:

- **Inbound (Collect).** :class:`NVSentinelProbe` is a ``ClusterProbe``: one
  read of the Kubernetes API covers every node, and -- unlike a per-node probe
  -- it keeps reporting for a node that has gone dark entirely, which is
  exactly the case that matters.
- **Outbound (Act).** :func:`build_health_event` turns a Ray Train
  ``HealthDecision`` into an NVSentinel ``HealthEvent``, so a fault only Ray
  Train can see (an NCCL hang, a persistent straggler) enters NVSentinel's
  remediation pipeline and the node gets cordoned and repaired.

Coordination note: Ray Train and NVSentinel must not both try to evacuate the
same node. Events Ray Train emits set ``drainOverrides.skip`` -- Ray Train has
already moved its own workers off via ``Evict`` -- while still asking for the
cordon, so nothing else lands there and remediation can proceed.
"""
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ray.train.v2._internal.execution.health.decision import (
    Cause,
    Evict,
    HealthDecision,
)
from ray.train.v2._internal.execution.health.policy import Evaluator, HealthPolicy
from ray.train.v2._internal.execution.health.probe import (
    ClusterContext,
    ClusterProbe,
    ProbeResult,
)
from ray.train.v2._internal.execution.health.state import HealthState

logger = logging.getLogger(__name__)

# Conditions NVSentinel sets, by the component they implicate. A condition with
# status=True is a fatal fault: NVSentinel only writes conditions for fatal
# events and emits Kubernetes Events for non-fatal ones.
# https://github.com/NVIDIA/NVSentinel/blob/main/docs/INTEGRATIONS.md
GPU_CONDITION_PREFIXES = ("Gpu", "SysLogs", "NVSwitch")
SYSTEM_CONDITIONS = frozenset({"DCGMError", "SyslogError"})
MAINTENANCE_CONDITIONS = frozenset({"CSPMaintenance"})

# "[DCGM_FR_FAULTY_MEMORY] GPU memory failure on GPU 0 - RecommendedAction: RESTART_VM"
_MESSAGE_RE = re.compile(
    r"^\s*(?:\[(?P<codes>[^\]]*)\]\s*)?(?P<text>.*?)"
    r"(?:\s*-\s*RecommendedAction:\s*(?P<action>\S+))?\s*$"
)

# NVSentinel's RecommendedAction enum values, by name.
# See data-models/protobufs/health_event.proto in the NVSentinel repo.
RECOMMENDED_ACTIONS = {
    "NONE": 0,
    "COMPONENT_RESET": 2,
    "CONTACT_SUPPORT": 5,
    "RUN_FIELDDIAG": 6,
    "RESTART_VM": 15,
    "RESTART_BM": 24,
    "REPLACE_VM": 25,
    "RUN_DCGMEUD": 26,
    "CUSTOM": 27,
    "UNKNOWN": 99,
}

#: Env var carrying the Kubernetes node name, injected via the downward API
#: (``fieldRef: spec.nodeName``). See ``resolve_node_names`` for why this is
#: the only reliable mapping in a non-hostNetwork pod.
K8S_NODE_NAME_ENV_VAR = "RAY_TRAIN_K8S_NODE_NAME"

#: Ray node label an operator can set instead of the env var.
K8S_NODE_NAME_LABEL = "ray.io/k8s-node-name"


@dataclass(frozen=True)
class NodeCondition:
    type: str
    status: str  # "True" | "False" | "Unknown"
    reason: str = ""
    message: str = ""

    @property
    def firing(self) -> bool:
        return self.status == "True"

    def parsed(self) -> Dict[str, Any]:
        """Split the message into error codes, text, and recommended action."""
        m = _MESSAGE_RE.match(self.message or "")
        if not m:
            return {"codes": [], "text": self.message or "", "action": "UNKNOWN"}
        codes = [c.strip() for c in (m.group("codes") or "").split(",") if c.strip()]
        return {
            "codes": codes,
            "text": (m.group("text") or "").strip(),
            "action": (m.group("action") or "UNKNOWN").strip(),
        }


@dataclass(frozen=True)
class K8sNodeStatus:
    """The slice of a Kubernetes ``Node`` this adapter cares about."""

    name: str
    conditions: Dict[str, NodeCondition] = field(default_factory=dict)
    taints: List[Dict[str, str]] = field(default_factory=list)
    unschedulable: bool = False

    @property
    def nvidia_taints(self) -> List[Dict[str, str]]:
        return [
            t for t in self.taints if str(t.get("key", "")).startswith("nvidia.com/")
        ]


class NodeStatusSource:
    """Where node status comes from. Swapped out in tests and for other backends."""

    def snapshot(self, node_names: List[str]) -> Dict[str, K8sNodeStatus]:
        raise NotImplementedError


class KubernetesNodeStatusSource(NodeStatusSource):
    """Reads ``Node`` objects from the Kubernetes API.

    One list call per poll covers the whole cluster. ``resource_version="0"``
    is served from the apiserver's watch cache, so this stays cheap even on a
    thousand-node cluster -- which is the reason this is a cluster probe and
    not a probe running on every NodeMonitor.

    Requires ``get``/``list`` on the cluster-scoped ``nodes`` resource for the
    pod's service account.
    """

    def __init__(self, label_selector: Optional[str] = None):
        self._label_selector = label_selector
        self._api = None

    def _client(self):
        if self._api is None:
            from kubernetes import client, config  # lazy: optional dependency

            try:
                config.load_incluster_config()
            except Exception:
                config.load_kube_config()
            self._api = client.CoreV1Api()
        return self._api

    def snapshot(self, node_names: List[str]) -> Dict[str, K8sNodeStatus]:
        kwargs = {"resource_version": "0"}
        if self._label_selector:
            kwargs["label_selector"] = self._label_selector
        nodes = self._client().list_node(**kwargs)

        wanted = set(node_names)
        out: Dict[str, K8sNodeStatus] = {}
        for node in nodes.items:
            name = node.metadata.name
            if wanted and name not in wanted:
                continue
            out[name] = K8sNodeStatus(
                name=name,
                conditions={
                    c.type: NodeCondition(
                        type=c.type,
                        status=str(c.status),
                        reason=c.reason or "",
                        message=c.message or "",
                    )
                    for c in (node.status.conditions or [])
                },
                taints=[
                    {"key": t.key, "value": t.value or "", "effect": t.effect or ""}
                    for t in (node.spec.taints or [])
                ],
                unschedulable=bool(node.spec.unschedulable),
            )
        return out


class StaticNodeStatusSource(NodeStatusSource):
    """A fixed snapshot. For tests and for driving the loop without a cluster."""

    def __init__(self, statuses: Optional[Dict[str, K8sNodeStatus]] = None):
        self.statuses: Dict[str, K8sNodeStatus] = dict(statuses or {})

    def snapshot(self, node_names: List[str]) -> Dict[str, K8sNodeStatus]:
        wanted = set(node_names)
        return {k: v for k, v in self.statuses.items() if not wanted or k in wanted}


def resolve_node_names(node_ids: List[str]) -> Dict[str, str]:
    """Map Ray node ids to Kubernetes node names.

    This is the one piece of the integration that needs a deployment decision.
    A Ray worker pod's hostname is the *pod* name and its IP is the *pod* IP,
    neither of which is the Kubernetes node name unless the pod runs with
    ``hostNetwork``. Resolution order:

    1. The Ray node label ``ray.io/k8s-node-name``, if the operator sets it.
    2. The node's IP matched against a ``kubernetes.io/hostname``-style Ray
       label, for hostNetwork deployments.

    Falls back to the Ray node id, which will simply match nothing -- the
    adapter reports no evidence rather than guessing wrong about which host a
    hardware fault is on.
    """
    import ray

    labels_by_id: Dict[str, Dict[str, str]] = {}
    try:
        for node in ray.nodes():
            labels_by_id[node["NodeID"]] = node.get("Labels", {}) or {}
    except Exception:
        logger.warning("Could not read Ray node labels.", exc_info=True)

    mapping: Dict[str, str] = {}
    for node_id in node_ids:
        labels = labels_by_id.get(node_id, {})
        name = labels.get(K8S_NODE_NAME_LABEL) or labels.get("kubernetes.io/hostname")
        mapping[node_id] = name or node_id
    return mapping


class NVSentinelProbe(ClusterProbe):
    """Reports NVSentinel's verdict on every node the run occupies.

    A ``ProbeResult`` per node carries:

    - ``metrics``: ``fatal_conditions``, ``cordoned``, ``tainted`` -- countable
      signals an evaluator can threshold on.
    - ``events``: ``condition:GpuMemWatch``, ``taint:nvidia.com/gpu-xid-error``,
      ``cordoned`` -- what fired, by name.
    - ``passed``: ``False`` when NVSentinel says this node is unhealthy.
    - ``detail``: the human-readable messages, for the emitted event.
    """

    name = "NVSentinelProbe"
    entity = "node"

    def __init__(
        self,
        source: Optional[NodeStatusSource] = None,
        node_name_resolver=resolve_node_names,
        interval_s: float = 10.0,
    ):
        self._source = source if source is not None else KubernetesNodeStatusSource()
        self._resolve = node_name_resolver
        self.interval_s = interval_s

    def poll(self, ctx: ClusterContext) -> Dict[str, ProbeResult]:
        node_ids = ctx.node_ids
        id_to_name = self._resolve(node_ids)
        statuses = self._source.snapshot(sorted(set(id_to_name.values())))

        results: Dict[str, ProbeResult] = {}
        for node_id in node_ids:
            status = statuses.get(id_to_name.get(node_id, ""))
            if status is None:
                # Nothing known about this host: report no evidence rather than
                # a clean bill of health.
                continue
            results[node_id] = self._to_result(status)
        return results

    @staticmethod
    def _to_result(status: K8sNodeStatus) -> ProbeResult:
        firing = [c for c in status.conditions.values() if c.firing]
        hardware = [
            c
            for c in firing
            if c.type.startswith(GPU_CONDITION_PREFIXES) or c.type in SYSTEM_CONDITIONS
        ]
        maintenance = [c for c in firing if c.type in MAINTENANCE_CONDITIONS]
        taints = status.nvidia_taints

        events = [f"condition:{c.type}" for c in firing]
        events += [f"taint:{t['key']}" for t in taints]
        if status.unschedulable:
            events.append("cordoned")

        details = []
        for c in firing:
            parsed = c.parsed()
            details.append(f"{c.type}: {parsed['text'] or c.message}".strip())

        return ProbeResult(
            metrics={
                "fatal_conditions": float(len(hardware)),
                "maintenance_conditions": float(len(maintenance)),
                "tainted": float(bool(taints)),
                "cordoned": float(status.unschedulable),
            },
            events=events,
            passed=not (hardware or maintenance or taints or status.unschedulable),
            detail="; ".join(details),
        )


class NVSentinelEvaluator(Evaluator):
    """Turns NVSentinel's verdict into a decision about *this run's* nodes.

    Only nodes this run actually occupies produce a decision -- a fault
    elsewhere in the cluster is not this run's problem.

    - A firing GPU/NVSwitch/syslog condition, or an ``nvidia.com/*`` taint, is
      ``Evict(cause=HARDWARE)``: NVSentinel has already classified it as fatal
      hardware, which is the only cause that authorizes quarantine.
    - ``CSPMaintenance`` or a bare cordon is ``Evict(cause=INFRASTRUCTURE)``:
      the node is on its way out, so move before the drain kills us.

    Args:
        evict_on_cordon: Whether a cordon with no NVSentinel condition behind it
            counts. Operators cordon nodes by hand for unrelated reasons, so
            this is on but separable.
        confirm_polls: How many consecutive polls a signal must persist before
            it is acted on. NVSentinel conditions are already debounced
            upstream, so the default of 1 is intentional -- raise it if a
            source is noisy.
    """

    def __init__(self, evict_on_cordon: bool = True, confirm_polls: int = 1):
        self._evict_on_cordon = evict_on_cordon
        self._confirm_polls = max(1, confirm_polls)
        self._streaks: Dict[str, int] = {}

    def on_worker_group_start(self) -> None:
        self._streaks = {}

    def evaluate(self, state: HealthState) -> List[HealthDecision]:
        results = state.results(NVSentinelProbe)
        decisions: List[HealthDecision] = []

        for node_id, result in results.items():
            ranks = state.ranks_on(node_id)
            if not ranks:
                continue  # we have no workers there

            hardware = result.metrics.get("fatal_conditions", 0) or result.metrics.get(
                "tainted", 0
            )
            maintenance = result.metrics.get("maintenance_conditions", 0)
            cordoned = result.metrics.get("cordoned", 0) and self._evict_on_cordon

            if not (hardware or maintenance or cordoned):
                self._streaks.pop(node_id, None)
                continue

            streak = self._streaks.get(node_id, 0) + 1
            self._streaks[node_id] = streak
            if streak < self._confirm_polls:
                continue

            cause = Cause.HARDWARE if hardware else Cause.INFRASTRUCTURE
            what = result.detail or ", ".join(result.events) or "unhealthy"
            decisions.append(
                Evict(
                    cause=cause,
                    reason=(
                        f"NVSentinel reports node {node_id} unhealthy "
                        f"(ranks {ranks}): {what}"
                    ),
                    target_nodes=[node_id],
                )
            )
        return decisions


# ----------------------------------------------------------------------
# Outbound: Ray Train's findings -> NVSentinel's remediation pipeline.
# ----------------------------------------------------------------------

_CAUSE_TO_ACTION = {
    Cause.HARDWARE: "RUN_FIELDDIAG",
    Cause.NO_PROGRESS: "RUN_FIELDDIAG",
    Cause.INFRASTRUCTURE: "NONE",
    Cause.APPLICATION: "NONE",
    Cause.UNKNOWN: "UNKNOWN",
}


def build_health_event(
    decision: HealthDecision,
    node_name: str,
    *,
    run_id: str = "",
    check_name: str = "RayTrainHealthDecision",
    shadow: bool = True,
    gpu_uuids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build one NVSentinel ``HealthEvent`` from a Ray Train decision.

    Mirrors ``data-models/protobufs/health_event.proto``. Returned as a plain
    dict so the transport (gRPC to the platform connector, or a Kubernetes
    object the object-monitor watches) stays pluggable.

    Args:
        decision: The merged decision the controller acted on.
        node_name: Kubernetes node name the fault is attributed to.
        run_id: Ray Train run id, carried in metadata for correlation.
        shadow: When True the event is emitted with
            ``processingStrategy=STORE_ONLY``: NVSentinel records it and
            nothing in the cluster changes. This is how the outbound path is
            rolled out -- observe first, remediate once the decisions are
            trusted.
        gpu_uuids: Specific GPUs to attribute, when the decision names them.

    Returns:
        A ``HealthEvent`` dict.
    """
    only_hardware = decision.cause is Cause.HARDWARE
    entities = [{"entityType": "Node", "entityValue": node_name}]
    for uuid in gpu_uuids or []:
        entities.append({"entityType": "GPU_UUID", "entityValue": uuid})

    return {
        "version": 1,
        "agent": "ray-train",
        "componentClass": "Node",
        "checkName": check_name,
        # Only a hardware attribution is allowed to be fatal: fatal is what
        # authorizes NVSentinel to quarantine and remediate the host.
        "isFatal": only_hardware and not shadow,
        "isHealthy": False,
        "message": f"[{decision.cause.name}] {decision.reason}",
        "recommendedAction": _CAUSE_TO_ACTION.get(decision.cause, "UNKNOWN"),
        "errorCode": [f"RAY_TRAIN_{decision.cause.name}"],
        "entitiesImpacted": entities,
        "metadata": {
            "ray_train_run_id": run_id,
            "ray_train_action": decision.action.name,
            "ray_train_cause": decision.cause.name,
        },
        "generatedTimestamp": time.time(),
        "nodeName": node_name,
        # Cordon the node so nothing else lands on it, but do NOT let the
        # node-drainer evict our pods: Ray Train has already moved its workers
        # off as part of acting on the Evict, and a concurrent drain would race
        # the restart it is doing.
        "quarantineOverrides": {"force": only_hardware, "skip": False},
        "drainOverrides": {"force": False, "skip": True},
        "processingStrategy": "STORE_ONLY" if shadow else "EXECUTE_REMEDIATION",
    }


class HealthEventSink:
    """Where outbound health events go."""

    def send(self, events: List[Dict[str, Any]]) -> None:
        raise NotImplementedError


class LoggingHealthEventSink(HealthEventSink):
    """Logs the event instead of sending it. The default, and shadow mode."""

    def send(self, events: List[Dict[str, Any]]) -> None:
        for event in events:
            logger.info("NVSentinel health event (not sent): %s", event)


class GrpcHealthEventSink(HealthEventSink):
    """Sends events to NVSentinel's ``PlatformConnector`` over gRPC.

    ``HealthEventOccurredV1`` is the same entry point NVSentinel's own health
    monitors use, so a Ray Train finding enters the identical pipeline:
    platform-connector -> node condition -> fault-quarantine -> remediation.

    Requires the generated stubs for
    ``data-models/protobufs/health_event.proto`` on the path.
    """

    def __init__(self, address: Optional[str] = None):
        self._address = address or os.environ.get(
            "RAY_TRAIN_NVSENTINEL_ADDRESS", "platform-connector.nvsentinel:5000"
        )
        self._stub = None

    def _get_stub(self):
        if self._stub is None:
            import grpc  # lazy: optional dependency
            from nvsentinel.protos import health_event_pb2_grpc  # generated stubs

            channel = grpc.insecure_channel(self._address)
            self._stub = health_event_pb2_grpc.PlatformConnectorStub(channel)
        return self._stub

    def send(self, events: List[Dict[str, Any]]) -> None:
        from google.protobuf.json_format import ParseDict
        from nvsentinel.protos import health_event_pb2

        payload = ParseDict(
            {"version": 1, "events": events}, health_event_pb2.HealthEvents()
        )
        self._get_stub().HealthEventOccurredV1(payload)


def nvsentinel_policy(
    source: Optional[NodeStatusSource] = None,
    *,
    evict_on_cordon: bool = True,
    confirm_polls: int = 1,
    interval_s: float = 10.0,
) -> HealthPolicy:
    """The inbound half, as one policy to drop into ``HealthConfig``.

    Example:
        >>> from ray.train import RunConfig  # doctest: +SKIP
        >>> RunConfig(  # doctest: +SKIP
        ...     health_config=HealthConfig(policies=[nvsentinel_policy()])
        ... )
    """
    return HealthPolicy(
        name="nvsentinel",
        probe_creator=lambda: [NVSentinelProbe(source=source, interval_s=interval_s)],
        evaluator_creator=lambda: [
            NVSentinelEvaluator(
                evict_on_cordon=evict_on_cordon, confirm_polls=confirm_polls
            )
        ],
    )
