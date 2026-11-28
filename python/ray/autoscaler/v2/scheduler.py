import copy
import itertools
import logging
import math
import time
import uuid
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import (
    Callable,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
)

from ray._private.protobuf_compat import message_to_dict
from ray._raylet import IMPLICIT_RESOURCE_PREFIX
from ray.autoscaler._private.constants import AUTOSCALER_CONSERVE_GPU_NODES
from ray.autoscaler._private.resource_demand_scheduler import (
    _fits,
    _inplace_subtract,
)
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.common import (
    _ACTIVE_CID_STATUSES,
    _DRAINING_INSTANCE_STATUSES,
    _TEARDOWN_INSTANCE_STATUSES,
    _TERMINAL_INSTANCE_STATUSES,
    _UNALLOCATED_INSTANCE_STATUSES,
    InstanceUtil,
    _has_teardown_history,
    _instance_cloud_start_ns,
    _is_shadowed_stopped,
    _MultiHostReplicaView,
)
from ray.autoscaler.v2.instance_manager.config import (
    InstanceReconcileConfig,
    NodeTypeConfig,
)
from ray.autoscaler.v2.schema import (
    AutoscalerInstance,
    IPPRGroupSpec,
    IPPRSpecs,
    IPPRStatus,
    NodeType,
)
from ray.autoscaler.v2.utils import ProtobufUtil, ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    LocalityRequirement,
    ResourceRequest,
    ResourceRequestByCount,
)
from ray.core.generated.common_pb2 import (
    LabelSelector,
    LabelSelectorConstraint,
    LabelSelectorOperator,
    PlacementStrategy,
)
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    LaunchRequest,
    NodeKind,
    TerminationRequest,
)

# ============= Resource Scheduling Service API =======================
#
#  ResourceSchedulerService is a service that schedules resource bundles
#  to nodes. It's used by the autoscaler to schedule resource bundles
#  to determine the desired cluster size to satisfy the current resource
#  demands.
#
logger = logging.getLogger(__name__)

# Protobuf enum attribute lookups are slow, so hoist the ones used per label check.
_LABEL_OPERATOR_IN = LabelSelectorOperator.LABEL_OPERATOR_IN
_LABEL_OPERATOR_NOT_IN = LabelSelectorOperator.LABEL_OPERATOR_NOT_IN

# Synthetic domain and replica IDs assigned in-memory to unregistered nodes
# during a schedule() pass (never serialized to LaunchRequest). Leading NUL
# prevents collisions with valid user label values.
_PENDING_DOMAIN_PREFIX = "\x00pending_domain_"

# Floating-point tolerance for comparing accumulated label selector scores.
_LABEL_SCORE_TOL = 1e-9

# Floating-point tolerance for comparing resource quantities.
_RESOURCE_EPSILON = 1e-9

# Role of a request on a node relative to its active selector.
_CONSTRAINED, _UNCONSTRAINED, _DEFERRED = 1, 0, -1


def _has_dynamic_selector_overlap(requests: Collection[ResourceRequest]) -> bool:
    """Return True if any label selector reads an anti-affinity label."""
    anti_affinity_keys = {
        c.anti_affinity.label_name
        for r in requests
        for c in r.placement_constraints
        if c.HasField("anti_affinity")
    }
    if not anti_affinity_keys:
        return False
    return any(
        c.label_key in anti_affinity_keys
        for r in requests
        for selector in r.label_selectors
        for c in selector.label_constraints
    )


def _shape_key(
    request: ResourceRequest, shape_keys: Optional[Dict[int, bytes]] = None
) -> bytes:
    return (
        sk
        if shape_keys is not None and (sk := shape_keys.get(id(request))) is not None
        else request.SerializeToString(deterministic=True)
    )


@dataclass
class SchedulingRequest:
    # If outdated node check through launch config is disabled.
    disable_launch_config_check: bool
    # Available node type configs
    node_type_configs: Dict[NodeType, NodeTypeConfig] = field(default_factory=dict)
    # Max number of worker nodes.
    max_num_nodes: Optional[int] = None
    # Idle timeout in seconds.
    idle_timeout_s: Optional[float] = None
    # Idle timeout in seconds for the pending nodes not needed by any resource
    # demands. None to never terminate them for not being needed.
    pending_idle_timeout_s: Optional[float] = None
    # TODO: This prob could be refactored into the ClusterStatus data class later.
    # The current ray resource requests.
    resource_requests: List[ResourceRequestByCount] = field(default_factory=list)
    # The Gang resource requests.
    gang_resource_requests: List[GangResourceRequest] = field(default_factory=list)
    # cluster resource constraints.
    cluster_resource_constraints: List[ClusterResourceConstraint] = field(
        default_factory=list
    )
    # The current instances.
    current_instances: List[AutoscalerInstance] = field(default_factory=list)
    # The cloud resource availability score. A low score indicates that resource
    # allocation for this node type has recently failed.
    cloud_resource_availabilities: Dict[NodeType, float] = field(default_factory=dict)
    # The recoverable cloud resource availability score.
    # Similar to cloud_resource_availabilities, but it will recover from 0.0 to 1.0
    # linearly over RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S seconds.
    recoverable_resource_availabilities: Dict[NodeType, float] = field(
        default_factory=dict
    )

    # IPPR (In-Place Pod Resize) typed specs (limits/timeouts).
    ippr_specs: Optional[IPPRSpecs] = None
    # Latest per-pod IPPR statuses keyed by cloud_instance_id (pod name).
    ippr_statuses: Dict[str, IPPRStatus] = field(default_factory=dict)
    # Optional pre-built multi-host replica view from Reconciler._scale_cluster.
    replica_view: Optional[_MultiHostReplicaView] = None
    # TTL in seconds for incomplete-replica backfill placeholders (defaults to
    # InstanceReconcileConfig.request_status_timeout_s when None).
    backfill_placeholder_ttl_s: Optional[float] = None
    # Optional lifetime first-transition lookup from InstanceManager so status_history
    # trimming never flips im_created_lids or had_running.
    first_status_transition_time_ns_fn: Optional[
        Callable[[Instance, Instance.InstanceStatus.ValueType], Optional[int]]
    ] = None


@dataclass
class SchedulingReply:
    # Instances to launch.
    to_launch: List[LaunchRequest] = field(default_factory=list)
    # IPPR resize actions to perform on existing pods.
    to_ippr: List[IPPRStatus] = field(default_factory=list)
    # To terminate.
    to_terminate: List[TerminationRequest] = field(default_factory=list)
    # The aggregate cluster resources after scheduling.
    cluster_resources: Dict[str, float] = field(default_factory=dict)
    # The infeasible resource bundles.
    infeasible_resource_requests: List[ResourceRequest] = field(default_factory=list)
    # The infeasible gang resource bundles.
    infeasible_gang_resource_requests: List[GangResourceRequest] = field(
        default_factory=list
    )
    # The infeasible cluster resource constraints.
    infeasible_cluster_resource_constraints: List[ClusterResourceConstraint] = field(
        default_factory=list
    )


class IResourceScheduler(ABC):
    """Interface for a resource scheduler.

    Implements the `instance_manager.proto ResourceSchedulerService` interface.
    """

    @abstractmethod
    def schedule(self, request: SchedulingRequest) -> SchedulingReply:
        """Given the resource requests and the current cluster state, calculate the
        target cluster shape by trying to schedule the resource requests on the
        nodes.
        """
        pass


def _collect_unique_resource_shapes(
    requests: List["ResourceRequest"],
) -> List[Dict[str, float]]:
    """Collect unique resource shapes from all requests for feasibility checks.

    Returns a deduplicated list of resource bundles (shapes). Each shape is a
    dict mapping resource names to their required amounts. Used by
    _can_fit_any_request to perform per-shape AND checks.
    """
    seen = set()
    shapes = []
    for r in requests:
        bundle = {k: v for k, v in r.resources_bundle.items() if v > 0}
        # An empty shape ({}) fits on any node, so keep it as a shape:
        # dropping it would let the pre-filter reject nodes that could
        # actually schedule zero-resource requests.
        key = frozenset(bundle.items())
        if key not in seen:
            seen.add(key)
            shapes.append(bundle)
    return shapes


def _can_fit_any_request(
    available: Dict[str, float],
    resource_shapes: List[Dict[str, float]],
) -> bool:
    """Quick pre-check: can this node possibly fit any pending request?

    Returns False only when the node definitely cannot schedule any request.
    For each unique request shape, checks that ALL resource dimensions are
    satisfied simultaneously (AND within a shape, OR across shapes).

    This is a conservative check (no false negatives): it ignores placement
    constraints and labels, so if it returns True the node may or may not
    actually fit a request (try_schedule decides precisely).

    Runs in O(S * D) where S is the number of unique shapes (typically small)
    and D is the number of resource dimensions per shape (typically 2-4).
    """
    if not resource_shapes:
        return True

    for shape in resource_shapes:
        if all(
            available.get(k, 1.0 if k.startswith(IMPLICIT_RESOURCE_PREFIX) else 0.0)
            >= v
            for k, v in shape.items()
        ):
            return True
    return False


class NodeStateCache:
    """Caches the scheduling states of nodes to avoid redundant try_schedule calls
    for identical nodes.
    """

    def __init__(self, source: "ResourceRequestSource"):
        self.seen_states = set()
        self.source = source

    def was_seen_or_mark(
        self,
        node: "SchedulingNode",
        is_preferred: bool = False,
        is_committed: bool = False,
    ) -> bool:
        """Generates a deterministic signature of the node's scheduling capacity.
        Returns True if this exact state has already been seen, otherwise adds it
        to the cache and returns False.

        We intentionally skip caching for running nodes because their resource states
        may be highly fragmented and they contain unique injected resources like node ID.
        """
        # Evaluate online nodes individually since they may contain dummy resources.
        if node.im_instance_status == Instance.RAY_RUNNING:
            return False

        avail_res = node.get_available_resources(self.source)
        state_key = (
            node.node_type,
            node.node_kind,
            node._is_backfill_placeholder,
            frozenset(node.total_resources.items()),
            frozenset(avail_res.items()),
            frozenset(node.labels.items()),
            is_preferred,
            is_committed,
        )

        if state_key in self.seen_states:
            return True

        self.seen_states.add(state_key)
        return False


class SchedulingNodeStatus(Enum):
    """The status of a scheduling node (`SchedulingNode`)"""

    # The node is added by the ResourceDemandScheduler.
    TO_LAUNCH = "TO_LAUNCH"
    # The node is pending, i.e. there's already an autoscaler instance being launched
    # The node is schedulable. It could be running ray or pending to run ray. Either
    # Way, it should be able to accept new resource requests/resource constraints.
    SCHEDULABLE = "SCHEDULABLE"
    # The node is to be terminated by the ResourceDemandScheduler
    TO_TERMINATE = "TO_TERMINATE"


class ResourceRequestSource(Enum):
    """The source of the resource request."""

    # The resource request is from demand, e.g. ray tasks/actors,
    # placement groups, etc.
    PENDING_DEMAND = "PENDING_DEMAND"
    # The resource request is from the cluster resource constraints, i.e.
    # from ray.autoscaler.sdk.request_resources().
    CLUSTER_RESOURCE_CONSTRAINT = "CLUSTER_RESOURCE_CONSTRAINT"


@dataclass
class SchedulingNode:
    """A abstraction of a node that can be scheduled on by the resource scheduler.

    A scheduling node is expected to be used as:

        node  = SchedulingNode.new(instance, node_configs)
        remaining, score = node.try_schedule(requests)

        .... do something with the score ....

    NOTE:
        One could also extend the scheduling behavior by overriding `try_schedule`
    """

    # Node type name.
    node_type: NodeType
    # Status
    status: SchedulingNodeStatus
    # Resource requests scheduled on this nodes for different sources.
    sched_requests: Dict[ResourceRequestSource, List[ResourceRequest]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # Available resources for different sources of requests.
    available_resources_for_sched: Dict[
        ResourceRequestSource, Dict[str, float]
    ] = field(default_factory=dict)
    # The node's current resource capacity.
    total_resources: Dict[str, float] = field(default_factory=dict)

    # IPPR state for this node's pod. None if IPPR doesn't apply.
    ippr_status: Optional[IPPRStatus] = None
    # IPPR group spec (min/max resources and timeout) for this node type.
    ippr_spec: Optional[IPPRGroupSpec] = None

    # Node's labels, including static or dynamic labels.
    # Note that dynamic labels are a deprecated feature. And it is only used for the
    # autoscaler’s strict-spread placement group scheduling (antiaffinity)
    labels: Dict[str, str] = field(default_factory=dict)
    # Observability descriptive message for why the node was launched in the
    # first place.
    launch_reason: Optional[str] = None
    # Termination request, none when the node is not being terminated.
    termination_request: Optional[TerminationRequest] = None
    # The instance id of the IM(Instance Manager) instance. None if the node
    # is not yet in IM.
    im_instance_id: Optional[str] = None
    # The instance status of the IM(Instance Manager) instance. None if the in-flight node
    # has not yet been assigned to an IM instance.
    im_instance_status: Optional[Instance.InstanceStatus.ValueType] = None
    # The ray node id of the ray node. None if the node is not included in
    # ray cluster's GCS report yet (not running ray yet).
    ray_node_id: Optional[str] = None
    # Idle duration in ms. Default not idle.
    idle_duration_ms: int = 0
    # Launch config hash.
    launch_config_hash: Optional[str] = None
    # node kind.
    node_kind: NodeKind = NodeKind.WORKER
    # The priority of the node type.
    priority: int = 0
    # Replica identifier used for atomic multi-worker replica grouping.
    replica_id: Optional[str] = None
    # Optional provider-assigned multi-worker replica identifier.
    provider_replica_id: Optional[str] = None
    # Optional cloud provider instance ID for reconciliation alignment.
    cloud_instance_id: str = ""
    # Optional IM launch_request_id for exact launch-batch replica attachment.
    launch_request_id: Optional[str] = None
    # Whether the node belongs to an authoritative runtime/provider replica group.
    _has_runtime_domain: bool = False
    # Whether the node belongs to a draining/teardown multi-worker replica.
    _is_draining_replica: bool = False
    # Whether the node is an in-memory backfill placeholder for a committed slot.
    _is_backfill_placeholder: bool = False
    # Whether this TO_LAUNCH node refills an incomplete unlabeled replica.
    _is_unlabeled_refill: bool = False
    # Whether this sub-replica QUEUED node was canceled by remainder cleanup.
    _is_sub_replica_queued_canceled: bool = False
    # Snapshot of a sibling's prior locality label during a trial domain placement.
    _saved_locality_label: Optional[Tuple[bool, Optional[str]]] = None

    def __init__(
        self,
        node_type: NodeType,
        total_resources: Dict[str, float],
        available_resources: Dict[str, float],
        labels: Dict[str, str],
        status: SchedulingNodeStatus,
        im_instance_id: str = "",
        im_instance_status: Optional[Instance.InstanceStatus.ValueType] = None,
        ray_node_id: str = "",
        idle_duration_ms: int = 0,
        launch_config_hash: str = "",
        node_kind: NodeKind = NodeKind.WORKER,
        termination_request: Optional[TerminationRequest] = None,
        priority: int = 0,
        provider_replica_id: Optional[str] = None,
        launch_request_id: Optional[str] = None,
        cloud_instance_id: str = "",
    ):
        self.node_type = node_type
        self.total_resources = total_resources
        self.available_resources_for_sched = {
            ResourceRequestSource.PENDING_DEMAND: dict(available_resources),
            ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT: dict(total_resources),
        }
        self.sched_requests = {
            ResourceRequestSource.PENDING_DEMAND: [],
            ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT: [],
        }
        self.labels = labels
        self.status = status
        self.im_instance_id = im_instance_id
        self.im_instance_status = im_instance_status
        self.ray_node_id = ray_node_id
        self.idle_duration_ms = idle_duration_ms
        self.launch_config_hash = launch_config_hash
        self.node_kind = node_kind
        self.termination_request = termination_request
        self.priority = priority
        self.replica_id: Optional[str] = None
        self.provider_replica_id: Optional[str] = provider_replica_id or None
        self.cloud_instance_id: str = cloud_instance_id
        self.launch_request_id: Optional[str] = launch_request_id or None
        self._has_runtime_domain: bool = False
        self._is_draining_replica: bool = False
        self._is_backfill_placeholder: bool = False
        self._is_unlabeled_refill: bool = False
        self._is_sub_replica_queued_canceled: bool = False
        self._saved_locality_label: Optional[Tuple[bool, Optional[str]]] = None
        self._cloud_start_ns: int = 0

    def __deepcopy__(self, memo: Dict[int, object]) -> "SchedulingNode":
        cls = self.__class__
        out = cls.__new__(cls)
        memo[id(self)] = out
        d = out.__dict__
        d.update(self.__dict__)
        d["total_resources"] = dict(self.total_resources)
        d["available_resources_for_sched"] = {
            k: dict(v) for k, v in self.available_resources_for_sched.items()
        }
        d["sched_requests"] = defaultdict(
            list, ((k, list(v)) for k, v in self.sched_requests.items())
        )
        d["labels"] = dict(self.labels)
        if self.ippr_status is not None:
            d["ippr_status"] = replace(self.ippr_status)
        if self.termination_request is not None:
            tr = TerminationRequest()
            tr.CopyFrom(self.termination_request)
            d["termination_request"] = tr
        return out

    def get_available_resources(self, resource_request_source: ResourceRequestSource):
        """Get the available resources for the given resource request source."""
        return self.available_resources_for_sched[resource_request_source]

    def ippr_capacity_bounds(
        self,
    ) -> Optional[Tuple[bool, bool, float, float]]:
        """Return `(in_progress, can_resize_up, cpu_bound, mem_bound)` when IPPR
        affects scheduling capacity, else `None`."""
        ippr = self.ippr_status
        if ippr is None:
            return None
        in_progress = ippr.is_in_progress()
        can_up = ippr.can_resize_up()
        if not (in_progress or can_up):
            return None
        cpu = float(self.total_resources.get("CPU", 0.0))
        mem = float(self.total_resources.get("memory", 0.0))
        if in_progress:
            cpu = float(ippr.desired_cpu)
            mem = float(ippr.desired_memory)
        if can_up:
            cpu = max(cpu, float(ippr.max_cpu()))
            mem = max(mem, float(ippr.max_memory()))
        return (in_progress, can_up, cpu, mem)

    def get_ippr_effective_available(
        self, resource_request_source: ResourceRequestSource
    ) -> Dict[str, float]:
        """Return available resources adjusted for in-progress or resize-up IPPR."""
        avail = self.get_available_resources(resource_request_source)
        bounds = self.ippr_capacity_bounds()
        if bounds is None:
            return avail
        _, _, cpu_bound, mem_bound = bounds
        adjusted = dict(avail)
        adjusted["CPU"] = max(
            0.0,
            adjusted.get("CPU", 0.0)
            + (cpu_bound - self.total_resources.get("CPU", 0.0)),
        )
        adjusted["memory"] = max(
            0.0,
            adjusted.get("memory", 0.0)
            + (mem_bound - self.total_resources.get("memory", 0.0)),
        )
        return adjusted

    def get_sched_requests(self, resource_request_source: ResourceRequestSource):
        """Get the resource requests for the given resource request source."""
        return self.sched_requests[resource_request_source]

    def update_total_resources(self, new_total_resources: Dict[str, float]) -> None:
        """Update the node's total capacity and adjust available resources.

        Applies per-resource deltas between the provided new totals and the
        current totals, and adds those deltas to the available resources for
        all scheduling sources.

        Args:
            new_total_resources: Mapping from resource name (e.g., "CPU",
                "memory") to the new total capacity to expose for scheduling.
        """
        for resource_name, new_total in new_total_resources.items():
            delta = new_total - self.total_resources.get(resource_name, 0.0)
            self.total_resources[resource_name] = max(0.0, new_total)
            for available in self.available_resources_for_sched.values():
                available[resource_name] = max(
                    0.0, available.get(resource_name, 0.0) + delta
                )

    def add_sched_request(
        self,
        request: ResourceRequest,
        resource_request_source: ResourceRequestSource,
    ):
        """Add the resource requests to the node.

        Args:
            request: The resource request to be added.
            resource_request_source: The source of the resource request.
        """
        self.sched_requests[resource_request_source].append(request)

    @staticmethod
    def new(
        instance: AutoscalerInstance,
        node_type_configs: Dict[NodeType, NodeTypeConfig],
        disable_launch_config_check: bool,
    ) -> Optional["SchedulingNode"]:
        """Create a new scheduling node from an autoscaler instance.

        It creates:
            - None if the instance is not schedulable by IM.
            - A schedulable node if the instance is running ray or pending to run ray,
              so it should be considered in the scheduling process.

        Args:
            instance: The instance.
            node_type_configs: The node type configs.
            disable_launch_config_check: If outdated node check through launch config is
                disabled.

        Returns:
            A scheduling node for the instance, or None if the instance is not
            schedulable.
        """
        if not SchedulingNode.is_schedulable(instance):
            return None

        node_config = node_type_configs.get(instance.im_instance.instance_type, None)

        if instance.im_instance.status == Instance.RAY_RUNNING:
            if instance.ray_node is None:
                # Defensive: a RAY_RUNNING instance whose ray_node we cannot
                # find in GCS indicates a transient inconsistency between the
                # instance manager and GCS (e.g. the worker pod restarted
                # during the drain window and the stuck-instance handler
                # reverted the instance back to RAY_RUNNING with a stale
                # node_id). Skip rather than asserting, so that a single bad
                # row does not crash the entire reconcile loop and block all
                # autoscaling decisions.
                logger.warning(
                    "Skipping RAY_RUNNING instance with ray_node=None (stale "
                    f"state): instance_id={instance.im_instance.instance_id}, "
                    f"node_id={instance.im_instance.node_id}. This usually "
                    "indicates a transient inconsistency between the instance "
                    "manager and GCS."
                )
                return None
            # An running ray node
            return SchedulingNode(
                node_type=instance.im_instance.instance_type,
                total_resources=dict(instance.ray_node.total_resources),
                # Available resources for scheduling requests of different
                # sources.
                available_resources=dict(instance.ray_node.available_resources),
                labels={
                    **(instance.ray_node.labels or {}),
                    # DEPRECATED: Dynamic labels are a deprecated feature. This field
                    # is used here only for the autoscaler’s strict-spread placement
                    # group scheduling (antiaffinity).
                    **(instance.ray_node.dynamic_labels or {}),
                },
                status=SchedulingNodeStatus.SCHEDULABLE,
                im_instance_id=instance.im_instance.instance_id,
                im_instance_status=instance.im_instance.status,
                ray_node_id=instance.im_instance.node_id,
                idle_duration_ms=instance.ray_node.idle_duration_ms,
                launch_config_hash=instance.im_instance.launch_config_hash,
                node_kind=instance.im_instance.node_kind,
                priority=node_config.priority if node_config else 0,
                provider_replica_id=instance.cloud_instance_replica_id,
                launch_request_id=instance.im_instance.launch_request_id or None,
                cloud_instance_id=instance.cloud_instance_id
                or instance.im_instance.cloud_instance_id,
            )

        # This is an instance pending to run ray. Initialize a schedulable node
        # from the node type config.
        if node_config is None:
            if disable_launch_config_check:
                # We are not terminating outdated nodes.
                logger.info(
                    f"Node config for {instance.im_instance.instance_type} is missing, "
                    "but we are not terminating the outdated node because "
                    "`disable_launch_config_check` is True in "
                    "the autoscaler's provider config."
                )
                return None

            # Configs might have been updated, and no more
            # node_type_configs for this node type. We should terminate it.
            return SchedulingNode(
                node_type=instance.im_instance.instance_type,
                total_resources={},
                available_resources={},
                labels={},
                status=SchedulingNodeStatus.TO_TERMINATE,
                im_instance_id=instance.im_instance.instance_id,
                im_instance_status=instance.im_instance.status,
                termination_request=TerminationRequest(
                    id=str(uuid.uuid4()),
                    instance_id=instance.im_instance.instance_id,
                    instance_status=instance.im_instance.status,
                    cause=TerminationRequest.Cause.OUTDATED,
                    instance_type=instance.im_instance.instance_type,
                ),
                node_kind=NodeKind.WORKER,
                provider_replica_id=instance.cloud_instance_replica_id,
                launch_request_id=instance.im_instance.launch_request_id or None,
                cloud_instance_id=instance.cloud_instance_id
                or instance.im_instance.cloud_instance_id,
            )

        node = SchedulingNode.from_node_config(
            node_config,
            SchedulingNodeStatus.SCHEDULABLE,
            node_kind=instance.im_instance.node_kind,
            im_instance_id=instance.im_instance.instance_id,
            im_instance_status=instance.im_instance.status,
            provider_replica_id=instance.cloud_instance_replica_id,
            launch_request_id=instance.im_instance.launch_request_id or None,
            cloud_instance_id=instance.cloud_instance_id
            or instance.im_instance.cloud_instance_id,
        )
        if instance.im_instance.status == Instance.RAY_STOP_REQUESTED:
            if instance.ray_node is not None and node_config.domain_label_key:
                dom_val = (instance.ray_node.labels or {}).get(
                    node_config.domain_label_key
                )
                if dom_val:
                    node.labels[node_config.domain_label_key] = dom_val
            if node_config.head_resources:
                ray_tot = (
                    instance.ray_node.total_resources or {}
                    if instance.ray_node is not None
                    else {}
                )
                for hk in node_config.head_resources:
                    if ray_tot.get(hk, 0) <= 0:
                        node.total_resources.pop(hk, None)
                        for avail in node.available_resources_for_sched.values():
                            avail.pop(hk, None)
        return node

    @staticmethod
    def is_schedulable(instance: AutoscalerInstance) -> bool:
        """Check if the instance is schedulable by IM.

        Args:
            instance: The instance.

        Returns:
            True if the instance is schedulable by IM.
        """
        if instance.im_instance is None:
            # We will skip any instances that are not yet in IM which
            # could be
            #   1. an out-of-band ray node
            #   2. an cloud instance running ray not yet discovered
            #      by the IM's Reconciler
            #   3. an cloud instance already terminated but ray state
            #      still lagging behind.
            #
            # In all of these cases, the instance is not schedulable or
            # shouldn't be managed by IM, so we don't consider them.
            return False

        # These are the statuses where there's a running ray node or
        # could eventually run ray.
        if InstanceUtil.is_ray_running_reachable(instance.im_instance.status):
            return True

        return False

    @staticmethod
    def from_node_config(
        node_config: NodeTypeConfig,
        status: SchedulingNodeStatus,
        node_kind: NodeKind,
        im_instance_id: Optional[str] = None,
        im_instance_status: Optional[str] = None,
        provider_replica_id: Optional[str] = None,
        launch_request_id: Optional[str] = None,
        cloud_instance_id: str = "",
    ) -> "SchedulingNode":
        """Create a scheduling node from a node config.

        Args:
            node_config: The node config.
            status: The status of the node.
            node_kind: The node kind.
            im_instance_id: The instance id of the im instance.
            im_instance_status: The instance status of the im instance.
            provider_replica_id: Optional provider replica group identifier.
            launch_request_id: Optional IM launch request identifier.
            cloud_instance_id: Optional cloud provider instance identifier.

        Returns:
            A scheduling node for the given node config.
        """
        return SchedulingNode(
            node_type=node_config.name,
            total_resources=dict(node_config.resources),
            available_resources=dict(node_config.resources),
            labels=dict(node_config.labels),
            status=status,
            im_instance_id=im_instance_id,
            im_instance_status=im_instance_status,
            node_kind=node_kind,
            priority=node_config.priority,
            provider_replica_id=provider_replica_id,
            launch_request_id=launch_request_id,
            cloud_instance_id=cloud_instance_id,
        )

    def __post_init__(self):
        assert self.node_type, "node_type should be set"

    def try_schedule(
        self,
        requests: List[ResourceRequest],
        resource_request_source: ResourceRequestSource,
        shape_keys: Optional[Dict[int, bytes]] = None,
        active_selectors: Optional[Dict[bytes, int]] = None,
        precomputed_matches: Optional[Dict[bytes, Tuple[int, bool]]] = None,
    ) -> Tuple[List[ResourceRequest], Tuple[int, bool, int, float, float]]:
        """Try to schedule the resource requests on this node.

        This modifies the node's available resources if the requests are schedulable.
        Pass 1 places requests whose active selector is constrained on this node;
        Pass 2 places requests whose active selector is unconstrained unless this
        call already placed a constrained request and an unplaced request has a
        fitting fallback selector on this node. Within each pass, requests are
        scheduled one by one in sorted order without backtracking, restarting from
        Pass 1 if anti-affinity grows `self.labels`.

        Args:
            requests: The resource requests to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.
            shape_keys: Precomputed serialization keys for each request. If None,
                keys are computed on the fly (fallback).
            active_selectors: Best (lowest) schedulable selector index across
                candidate nodes by shape key. If None, each request defaults to
                its first matching selector on this node.
            precomputed_matches: Optional schedulable-selector results by shape
                key valid for this node's initial `labels`.

        Returns:
            A tuple of:
                - list of remaining requests that cannot be scheduled on this node.
                - the utilization score for this node with respect to the current
                resource requests being scheduled.
        """

        pending = [(r, _shape_key(r, shape_keys)) for r in requests]
        sample_by_key = {sk: r for r, sk in pending}
        available_resources = self.get_available_resources(resource_request_source)
        initial_num_labels = len(self.labels)
        scheduled_constrained = False
        roles: Dict[bytes, Optional[int]] = {}

        def role(sk: bytes, ms: Optional[Tuple[int, bool]]) -> Optional[int]:
            if ms is None:
                return None
            if active_selectors is not None:
                act = active_selectors.get(sk)
                if act is None:
                    return None
                if ms[0] > act:
                    return _DEFERRED
            return _CONSTRAINED if ms[1] else _UNCONSTRAINED

        def classify() -> Dict[bytes, Optional[int]]:
            """Role of each pending shape under this node's current labels."""
            precomputed = (
                precomputed_matches if len(self.labels) == initial_num_labels else None
            )
            out: Dict[bytes, Optional[int]] = {}
            for r, sk in pending:
                if sk not in out:
                    ms = (
                        precomputed.get(sk)
                        if precomputed is not None
                        else self._first_matching_selector(r)
                    )
                    out[sk] = role(sk, ms)
            return out

        def place(target_role: int) -> bool:
            """Greedily place pending requests with `target_role` in order.

            Return True if a placement added a node label, invalidating `roles`.
            """
            nonlocal pending, scheduled_constrained
            eligible = {sk for sk, r_role in roles.items() if r_role == target_role}
            num_labels = len(self.labels)
            kept = []
            for idx, (r, sk) in enumerate(pending):
                if sk in eligible:
                    if self._try_schedule_one(
                        r, resource_request_source, available_resources
                    ):
                        scheduled_constrained |= target_role == _CONSTRAINED
                        if len(self.labels) > num_labels:
                            pending = kept + pending[idx + 1 :]
                            return True
                        continue
                    eligible.remove(sk)
                kept.append((r, sk))
            pending = kept
            return False

        while True:
            roles = classify()
            if place(_CONSTRAINED):
                continue
            if scheduled_constrained and any(
                r_role == _DEFERRED
                and self._can_fit_request(sample_by_key[sk], available_resources)
                for sk, r_role in roles.items()
            ):
                break
            if not place(_UNCONSTRAINED):
                break

        unschedulable_requests = [r for r, _ in pending]
        return unschedulable_requests, self._compute_score(
            resource_request_source, int(scheduled_constrained)
        )

    def _compute_score(
        self,
        resource_request_source: ResourceRequestSource,
        label_score: int,
    ) -> Tuple[int, bool, int, float, float]:
        """Compute the utilization score for this node with respect to the current resource
        request being scheduled.

        A "higher" score means that this node is more suitable for scheduling the
        current scheduled resource requests.

        The score is a tuple of 5 values:
            1. Whether this node scheduled any constrained label_selector request
                in the current `try_schedule` pass:
                    0: if no constrained label selector was matched in this pass.
                    1: if at least one request matching a constrained active label
                       selector was scheduled in this pass.
            2. Whether this node is a GPU node and the current resource request has
                GPU requirements:
                    0: if this node is a GPU node and the current resource request
                    placed onto the node has no GPU requirements.
                    1: if this node is not a GPU node or the current resource request
                    placed onto the node has GPU requirements.
            3. The number of resource types being scheduled.
            4. The minimum utilization rate across all resource types.
            5. The average utilization rate across all resource types.

        NOTE:
            This function is adapted from  _resource_based_utilization_scorer from
            autoscaler v1.

        TODO(rickyx): add pluggable scoring functions here.

        Args:
            resource_request_source: The resource request source to score
                against.
            label_score: 1 if the current `try_schedule` pass scheduled at least
                one constrained label-selector request, else 0.

        Returns:
            A utilization score for this node.
        """

        sched_requests = self.get_sched_requests(resource_request_source)
        available_resources = self.get_available_resources(resource_request_source)

        # Compute the number of resource types being scheduled.
        num_matching_resource_types = 0
        sched_resource_types = set()
        for req in sched_requests:
            for resource_name, v in req.resources_bundle.items():
                if v > 0:
                    sched_resource_types.add(resource_name)

        for sched_resource_type in sched_resource_types:
            if sched_resource_type in self.total_resources:
                num_matching_resource_types += 1

        # Compute the utilization rate for each resource type
        util_by_resources = []
        for k, v in self.total_resources.items():
            if v == 0:
                # Skip any zero values.
                continue
            if k in available_resources:
                util = (v - available_resources.get(k, 0)) / v
                assert util >= 0 and util <= 1, f"Invalid utilization: {util}"
                util_by_resources.append(v * (util**3))

        # Prefer not to launch a GPU node if there aren't any GPU requirements in the
        # resource bundle.
        gpu_ok = True
        if AUTOSCALER_CONSERVE_GPU_NODES:
            # TODO: we should also generalize this optimization for accelerators.
            # https://github.com/ray-project/ray/issues/43079
            is_gpu_node = self.total_resources.get("GPU", 0) > 0
            any_gpu_requests = any("GPU" in r.resources_bundle for r in sched_requests)
            if is_gpu_node and not any_gpu_requests:
                gpu_ok = False

        # Prioritize avoiding gpu nodes for non-gpu workloads first,
        # then prioritize matching multiple resource types,
        # then prioritize using all resources,
        # then prioritize overall balance of multiple resources.
        return (
            label_score,
            gpu_ok,
            num_matching_resource_types,
            min(util_by_resources) if util_by_resources else 0,
            float(sum(util_by_resources)) / len(util_by_resources)
            if util_by_resources
            else 0,
        )

    def _first_matching_selector(
        self, request: ResourceRequest
    ) -> Optional[Tuple[int, bool]]:
        """Return `(selector_idx, is_constrained)` for the first selector this node
        satisfies, `(0, False)` if none are specified, or `None` if none match."""
        if not request.label_selectors:
            return (0, False)
        for i, selector in enumerate(request.label_selectors):
            if not selector.label_constraints:
                return (i, False)
            all_constraints_pass = True
            for constraint in selector.label_constraints:
                values = constraint.label_values
                op = constraint.operator
                node_val = self.labels.get(constraint.label_key)
                if op == _LABEL_OPERATOR_IN:
                    if node_val not in values:
                        all_constraints_pass = False
                        break
                elif op == _LABEL_OPERATOR_NOT_IN:
                    if node_val in values:
                        all_constraints_pass = False
                        break
                else:
                    all_constraints_pass = False
                    break
            if all_constraints_pass:
                return (i, True)
        return None

    def _can_fit_request(
        self,
        request: ResourceRequest,
        available_resources: Dict[str, float],
    ) -> bool:
        """Return True if `request` satisfies anti-affinity and fits on this node."""
        for constraint in request.placement_constraints:
            if constraint.HasField("anti_affinity"):
                anti_affinity = constraint.anti_affinity
                if (
                    self.labels.get(anti_affinity.label_name)
                    == anti_affinity.label_value
                ):
                    return False
        return _fits(available_resources, request.resources_bundle)

    def _schedulable_selector(
        self,
        request: ResourceRequest,
        available_resources: Dict[str, float],
    ) -> Optional[Tuple[int, bool]]:
        """Return `_first_matching_selector` if `request` fits, else `None`."""
        if not self._can_fit_request(request, available_resources):
            return None
        return self._first_matching_selector(request)

    def _try_schedule_one(
        self,
        request: ResourceRequest,
        resource_request_source: ResourceRequestSource,
        available_resources: Dict[str, float],
    ) -> bool:
        """Try to schedule one resource request on this node.

        Args:
            request: The resource request to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.
            available_resources: Available resources dict for
                `resource_request_source`.

        Returns:
            True if the resource request is scheduled on this node.
        """
        if not self._can_fit_request(request, available_resources):
            return False

        # Schedule the request, update resources
        _inplace_subtract(available_resources, dict(request.resources_bundle))

        # Add the request to the node.
        self.add_sched_request(request, resource_request_source)

        # Update the placement group in labels if there's any
        for constraint in request.placement_constraints:
            # We don't need to check for affinity constraints here since
            # we have already combined resource requests with the affinity
            # constraints into the same request at `combine_requests_with_affinity`.
            # We don't need node labels for enforcing affinity.
            if constraint.HasField("anti_affinity"):
                anti_affinity = constraint.anti_affinity
                self._add_label(anti_affinity.label_name, anti_affinity.label_value)

        return True

    def _add_label(self, label_name: str, label_value: str):
        """Add a label to the node.
        This assumes a label key can only have one value.
        """
        assert (
            self.labels.get(label_name) is None
            or self.labels[label_name] == label_value
        ), (
            f"Label {label_name} already exists with value "
            f"{self.labels[label_name]}, cannot set to "
            f"{label_value}"
        )
        self.labels[label_name] = label_value

    @property
    def is_running(self) -> bool:
        """Whether this node is backed by a live Ray node."""
        return bool(self.ray_node_id) or self.im_instance_status == Instance.RAY_RUNNING

    @property
    def is_leaving(self) -> bool:
        """Whether this node is draining or requested to stop Ray."""
        return (
            self._is_draining_replica
            or self.im_instance_status == Instance.RAY_STOP_REQUESTED
        )

    @property
    def progress_rank(self) -> int:
        """Lifecycle rank (-1=running, 0..3=pending IM progression, 5=to_launch)."""
        if self.is_running:
            return -1
        if self.status == SchedulingNodeStatus.TO_LAUNCH:
            return 5
        return ResourceDemandScheduler._PENDING_PROGRESSION.get(
            self.im_instance_status, 4
        )

    def __repr__(self) -> str:
        return (
            "SchedulingNode(node_type={node_type}, "
            "node_kind={node_kind}, "
            "instance_id={instance_id},"
            "instance_status={instance_status},"
            "ray_node_id={ray_node_id},"
            "idle_duration_ms={idle_duration_ms},"
            "termination_request={termination_request},"
            "status={status}, "
            "total_resources={total_resources}, "
            "available_resources_for_demand={available_resources_for_demand}, "
            "available_resources_for_cluster_resource_constraints="
            "{available_resources_for_cluster_resource_constraints},"
            "labels={labels}, launch_reason={launch_reason}), "
            "sched_requests_for_demand={sched_requests_for_demand}), "
            "sched_requests_for_cluster_resource_constraints="
            "{sched_requests_for_cluster_resources_constraint})"
        ).format(
            node_type=self.node_type,
            node_kind=self.node_kind,
            instance_id=self.im_instance_id,
            instance_status=self.im_instance_status,
            ray_node_id=self.ray_node_id,
            idle_duration_ms=self.idle_duration_ms,
            termination_request=str(message_to_dict(self.termination_request))
            if self.termination_request
            else None,
            status=self.status,
            total_resources=self.total_resources,
            available_resources_for_demand=self.available_resources_for_sched[
                ResourceRequestSource.PENDING_DEMAND
            ],
            available_resources_for_cluster_resource_constraints=self.available_resources_for_sched[  # noqa
                ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT
            ],
            labels=self.labels,
            launch_reason=self.launch_reason,
            sched_requests_for_demand="|".join(
                str(message_to_dict(r))
                for r in self.sched_requests[ResourceRequestSource.PENDING_DEMAND]
            ),
            sched_requests_for_cluster_resources_constraint="|".join(
                str(message_to_dict(r))
                for r in self.sched_requests[
                    ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT
                ]
            ),
        )


def _coalesce_runs(
    runs: Iterable[Tuple[bytes, int]],
) -> Tuple[Tuple[bytes, int], ...]:
    out: List[Tuple[bytes, int]] = []
    for k, c in runs:
        if out and out[-1][0] == k:
            out[-1] = (k, out[-1][1] + c)
        else:
            out.append((k, c))
    return tuple(out)


def _consume_stream(
    coalesced: Tuple[Tuple[bytes, int], ...], remaining_counts: Dict[bytes, int]
) -> Tuple[Tuple[Tuple[bytes, int, bool], ...], FrozenSet[bytes]]:
    """Record `(shape, accepted, took_all)` through the last accepted run and
    the shapes rejected after it."""
    prefix: List[Tuple[bytes, int, bool]] = []
    last_idx = -1
    for idx, (k, c) in enumerate(coalesced):
        n = remaining_counts.get(k, 0)
        take = min(c, n)
        if take:
            remaining_counts[k] = n - take
            last_idx = idx
        prefix.append((k, take, take == c))
    return (
        tuple(prefix[: last_idx + 1]),
        frozenset(k for k, _ in coalesced[last_idx + 1 :]),
    )


def _stream_matches(
    coalesced: Tuple[Tuple[bytes, int], ...],
    prefix: Tuple[Tuple[bytes, int, bool], ...],
    rejected: FrozenSet[bytes],
) -> bool:
    """Return True if each prefix run still offers its accepted count (or exact
    count when exhausted) and every trailing run's shape was already rejected."""
    m = len(prefix)
    if len(coalesced) < m:
        return False
    for (k, c), (pk, take, exact) in zip(coalesced, prefix):
        if k != pk or (c != take if exact else c < take):
            return False
    return all(coalesced[j][0] in rejected for j in range(m, len(coalesced)))


@dataclass
class _NodeAttempt:
    """Cached scheduling attempt for one candidate node in `_sched_best_node`."""

    matched: Dict[bytes, Tuple[int, bool]]
    c_matched: Tuple[Tuple[bytes, int], ...] = ()
    u_matched: Tuple[Tuple[bytes, int], ...] = ()
    # Rejected-shape sets keep cache reuse sound independent of candidate pruning.
    c_prefix: Tuple[Tuple[bytes, int, bool], ...] = ()
    c_rejected: FrozenSet[bytes] = frozenset()
    u_prefix: Tuple[Tuple[bytes, int, bool], ...] = ()
    u_rejected: FrozenSet[bytes] = frozenset()
    fallback_blockers: FrozenSet[bytes] = frozenset()
    node: Optional[SchedulingNode] = None
    scheduled_requests: List[ResourceRequest] = field(default_factory=list)
    score: Tuple = ()

    @classmethod
    def from_matched(cls, matched: Dict[bytes, Tuple[int, bool]]) -> "_NodeAttempt":
        return cls(
            matched=matched,
            c_matched=tuple((k, idx) for k, (idx, is_c) in matched.items() if is_c),
            u_matched=tuple((k, idx) for k, (idx, is_c) in matched.items() if not is_c),
        )

    def has_constrained(self, active_selectors: Dict[bytes, int]) -> bool:
        for k, idx in self.c_matched:
            if active_selectors.get(k) == idx:
                return True
        return False

    def matches(
        self,
        stream_for: Callable[
            [Tuple[Tuple[bytes, int], ...]], Tuple[Tuple[bytes, int], ...]
        ],
        active_selectors: Dict[bytes, int],
    ) -> bool:
        if not _stream_matches(
            stream_for(self.c_matched), self.c_prefix, self.c_rejected
        ):
            return False
        if self.fallback_blockers:
            for k in self.fallback_blockers:
                act = active_selectors.get(k)
                if act is not None and act < self.matched[k][0]:
                    return True
            return False
        return _stream_matches(
            stream_for(self.u_matched), self.u_prefix, self.u_rejected
        )

    def with_schedule(
        self,
        active_selectors: Dict[bytes, int],
        node_copy: SchedulingNode,
        new_sched: List[ResourceRequest],
        score: Tuple,
        stream_for: Callable[
            [Tuple[Tuple[bytes, int], ...]], Tuple[Tuple[bytes, int], ...]
        ],
        shape_keys: Optional[Dict[int, bytes]],
        requests_by_key: Dict[bytes, ResourceRequest],
        resource_request_source: ResourceRequestSource,
    ) -> "_NodeAttempt":
        counts = Counter(_shape_key(r, shape_keys) for r in new_sched)
        c_prefix, c_rejected = _consume_stream(stream_for(self.c_matched), counts)
        blockers: FrozenSet[bytes] = frozenset()
        if not any(counts.values()):
            avail = node_copy.get_available_resources(resource_request_source)
            blockers = frozenset(
                k
                for k, (m_idx, _) in self.matched.items()
                if (act := active_selectors.get(k)) is not None
                and act < m_idx
                and node_copy._can_fit_request(requests_by_key[k], avail)
            )
        u_prefix, u_rejected = (
            ((), frozenset())
            if blockers
            else _consume_stream(stream_for(self.u_matched), counts)
        )
        return _NodeAttempt(
            matched=self.matched,
            c_matched=self.c_matched,
            u_matched=self.u_matched,
            c_prefix=c_prefix,
            c_rejected=c_rejected,
            u_prefix=u_prefix,
            u_rejected=u_rejected,
            fallback_blockers=blockers,
            node=node_copy,
            scheduled_requests=new_sched,
            score=score,
        )


@dataclass
class _GangSearch:
    """Evaluates single-domain and unlabeled gang placements for a STRICT_PACK request."""

    gang_ctx: "ResourceDemandScheduler.ScheduleContext"
    all_nodes: List[SchedulingNode]
    requests: List[ResourceRequest]
    locality_label: str
    pinned: bool
    max_bundles_per_node: int
    max_label_score: float
    domains: Dict[str, List[SchedulingNode]]
    static_domains: Set[str]
    exclusive_domain_type_set: Set[NodeType]
    step5_type_set: Set[NodeType]
    unlabeled_pending_by_type: Dict[NodeType, List[SchedulingNode]]
    node_type_available: Dict[NodeType, int]
    node_type_configs: Dict[NodeType, NodeTypeConfig]
    availabilities: Dict[NodeType, float]
    recoverable: Dict[NodeType, float]
    headroom: Optional[int]
    best: Optional[Tuple[Tuple, List[SchedulingNode]]] = None

    def __post_init__(self) -> None:
        self.req_resource_totals: Dict[str, float] = defaultdict(float)
        for req in self.requests:
            for res_name, qty in req.resources_bundle.items():
                if qty > 0:
                    self.req_resource_totals[res_name] += qty
        self.shapes = _collect_unique_resource_shapes(self.requests)
        self.min_nodes_needed = (
            math.ceil(len(self.requests) / self.max_bundles_per_node)
            if self.max_bundles_per_node > 0
            else len(self.requests)
        )
        self.requests_ref_locality = any(
            c.label_key == self.locality_label
            for r in self.requests
            for sel in r.label_selectors
            for c in sel.label_constraints
        ) or any(
            c.HasField("anti_affinity")
            and c.anti_affinity.label_name == self.locality_label
            for r in self.requests
            for c in r.placement_constraints
        )
        self.orig_to_launch = Counter(
            n.node_type
            for n in self.all_nodes
            if n.status == SchedulingNodeStatus.TO_LAUNCH
        )
        self.orig_used_pending = sum(
            not n.is_running
            and bool(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in self.all_nodes
        )
        self.orig_used_placeholders = sum(
            n._is_backfill_placeholder
            and bool(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in self.all_nodes
        )
        self.visit_idx = itertools.count()

    def node_type_rank(self, nt: NodeType) -> Tuple[float, int, float]:
        cfg = self.node_type_configs.get(nt)
        return (
            self.recoverable.get(nt, 1.0),
            cfg.priority if cfg else 0,
            self.availabilities.get(nt, 1.0),
        )

    def domain_rank(self, domain_val: str) -> Tuple:
        domain_nodes = self.domains.get(domain_val, [])
        nts = {n.node_type for n in domain_nodes} | {
            nt
            for nt, cfg in self.node_type_configs.items()
            if cfg.labels.get(self.locality_label) == domain_val
        }
        has_running = any(n.is_running for n in domain_nodes)
        best_prog = min(
            (n.progress_rank for n in domain_nodes if not n.is_running),
            default=-1 if has_running else 5,
        )
        return (
            not has_running,
            not any(n.status == SchedulingNodeStatus.SCHEDULABLE for n in domain_nodes),
            best_prog,
            tuple(-x for x in max(map(self.node_type_rank, nts))),
            domain_val,
        )

    def _consider(
        self,
        nodes: Optional[List[SchedulingNode]],
        target_domain: str,
        prior_score: float = 0.0,
        added: Optional[List[SchedulingNode]] = None,
        replaced: Optional[List[SchedulingNode]] = None,
    ) -> bool:
        label_score_fn = ResourceDemandScheduler._node_label_score
        cand = added if added is not None else nodes
        assert cand is not None
        domain_nodes = [
            n
            for n in cand
            if n.labels.get(self.locality_label) == target_domain
            and n.status != SchedulingNodeStatus.TO_TERMINATE
            and not n.is_leaving
        ]

        def _used_counts(seq: List[SchedulingNode]) -> Tuple[int, int]:
            pend = 0
            ph = 0
            for n in seq:
                if n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND):
                    if not n.is_running:
                        pend += 1
                    if n._is_backfill_placeholder:
                        ph += 1
            return pend, ph

        cand_pend, cand_ph = _used_counts(cand)
        if replaced is not None:
            base_len = len(replaced)
            base_pend, base_ph = _used_counts(replaced)
            base_tl = (
                Counter(
                    n.node_type
                    for n in replaced
                    if n.status == SchedulingNodeStatus.TO_LAUNCH
                )
                if len(cand) > base_len
                else None
            )
        else:
            base_len = len(self.all_nodes)
            base_pend = self.orig_used_pending
            base_ph = self.orig_used_placeholders
            base_tl = self.orig_to_launch
        num_to_launch = len(cand) - base_len
        used_pending = cand_pend - base_pend
        used_placeholders = cand_ph - base_ph
        launch_neg_rank = (0.0, 0, 0.0)
        if num_to_launch > 0:
            launched = (
                Counter(
                    n.node_type
                    for n in cand
                    if n.status == SchedulingNodeStatus.TO_LAUNCH
                )
                - base_tl
            )
            launch_neg_rank = tuple(-x for x in min(map(self.node_type_rank, launched)))
        label_score = (
            math.fsum(label_score_fn(n) for n in domain_nodes) - prior_score
            if self.max_label_score
            else 0.0
        )
        has_running = any(n.is_running for n in domain_nodes)
        tail = (
            launch_neg_rank,
            used_placeholders,
            not has_running,
            used_pending,
            next(self.visit_idx),
        )
        is_better = False
        if self.best is None:
            is_better = True
        else:
            best_launch, best_ls, best_tail = self.best[0]
            if num_to_launch != best_launch:
                is_better = num_to_launch < best_launch
            elif abs(label_score - best_ls) > _LABEL_SCORE_TOL:
                is_better = label_score > best_ls
            else:
                is_better = tail < best_tail
        if is_better:
            if nodes is None:
                assert added is not None and replaced is not None
                rep_ids = {id(n) for n in replaced}
                nodes = [n for n in self.all_nodes if id(n) not in rep_ids] + list(
                    added
                )
            self.best = ((num_to_launch, label_score, tail), nodes)
        return (
            num_to_launch == 0
            and used_pending == 0
            and has_running
            and label_score >= self.max_label_score - _LABEL_SCORE_TOL
        )

    @staticmethod
    def _prep_siblings(
        pending_siblings: List[SchedulingNode],
        locality_label: str,
        domain_val: str,
    ) -> List[SchedulingNode]:
        siblings = copy.deepcopy(pending_siblings)
        for sib in siblings:
            sib._saved_locality_label = (
                locality_label in sib.labels,
                sib.labels.get(locality_label),
            )
            sib.labels[locality_label] = domain_val
        return siblings

    @staticmethod
    def _restore_unused_sibling_labels(
        scheduled: List[SchedulingNode], locality_label: str
    ) -> None:
        used_replicas = {
            (n.node_type, n.replica_id)
            for n in scheduled
            if n.replica_id is not None
            and n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
        }
        for n in scheduled:
            saved = n._saved_locality_label
            if saved is None:
                continue
            n._saved_locality_label = None
            if not n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND) and (
                n.replica_id is None or (n.node_type, n.replica_id) not in used_replicas
            ):
                had_key, prior_val = saved
                if had_key:
                    n.labels[locality_label] = prior_val
                else:
                    n.labels.pop(locality_label, None)

    @staticmethod
    def _group_nodes_by_replica(
        nodes: List[SchedulingNode],
        *,
        shapes: Optional[List[Dict[str, float]]] = None,
        min_nodes_needed: int = 0,
    ) -> List[SchedulingNode]:
        """Group nodes sharing `(node_type, replica_id)` contiguously."""
        if len(nodes) <= 1:
            return list(nodes)
        groups: Dict[Tuple, List[SchedulingNode]] = {}
        for idx, node in enumerate(nodes):
            key = (
                (node.node_type, node.replica_id)
                if node.replica_id is not None
                else ("", idx)
            )
            groups.setdefault(key, []).append(node)
        if len(groups) == len(nodes):
            return list(nodes)
        ordered = list(groups.values())
        if shapes is not None and min_nodes_needed > 0:

            def _group_key(
                grp: List[SchedulingNode],
            ) -> Tuple[bool, int, int, int]:
                free_cnt = 0
                running_free_cnt = 0
                for m in grp:
                    if _can_fit_any_request(
                        m.get_available_resources(ResourceRequestSource.PENDING_DEMAND),
                        shapes,
                    ):
                        free_cnt += 1
                        if m.is_running:
                            running_free_cnt += 1
                return (
                    not (free_cnt >= min_nodes_needed),
                    -(len(grp) - free_cnt),
                    -free_cnt,
                    -running_free_cnt,
                )

            ordered.sort(key=_group_key)
        return [n for grp in ordered for n in grp]

    @staticmethod
    def schedule_in_domain(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        all_nodes: List[SchedulingNode],
        domain_nodes: List[SchedulingNode],
        replaced: List[SchedulingNode],
        pending_siblings: List[SchedulingNode],
        requests: List[ResourceRequest],
        locality_label: str,
        domain_val: str,
        return_delta: bool = False,
    ):
        """Schedule `requests` within a single dynamic domain and any booting
        `pending_siblings` without launching new nodes. Returns the updated node
        list, or None if the gang does not fit."""
        shapes = _collect_unique_resource_shapes(requests)
        max_bpn = ResourceDemandScheduler._max_bundles_per_node(requests)
        min_nodes_needed = (
            math.ceil(len(requests) / max_bpn) if max_bpn > 0 else len(requests)
        )
        domain_nodes = _GangSearch._group_nodes_by_replica(
            domain_nodes, shapes=shapes, min_nodes_needed=min_nodes_needed
        )
        label_score_fn = ResourceDemandScheduler._node_label_score
        running = _GangSearch._group_nodes_by_replica(
            [n for n in domain_nodes if n.is_running],
            shapes=shapes,
            min_nodes_needed=min_nodes_needed,
        )
        non_running = [n for n in domain_nodes if not n.is_running]
        max_ls = sum(1.0 for r in requests if len(r.label_selectors) > 1)
        prior_ls = math.fsum(label_score_fn(n) for n in domain_nodes) if max_ls else 0.0
        run_fit: Optional[Tuple[List[SchedulingNode], float]] = None
        if running and non_running:
            sched_run, inf_run = ResourceDemandScheduler._try_schedule(
                ctx.restricted_to(running),
                requests,
                ResourceRequestSource.PENDING_DEMAND,
            )
            if not inf_run:
                cand_run = sched_run + copy.deepcopy(non_running)
                ls_run = (
                    math.fsum(label_score_fn(n) for n in cand_run) - prior_ls
                    if max_ls
                    else 0.0
                )
                if ls_run >= max_ls - _LABEL_SCORE_TOL:
                    if return_delta:
                        return cand_run, list(replaced)
                    rep_ids = {id(n) for n in replaced}
                    return [n for n in all_nodes if id(n) not in rep_ids] + cand_run
                run_fit = (cand_run, ls_run)

        scheduled, infeasible = ResourceDemandScheduler._try_schedule(
            ctx.restricted_to(domain_nodes),
            requests,
            ResourceRequestSource.PENDING_DEMAND,
        )
        if not infeasible and run_fit is not None:
            ls_dom = (
                math.fsum(label_score_fn(n) for n in scheduled) - prior_ls
                if max_ls
                else 0.0
            )
            if ls_dom <= run_fit[1] + _LABEL_SCORE_TOL:
                scheduled = run_fit[0]
        if infeasible and pending_siblings:
            if len(infeasible) < len(requests):
                scheduled, infeasible = ResourceDemandScheduler._try_schedule(
                    ctx.restricted_to(
                        scheduled
                        + _GangSearch._prep_siblings(
                            pending_siblings, locality_label, domain_val
                        )
                    ),
                    infeasible,
                    ResourceRequestSource.PENDING_DEMAND,
                )
            if infeasible:
                # Retry jointly across domain_nodes + siblings so fallback-tier
                # bundles on earlier nodes do not starve stricter sibling slots.
                scheduled, infeasible = ResourceDemandScheduler._try_schedule(
                    ctx.restricted_to(
                        domain_nodes
                        + _GangSearch._prep_siblings(
                            pending_siblings, locality_label, domain_val
                        )
                    ),
                    requests,
                    ResourceRequestSource.PENDING_DEMAND,
                )
            replaced = list(replaced) + list(pending_siblings)
            if not infeasible:
                _GangSearch._restore_unused_sibling_labels(scheduled, locality_label)
        if infeasible:
            return None
        if return_delta:
            return scheduled, list(replaced)
        rep_ids = {id(n) for n in replaced}
        return [n for n in all_nodes if id(n) not in rep_ids] + scheduled

    def _collect_pending_siblings(
        self, domain_nodes: List[SchedulingNode], domain_val: str
    ) -> List[SchedulingNode]:
        if (
            not self.unlabeled_pending_by_type
            or not domain_nodes
            or domain_val.startswith(_PENDING_DOMAIN_PREFIX)
            or any(
                dn.node_type in self.exclusive_domain_type_set for dn in domain_nodes
            )
        ):
            return []
        candidate_types = dict.fromkeys(
            [dn.node_type for dn in domain_nodes]
            + sorted(self.step5_type_set, key=self.node_type_rank, reverse=True)
        )
        if not any(self.unlabeled_pending_by_type.get(nt) for nt in candidate_types):
            return []
        if not ResourceDemandScheduler._has_domain_spare_capacity(
            domain_nodes, self.requests, self.max_bundles_per_node
        ):
            return []
        domain_rids = {
            dn.replica_id for dn in domain_nodes if dn.replica_id is not None
        }
        siblings: List[SchedulingNode] = []
        for nt in candidate_types:
            if self.locality_label in self.gang_ctx.get_suppressed_runtime_keys(nt):
                continue
            siblings.extend(
                pn
                for pn in self.unlabeled_pending_by_type.get(nt, ())
                if pn.status == SchedulingNodeStatus.SCHEDULABLE
            )
        if domain_rids and len(siblings) > 1:
            siblings.sort(
                key=lambda pn: (pn.replica_id not in domain_rids, pn.progress_rank)
            )
        return siblings

    def _prepare_scoped_refill_ctx(
        self,
        pool: List[SchedulingNode],
        candidate_types: Iterable[NodeType],
        in_scope_pred,
    ) -> Tuple[
        Optional["ResourceDemandScheduler.ScheduleContext"],
        Dict[NodeType, List[SchedulingNode]],
    ]:
        launch_avail: Dict[NodeType, int] = {}
        dom_refill_gaps: Dict[NodeType, int] = {}
        prebuilt_refills: Dict[NodeType, List[SchedulingNode]] = {}
        other_refills_by_type: Dict[NodeType, List[SchedulingNode]] = {}
        co_launch_overhead: Dict[NodeType, int] = {}
        for nt in candidate_types:
            size = self.node_type_configs[nt].workers_per_replica
            avail = self.node_type_available.get(nt, 0)
            total_gap = self.gang_ctx.get_unbacked_refill_gap(nt)
            if total_gap > 0:
                dom_refills = []
                other_refills = []
                for rn in self.gang_ctx.make_refill_nodes(nt, total_gap):
                    if in_scope_pred(rn):
                        dom_refills.append(rn)
                    else:
                        other_refills.append(rn)
                dom_gap = len(dom_refills)
                other_gap = len(other_refills)
                need = total_gap if dom_gap > 0 else other_gap + size
            else:
                dom_refills = []
                other_refills = []
                dom_gap = 0
                other_gap = 0
                need = size
            eff_avail = avail + (
                self.gang_ctx.get_own_pad_credit(nt) if total_gap > 0 else 0
            )
            if eff_avail >= need and (
                self.headroom is None
                or self.headroom
                + self.gang_ctx.get_headroom_credit(nt, includes_pad=total_gap > 0)
                >= need
            ):
                launch_avail[nt] = avail
                if dom_gap > 0:
                    dom_refill_gaps[nt] = dom_gap
                    prebuilt_refills[nt] = dom_refills
                if other_gap > 0:
                    co_launch_overhead[nt] = other_gap
                    other_refills_by_type[nt] = other_refills
        if not pool and not launch_avail:
            return None, {}
        return (
            self.gang_ctx.restricted_to(
                pool,
                node_type_available=launch_avail or None,
                refill_gaps=dom_refill_gaps,
                prebuilt_refills=prebuilt_refills,
                co_launch_overhead=co_launch_overhead,
            ),
            other_refills_by_type,
        )

    @staticmethod
    def _collect_launched_co_refills(
        pool: List[SchedulingNode],
        scheduled: List[SchedulingNode],
        other_refills_by_type: Dict[NodeType, List[SchedulingNode]],
    ) -> List[SchedulingNode]:
        if not other_refills_by_type:
            return []
        orig_tl = Counter(
            n.node_type for n in pool if n.status == SchedulingNodeStatus.TO_LAUNCH
        )
        sched_tl = Counter(
            n.node_type for n in scheduled if n.status == SchedulingNodeStatus.TO_LAUNCH
        )
        extra: List[SchedulingNode] = []
        for nt, cnt in sched_tl.items():
            if cnt > orig_tl[nt] and nt in other_refills_by_type:
                extra.extend(other_refills_by_type[nt])
        return extra

    def _schedule_shared_domain_with_launches(
        self,
        domain_nodes: List[SchedulingNode],
        originals: List[SchedulingNode],
        pending_siblings: List[SchedulingNode],
        domain_val: str,
    ) -> Optional[List[SchedulingNode]]:
        if (
            self.pinned
            or not domain_nodes
            or domain_val in self.static_domains
            or domain_val.startswith(_PENDING_DOMAIN_PREFIX)
            or any(
                dn.node_type in self.exclusive_domain_type_set for dn in domain_nodes
            )
            or not ResourceDemandScheduler._has_domain_spare_capacity(
                domain_nodes, self.requests, self.max_bundles_per_node
            )
        ):
            return None
        siblings = (
            self._prep_siblings(pending_siblings, self.locality_label, domain_val)
            if pending_siblings
            else []
        )
        pool = domain_nodes + siblings
        sibling_rids = {s.replica_id for s in siblings if s.replica_id is not None}
        cand_types = [
            nt
            for nt in dict.fromkeys(
                [n.node_type for n in pool]
                + sorted(self.step5_type_set, key=self.node_type_rank, reverse=True)
            )
            if nt in self.step5_type_set
        ]
        sub_ctx, other_refills_by_type = self._prepare_scoped_refill_ctx(
            pool,
            cand_types,
            lambda rn: rn.labels.get(self.locality_label) == domain_val
            or rn.replica_id in sibling_rids,
        )
        if sub_ctx is None or not sub_ctx.get_node_type_available():
            return None
        before_orig_reqs = sum(
            len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in domain_nodes
            if n._saved_locality_label is None
        )
        scheduled, infeasible = ResourceDemandScheduler._try_schedule(
            sub_ctx,
            self.requests,
            ResourceRequestSource.PENDING_DEMAND,
        )
        if infeasible:
            return None
        after_orig_reqs = sum(
            len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in scheduled
            if n.labels.get(self.locality_label) == domain_val
            and n._saved_locality_label is None
        )
        if after_orig_reqs <= before_orig_reqs:
            return None
        self._restore_unused_sibling_labels(scheduled, self.locality_label)
        sched_new_rids = {
            (n.node_type, n.replica_id)
            for n in scheduled
            if n.replica_id is not None
            and n.labels.get(self.locality_label) != domain_val
            and n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
        }
        for n in scheduled:
            if n.labels.get(self.locality_label) != domain_val and (
                n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
                or (n.node_type, n.replica_id) in sched_new_rids
            ):
                n.labels[self.locality_label] = domain_val
        extra_co_refills = self._collect_launched_co_refills(
            pool, scheduled, other_refills_by_type
        )
        replaced = list(originals) + list(pending_siblings)
        rep_ids = {id(n) for n in replaced}
        return (
            [n for n in self.all_nodes if id(n) not in rep_ids]
            + scheduled
            + extra_co_refills
        )

    def evaluate_domains(
        self,
        existing_domains: List[str],
        launch_candidates: List[Tuple[str, List[SchedulingNode], List[SchedulingNode]]],
    ) -> bool:
        """Evaluate existing domains and launch candidates; return True on optimal hit."""
        label_score_fn = ResourceDemandScheduler._node_label_score
        seen_domain_sigs: Set[Tuple] = set()
        uniform_in_keys: Optional[Tuple[str, ...]] = None
        if self.requests and all(
            len(r.label_selectors) <= 1 and not r.placement_constraints
            for r in self.requests
        ):
            req_key_sets = set()
            valid_in = True
            for r in self.requests:
                if r.label_selectors:
                    cs = r.label_selectors[0].label_constraints
                    if any(
                        c.operator != _LABEL_OPERATOR_IN or len(c.label_values) != 1
                        for c in cs
                    ):
                        valid_in = False
                        break
                    req_key_sets.add(tuple(sorted(c.label_key for c in cs)))
                else:
                    req_key_sets.add(())
            if valid_in and len(req_key_sets) == 1:
                uniform_in_keys = next(iter(req_key_sets))

        for domain_val, cand_nodes, cand_originals in [
            (dom_val, [], []) for dom_val in existing_domains
        ] + launch_candidates:
            if cand_nodes and self.best is not None and self.best[0][0] == 0:
                continue
            if domain_val not in self.static_domains:
                domain_nodes, originals = (
                    (cand_nodes, cand_originals)
                    if cand_nodes
                    else (self.domains[domain_val], self.domains[domain_val])
                )
                pending_siblings = (
                    []
                    if cand_nodes
                    else self._collect_pending_siblings(domain_nodes, domain_val)
                )
                pool = (
                    domain_nodes
                    if not pending_siblings
                    else (domain_nodes + pending_siblings)
                )
                if (
                    self.max_bundles_per_node <= 0
                    or not ResourceDemandScheduler._domain_fits_resource_totals(
                        pool,
                        self.req_resource_totals,
                        self.shapes,
                        self.min_nodes_needed,
                    )
                ):
                    continue
                if (
                    not cand_nodes
                    and not pending_siblings
                    and (
                        domain_val.startswith(_PENDING_DOMAIN_PREFIX)
                        or (not self.pinned and not self.requests_ref_locality)
                    )
                ):
                    raw_sig = ResourceDemandScheduler._pending_domain_signature(
                        domain_nodes, self.locality_label
                    )
                    use_multiset = False
                    if (
                        uniform_in_keys is not None
                        and len({(dn.node_type, dn.replica_id) for dn in domain_nodes})
                        <= 1
                    ):
                        if len(self.shapes) <= 1:
                            use_multiset = True
                        else:
                            seen_cls: Dict[Tuple, Tuple] = {}
                            use_multiset = True
                            for dn, nsig in zip(domain_nodes, raw_sig):
                                cls_k = tuple(dn.labels.get(k) for k in uniform_in_keys)
                                prev_sig = seen_cls.get(cls_k)
                                if prev_sig is not None and prev_sig != nsig:
                                    use_multiset = False
                                    break
                                seen_cls[cls_k] = nsig
                    domain_sig = (
                        (True, frozenset(Counter(raw_sig).items()))
                        if use_multiset
                        else (False, raw_sig)
                    )
                    if domain_sig in seen_domain_sigs:
                        continue
                    seen_domain_sigs.add(domain_sig)
                prior_score = (
                    math.fsum(
                        label_score_fn(n)
                        for n in (cand_originals if cand_nodes else domain_nodes)
                    )
                    if self.max_label_score
                    else 0.0
                )
                dom_res = _GangSearch.schedule_in_domain(
                    self.gang_ctx,
                    self.all_nodes,
                    domain_nodes,
                    originals,
                    pending_siblings,
                    self.requests,
                    self.locality_label,
                    domain_val,
                    return_delta=True,
                )
                if dom_res is None:
                    continue
                added_nodes, replaced_nodes = dom_res
                if cand_nodes:
                    cand_nt = cand_nodes[-1].node_type
                    used_refill = sum(
                        1
                        for n in cand_nodes[len(cand_originals) :]
                        if n._is_unlabeled_refill and n.node_type == cand_nt
                    )
                    rem_rg = (
                        self.gang_ctx.get_unbacked_refill_gap(cand_nt) - used_refill
                    )
                    if rem_rg > 0:
                        rep_ids = {id(n) for n in replaced_nodes}
                        live_preview = [
                            n for n in self.all_nodes if id(n) not in rep_ids
                        ] + added_nodes
                        added_nodes = added_nodes + self.gang_ctx.make_refill_nodes(
                            cand_nt, rem_rg, live_nodes=live_preview
                        )
                if self._consider(
                    None,
                    domain_val,
                    prior_score,
                    added=added_nodes,
                    replaced=replaced_nodes,
                ):
                    return True
            else:
                prior_score = (
                    math.fsum(
                        label_score_fn(n)
                        for n in self.all_nodes
                        if n.labels.get(self.locality_label) == domain_val
                        and n.status != SchedulingNodeStatus.TO_TERMINATE
                        and not n.is_leaving
                    )
                    if self.max_label_score
                    else 0.0
                )
                domain_reqs = (
                    ResourceDemandScheduler._add_label_constraints_to_requests(
                        self.requests,
                        [
                            LabelSelectorConstraint(
                                label_key=self.locality_label,
                                operator=_LABEL_OPERATOR_IN,
                                label_values=[domain_val],
                            )
                        ],
                    )
                )
                nodes, infeasible = ResourceDemandScheduler._try_schedule(
                    self.gang_ctx, domain_reqs, ResourceRequestSource.PENDING_DEMAND
                )
                if not infeasible and self._consider(nodes, domain_val, prior_score):
                    return True
        return False

    def evaluate_shared_domains_with_launches(
        self, existing_domains: List[str]
    ) -> None:
        """Fallback when Step 5 alone cannot fit within node/type limits:
        combine spare shared-domain capacity with bounded launches."""
        label_score_fn = ResourceDemandScheduler._node_label_score
        for domain_val in existing_domains:
            if domain_val in self.static_domains or domain_val.startswith(
                _PENDING_DOMAIN_PREFIX
            ):
                continue
            domain_nodes = self.domains[domain_val]
            pending_siblings = self._collect_pending_siblings(domain_nodes, domain_val)
            nodes = self._schedule_shared_domain_with_launches(
                domain_nodes, domain_nodes, pending_siblings, domain_val
            )
            if nodes is not None:
                prior_score = (
                    math.fsum(label_score_fn(n) for n in domain_nodes)
                    if self.max_label_score
                    else 0.0
                )
                self._consider(nodes, domain_val, prior_score)

    def schedule_unlabeled_for_node_type(self, nt: NodeType) -> None:
        """Evaluate unlabeled/scale-from-zero gang placement for one candidate node type."""
        pool = self.unlabeled_pending_by_type.get(nt, [])
        pool_rids = {pn.replica_id for pn in pool if pn.replica_id is not None}
        nt_ctx, other_refills_by_type = self._prepare_scoped_refill_ctx(
            pool, [nt], lambda rn: rn.replica_id in pool_rids
        )
        if nt_ctx is None:
            return
        scheduled, infeasible = ResourceDemandScheduler._try_schedule(
            nt_ctx, self.requests, ResourceRequestSource.PENDING_DEMAND
        )
        if infeasible:
            return
        pending_domain = self.gang_ctx.new_pending_domain(nt)
        sched_rids = {
            n.replica_id
            for n in scheduled
            if n.replica_id is not None
            and n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
        }
        for n in scheduled:
            if (
                n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
                or n.replica_id in sched_rids
            ):
                n.labels[self.locality_label] = pending_domain
        pool_ids = {id(n) for n in pool}
        nodes = (
            [n for n in self.all_nodes if id(n) not in pool_ids]
            + scheduled
            + self._collect_launched_co_refills(pool, scheduled, other_refills_by_type)
        )
        self._consider(nodes, pending_domain, 0.0)


def _assign_node_replica_slot(
    node: SchedulingNode,
    cfg: Optional[NodeTypeConfig],
    replica_id: str,
    host_index: int,
    keep_head: Optional[bool] = None,
    domain_val: Optional[str] = None,
    shared_runtime_labels: Optional[Dict[str, str]] = None,
    suppressed_runtime_keys: Optional[Set[str]] = None,
) -> None:
    """Assign `replica_id`, slot/domain/runtime labels, and head resources."""
    node.replica_id = replica_id
    rc = cfg.replica_config if cfg is not None else None
    if rc is None:
        return
    suppressed = suppressed_runtime_keys or ()
    if (
        rc.domain_label_key
        and not cfg.labels.get(rc.domain_label_key)
        and rc.domain_label_key not in suppressed
    ):
        node.labels[rc.domain_label_key] = (
            domain_val if domain_val is not None else replica_id
        )
    if (
        rc.worker_index_label_key
        and not cfg.labels.get(rc.worker_index_label_key)
        and rc.worker_index_label_key not in suppressed
    ):
        node.labels[rc.worker_index_label_key] = str(host_index)
    if shared_runtime_labels and (
        node.progress_rank < 3
        or node.im_instance_status == Instance.QUEUED
        or node._is_backfill_placeholder
        or node._is_unlabeled_refill
    ):
        for key, val in shared_runtime_labels.items():
            if not node.labels.get(key):
                node.labels[key] = val
    if not rc.head_resources:
        return
    if (host_index == 0) if keep_head is None else keep_head:
        head_res = {
            k: float(cfg.resources[k])
            for k in rc.head_resources
            if cfg.resources.get(k, 0) > 0
        }
        if head_res:
            node.update_total_resources(head_res)
    else:
        for key in rc.head_resources:
            node.total_resources.pop(key, None)
            for avail in node.available_resources_for_sched.values():
                avail.pop(key, None)


@dataclass
class _Replica:
    """In-memory multi-worker replica assembled during scheduling setup."""

    rid: str
    domain: Optional[str] = None
    members: List[SchedulingNode] = field(default_factory=list)
    lids: Set[str] = field(default_factory=set)
    shared_runtime_labels: Dict[str, str] = field(default_factory=dict)
    allow_multi_replica: bool = False
    is_provider: bool = False
    no_backfill: bool = False
    allow_refill_attach: bool = False

    @staticmethod
    def _slots_for(count: int, size: int, multi: bool) -> Tuple[int, int]:
        """Return `(open_slots, committed_slots)` for a replica group of `count` nodes."""
        if multi:
            committed = ((count + size - 1) // size) * size
            return committed - count, committed
        return max(0, size - count), max(size, count)


@dataclass(frozen=True)
class _ReplicaGroupResult:
    """Output of `_ReplicaGrouper.group()` for one multi-worker node type."""

    placeholders: List[SchedulingNode]
    replicas: Dict[str, _Replica]
    active_placeholder_keys: Set[Tuple[str, str, str]]
    extra_lid_backfill: int
    suppressed_runtime_keys: Set[str] = field(default_factory=set)


def _compute_suppressed_runtime_keys(
    cfg: NodeTypeConfig, full_running_groups: Iterable[List[SchedulingNode]]
) -> Set[str]:
    """Return runtime label keys to stop simulating for `cfg` when at least one
    full RAY_RUNNING replica exists and no worker across those replicas carries that key."""
    groups = list(full_running_groups)
    if not groups:
        return set()
    rc = cfg.replica_config
    expected = {
        k
        for k in (
            *cfg.runtime_label_keys,
            *((rc.domain_label_key, rc.worker_index_label_key) if rc else ()),
        )
        if k and not cfg.labels.get(k)
    }
    if not expected:
        return set()
    return {
        k for k in expected if not any(m.labels.get(k) for grp in groups for m in grp)
    }


def _slot_plan_for(
    cfg: NodeTypeConfig, members: List[SchedulingNode], add_count: int
) -> Tuple[int, List[int]]:
    """Return `(heads_needed, slots)` for placing `add_count` nodes into `members`."""
    if add_count <= 0:
        return 0, []
    rc = cfg.replica_config
    size = rc.workers_per_replica
    wid_key = rc.worker_index_label_key
    head_keys = set(rc.head_resources)
    total_target = len(members) + add_count
    target_reps = (total_target + size - 1) // size
    target_heads = total_target // size
    wid_counts: Counter = Counter(
        m.labels.get(wid_key) for m in members if wid_key and m.labels.get(wid_key)
    )
    if wid_key:
        unindexed = sum(1 for m in members if not m.labels.get(wid_key))
        if unindexed:
            for cap in range(1, target_reps + 1):
                for h in list(range(1, size)) + [0]:
                    if unindexed <= 0:
                        break
                    if wid_counts[str(h)] < cap:
                        wid_counts[str(h)] += 1
                        unindexed -= 1
                if unindexed <= 0:
                    break
    if head_keys:
        existing_zero = sum(
            1
            for m in members
            if any(m.total_resources.get(hk, 0) > 0 for hk in head_keys)
        )
    elif wid_key:
        existing_zero = wid_counts["0"]
    else:
        existing_zero = len(members) // size
    zero_slots = min(add_count, max(0, target_heads - existing_zero))
    non_head_count = add_count - zero_slots
    non_head_slots: List[int] = []
    fallback_cap = max(wid_counts.values(), default=0) + non_head_count
    for cap in (target_reps, fallback_cap):
        for h in range(1, size):
            deficit = min(
                non_head_count - len(non_head_slots),
                max(0, cap - wid_counts[str(h)]),
            )
            if deficit > 0:
                non_head_slots.extend([h] * deficit)
                wid_counts[str(h)] += deficit
    return (
        zero_slots if head_keys else 0,
        non_head_slots + [0] * zero_slots,
    )


def _assign_planned_slots(
    nodes: List[SchedulingNode],
    cfg: NodeTypeConfig,
    members: List[SchedulingNode],
    rid: str,
    domain_val: Optional[str] = None,
    shared_runtime_labels: Optional[Dict[str, str]] = None,
    suppressed_runtime_keys: Optional[Set[str]] = None,
) -> None:
    """Assign planned replica slots and head resources across `nodes`."""
    if not nodes:
        return
    heads_needed, slots = _slot_plan_for(cfg, members, len(nodes))
    has_head_cfg = bool(cfg.head_resources)
    for node, h in zip(nodes, slots):
        keep_head = has_head_cfg and h == 0 and heads_needed > 0
        _assign_node_replica_slot(
            node,
            cfg,
            rid,
            h,
            keep_head=keep_head,
            domain_val=domain_val,
            shared_runtime_labels=shared_runtime_labels,
            suppressed_runtime_keys=suppressed_runtime_keys,
        )
        if keep_head:
            heads_needed -= 1


def _refill_attach_cap(total_src: int, open_slots: int, size: int) -> int:
    """Max pods from `total_src` attachable to `open_slots` without breaking full `size` chunks."""
    if total_src <= 0:
        return 0
    rem = total_src % size
    return rem + min(total_src - rem, (max(0, open_slots - rem) // size) * size)


def _is_current_solicited_teardown(im: Instance) -> bool:
    """Return True iff `im` is in an active or completed solicited teardown."""
    if im.status in _DRAINING_INSTANCE_STATUSES:
        return True
    if im.status not in (_TEARDOWN_INSTANCE_STATUSES | _TERMINAL_INSTANCE_STATUSES):
        return False
    hist = im.status_history
    if not hist:
        return im.status in _TEARDOWN_INSTANCE_STATUSES
    saw_terminating = im.status == Instance.TERMINATING
    for idx in range(len(hist) - 1, -1, -1):
        st = hist[idx].instance_status
        if st == Instance.TERMINATING:
            saw_terminating = True
            continue
        if st in (Instance.TERMINATED, Instance.TERMINATION_FAILED):
            continue
        if st in (Instance.RAY_STOPPING, Instance.RAY_STOPPED):
            for prev_idx in range(idx - 1, -1, -1):
                prev_st = hist[prev_idx].instance_status
                if prev_st == Instance.RAY_STOPPING:
                    continue
                return prev_st == Instance.RAY_STOP_REQUESTED
            return False
        if st == Instance.RAY_STOP_REQUESTED:
            return True
        if st in (Instance.ALLOCATED, Instance.RAY_INSTALLING):
            return saw_terminating
        return False
    return False


class _ReplicaGrouper:
    """Groups running and pending worker nodes of one multi-worker node type."""

    def __init__(
        self,
        ctx: "ResourceDemandScheduler.ScheduleContext",
        node_type: NodeType,
        cfg: NodeTypeConfig,
        type_nodes: List[SchedulingNode],
    ):
        self._ctx = ctx
        self.node_type = node_type
        self.cfg = cfg
        self.rc = cfg.replica_config
        self.size = self.rc.workers_per_replica
        self.dom_key = self.rc.domain_label_key
        self.is_exclusive_domain = cfg.is_exclusive_domain
        self.is_shared_domain = bool(self.dom_key) and not self.rc.domain_exclusive
        self.wid_key = self.rc.worker_index_label_key
        self.head_keys = set(self.rc.head_resources)
        self.type_nodes = type_nodes
        self.replicas: Dict[str, _Replica] = {}
        self.active_placeholder_keys: Set[Tuple[str, str, str]] = set()
        self.extra_lid_backfill: int = 0
        self.suppressed_runtime_keys: Set[str] = set()
        self.view = ctx.get_replica_view()
        self.expired_placeholder_keys = ctx.get_expired_placeholder_keys()
        self.draining_prov_reps = {
            rid for nt, rid in self.view.non_fillable_reps if nt == node_type
        }
        self.torn_down_lids = {
            lid for nt, lid in self.view.torn_down_lids if nt == node_type
        } | {
            lid
            for (nt, rid), lids in self.view.rep_lids.items()
            if nt == node_type and (nt, rid) in self.view.non_fillable_reps
            for lid in lids
        }
        ps = ctx._prescan
        self.active_draining_lids = set(ps.active_draining_lids.get(node_type, ()))
        self.draining_lid_domains = set(ps.draining_lid_domains.get(node_type, ()))
        self.non_stopping_by_lid: Counter = Counter(
            n.launch_request_id
            for n in type_nodes
            if n.launch_request_id
            and n.im_instance_status != Instance.RAY_STOP_REQUESTED
            and not n.provider_replica_id
        )
        self.draining_doms = (
            set(ps.draining_domains.get(node_type, ()))
            if self.is_exclusive_domain
            else set()
        )
        self.torn_down_dom_max_ns = (
            dict(ps.torn_down_dom_max_ns.get(node_type, {})) if self.dom_key else {}
        )
        self.untagged_td_end_ns = list(ps.untagged_td_end_ns.get(node_type, ()))
        self.lidless_mod_deficit = (
            -(
                sum(
                    1
                    for n in self.type_nodes
                    if n.node_kind != NodeKind.HEAD
                    and not n.is_leaving
                    and n.status != SchedulingNodeStatus.TO_LAUNCH
                    and n.im_instance_status != Instance.QUEUED
                    and not n.provider_replica_id
                    and not n.launch_request_id
                )
            )
        ) % self.size

    @staticmethod
    def _rep_start_ns(rep: _Replica) -> int:
        starts = [m._cloud_start_ns for m in rep.members if m._cloud_start_ns > 0]
        return min(starts) if starts else 0

    @staticmethod
    def _teardown_applies(rep_start_ns: int, td_end_ns: Optional[int]) -> bool:
        return (
            td_end_ns is None
            or td_end_ns == 0
            or rep_start_ns == 0
            or rep_start_ns <= td_end_ns
        )

    @staticmethod
    def _sort_by_termination(nodes: List[SchedulingNode]) -> None:
        nodes.sort(
            key=lambda n: (
                ResourceDemandScheduler._sort_nodes_for_termination(n),
                n.im_instance_id or "",
            )
        )

    def _nonprov_lid_rep_counts(self) -> Tuple[Counter, Counter]:
        inc_reps: Counter = Counter()
        live_reps: Counter = Counter()
        for rep in self.replicas.values():
            if rep.is_provider:
                continue
            is_inc = len(rep.members) % self.size != 0
            for lid in rep.lids:
                live_reps[lid] += 1
                if is_inc:
                    inc_reps[lid] += 1
        return inc_reps, live_reps

    def _max_lid_slots_on_rep(
        self, rep: _Replica, lid: str, live_reps_by_lid: Counter
    ) -> Tuple[int, int]:
        live_lid = sum(
            1
            for m in rep.members
            if m.launch_request_id == lid
            and m.im_instance_status != Instance.RAY_STOP_REQUESTED
        )
        if not self.is_exclusive_domain:
            return live_lid, live_lid + ((-len(rep.members)) % self.size)
        other_live = sum(
            1
            for m in rep.members
            if not m.launch_request_id
            and m.im_instance_status != Instance.RAY_STOP_REQUESTED
        )
        other_unsolic = 0
        for olid in rep.lids:
            if olid == lid:
                continue
            live_o = sum(
                1
                for m in rep.members
                if m.launch_request_id == olid
                and m.im_instance_status != Instance.RAY_STOP_REQUESTED
            )
            other_live += live_o
            if live_reps_by_lid[olid] <= 1:
                ps = self._ctx._prescan
                okey = (self.node_type, olid)
                exited_o = max(
                    0,
                    ps.total_cloud_pods_by_lid.get(okey, 0)
                    - self.size * max(1, live_reps_by_lid[olid]),
                )
                other_unsolic += max(
                    0,
                    ps.unsolicited_by_lid.get(okey, 0)
                    - max(
                        0,
                        exited_o - ps.solicited_by_lid.get(okey, 0),
                    ),
                )
        other_slots = other_live
        if (
            self._ctx._prescan.total_cloud_pods_by_lid.get((self.node_type, lid), 0)
            > self.size - other_live
        ):
            other_slots += other_unsolic
        return live_lid, max(live_lid, self.size - other_slots)

    def _is_nonprov_lid_draining(self, lid: str) -> bool:
        live_cnt = self.non_stopping_by_lid[lid]
        if live_cnt % self.size == 0:
            return False
        if lid in self.active_draining_lids:
            total_pods = self._ctx._prescan.total_cloud_pods_by_lid.get(
                (self.node_type, lid), 0
            )
            live_rep_slots = ((live_cnt + self.size - 1) // self.size) * self.size
            exited_slots = max(0, total_pods - live_rep_slots)
            if total_pods == 0 or self._ctx.has_excess_solicited_on_lid(
                self.node_type, lid, exited_slots
            ):
                return True
        live_domains = {
            r.domain
            for r in self.replicas.values()
            if r.domain and not r.domain.startswith(_PENDING_DOMAIN_PREFIX)
        }
        return any(
            d_lid == lid and dom not in live_domains
            for d_lid, dom in self.draining_lid_domains
        )

    def _is_rep_lid_draining(
        self,
        rep: _Replica,
        lid: str,
        include_unscoped_lid: bool = True,
        live_reps_by_lid: Optional[Counter] = None,
    ) -> bool:
        if self.non_stopping_by_lid[lid] % self.size == 0:
            return False
        has_real_domain = bool(
            rep.domain and not rep.domain.startswith(_PENDING_DOMAIN_PREFIX)
        )
        if has_real_domain and (lid, rep.domain) in self.draining_lid_domains:
            return True
        if include_unscoped_lid and lid in self.active_draining_lids:
            if not has_real_domain:
                return self._is_nonprov_lid_draining(lid)
            l_counts = (
                live_reps_by_lid
                if live_reps_by_lid is not None
                else self._nonprov_lid_rep_counts()[1]
            )
            live_lid, cap_rep = self._max_lid_slots_on_rep(rep, lid, l_counts)
            if cap_rep <= live_lid:
                return False
            sum_cap = sum(
                self._max_lid_slots_on_rep(r, lid, l_counts)[
                    0
                    if (
                        (self.is_exclusive_domain and r.domain in self.draining_doms)
                        or (
                            r.domain
                            and not r.domain.startswith(_PENDING_DOMAIN_PREFIX)
                            and (lid, r.domain) in self.draining_lid_domains
                        )
                    )
                    else 1
                ]
                for r in self.replicas.values()
                if not r.is_provider and lid in r.lids
            )
            total_pods = self._ctx._prescan.total_cloud_pods_by_lid.get(
                (self.node_type, lid), 0
            )
            exited_slots = max(0, total_pods - sum_cap)
            return total_pods == 0 or self._ctx.has_excess_solicited_on_lid(
                self.node_type, lid, exited_slots
            )
        return False

    def _placeholder_key(
        self, rep: _Replica, prov_rid: Optional[str]
    ) -> Tuple[str, str, str]:
        if prov_rid:
            return (self.node_type, "prov", prov_rid)
        if (
            (self.is_exclusive_domain or not rep.lids)
            and rep.domain
            and not rep.domain.startswith(_PENDING_DOMAIN_PREFIX)
        ):
            return (self.node_type, "dom", rep.domain)
        return (self.node_type, "lid", min(rep.lids))

    def _new_replica(
        self,
        rid: Optional[str] = None,
        domain: Optional[str] = None,
        lids: Optional[Iterable[str]] = None,
        shared_runtime_labels: Optional[Dict[str, str]] = None,
        allow_multi_replica: bool = False,
        is_provider: bool = False,
        no_backfill: bool = False,
        allow_refill_attach: bool = False,
    ) -> _Replica:
        rep_id = rid or self._ctx.new_pending_domain(self.node_type)
        rep = _Replica(
            rid=rep_id,
            domain=domain if domain is not None else rep_id,
            lids=set(lids or ()),
            shared_runtime_labels=dict(shared_runtime_labels or {}),
            allow_multi_replica=allow_multi_replica,
            is_provider=is_provider,
            no_backfill=no_backfill,
            allow_refill_attach=allow_refill_attach,
        )
        self.replicas[rep_id] = rep
        return rep

    def _open_slots_for_placement(
        self,
        rep: _Replica,
        allow_refill: bool = False,
        expand_skewed: bool = True,
    ) -> int:
        if rep.no_backfill and not (allow_refill and rep.allow_refill_attach):
            return 0
        count = len(rep.members)
        if count <= 0:
            return self.size
        if rep.allow_multi_replica:
            min_reps = (count + self.size - 1) // self.size
            if expand_skewed and not self.is_exclusive_domain and not rep.no_backfill:
                if self.head_keys:
                    heads = sum(
                        1
                        for m in rep.members
                        if any(
                            m.total_resources.get(hk, 0) > 0 for hk in self.head_keys
                        )
                    )
                    min_reps = max(min_reps, heads)
                if self.wid_key:
                    wid_cnts = Counter(
                        m.labels[self.wid_key]
                        for m in rep.members
                        if m.labels.get(self.wid_key)
                    )
                    if wid_cnts:
                        min_reps = max(min_reps, max(wid_cnts.values()))
            return max(0, min_reps * self.size - count)
        return max(0, self.size - count)

    def _place(
        self,
        rep: _Replica,
        src: List[SchedulingNode],
        take: int,
        known: bool = True,
        draining: bool = False,
        allow_refill: bool = False,
        expand_skewed: bool = True,
    ) -> int:
        take = min(
            self._open_slots_for_placement(
                rep, allow_refill=allow_refill, expand_skewed=expand_skewed
            ),
            take,
            len(src),
        )
        if take <= 0:
            return 0
        chosen = src[:take]
        del src[:take]
        _assign_planned_slots(
            chosen,
            self.cfg,
            rep.members,
            rep.rid,
            domain_val=rep.domain,
            shared_runtime_labels=rep.shared_runtime_labels,
            suppressed_runtime_keys=self.suppressed_runtime_keys,
        )
        for node in chosen:
            if known:
                node._has_runtime_domain = True
            if draining:
                node._is_draining_replica = True
            rep.members.append(node)
        return take

    def _add_placeholders(
        self,
        rep: _Replica,
        count: int,
        prov_rid: Optional[str],
        out_nodes: List[SchedulingNode],
        vanished_lids: Optional[List[str]] = None,
    ) -> None:
        if count <= 0:
            return
        phs: List[SchedulingNode] = []
        for idx in range(count):
            ph_lid = (
                vanished_lids[idx]
                if vanished_lids and idx < len(vanished_lids)
                else None
            )
            ph = SchedulingNode.from_node_config(
                self.cfg,
                status=SchedulingNodeStatus.SCHEDULABLE,
                node_kind=NodeKind.WORKER,
                im_instance_id="",
                im_instance_status=None,
                provider_replica_id=prov_rid,
                launch_request_id=ph_lid,
            )
            ph._is_backfill_placeholder = True
            ph._has_runtime_domain = True
            phs.append(ph)
        _assign_planned_slots(
            phs,
            self.cfg,
            rep.members,
            rep.rid,
            domain_val=rep.domain,
            shared_runtime_labels=rep.shared_runtime_labels,
            suppressed_runtime_keys=self.suppressed_runtime_keys,
        )
        rep.members.extend(phs)
        out_nodes.extend(phs)

    def _compute_shared_runtime_labels(
        self, running_nodes: List[SchedulingNode]
    ) -> Dict[str, str]:
        """Extract orthogonal runtime labels shared uniformly across `running_nodes`."""
        if not (self.cfg.runtime_label_keys and running_nodes):
            return {}
        shared_rt: Dict[str, str] = {}
        for key in self.cfg.runtime_label_keys:
            if key in (self.dom_key, self.wid_key) or key in self.cfg.labels:
                continue
            vals = {m.labels.get(key) for m in running_nodes}
            if len(vals) == 1:
                only_val = next(iter(vals))
                if only_val:
                    shared_rt[key] = only_val
        return shared_rt

    def _group_provider_replicas(
        self, by_prov: Dict[str, List[SchedulingNode]]
    ) -> None:
        """Group multi-host nodes carrying authoritative provider replica IDs."""
        for prov_id, p_nodes in sorted(by_prov.items()):
            p_run = [m for m in p_nodes if m.is_running]
            p_pend = sorted(
                (
                    m
                    for m in p_nodes
                    if not m.is_running
                    and m.status
                    in (
                        SchedulingNodeStatus.SCHEDULABLE,
                        SchedulingNodeStatus.TO_LAUNCH,
                    )
                ),
                key=lambda m: (m.progress_rank, m.im_instance_id or ""),
            )
            valid_doms = [
                m.labels[self.dom_key]
                for m in p_run
                if self.dom_key and m.labels.get(self.dom_key)
            ]
            bound_lids = set(self.view.rep_lids.get((self.node_type, prov_id), ()))
            for m in p_nodes:
                if m.launch_request_id:
                    bound_lids.add(m.launch_request_id)
            is_draining = (
                prov_id in self.draining_prov_reps
                or any(
                    m.im_instance_status == Instance.RAY_STOP_REQUESTED for m in p_nodes
                )
                or any(
                    lid in self.active_draining_lids
                    and len(
                        self.view.claimed_reps_by_lid.get((self.node_type, lid), ())
                    )
                    <= 1
                    for lid in bound_lids
                )
                or (
                    self.is_exclusive_domain
                    and any(dom_val in self.draining_doms for dom_val in valid_doms)
                )
            )
            if self.is_exclusive_domain:
                dom_val = valid_doms[0] if valid_doms else None
                existing = self.replicas.get(dom_val) if dom_val is not None else None
                can_merge_existing = (
                    existing is not None
                    and not any(m.provider_replica_id for m in existing.members)
                    and len(existing.members) + len(p_nodes) <= self.size
                    and not (
                        self.wid_key
                        and {
                            m.labels[self.wid_key]
                            for m in existing.members
                            if m.labels.get(self.wid_key)
                        }
                        & {
                            m.labels[self.wid_key]
                            for m in p_nodes
                            if m.labels.get(self.wid_key)
                        }
                    )
                    and not (
                        self.head_keys
                        and any(
                            m.total_resources.get(hk, 0) > 0
                            for m in existing.members
                            for hk in self.head_keys
                        )
                        and any(
                            m.total_resources.get(hk, 0) > 0
                            for m in p_run
                            for hk in self.head_keys
                        )
                    )
                )
                if can_merge_existing:
                    rep = existing
                    rep.is_provider = True
                    rep.lids.update(bound_lids)
                else:
                    if dom_val is None:
                        rid = None
                    elif existing is None:
                        rid = dom_val
                    else:
                        rid = self._ctx.new_pending_domain(
                            f"{self.node_type}:prov:{prov_id}"
                        )
                    rep = self._new_replica(
                        rid=rid,
                        domain=dom_val,
                        lids=bound_lids,
                        is_provider=True,
                    )
            else:
                rid = self._ctx.new_pending_domain(f"{self.node_type}:prov:{prov_id}")
                dom_val = (
                    valid_doms[0]
                    if valid_doms
                    else self._ctx.new_pending_domain(self.node_type)
                )
                rep = self._new_replica(
                    rid=rid,
                    domain=dom_val,
                    lids=bound_lids,
                    is_provider=True,
                )
            for m in p_run:
                m._has_runtime_domain = True
                m.replica_id = rep.rid
                rep.members.append(m)
            rep.shared_runtime_labels = self._compute_shared_runtime_labels(
                [m for m in rep.members if m.is_running]
            )
            is_draining = is_draining or any(
                m._is_draining_replica
                or m.im_instance_status == Instance.RAY_STOP_REQUESTED
                for m in rep.members
            )
            if is_draining:
                for m in rep.members:
                    m._is_draining_replica = True
            if p_pend:
                self._place(rep, p_pend, len(p_pend), draining=is_draining)
                while p_pend:
                    extra_rid = self._ctx.new_pending_domain(self.node_type)
                    extra_dom = (
                        extra_rid
                        if self.is_exclusive_domain
                        else (valid_doms[0] if valid_doms else extra_rid)
                    )
                    extra_rep = self._new_replica(
                        rid=extra_rid,
                        domain=extra_dom,
                        shared_runtime_labels=rep.shared_runtime_labels or None,
                        is_provider=True,
                    )
                    self._place(extra_rep, p_pend, self.size, draining=is_draining)

    def _split_stopping_chunks(
        self, group_nodes: List[SchedulingNode]
    ) -> List[List[SchedulingNode]]:
        """Isolate complete `RAY_STOP_REQUESTED` replica multiples from healthy multiples."""
        stopping = [
            m
            for m in group_nodes
            if m.im_instance_status == Instance.RAY_STOP_REQUESTED
        ]
        live = [
            m
            for m in group_nodes
            if m.im_instance_status != Instance.RAY_STOP_REQUESTED
        ]
        if (
            stopping
            and live
            and len(stopping) % self.size == 0
            and len(live) % self.size == 0
        ):
            return [stopping, live]
        return [group_nodes]

    @staticmethod
    def _has_unused_full_capacity(node: SchedulingNode) -> bool:
        avail = node.get_available_resources(ResourceRequestSource.PENDING_DEMAND)
        return all(
            avail.get(r, 0) >= total
            for r, total in node.total_resources.items()
            if total > 0
        )

    def _is_rep_occupied(self, rep: _Replica) -> bool:
        return any(
            m.is_running and not self._has_unused_full_capacity(m) for m in rep.members
        )

    def _is_entry_idle(self, node: SchedulingNode) -> bool:
        if not node.is_running:
            return False
        eff_idle_ms = self._ctx.effective_idle_timeout_ms(self.cfg)
        if (
            node.idle_duration_ms <= eff_idle_ms
            if eff_idle_ms is not None
            else node.idle_duration_ms <= 0
        ):
            return False
        return self._has_unused_full_capacity(node)

    def _split_full_chunks(
        self,
        nodes: List[SchedulingNode],
        peers: Iterable[SchedulingNode] = (),
    ) -> List[List[SchedulingNode]]:
        """Partition all-idle full-replica multiples into `size`-node chunks while
        spreading distinct slot keys across chunks; keep pre-existing mixed
        busy/idle ambiguous groups intact so active physical slices stay whole."""
        if len(nodes) <= self.size:
            return [nodes] if nodes else []
        if not (
            all(self._is_entry_idle(n) for n in nodes)
            and all(self._is_entry_idle(n) for n in peers)
        ):
            return [nodes]
        if not (self.wid_key or self.head_keys):
            return [nodes[i : i + self.size] for i in range(0, len(nodes), self.size)]

        def _slot_sig(node: SchedulingNode) -> Optional[Tuple]:
            has_head = (
                any(node.total_resources.get(hk, 0) > 0 for hk in self.head_keys)
                if self.head_keys
                else False
            )
            wid = node.labels.get(self.wid_key) if self.wid_key else None
            if not has_head and wid is None:
                return None
            return (has_head, wid)

        pool = list(nodes)
        chunks: List[List[SchedulingNode]] = []
        while pool:
            chunk: List[SchedulingNode] = []
            seen_sigs: Set[Tuple] = set()
            while len(chunk) < self.size and pool:
                pick_idx = 0
                if chunk:
                    for idx, cand in enumerate(pool):
                        sig = _slot_sig(cand)
                        if sig is not None and sig not in seen_sigs:
                            pick_idx = idx
                            break
                chosen = pool.pop(pick_idx)
                chunk.append(chosen)
                sig = _slot_sig(chosen)
                if sig is not None:
                    seen_sigs.add(sig)
            chunks.append(chunk)
        return chunks

    def _group_shared_domain_replicas(
        self,
        shared_running_by_dom: Dict[str, List[SchedulingNode]],
        booting_with_lid: Dict[str, List[SchedulingNode]],
    ) -> None:
        """Group running nodes in non-exclusive domains without provider replica IDs."""
        if not shared_running_by_dom:
            return
        rem_lidless_ttl = self.lidless_mod_deficit
        for dom_val, dom_nodes in sorted(shared_running_by_dom.items()):
            by_lid: Dict[str, List[SchedulingNode]] = defaultdict(list)
            no_lid: List[SchedulingNode] = []
            for m in dom_nodes:
                if m.launch_request_id:
                    by_lid[m.launch_request_id].append(m)
                else:
                    no_lid.append(m)
            groups: List[Tuple[Optional[str], List[SchedulingNode]]] = sorted(
                by_lid.items()
            )
            if no_lid:
                groups.append((None, no_lid))
            coalesced_rem: List[SchedulingNode] = []
            for lid, raw_nodes in groups:
                self._sort_by_termination(raw_nodes)
                for grp in self._split_stopping_chunks(raw_nodes):
                    is_grp_draining = any(
                        m.im_instance_status == Instance.RAY_STOP_REQUESTED for m in grp
                    ) or bool(
                        lid
                        and (
                            (lid, dom_val) in self.draining_lid_domains
                            or (
                                len(grp) % self.size != 0
                                and self._is_nonprov_lid_draining(lid)
                            )
                        )
                    )
                    if is_grp_draining:
                        rep = self._new_replica(
                            domain=dom_val,
                            lids={lid} if lid else None,
                            allow_multi_replica=True,
                            no_backfill=lid is None,
                        )
                        for m in grp:
                            m.replica_id = rep.rid
                            m._has_runtime_domain = True
                            m._is_draining_replica = True
                            rep.members.append(m)
                        continue
                    full_cnt = (len(grp) // self.size) * self.size
                    rem_l = grp[full_cnt:]
                    if full_cnt > 0:
                        for chunk in self._split_full_chunks(
                            grp[:full_cnt], peers=rem_l
                        ):
                            rep = self._new_replica(
                                domain=dom_val,
                                lids={lid} if lid else None,
                                allow_multi_replica=True,
                                no_backfill=lid is None,
                            )
                            for m in chunk:
                                m.replica_id = rep.rid
                                m._has_runtime_domain = True
                                rep.members.append(m)
                    if not rem_l:
                        continue
                    if lid and bool(booting_with_lid.get(lid)):
                        rep = self._new_replica(
                            domain=dom_val,
                            lids={lid},
                            allow_multi_replica=True,
                            no_backfill=False,
                        )
                        for m in rem_l:
                            m.replica_id = rep.rid
                            m._has_runtime_domain = True
                            rep.members.append(m)
                    else:
                        coalesced_rem.extend(rem_l)

            if coalesced_rem:
                self._sort_by_termination(coalesced_rem)
                for i in range(0, len(coalesced_rem), self.size):
                    chunk = coalesced_rem[i : i + self.size]
                    chunk_lids = {
                        m.launch_request_id for m in chunk if m.launch_request_id
                    }
                    is_full = len(chunk) == self.size
                    no_bf = is_full
                    if not is_full and not chunk_lids:
                        open_c = self.size - len(chunk)
                        if rem_lidless_ttl > 0:
                            rem_lidless_ttl = max(0, rem_lidless_ttl - open_c)
                            no_bf = False
                        else:
                            no_bf = True
                    rep = self._new_replica(
                        domain=dom_val,
                        lids=chunk_lids or None,
                        allow_multi_replica=True,
                        no_backfill=no_bf,
                    )
                    for m in chunk:
                        m.replica_id = rep.rid
                        m._has_runtime_domain = True
                        rep.members.append(m)

    def _tag_draining_and_lids(self) -> None:
        for rep in self.replicas.values():
            for m in rep.members:
                if m.launch_request_id:
                    rep.lids.add(m.launch_request_id)
        inc_reps_by_lid, live_reps_by_lid = self._nonprov_lid_rep_counts()
        for rep in self.replicas.values():
            dom_td = (
                bool(self.dom_key)
                and rep.domain in self.torn_down_dom_max_ns
                and (self.is_exclusive_domain or len(rep.members) % self.size != 0)
                and self._teardown_applies(
                    self._rep_start_ns(rep),
                    self.torn_down_dom_max_ns[rep.domain],
                )
            )
            is_td = not rep.is_provider and (
                bool(rep.lids & self.torn_down_lids) or dom_td
            )
            if is_td:
                rep.no_backfill = True
            elif (
                not rep.no_backfill
                and not any(m.is_leaving for m in rep.members)
                and len(rep.members) % self.size != 0
            ):
                prov_rid = next(
                    (
                        m.provider_replica_id
                        for m in rep.members
                        if m.provider_replica_id
                    ),
                    None,
                )
                if (
                    prov_rid
                    or rep.lids
                    or (
                        self.dom_key
                        and rep.domain
                        and not rep.domain.startswith(_PENDING_DOMAIN_PREFIX)
                    )
                ):
                    pkey = self._placeholder_key(rep, prov_rid)
                    if pkey in self.expired_placeholder_keys:
                        self.active_placeholder_keys.add(pkey)
                        rep.no_backfill = True
                        is_td = not rep.is_provider
            if not rep.shared_runtime_labels:
                running_members = [m for m in rep.members if m.is_running]
                if running_members:
                    rep.shared_runtime_labels = self._compute_shared_runtime_labels(
                        running_members
                    )
            if any(m._is_draining_replica for m in rep.members) or (
                (self.is_exclusive_domain and rep.domain in self.draining_doms)
                or any(
                    m.im_instance_status == Instance.RAY_STOP_REQUESTED
                    for m in rep.members
                )
                or (
                    not rep.is_provider
                    and len(rep.members) % self.size != 0
                    and any(
                        self._is_rep_lid_draining(
                            rep,
                            lid,
                            include_unscoped_lid=inc_reps_by_lid[lid] <= 1,
                            live_reps_by_lid=live_reps_by_lid,
                        )
                        for lid in rep.lids
                    )
                )
            ):
                for m in rep.members:
                    m._is_draining_replica = True
            elif is_td:
                rep.allow_refill_attach = True
            elif (
                not rep.is_provider
                and not rep.no_backfill
                and len(rep.members) % self.size != 0
                and len(rep.lids) == 1
                and not any(m.is_leaving or m._is_draining_replica for m in rep.members)
                and self._ctx.is_completed_subreplica_batch(
                    self.node_type,
                    next(iter(rep.lids)),
                    len(rep.members),
                    self.size,
                )
            ):
                rep.no_backfill = True
                rep.allow_refill_attach = True

        rem_untagged = list(self.untagged_td_end_ns)
        if self.dom_key and rem_untagged:
            for rep in sorted(self.replicas.values(), key=lambda r: r.rid):
                if not rem_untagged:
                    break
                if (
                    rep.is_provider
                    or rep.allow_refill_attach
                    or rep.lids
                    or any(m.is_leaving or m._is_draining_replica for m in rep.members)
                ):
                    continue
                open_slots = (self.size - (len(rep.members) % self.size)) % self.size
                if open_slots <= 0:
                    continue
                rep_start = self._rep_start_ns(rep)
                matched_idx = [
                    idx
                    for idx, td_ns in enumerate(rem_untagged)
                    if self._teardown_applies(rep_start, td_ns)
                ][:open_slots]
                if matched_idx:
                    rep.no_backfill = True
                    rep.allow_refill_attach = True
                    for idx in reversed(matched_idx):
                        rem_untagged.pop(idx)

    def _group_unlabeled_running(
        self,
        running_unlabeled: List[SchedulingNode],
        booting_with_lid: Dict[str, List[SchedulingNode]],
    ) -> None:
        """Group running nodes without provider_replica_id or domain labels."""
        if not running_unlabeled:
            return
        by_lid: Dict[str, List[SchedulingNode]] = defaultdict(list)
        unmatched: List[SchedulingNode] = []
        for m in running_unlabeled:
            if m.launch_request_id:
                by_lid[m.launch_request_id].append(m)
            else:
                unmatched.append(m)

        inc_reps_by_lid, live_reps_by_lid = self._nonprov_lid_rep_counts()
        for lid, lid_nodes in sorted(by_lid.items()):
            self._sort_by_termination(lid_nodes)
            # First complete any non-provider runtime-labeled partial replica with same lid.
            for rep in sorted(self.replicas.values(), key=lambda r: r.rid):
                if not lid_nodes:
                    break
                open_slots = self._open_slots_for_placement(rep)
                if lid in rep.lids and open_slots > 0 and not rep.is_provider:
                    take = min(open_slots, len(lid_nodes))
                    rep_draining = any(
                        m._is_draining_replica for m in rep.members
                    ) or self._is_rep_lid_draining(
                        rep,
                        lid,
                        include_unscoped_lid=inc_reps_by_lid[lid] <= 1,
                        live_reps_by_lid=live_reps_by_lid,
                    )
                    for m in lid_nodes[:take]:
                        m.replica_id = rep.rid
                        m._has_runtime_domain = True
                        if (
                            rep_draining
                            or m.im_instance_status == Instance.RAY_STOP_REQUESTED
                        ):
                            m._is_draining_replica = True
                        rep.members.append(m)
                    if any(m._is_draining_replica for m in rep.members):
                        for m in rep.members:
                            m._is_draining_replica = True
                        rep.allow_refill_attach = False
                    rep.shared_runtime_labels = self._compute_shared_runtime_labels(
                        [m for m in rep.members if m.is_running]
                    )
                    del lid_nodes[:take]

            if not lid_nodes:
                continue
            has_booting = bool(booting_with_lid.get(lid))
            has_vanished = bool(
                self.view.vanished_cids_by_lid.get((self.node_type, lid))
            )
            is_lid_draining = (
                len(lid_nodes) % self.size != 0 and self._is_nonprov_lid_draining(lid)
            ) or any(
                m.im_instance_status == Instance.RAY_STOP_REQUESTED for m in lid_nodes
            )
            if (
                len(lid_nodes) % self.size == 0
                or has_booting
                or has_vanished
                or is_lid_draining
                or lid in self.torn_down_lids
            ):
                for grp in self._split_stopping_chunks(lid_nodes):
                    is_draining = (
                        len(grp) % self.size != 0 and self._is_nonprov_lid_draining(lid)
                    ) or any(
                        m.im_instance_status == Instance.RAY_STOP_REQUESTED for m in grp
                    )
                    is_td = lid in self.torn_down_lids
                    if is_draining or (
                        len(grp) > self.size
                        and not all(self._is_entry_idle(n) for n in grp)
                    ):
                        parts = [grp]
                    else:
                        full_cnt = (len(grp) // self.size) * self.size
                        parts = self._split_full_chunks(grp[:full_cnt])
                        if grp[full_cnt:]:
                            parts.append(grp[full_cnt:])
                    for part in parts:
                        part_td = is_td
                        if (
                            not part_td
                            and not is_draining
                            and len(part) % self.size != 0
                            and (self.node_type, "lid", lid)
                            in self.expired_placeholder_keys
                        ):
                            self.active_placeholder_keys.add(
                                (self.node_type, "lid", lid)
                            )
                            part_td = True
                        rep = self._new_replica(
                            lids={lid},
                            shared_runtime_labels=self._compute_shared_runtime_labels(
                                [m for m in part if m.is_running]
                            ),
                            allow_multi_replica=(
                                not self.is_exclusive_domain
                                or (
                                    len(part) > self.size and len(part) % self.size == 0
                                )
                            ),
                            no_backfill=part_td,
                            allow_refill_attach=not self.dom_key
                            and part_td
                            and not is_draining,
                        )
                        for m in part:
                            m.replica_id = rep.rid
                            m._has_runtime_domain = True
                            if is_draining:
                                m._is_draining_replica = True
                            rep.members.append(m)
            else:
                unmatched.extend(lid_nodes)

        if not self.dom_key and unmatched:
            inc_reps = sorted(
                (
                    r
                    for r in self.replicas.values()
                    if not r.is_provider
                    and not any(
                        m.is_leaving or m._is_draining_replica for m in r.members
                    )
                    and self._open_slots_for_placement(
                        r, allow_refill=True, expand_skewed=False
                    )
                    > 0
                ),
                key=lambda r: (
                    self._open_slots_for_placement(
                        r, allow_refill=True, expand_skewed=False
                    ),
                    r.rid,
                ),
            )
            if inc_reps:
                self._sort_by_termination(unmatched)
                rem_sum_open = sum(
                    self._open_slots_for_placement(
                        r, allow_refill=True, expand_skewed=False
                    )
                    for r in inc_reps
                )
                donatable_groups: List[List[SchedulingNode]] = []
                d_no_lid = [
                    m for m in unmatched if not m.is_leaving and not m.launch_request_id
                ]
                if d_no_lid:
                    donatable_groups.append(d_no_lid)
                d_by_lid: Dict[str, List[SchedulingNode]] = defaultdict(list)
                for m in unmatched:
                    if not m.is_leaving and m.launch_request_id:
                        d_by_lid[m.launch_request_id].append(m)
                for _, d_grp in sorted(d_by_lid.items()):
                    donatable_groups.append(d_grp)
                donor_pool: List[SchedulingNode] = []
                for d_grp in donatable_groups:
                    if rem_sum_open <= 0:
                        break
                    donate = min(
                        rem_sum_open,
                        _refill_attach_cap(len(d_grp), rem_sum_open, self.size),
                    )
                    if donate > 0:
                        donor_pool.extend(d_grp[-donate:])
                        rem_sum_open -= donate
                if donor_pool:
                    donated_ids = {id(m) for m in donor_pool}
                    unmatched = [m for m in unmatched if id(m) not in donated_ids]
                    for r in inc_reps:
                        if not donor_pool:
                            break
                        take = min(
                            self._open_slots_for_placement(
                                r, allow_refill=True, expand_skewed=False
                            ),
                            len(donor_pool),
                        )
                        if take <= 0:
                            continue
                        for m in donor_pool[:take]:
                            m.replica_id = r.rid
                            m._has_runtime_domain = True
                            r.members.append(m)
                        del donor_pool[:take]
                        r.shared_runtime_labels = self._compute_shared_runtime_labels(
                            [m for m in r.members if m.is_running]
                        )
                    if donor_pool:
                        unmatched.extend(donor_pool)

        if unmatched:
            self._sort_by_termination(unmatched)
            unmatched_no_lid = [m for m in unmatched if not m.launch_request_id]
            unmatched_with_lid = [m for m in unmatched if m.launch_request_id]
            unmatched_slices: List[Tuple[List[SchedulingNode], bool]] = [
                (unmatched_no_lid[i : i + self.size], True)
                for i in range(0, len(unmatched_no_lid), self.size)
            ]
            if not self.dom_key:
                u_by_lid: Dict[str, List[SchedulingNode]] = defaultdict(list)
                for m in unmatched_with_lid:
                    u_by_lid[m.launch_request_id].append(m)
                for _, lid_u_nodes in sorted(u_by_lid.items()):
                    unmatched_slices.append((lid_u_nodes, False))
            else:
                unmatched_slices.extend(([m], False) for m in unmatched_with_lid)
            for slice_nodes, refillable in unmatched_slices:
                is_draining = any(
                    m.im_instance_status == Instance.RAY_STOP_REQUESTED
                    or (
                        len(slice_nodes) < self.size
                        and m.launch_request_id
                        and self._is_nonprov_lid_draining(m.launch_request_id)
                    )
                    for m in slice_nodes
                )
                slice_lids = {
                    m.launch_request_id for m in slice_nodes if m.launch_request_id
                }
                is_plain_inc = (
                    not self.dom_key
                    and not is_draining
                    and len(slice_nodes) % self.size != 0
                    and len(slice_lids) == 1
                )
                s_lid = next(iter(slice_lids)) if is_plain_inc else None
                is_completed_sub = (
                    s_lid is not None
                    and not booting_with_lid.get(s_lid)
                    and self._ctx.is_completed_subreplica_batch(
                        self.node_type, s_lid, len(slice_nodes), self.size
                    )
                )
                is_plain_exp = (
                    is_plain_inc
                    and not is_completed_sub
                    and s_lid is not None
                    and (self.node_type, "lid", s_lid) in self.expired_placeholder_keys
                )
                if is_plain_exp and s_lid is not None:
                    self.active_placeholder_keys.add((self.node_type, "lid", s_lid))
                rep = self._new_replica(
                    lids=slice_lids,
                    shared_runtime_labels=(
                        self._compute_shared_runtime_labels(
                            [m for m in slice_nodes if m.is_running]
                        )
                        if is_plain_inc
                        else None
                    ),
                    allow_multi_replica=not self.is_exclusive_domain,
                    no_backfill=(not is_plain_inc) or is_plain_exp or is_completed_sub,
                    allow_refill_attach=(
                        is_plain_exp
                        or is_completed_sub
                        or (
                            refillable
                            and len(slice_nodes) < self.size
                            and not is_draining
                        )
                    ),
                )
                for m in slice_nodes:
                    m.replica_id = rep.rid
                    if is_plain_inc:
                        m._has_runtime_domain = True
                    if is_draining:
                        m._is_draining_replica = True
                    rep.members.append(m)

    @staticmethod
    def _replica_inc_priority(
        members: List[SchedulingNode],
        pinned_domains: Dict[str, Set[str]],
        dom_key: Optional[str],
        eff_idle_ms: Optional[float],
    ) -> int:
        if ResourceDemandScheduler._is_replica_pinned(members, pinned_domains, dom_key):
            return 0
        if not any(m.is_running for m in members):
            return 1
        if any(not m.is_running for m in members):
            return 2
        if eff_idle_ms is None or any(
            m.idle_duration_ms <= eff_idle_ms for m in members
        ):
            return 3
        return 4

    def _inc_priority(
        self,
        members: List[SchedulingNode],
        pinned_domains: Dict[str, Set[str]],
        eff_idle_ms: Optional[float],
    ) -> int:
        return self._replica_inc_priority(
            members, pinned_domains, self.dom_key, eff_idle_ms
        )

    def _real_active_count(self) -> int:
        return self._ctx.get_omitted_occupying_slots(self.node_type) + sum(
            1
            for n in self.type_nodes
            if not n.is_leaving
            and not (
                n.status == SchedulingNodeStatus.TO_TERMINATE
                and n.im_instance_status == Instance.QUEUED
            )
        )

    def _is_rem_only_refill_rep(self, rep: _Replica, open_cnt: int) -> bool:
        return (self._is_rep_occupied(rep) and open_cnt < self.size) or (
            bool(self.dom_key)
            and (rep.domain is None or rep.domain.startswith(_PENDING_DOMAIN_PREFIX))
            and any(m.is_running for m in rep.members)
        )

    def _refill_budgets(self, src_count: int) -> Tuple[int, int]:
        """Return `(total_attach_cap, full_chunk_cap)` for `src_count` pending pods."""
        if src_count <= 0 or not self.replicas:
            return 0, 0
        rem = src_count % self.size
        td_full_open = 0
        td_rem_open = 0
        unlabeled_dom_open = 0
        non_td_full_open = 0
        non_td_rem_open = 0
        for r in self.replicas.values():
            if r.is_provider or any(m.is_leaving for m in r.members):
                continue
            r_open = self._open_slots_for_placement(
                r, allow_refill=True, expand_skewed=False
            )
            if r_open <= 0:
                continue
            occupied = self._is_rep_occupied(r) and r_open < self.size
            if r.no_backfill:
                if not r.allow_refill_attach:
                    continue
                if (
                    self.dom_key
                    and (
                        r.domain is None or r.domain.startswith(_PENDING_DOMAIN_PREFIX)
                    )
                    and any(m.is_running for m in r.members)
                ):
                    unlabeled_dom_open += r_open
                elif occupied:
                    td_rem_open += r_open
                else:
                    td_full_open += r_open
            elif occupied:
                non_td_rem_open += r_open
            else:
                non_td_full_open += r_open
        non_td_open = non_td_full_open + non_td_rem_open
        forced_open = max(
            0, self._real_active_count() + non_td_open - self.cfg.max_worker_nodes
        )
        forced_rem = min(non_td_rem_open, forced_open)
        forced_full = min(non_td_full_open, max(0, forced_open - forced_rem))
        rem_only = td_rem_open + unlabeled_dom_open + forced_rem
        full_open = td_full_open + forced_full
        rem_on_full = max(0, rem - rem_only)
        full_cap = min(
            src_count - rem,
            (max(0, full_open - rem_on_full) // self.size) * self.size,
        )
        return rem + full_cap, full_cap

    def _attach_refill_remainder(self, src_nodes: List[SchedulingNode]) -> None:
        """Attach refill pods from `src_nodes` to incomplete non-provider replicas."""
        if not src_nodes or not self.replicas:
            return
        total_cap, full_cap = self._refill_budgets(len(src_nodes))
        if total_cap <= 0:
            return
        rem_cap = total_cap - full_cap
        pinned_domains = self._ctx.peek_pinned_domains()
        eff_idle_ms = self._ctx.effective_idle_timeout_ms(self.cfg)
        candidates = sorted(
            (
                rep
                for rep in self.replicas.values()
                if not rep.is_provider
                and self._open_slots_for_placement(
                    rep, allow_refill=True, expand_skewed=False
                )
                > 0
                and not any(m.is_leaving for m in rep.members)
            ),
            key=lambda rep: (
                not rep.no_backfill,
                self._inc_priority(rep.members, pinned_domains, eff_idle_ms),
                self._open_slots_for_placement(
                    rep, allow_refill=True, expand_skewed=False
                ),
                rep.domain is None or rep.domain.startswith(_PENDING_DOMAIN_PREFIX),
                rep.rid,
            ),
        )
        for rep in candidates:
            if (rem_cap + full_cap) <= 0 or not src_nodes:
                break
            open_cnt = self._open_slots_for_placement(
                rep, allow_refill=True, expand_skewed=False
            )
            is_rem_only = self._is_rem_only_refill_rep(rep, open_cnt)
            allowed = rem_cap if is_rem_only else (rem_cap + full_cap)
            if allowed <= 0:
                continue
            placed_lids = {
                n.launch_request_id
                for n in src_nodes[: min(allowed, open_cnt)]
                if n.launch_request_id
            }
            took = self._place(
                rep,
                src_nodes,
                allowed,
                known=any(m._has_runtime_domain for m in rep.members),
                draining=False,
                allow_refill=True,
                expand_skewed=False,
            )
            if took > 0:
                rep.lids.update(placed_lids)
                if is_rem_only:
                    rem_cap -= took
                else:
                    use_rem = min(rem_cap, took)
                    rem_cap -= use_rem
                    full_cap -= took - use_rem

    def _attach_attributed_booting(
        self, booting_with_lid: Dict[str, List[SchedulingNode]]
    ) -> Tuple[List[Tuple[_Replica, int]], List[SchedulingNode]]:
        """Attach booting nodes with a launch_request_id to matching replicas."""
        vanished_batches: List[Tuple[_Replica, int]] = []
        scrambled_cloud_booting: List[SchedulingNode] = []
        pinned_domains = self._ctx.peek_pinned_domains()
        nonprov_reps = [r for r in self.replicas.values() if not r.is_provider]
        inc_lids = Counter(
            lid
            for r in nonprov_reps
            if self._open_slots_for_placement(r) > 0
            for lid in r.lids
        )
        has_spanning_inc_lid = self.is_exclusive_domain and any(
            cnt > 1 for cnt in inc_lids.values()
        )
        multi_rep_lids = {
            lid for r in nonprov_reps if len(r.lids) > 1 for lid in r.lids
        }

        for lid, lid_nodes in sorted(
            booting_with_lid.items(),
            key=lambda kv: (
                kv[0] not in self.torn_down_lids,
                not bool(self.view.vanished_cids_by_lid.get((self.node_type, kv[0]))),
                kv[0],
            ),
        ):
            is_lid_torn_down = lid in self.torn_down_lids
            if is_lid_torn_down:
                doomed_req = [m for m in lid_nodes if m.progress_rank >= 2]
                lid_nodes = [m for m in lid_nodes if m.progress_rank < 2]
                for i in range(0, len(doomed_req), self.size):
                    d_rid = self._ctx.new_pending_domain(self.node_type)
                    d_chunk = doomed_req[i : i + self.size]
                    d_rep = self._new_replica(rid=d_rid, lids={lid}, no_backfill=True)
                    _assign_planned_slots(
                        d_chunk,
                        self.cfg,
                        [],
                        d_rid,
                        suppressed_runtime_keys=self.suppressed_runtime_keys,
                    )
                    for node in d_chunk:
                        node._is_draining_replica = True
                        d_rep.members.append(node)
                if not lid_nodes:
                    continue
            matching_reps = sorted(
                (rep for rep in self.replicas.values() if lid in rep.lids),
                key=lambda rep: (
                    self._open_slots_for_placement(rep, allow_refill=True) == 0,
                    any(m._is_draining_replica for m in rep.members),
                    not ResourceDemandScheduler._is_replica_pinned(
                        rep.members, pinned_domains, self.dom_key
                    ),
                    self._open_slots_for_placement(rep, allow_refill=True),
                    rep.rid,
                ),
            )
            if self.is_shared_domain and matching_reps:
                total_on_lid = sum(
                    len(r.members) for r in matching_reps if not r.is_provider
                )
                lid_open_cap = (-total_on_lid) % self.size
                if lid_open_cap == 0 and any(
                    self._open_slots_for_placement(r) > 0
                    for r in matching_reps
                    if not r.is_provider
                ):
                    for r in matching_reps:
                        if not r.is_provider:
                            r.no_backfill = True
            for rep in matching_reps:
                if not lid_nodes:
                    break
                rep_draining = any(m._is_draining_replica for m in rep.members)
                self._place(
                    rep,
                    lid_nodes,
                    len(lid_nodes),
                    known=True,
                    draining=rep_draining,
                    allow_refill=not rep_draining,
                )
            if not lid_nodes:
                continue
            vanished_cids = self.view.vanished_cids_by_lid.get(
                (self.node_type, lid), set()
            )
            if (
                not is_lid_torn_down
                and vanished_cids
                and 0 < len(lid_nodes) < self.size
            ):
                pkey = (self.node_type, "lid", lid)
                is_vanished_exp = pkey in self.expired_placeholder_keys
                rep = (
                    self._new_replica(
                        rid=self._ctx.new_pending_domain(self.node_type),
                        lids={lid},
                        no_backfill=True,
                        allow_refill_attach=True,
                    )
                    if is_vanished_exp
                    else _Replica(
                        rid=self._ctx.new_pending_domain(self.node_type),
                        lids={lid},
                    )
                )
                rep.domain = rep.rid
                self._place(rep, lid_nodes, len(lid_nodes), known=True, draining=False)
                if is_vanished_exp:
                    self.active_placeholder_keys.add(pkey)
                else:
                    vanished_batches.append(
                        (rep, min(self.size - len(rep.members), len(vanished_cids)))
                    )
                continue
            if not is_lid_torn_down or lid in multi_rep_lids:
                self._attach_refill_remainder(lid_nodes)
                if not lid_nodes:
                    continue
            total_nonprov_open = sum(
                self._open_slots_for_placement(r)
                for r in self.replicas.values()
                if not r.is_provider and not any(m.is_leaving for m in r.members)
            )
            if not is_lid_torn_down and (
                (
                    has_spanning_inc_lid
                    and (
                        len(lid_nodes) % self.size != 0
                        or total_nonprov_open >= self.size
                    )
                )
                or (
                    self.is_exclusive_domain
                    and lid in multi_rep_lids
                    and len(lid_nodes) % self.size != 0
                )
            ):
                scrambled_cloud_booting.extend(
                    m for m in lid_nodes if m.progress_rank < 2
                )
                lid_nodes = [m for m in lid_nodes if m.progress_rank >= 2]
                if not lid_nodes:
                    continue
            for i in range(0, len(lid_nodes), self.size):
                rid = self._ctx.new_pending_domain(self.node_type)
                chunk_nodes = lid_nodes[i : i + self.size]
                is_chunk_draining = len(
                    chunk_nodes
                ) < self.size and self._is_nonprov_lid_draining(lid)
                chunk_rep = (
                    self._new_replica(
                        rid=rid,
                        lids={lid},
                        no_backfill=is_lid_torn_down,
                        allow_refill_attach=is_lid_torn_down and not is_chunk_draining,
                    )
                    if len(chunk_nodes) < self.size
                    else None
                )
                _assign_planned_slots(
                    chunk_nodes,
                    self.cfg,
                    [],
                    rid,
                    suppressed_runtime_keys=self.suppressed_runtime_keys,
                )
                for node in chunk_nodes:
                    if is_chunk_draining:
                        node._is_draining_replica = True
                    if chunk_rep is not None:
                        chunk_rep.members.append(node)
        post_inc_reps_by_lid, post_live_reps_by_lid = self._nonprov_lid_rep_counts()
        for rep in self.replicas.values():
            if (
                not rep.is_provider
                and len(rep.members) % self.size != 0
                and any(
                    self._is_rep_lid_draining(
                        rep,
                        lid,
                        include_unscoped_lid=post_inc_reps_by_lid[lid] <= 1,
                        live_reps_by_lid=post_live_reps_by_lid,
                    )
                    for lid in rep.lids
                )
            ):
                for m in rep.members:
                    m._is_draining_replica = True
                rep.allow_refill_attach = False
        return vanished_batches, scrambled_cloud_booting

    def _attach_unattributed_booting(
        self,
        booting_no_lid: List[SchedulingNode],
        fill_all_open: bool = False,
    ) -> List[SchedulingNode]:
        """Attach unattributed or cross-slice cloud-booting nodes to incomplete replicas."""
        if not (self.replicas and booting_no_lid):
            return booting_no_lid
        allow_refill = not fill_all_open
        pinned_domains = self._ctx.peek_pinned_domains()
        eff_idle_ms = self._ctx.effective_idle_timeout_ms(self.cfg)
        incomplete = sorted(
            (
                rep
                for rep in self.replicas.values()
                if self._open_slots_for_placement(
                    rep, allow_refill=allow_refill, expand_skewed=not allow_refill
                )
                > 0
                and not any(m.is_leaving for m in rep.members)
            ),
            key=lambda rep: (
                not rep.no_backfill if allow_refill else False,
                self._inc_priority(rep.members, pinned_domains, eff_idle_ms),
                self._open_slots_for_placement(
                    rep, allow_refill=allow_refill, expand_skewed=not allow_refill
                ),
                rep.domain is None or rep.domain.startswith(_PENDING_DOMAIN_PREFIX),
                rep.rid,
            ),
        )
        cloud_booting = list(booting_no_lid)
        prov_rids = {rep.rid for rep in incomplete if rep.is_provider}
        for rep in incomplete:
            if rep.rid in prov_rids or rep.no_backfill:
                continue
            if ResourceDemandScheduler._is_replica_pinned(
                rep.members, pinned_domains, self.dom_key
            ) or not any(m.is_running for m in rep.members):
                self._place(
                    rep,
                    cloud_booting,
                    self._open_slots_for_placement(rep, expand_skewed=not allow_refill),
                    known=False,
                    expand_skewed=not allow_refill,
                )
        if fill_all_open:
            rem_open = sum(
                self._open_slots_for_placement(r)
                for r in incomplete
                if r.rid not in prov_rids
            )
            spare = min(
                len(cloud_booting),
                (rem_open // self.size) * self.size + (len(cloud_booting) % self.size),
            )
        else:
            total_cap, full_cap = self._refill_budgets(len(cloud_booting))
            rem_cap = total_cap - full_cap
        for rep in incomplete:
            open_cnt = self._open_slots_for_placement(
                rep, allow_refill=allow_refill, expand_skewed=not allow_refill
            )
            if open_cnt == 0 or rep.rid in prov_rids:
                continue
            if fill_all_open:
                if spare <= 0:
                    continue
                spare -= self._place(
                    rep,
                    cloud_booting,
                    spare,
                    known=False,
                    allow_refill=allow_refill,
                    expand_skewed=not allow_refill,
                )
            else:
                is_rem_only = self._is_rem_only_refill_rep(rep, open_cnt)
                allowed = rem_cap if is_rem_only else (rem_cap + full_cap)
                if allowed <= 0:
                    continue
                took = self._place(
                    rep,
                    cloud_booting,
                    allowed,
                    known=False,
                    allow_refill=allow_refill,
                    expand_skewed=not allow_refill,
                )
                if took > 0:
                    if is_rem_only:
                        rem_cap -= took
                    else:
                        use_rem = min(rem_cap, took)
                        rem_cap -= use_rem
                        full_cap -= took - use_rem
        return cloud_booting

    def _populate_backfill_placeholders(
        self, vanished_batches: List[Tuple[_Replica, int]]
    ) -> List[SchedulingNode]:
        """Add committed missing-slot backfill placeholders for incomplete replicas."""
        placeholders: List[SchedulingNode] = []
        remaining_ph = max(0, self.cfg.max_worker_nodes - self._real_active_count())
        rem_vanished_by_lid = Counter(
            self.view.unclaimed_vanished_by_lid(self.node_type)
        )
        np_ph_budget: Optional[int] = None
        if not self.is_exclusive_domain:
            np_count = (
                ResourceDemandScheduler.ScheduleContext._nonprov_active_pod_count(
                    self.type_nodes,
                    self.replicas,
                    self.node_type,
                    self.torn_down_lids,
                )
            )
            np_ph_budget = max(
                (-np_count) % self.size,
                sum(rem_vanished_by_lid.values()),
            )
        if self.is_shared_domain:
            total_lid_cnt: Counter = Counter(
                m.launch_request_id
                for r in self.replicas.values()
                for m in r.members
                if not r.is_provider and not m.is_leaving and m.launch_request_id
            )
            reps_by_lid: Dict[str, List[_Replica]] = defaultdict(list)
            for rep in self.replicas.values():
                if (
                    not rep.is_provider
                    and not rep.no_backfill
                    and not any(m.is_leaving for m in rep.members)
                    and len(rep.lids) == 1
                ):
                    reps_by_lid[next(iter(rep.lids))].append(rep)
            for lid, l_reps in reps_by_lid.items():
                rep_member_sum = sum(len(r.members) for r in l_reps)
                total_cnt = total_lid_cnt[lid]
                if len(l_reps) <= 1 and total_cnt <= rep_member_sum:
                    continue
                lid_deficit = (-total_cnt) % self.size
                sum_open = sum(
                    self._open_slots_for_placement(r, expand_skewed=False)
                    for r in l_reps
                )
                if sum_open > lid_deficit:
                    for r in l_reps:
                        r.no_backfill = True
                        if not any(m._is_draining_replica for m in r.members):
                            r.allow_refill_attach = True
                    if lid_deficit > 0 and lid not in self.torn_down_lids:
                        pkey = (self.node_type, "lid", lid)
                        self.active_placeholder_keys.add(pkey)
                        if pkey not in self.expired_placeholder_keys:
                            assert np_ph_budget is not None
                            take_extra = min(
                                lid_deficit,
                                remaining_ph,
                                np_ph_budget,
                            )
                            if take_extra > 0:
                                self.extra_lid_backfill += take_extra
                                remaining_ph -= take_extra
                                np_ph_budget -= take_extra
            for rep in self.replicas.values():
                if (
                    not rep.is_provider
                    and not rep.no_backfill
                    and not any(
                        m.is_leaving or m._is_draining_replica for m in rep.members
                    )
                    and len(rep.lids) > 1
                    and len(rep.members) % self.size != 0
                    and all((-total_lid_cnt[l]) % self.size == 0 for l in rep.lids)
                ):
                    rep.no_backfill = True
                    rep.allow_refill_attach = True

        ordered_reps = sorted(
            self.replicas.values(),
            key=(
                (
                    lambda r: (
                        not any(rem_vanished_by_lid.get(lid, 0) > 0 for lid in r.lids),
                        self._open_slots_for_placement(r, expand_skewed=False),
                        r.rid,
                    )
                )
                if not self.is_exclusive_domain
                else (lambda r: r.rid)
            ),
        )
        for rep in ordered_reps:
            if rep.no_backfill or any(m.is_leaving for m in rep.members):
                continue
            prov_rid = next(
                (m.provider_replica_id for m in rep.members if m.provider_replica_id),
                None,
            )
            has_attr = bool(prov_rid or rep.lids)
            if not has_attr and not (
                self.dom_key
                and rep.domain
                and not rep.domain.startswith(_PENDING_DOMAIN_PREFIX)
            ):
                continue
            open_slots = self._open_slots_for_placement(rep, expand_skewed=False)
            if open_slots == 0:
                continue
            pkey = self._placeholder_key(rep, prov_rid)
            self.active_placeholder_keys.add(pkey)
            if pkey in self.expired_placeholder_keys:
                rep.no_backfill = True
                if not rep.is_provider:
                    rep.allow_refill_attach = True
                continue
            if not has_attr:
                continue
            if not rep.is_provider and np_ph_budget is not None:
                if np_ph_budget <= 0:
                    rep.no_backfill = True
                    continue
                emit_cnt = min(open_slots, remaining_ph, np_ph_budget)
                if emit_cnt <= 0:
                    continue
                np_ph_budget -= emit_cnt
            else:
                emit_cnt = min(open_slots, remaining_ph)
                if emit_cnt <= 0:
                    continue
            v_lids: List[str] = []
            if not prov_rid:
                for v_lid in sorted(rep.lids):
                    take_v = min(emit_cnt - len(v_lids), rem_vanished_by_lid[v_lid])
                    if take_v > 0:
                        v_lids.extend([v_lid] * take_v)
                        rem_vanished_by_lid[v_lid] -= take_v
            self._add_placeholders(
                rep, emit_cnt, prov_rid, placeholders, vanished_lids=v_lids
            )
            remaining_ph -= emit_cnt
        for rep, missing_cnt in vanished_batches:
            self.replicas[rep.rid] = rep
            pkey = self._placeholder_key(rep, None)
            self.active_placeholder_keys.add(pkey)
            if pkey in self.expired_placeholder_keys:
                rep.no_backfill = True
                if not any(m.is_leaving or m._is_draining_replica for m in rep.members):
                    rep.allow_refill_attach = True
                continue
            emit_cnt = min(missing_cnt, remaining_ph)
            if emit_cnt <= 0:
                continue
            vb_lid = next(iter(rep.lids), None)
            self._add_placeholders(
                rep,
                emit_cnt,
                None,
                placeholders,
                vanished_lids=[vb_lid] * emit_cnt if vb_lid else None,
            )
            remaining_ph -= emit_cnt
        return placeholders

    def group(self) -> _ReplicaGroupResult:
        """Group all worker nodes of `self.node_type` and return backfill metadata."""
        if self.size == 1:
            self.suppressed_runtime_keys = _compute_suppressed_runtime_keys(
                self.cfg, ([n] for n in self.type_nodes if n.is_running)
            )
            running_unlabeled: List[SchedulingNode] = []
            for n in self.type_nodes:
                if n.is_running or n.im_instance_status == Instance.RAY_STOP_REQUESTED:
                    dom_val = n.labels.get(self.dom_key) if self.dom_key else None
                    if dom_val:
                        n.replica_id = (
                            dom_val
                            if self.is_exclusive_domain
                            else (
                                n.provider_replica_id
                                or self._ctx.new_pending_domain(self.node_type)
                            )
                        )
                        n._has_runtime_domain = True
                    else:
                        if n.provider_replica_id:
                            n._has_runtime_domain = True
                        running_unlabeled.append(n)
                elif n.status in (
                    SchedulingNodeStatus.SCHEDULABLE,
                    SchedulingNodeStatus.TO_LAUNCH,
                ):
                    if n.provider_replica_id:
                        n._has_runtime_domain = True
                    _assign_node_replica_slot(
                        n,
                        self.cfg,
                        self._ctx.new_pending_domain(self.node_type),
                        0,
                        suppressed_runtime_keys=self.suppressed_runtime_keys,
                    )
            if running_unlabeled:
                self._sort_by_termination(running_unlabeled)
                for m in running_unlabeled:
                    m.replica_id = self._ctx.new_pending_domain(self.node_type)
            return _ReplicaGroupResult(
                [],
                self.replicas,
                self.active_placeholder_keys,
                0,
                self.suppressed_runtime_keys,
            )

        shared_running_by_dom: Dict[str, List[SchedulingNode]] = defaultdict(list)
        by_prov: Dict[str, List[SchedulingNode]] = defaultdict(list)
        running_unlabeled = []
        pending: List[SchedulingNode] = []

        for n in self.type_nodes:
            if n.provider_replica_id:
                by_prov[n.provider_replica_id].append(n)
            elif n.is_running or n.im_instance_status == Instance.RAY_STOP_REQUESTED:
                dom_val = n.labels.get(self.dom_key) if self.dom_key else None
                if dom_val:
                    if self.is_exclusive_domain:
                        rep = self.replicas.get(dom_val)
                        if rep is None:
                            rep = self._new_replica(rid=dom_val, domain=dom_val)
                        n.replica_id = dom_val
                        n._has_runtime_domain = True
                        rep.members.append(n)
                    else:
                        shared_running_by_dom[dom_val].append(n)
                else:
                    running_unlabeled.append(n)
            elif n.status in (
                SchedulingNodeStatus.SCHEDULABLE,
                SchedulingNodeStatus.TO_LAUNCH,
            ):
                pending.append(n)

        pending.sort(key=lambda n: (n.progress_rank, n.im_instance_id or ""))
        booting_pods = [p for p in pending if p.progress_rank < 3]
        queued_nodes = [p for p in pending if p.im_instance_status == Instance.QUEUED]
        other_pending = [
            p
            for p in pending
            if p.progress_rank >= 3 and p.im_instance_status != Instance.QUEUED
        ]

        booting_with_lid: Dict[str, List[SchedulingNode]] = defaultdict(list)
        booting_no_lid: List[SchedulingNode] = []
        for p in booting_pods:
            if p.launch_request_id:
                booting_with_lid[p.launch_request_id].append(p)
            else:
                booting_no_lid.append(p)

        # Stage 1: Authoritative physical/runtime replica grouping.
        self._group_provider_replicas(by_prov)
        self._group_shared_domain_replicas(shared_running_by_dom, booting_with_lid)
        self._tag_draining_and_lids()

        self._group_unlabeled_running(running_unlabeled, booting_with_lid)
        self.suppressed_runtime_keys = _compute_suppressed_runtime_keys(
            self.cfg,
            (
                rm
                for rep in self.replicas.values()
                if len(rm := [m for m in rep.members if m.is_running]) >= self.size
            ),
        )

        # Stage 2: Exact launch_request_id attachment for booting pods.
        vanished_batches, scrambled_cloud = self._attach_attributed_booting(
            booting_with_lid
        )

        # Stage 3: Unattributed and cross-slice cloud-booting attachment.
        if scrambled_cloud:
            scrambled_cloud = self._attach_unattributed_booting(
                scrambled_cloud, fill_all_open=True
            )
        booting_no_lid = scrambled_cloud + self._attach_unattributed_booting(
            booting_no_lid
        )

        def _chunk_pending_group(
            group: List[SchedulingNode], *, register_sub_rep: bool
        ) -> None:
            for i in range(0, len(group), self.size):
                rid = self._ctx.new_pending_domain(self.node_type)
                chunk_nodes = group[i : i + self.size]
                _assign_planned_slots(
                    chunk_nodes,
                    self.cfg,
                    [],
                    rid,
                    suppressed_runtime_keys=self.suppressed_runtime_keys,
                )
                is_sub_chunk = len(chunk_nodes) < self.size
                is_chunk_draining = False
                for p in chunk_nodes:
                    if p.im_instance_status == Instance.RAY_STOP_REQUESTED or (
                        p.launch_request_id
                        and (
                            (
                                is_sub_chunk
                                and self._is_nonprov_lid_draining(p.launch_request_id)
                            )
                            or (
                                p.progress_rank >= 2
                                and p.launch_request_id in self.torn_down_lids
                            )
                        )
                    ):
                        p._is_draining_replica = True
                        is_chunk_draining = True
                if (
                    register_sub_rep
                    and is_sub_chunk
                    and not any(p.provider_replica_id for p in chunk_nodes)
                ):
                    chunk_rep = self._new_replica(
                        rid=rid,
                        domain=rid,
                        no_backfill=True,
                        allow_refill_attach=not is_chunk_draining,
                    )
                    chunk_rep.members.extend(chunk_nodes)

        _chunk_pending_group(booting_no_lid, register_sub_rep=True)
        if queued_nodes and not any(
            p.launch_request_id and p.launch_request_id in self.torn_down_lids
            for p in queued_nodes
        ):
            q_doomed = self._ctx.doomed_queued_count(
                self.node_type, self.size, len(queued_nodes)
            )
            launchable_q = (
                queued_nodes[:-q_doomed] if q_doomed > 0 else list(queued_nodes)
            )
            doomed_q = queued_nodes[-q_doomed:] if q_doomed > 0 else []
            self._attach_refill_remainder(launchable_q)
            queued_nodes = launchable_q + doomed_q
        _chunk_pending_group(queued_nodes, register_sub_rep=False)
        _chunk_pending_group(other_pending, register_sub_rep=False)

        # Stage 4: Committed missing-slot backfill placeholders.
        placeholders = self._populate_backfill_placeholders(vanished_batches)
        return _ReplicaGroupResult(
            placeholders=placeholders,
            replicas=self.replicas,
            active_placeholder_keys=self.active_placeholder_keys,
            extra_lid_backfill=self.extra_lid_backfill,
            suppressed_runtime_keys=self.suppressed_runtime_keys,
        )


class ResourceDemandScheduler(IResourceScheduler):
    """A resource demand scheduler that schedules resource requests based on the
    following rules:
        1. Enforce the minimal count of nodes for each worker node type.
        2. Enforce the cluster resource constraints.
        3. Schedule the gang resource requests.
        4. Schedule the tasks/actor resource requests
    """

    # Statuses of pending (not yet running ray) instances that could be terminated
    # without draining ray when they are not needed. REQUESTED is excluded since
    # an in-flight launch request can't be cancelled.
    PENDING_TERMINABLE_STATUSES = (
        Instance.QUEUED,
        Instance.ALLOCATED,
        Instance.RAY_INSTALLING,
    )
    _PENDING_PROGRESSION = {
        Instance.RAY_INSTALLING: 0,
        Instance.ALLOCATED: 1,
        Instance.REQUESTED: 2,
        Instance.QUEUED: 3,
    }
    # Cancelability order for max-worker scale-down: prefer un-queued TO_LAUNCH
    # nodes (-1) and immediately cancelable pending states (QUEUED -> ALLOCATED ->
    # RAY_INSTALLING) before in-flight REQUESTED instances that cannot be canceled
    # until cloud allocation completes.
    _PENDING_TERMINATION_ORDER = {
        None: -1,
        Instance.QUEUED: 0,
        Instance.ALLOCATED: 1,
        Instance.RAY_INSTALLING: 2,
        Instance.REQUESTED: 3,
    }

    def __init__(self, event_logger: Optional[AutoscalerEventLogger] = None):
        self._event_logger = event_logger
        # Pending instances have no idle duration reported by GCS. For each node
        # type, the k-th entry is the monotonic time since which at least k + 1
        # pending nodes of that type have been continuously not needed by any
        # resource demands. It's tracked by counts rather than by instances, since
        # pending nodes of the same type are interchangeable for scheduling.
        self._pending_unneeded_since_s: Dict[NodeType, List[float]] = {}
        self._pending_node_since_s: Dict[Tuple[NodeType, str], float] = {}
        self._placeholder_since_s: Dict[Tuple[str, str, str], float] = {}
        self._instance_domains: Dict[Tuple[NodeType, str], str] = {}

    @staticmethod
    def _is_node_pinned(
        node: SchedulingNode,
        pinned_domains: Dict[str, Set[str]],
        dom_key: Optional[str] = None,
    ) -> bool:
        for label_key, pinned_vals in pinned_domains.items():
            if node.labels.get(label_key) in pinned_vals:
                return True
            if dom_key and label_key == dom_key and node.replica_id in pinned_vals:
                return True
        return False

    @staticmethod
    def _is_replica_pinned(
        r_nodes: Iterable[SchedulingNode],
        pinned_domains: Dict[str, Set[str]],
        dom_key: Optional[str] = None,
    ) -> bool:
        return bool(pinned_domains) and any(
            ResourceDemandScheduler._is_node_pinned(m, pinned_domains, dom_key)
            for m in r_nodes
        )

    @staticmethod
    def _shared_pinned_pairs(
        target_nodes: Iterable[SchedulingNode],
        cfg: Optional[NodeTypeConfig],
        pinned_domains: Dict[str, Set[str]],
    ) -> Set[Tuple[str, str]]:
        """Return pinned `(label_key, label_val)` pairs shared by `target_nodes`,
        excluding `cfg`'s own exclusive domain key."""
        if not pinned_domains:
            return set()
        is_exclusive = cfg.is_exclusive_domain if cfg is not None else False
        dom_key = cfg.domain_label_key if cfg is not None else None
        pairs: Set[Tuple[str, str]] = set()
        for key, pinned_vals in pinned_domains.items():
            if is_exclusive and key == dom_key:
                continue
            for m in target_nodes:
                val = m.labels.get(key)
                if val in pinned_vals:
                    pairs.add((key, val))
        return pairs

    @dataclass(frozen=True)
    class _ReplicaPreScan:
        """Pre-scanned multi-worker IM history and teardown summaries."""

        draining_domains: Dict[NodeType, Set[str]] = field(default_factory=dict)
        torn_down_dom_max_ns: Dict[NodeType, Dict[str, Optional[int]]] = field(
            default_factory=dict
        )
        untagged_td_end_ns: Dict[NodeType, List[Optional[int]]] = field(
            default_factory=dict
        )
        active_draining_lids: Dict[NodeType, Set[str]] = field(default_factory=dict)
        draining_lid_domains: Dict[NodeType, Set[Tuple[str, str]]] = field(
            default_factory=dict
        )
        omitted_stopping_cnt: Dict[NodeType, int] = field(default_factory=dict)
        reconciler_stopping_cnt: Optional[Dict[NodeType, int]] = None
        omitted_prov_rids: Dict[NodeType, Set[str]] = field(default_factory=dict)
        solicited_by_lid: Dict[Tuple[NodeType, str], int] = field(default_factory=dict)
        unsolicited_by_lid: Dict[Tuple[NodeType, str], int] = field(
            default_factory=dict
        )
        active_stopping_by_lid: Dict[Tuple[NodeType, str], int] = field(
            default_factory=dict
        )
        total_cloud_pods_by_lid: Dict[Tuple[NodeType, str], int] = field(
            default_factory=dict
        )
        im_total_rows_by_lid: Dict[Tuple[NodeType, str], int] = field(
            default_factory=dict
        )
        im_created_lids: Set[Tuple[NodeType, str]] = field(default_factory=set)

    @dataclass
    class ScheduleContext:
        """Encapsulates the context for processing one scheduling request.

        This exposes functions to read and write the scheduling nodes, to prevent
        accidental modification of the internal state.
        """

        # The node type configs for this scheduling request.
        _node_type_configs: Dict[NodeType, NodeTypeConfig]
        # If outdated node check through launch config is disabled.
        _disable_launch_config_check: bool
        # The max number of nodes for the entire cluster.
        _max_num_nodes: Optional[int] = None
        # The idle timeout in seconds.
        _idle_timeout_s: Optional[float] = None
        # The idle timeout in seconds for the pending nodes not needed by any
        # resource demands.
        _pending_idle_timeout_s: Optional[float] = None
        # The current schedulable nodes (including pending nodes and pending requests).
        _nodes: List[SchedulingNode] = field(default_factory=list)
        # The number of nodes by node types available for launching based on the max
        # number of workers in the config. This takes into account any pending/running
        # nodes.
        _node_type_available: Dict[NodeType, int] = field(default_factory=dict)
        # The IPPR specs for the scheduling request.
        _ippr_specs: Optional[IPPRSpecs] = None
        # The availability scores of cloud resource. A low score suggests that
        # this type of resource has historically experienced allocation failures,
        # and the weight of this type should be reduced during scheduling.
        _cloud_resource_availabilities: Dict[NodeType, float] = field(
            default_factory=dict
        )
        # The recoverable cloud resource availability score.
        # Similar to _cloud_resource_availabilities, but it will recover from 0.0 to 1.0
        # linearly over RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S seconds.
        _recoverable_resource_availabilities: Dict[NodeType, float] = field(
            default_factory=dict
        )

        def __init__(
            self,
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
            cloud_resource_availabilities: Dict[NodeType, float],
            recoverable_resource_availabilities: Dict[NodeType, float],
            disable_launch_config_check: bool,
            max_num_nodes: Optional[int] = None,
            idle_timeout_s: Optional[float] = None,
            ippr_specs: Optional[IPPRSpecs] = None,
            pending_idle_timeout_s: Optional[float] = None,
            pinned_domains: Optional[Dict[str, Set[str]]] = None,
            expired_placeholder_keys: Optional[Set[Tuple[str, str, str]]] = None,
            replica_view: Optional[_MultiHostReplicaView] = None,
            prescan: Optional["ResourceDemandScheduler._ReplicaPreScan"] = None,
        ):
            self._nodes = nodes
            self._all_nodes: List[SchedulingNode] = self._nodes
            self._node_type_configs = node_type_configs
            self._max_num_nodes = max_num_nodes
            self._idle_timeout_s = idle_timeout_s
            self._pending_idle_timeout_s = pending_idle_timeout_s
            self._disable_launch_config_check = disable_launch_config_check
            self._ippr_specs = ippr_specs
            self._cloud_resource_availabilities = cloud_resource_availabilities
            self._recoverable_resource_availabilities = (
                recoverable_resource_availabilities
            )
            self._pending_domain_ids = itertools.count()
            self._pinned_domains: Dict[str, Set[str]] = {
                k: set(v) for k, v in (pinned_domains or {}).items()
            }
            ps = prescan or ResourceDemandScheduler._ReplicaPreScan()
            self._prescan = ps
            self._expired_placeholder_keys: Set[Tuple[str, str, str]] = set(
                expired_placeholder_keys or ()
            )
            self._active_placeholder_keys: Set[Tuple[str, str, str]] = set()
            self._replicas_by_type: Dict[NodeType, Dict[str, _Replica]] = {}
            self._extra_lid_backfill: Dict[NodeType, int] = {}
            self._suppressed_runtime_keys: Dict[NodeType, Set[str]] = {}
            self._missing_replica_backfill: Dict[NodeType, int] = {}
            self._unbacked_refill_gaps: Dict[NodeType, int] = {}
            self._unbacked_missing_slots: Dict[NodeType, int] = {}
            self._launching_types: Set[NodeType] = set()
            self._prebuilt_refills: Dict[NodeType, List[SchedulingNode]] = {}
            self._co_launch_overhead: Dict[NodeType, int] = {}
            self._has_replica_view = replica_view is not None
            self._replica_view = replica_view or _MultiHostReplicaView()
            self._omitted_stopping_cnt: Dict[NodeType, int] = dict(
                ps.omitted_stopping_cnt
            )
            self._omitted_prov_rids: Dict[NodeType, Set[str]] = {
                k: set(v) for k, v in ps.omitted_prov_rids.items()
            }
            for rk_nt, rk_rid in self._replica_view.rep_occupying:
                self._omitted_prov_rids.setdefault(rk_nt, set()).add(rk_rid)
            replica_sizes_init = self._multihost_replica_sizes(self._node_type_configs)
            stop_all = (
                ps.reconciler_stopping_cnt
                if ps.reconciler_stopping_cnt is not None
                else self._omitted_stopping_cnt
            )
            self._reconciler_stopping_base: Dict[NodeType, int] = {
                nt: stop_all.get(nt, 0) for nt in replica_sizes_init
            }
            self._reconciler_nt_avail: Dict[NodeType, int] = {}
            self._reconciler_global_avail: Optional[int] = None
            self._unlabeled_view_types: Set[NodeType] = set()
            for nt, cfg in self._node_type_configs.items():
                if not cfg.is_multi_worker_replica or not (
                    self._replica_view.unlabeled_occupying_cids.get(nt)
                ):
                    continue
                non_prov_occ = sum(
                    1
                    for n in self._nodes
                    if n.node_type == nt
                    and n.node_kind != NodeKind.HEAD
                    and n.status
                    not in (
                        SchedulingNodeStatus.TO_TERMINATE,
                        SchedulingNodeStatus.TO_LAUNCH,
                    )
                    and n.im_instance_status != Instance.QUEUED
                    and not (
                        n.im_instance_status == Instance.REQUESTED
                        and (
                            (nt, n.launch_request_id or "")
                            in self._replica_view.torn_down_lids
                            or bool(
                                self._replica_view.claimed_reps_by_lid.get(
                                    (nt, n.launch_request_id or "")
                                )
                            )
                        )
                    )
                    and not n._is_backfill_placeholder
                    and not n.provider_replica_id
                )
                total_occ = (
                    non_prov_occ
                    + self._omitted_stopping_cnt.get(nt, 0)
                    + sum(self._replica_view.unclaimed_vanished_by_lid(nt).values())
                )
                vanished_by_lid = self._replica_view.unclaimed_vanished_by_lid(nt)
                view_occ = len(
                    self._replica_view.unlabeled_occupying_cids.get(nt, ())
                ) + sum(
                    cnt + vanished_by_lid.get(lid, 0)
                    for (t, lid), cnt in self._replica_view.requested_by_lid.items()
                    if t == nt
                    and cnt > 0
                    and (t, lid) not in self._replica_view.torn_down_lids
                    and not self._replica_view.claimed_reps_by_lid.get((t, lid))
                )
                if view_occ == total_occ:
                    self._unlabeled_view_types.add(nt)
            self._init_replicas()
            self._refresh_capacity_stats()

        def get_active_placeholder_keys(self) -> Set[Tuple[str, str, str]]:
            return set(self._active_placeholder_keys)

        def get_expired_placeholder_keys(self) -> Set[Tuple[str, str, str]]:
            return self._expired_placeholder_keys

        def get_replica_view(self) -> _MultiHostReplicaView:
            return self._replica_view

        def has_excess_solicited_on_lid(
            self, node_type: NodeType, lid: str, exited_lid_slots: int
        ) -> bool:
            key = (node_type, lid)
            ps = self._prescan
            solic = ps.solicited_by_lid.get(key, 0)
            if solic > exited_lid_slots:
                return True
            return (
                ps.active_stopping_by_lid.get(key, 0) > 0
                and solic + ps.unsolicited_by_lid.get(key, 0) > exited_lid_slots
            )

        def _missing_prov_replica_slots(self, node_type: NodeType) -> int:
            cfg = self._node_type_configs.get(node_type)
            if cfg is None or not cfg.is_multi_worker_replica:
                return 0
            live_prov_rids = {
                n.provider_replica_id
                for n in self._nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and n.provider_replica_id
                and not self._is_canceled_unallocated(n)
            }
            missing_prov_reps = sum(
                1
                for rid in self._omitted_prov_rids.get(node_type, ())
                if rid not in live_prov_rids
            )
            return missing_prov_reps * cfg.workers_per_replica

        def get_omitted_occupying_slots(self, node_type: NodeType) -> int:
            return self._omitted_stopping_cnt.get(
                node_type, 0
            ) + self._missing_prov_replica_slots(node_type)

        def get_suppressed_runtime_keys(self, node_type: NodeType) -> Set[str]:
            return set(self._suppressed_runtime_keys.get(node_type, ()))

        def is_completed_subreplica_batch(
            self, node_type: NodeType, lid: str, live_cnt: int, size: int
        ) -> bool:
            """Return True when an IM-created sub-replica batch arrived in full with
            no remaining REQUESTED slots."""
            key = (node_type, lid)
            ps = self._prescan
            return (
                live_cnt % size != 0
                and key in ps.im_created_lids
                and self._replica_view.requested_by_lid.get(key, 0) == 0
                and ps.im_total_rows_by_lid.get(key, 0) == live_cnt
            )

        def get_own_pad_credit(
            self,
            node_type: NodeType,
            extra_launched_types: Iterable[NodeType] = (),
        ) -> int:
            if node_type in self._launching_types or node_type in extra_launched_types:
                return 0
            pad = self._unbacked_missing_slots.get(node_type, 0)
            if self._reconciler_nt_avail:
                r_avail = self._reconciler_nt_avail.get(node_type)
                if r_avail is not None:
                    nt_avail = self._node_type_available.get(node_type, 0)
                    pad = min(pad, max(0, r_avail - nt_avail))
            return max(0, pad)

        def get_headroom_credit(
            self,
            node_type: NodeType,
            includes_pad: bool = False,
            extra_launched_types: Iterable[NodeType] = (),
        ) -> int:
            launched = self._launching_types.union(extra_launched_types)
            other_slots = sum(
                slots
                for nt, slots in self._unbacked_missing_slots.items()
                if nt != node_type and nt not in launched
            )
            own_slots = (
                self.get_own_pad_credit(node_type, launched) if includes_pad else 0
            )
            credit = other_slots + own_slots
            if self._reconciler_global_avail is not None and self._headroom is not None:
                unlab_own = (
                    self._unbacked_missing_slots.get(node_type, 0)
                    if (
                        not includes_pad
                        and node_type in self._unlabeled_view_types
                        and node_type not in launched
                    )
                    else 0
                )
                max_credit = max(
                    0,
                    self._reconciler_global_avail - unlab_own - self._headroom,
                )
                credit = min(credit, max_credit)
            return credit

        def has_unlabeled_view(self, node_type: NodeType) -> bool:
            return node_type in self._unlabeled_view_types

        def get_unlabeled_view_types(self) -> Set[NodeType]:
            return set(self._unlabeled_view_types)

        def effective_idle_timeout_s(
            self, cfg: Optional[NodeTypeConfig]
        ) -> Optional[float]:
            return (
                cfg.idle_timeout_s
                if cfg is not None and cfg.idle_timeout_s is not None
                else self._idle_timeout_s
            )

        def effective_idle_timeout_ms(
            self, cfg: Optional[NodeTypeConfig]
        ) -> Optional[float]:
            eff_s = self.effective_idle_timeout_s(cfg)
            return eff_s * 1000.0 if eff_s is not None else None

        def peek_pinned_domains(self) -> Dict[str, Set[str]]:
            return self._pinned_domains

        def get_replicas_by_type(self) -> Dict[NodeType, Dict[str, _Replica]]:
            return self._replicas_by_type

        def get_unbacked_refill_gap(self, node_type: NodeType) -> int:
            return self._unbacked_refill_gaps.get(node_type, 0)

        def _live_nodes_by_rid(
            self,
            node_type: NodeType,
            scan_nodes: Optional[List[SchedulingNode]] = None,
        ) -> Dict[str, List[SchedulingNode]]:
            live_by_rid: Dict[str, List[SchedulingNode]] = defaultdict(list)
            for n in scan_nodes if scan_nodes is not None else self._nodes:
                if (
                    n.node_type == node_type
                    and n.node_kind != NodeKind.HEAD
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                    and not n.is_leaving
                    and n.replica_id
                ):
                    live_by_rid[n.replica_id].append(n)
            return live_by_rid

        def _queued_and_to_launch_count(self, node_type: NodeType) -> int:
            return sum(
                1
                for n in self._nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and n.status != SchedulingNodeStatus.TO_TERMINATE
                and (
                    n.im_instance_status == Instance.QUEUED
                    or n.status == SchedulingNodeStatus.TO_LAUNCH
                )
            )

        def get_unattributed_open_slots(self, node_type: NodeType) -> int:
            cfg = self._node_type_configs.get(node_type)
            if cfg is None or not cfg.is_multi_worker_replica:
                return 0
            size = cfg.workers_per_replica
            is_exclusive = cfg.is_exclusive_domain
            live_by_rid = self._live_nodes_by_rid(node_type)
            return sum(
                _Replica._slots_for(len(live_by_rid[rep.rid]), size, not is_exclusive)[
                    0
                ]
                for rep in self._replicas_by_type.get(node_type, {}).values()
                if rep.no_backfill
                and rep.allow_refill_attach
                and not rep.is_provider
                and not rep.lids
                and live_by_rid.get(rep.rid)
                and not any(m.is_leaving for m in rep.members)
            )

        def get_co_launch_overhead(self, node_type: NodeType) -> int:
            return self._co_launch_overhead.get(node_type, 0)

        def get_unlabeled_backfill_gap(
            self,
            node_type: NodeType,
            size: int,
            extra_queued: int = 0,
            *,
            deltas: Optional[
                Tuple[
                    Set[int],
                    Set[Tuple[NodeType, str]],
                    Set[Tuple[NodeType, str]],
                    Set[Tuple[NodeType, str]],
                ]
            ] = None,
        ) -> int:
            if node_type not in self._unlabeled_view_types:
                return 0
            _, dropped_cids, extra_td_lids, _ = (
                deltas if deltas is not None else self._pending_termination_deltas()
            )
            return self._replica_view.unlabeled_backfill_gap(
                node_type,
                size,
                extra_queued=extra_queued,
                dropped_cids=dropped_cids,
                extra_td_lids=extra_td_lids,
            )

        def doomed_queued_count(
            self, node_type: NodeType, size: int, queued_cnt: int
        ) -> int:
            if queued_cnt <= 0 or size <= 1:
                return 0
            gap = self.get_unlabeled_backfill_gap(node_type, size)
            return queued_cnt if (0 < queued_cnt < gap) else (queued_cnt - gap) % size

        def new_pending_domain(self, tag: str = "") -> str:
            return f"{_PENDING_DOMAIN_PREFIX}{f'{tag}:' if tag else ''}{next(self._pending_domain_ids)}"

        @staticmethod
        def _launch_resources(
            cfg: NodeTypeConfig, ippr_specs: Optional[IPPRSpecs]
        ) -> Dict[str, float]:
            resources = dict(cfg.resources)
            if ippr_specs and cfg.name in ippr_specs.groups:
                grp = ippr_specs.groups[cfg.name]
                resources["CPU"] = float(max(grp.max_cpu, resources.get("CPU", 0)))
                resources["memory"] = float(
                    max(grp.max_memory, resources.get("memory", 0))
                )
            return resources

        def get_launch_resources(self, node_type: NodeType) -> Dict[str, float]:
            return self._launch_resources(
                self._node_type_configs[node_type], self._ippr_specs
            )

        def _init_replicas_for_node_type(
            self, node_type: NodeType, cfg: NodeTypeConfig
        ) -> None:
            """Group running and pending worker nodes of one node type into fixed replicas."""
            type_nodes = [
                n
                for n in self._nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and n.status != SchedulingNodeStatus.TO_TERMINATE
            ]
            if cfg.replica_config is None:
                if cfg.runtime_label_keys:
                    suppressed = _compute_suppressed_runtime_keys(
                        cfg, ([n] for n in type_nodes if n.is_running)
                    )
                    if suppressed:
                        self._suppressed_runtime_keys[node_type] = suppressed
                return
            res = _ReplicaGrouper(self, node_type, cfg, type_nodes).group()
            self._replicas_by_type[node_type] = res.replicas
            self._active_placeholder_keys.update(res.active_placeholder_keys)
            if res.extra_lid_backfill > 0:
                self._extra_lid_backfill[node_type] = res.extra_lid_backfill
            if res.suppressed_runtime_keys:
                self._suppressed_runtime_keys[node_type] = res.suppressed_runtime_keys
            if res.placeholders:
                self._nodes.extend(res.placeholders)

        def _init_replicas(self) -> None:
            """Group running and pending worker nodes into canonical fixed replicas."""
            for node_type, cfg in self._node_type_configs.items():
                self._init_replicas_for_node_type(node_type, cfg)

        @staticmethod
        def _missing_replica_backfill_and_nonprov_by_type(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
            replicas_by_type: Optional[Dict[NodeType, Dict[str, _Replica]]] = None,
            extra_lid_backfill: Optional[Dict[NodeType, int]] = None,
            torn_down_lids: Optional[Set[Tuple[NodeType, str]]] = None,
            omitted_stopping_cnt: Optional[Dict[NodeType, int]] = None,
            unlabeled_view_types: Optional[Set[NodeType]] = None,
            deferred_term_node_ids: Optional[Set[int]] = None,
        ) -> Tuple[Dict[NodeType, int], Dict[NodeType, int], Dict[NodeType, int],]:
            """Return `(total_missing, non_prov_missing, unconsumed_align_pad)`."""
            missing: Dict[NodeType, int] = {}
            np_missing: Dict[NodeType, int] = {}
            unconsumed_pad: Dict[NodeType, int] = {}
            td_lids = torn_down_lids or set()
            stop_map = omitted_stopping_cnt or {}
            unlab_types = unlabeled_view_types or set()
            def_term_ids = deferred_term_node_ids or set()
            for nt, cfg in node_type_configs.items():
                if not cfg.is_multi_worker_replica:
                    continue
                size = cfg.workers_per_replica
                is_exclusive = cfg.is_exclusive_domain
                reps = (replicas_by_type or {}).get(nt, {})
                extra_lid_gap = (extra_lid_backfill or {}).get(nt, 0)
                k8s_count = np_k8s_count = booting_count = refill_tl_cnt = 0
                dom_counts: Counter = Counter()
                np_dom_counts: Counter = Counter()
                np_real_by_rid: Counter = Counter()
                draining_dom_counts: Counter = Counter()
                np_draining_dom_counts: Counter = Counter()
                draining_unassigned = 0
                for n in nodes:
                    if n.node_type != nt or n.node_kind == NodeKind.HEAD:
                        continue
                    if n.status == SchedulingNodeStatus.TO_LAUNCH:
                        if n._is_unlabeled_refill and not n.provider_replica_id:
                            refill_tl_cnt += 1
                        continue
                    if ResourceDemandScheduler.ScheduleContext._is_canceled_unallocated(
                        n
                    ) or (
                        n.status == SchedulingNodeStatus.TO_TERMINATE
                        and (
                            n._is_backfill_placeholder
                            or (
                                n.im_instance_status
                                in (Instance.ALLOCATED, Instance.RAY_INSTALLING)
                                and id(n) not in def_term_ids
                            )
                        )
                    ):
                        continue
                    rep_n = reps.get(n.replica_id) if n.replica_id else None
                    if n.im_instance_status == Instance.QUEUED and not (
                        rep_n is not None
                        and any(
                            m.im_instance_status != Instance.QUEUED
                            for m in rep_n.members
                        )
                    ):
                        continue
                    is_prov = bool(n.provider_replica_id) or (
                        rep_n is not None and rep_n.is_provider
                    )
                    if n.is_leaving or n.status == SchedulingNodeStatus.TO_TERMINATE:
                        if (
                            n.im_instance_status == Instance.REQUESTED
                            and (nt, n.launch_request_id or "") in td_lids
                        ):
                            continue
                        if n.replica_id:
                            draining_dom_counts[n.replica_id] += 1
                            if not is_prov:
                                np_draining_dom_counts[n.replica_id] += 1
                        elif not is_prov:
                            draining_unassigned += 1
                        continue
                    k8s_count += 1
                    if not is_prov:
                        np_k8s_count += 1
                    if n.replica_id and (
                        is_prov or n._has_runtime_domain or rep_n is not None
                    ):
                        dom_counts[n.replica_id] += 1
                        if not is_prov:
                            np_dom_counts[n.replica_id] += 1
                            if not n._is_backfill_placeholder and not n.is_leaving:
                                np_real_by_rid[n.replica_id] += 1
                    elif not n.is_running and not is_prov:
                        booting_count += 1

                unrounded_omit = stop_map.get(nt, 0)
                np_draining_in_nodes = (
                    sum(np_draining_dom_counts.values()) + draining_unassigned
                )
                refillable_open_cnt = sum(
                    _Replica._slots_for(
                        np_real_by_rid[rep.rid], size, not is_exclusive
                    )[0]
                    for rep in reps.values()
                    if not rep.is_provider
                    and rep.no_backfill
                    and rep.allow_refill_attach
                    and np_real_by_rid.get(rep.rid, 0) > 0
                    and not any(m.is_leaving for m in rep.members)
                )

                def _slots_for_rid(
                    rid: str, cnt: int, *, is_draining: bool = False
                ) -> Tuple[int, int]:
                    rep = reps.get(rid)
                    if rep is not None:
                        if rep.no_backfill and not is_draining:
                            return 0, cnt
                        return _Replica._slots_for(cnt, size, rep.allow_multi_replica)
                    return _Replica._slots_for(
                        cnt,
                        size,
                        not cfg.is_exclusive_domain
                        or rid.startswith(_PENDING_DOMAIN_PREFIX),
                    )

                def _compute_gap(
                    cur_k8s: int,
                    cur_dom: Counter,
                    cur_draining: Counter,
                    *,
                    include_omit_base: bool = True,
                ) -> Tuple[int, int]:
                    prov_drain_gap = (
                        sum(
                            _slots_for_rid(rid, cnt, is_draining=True)[0]
                            for rid, cnt in cur_draining.items()
                            if rid not in np_draining_dom_counts
                        )
                        if cur_draining is draining_dom_counts
                        else 0
                    )
                    dom_committed = raw_gap = unrounded_cnt = 0
                    for rid, cnt in cur_dom.items():
                        rep = reps.get(rid)
                        if rep is not None and rep.no_backfill:
                            unrounded_cnt += cnt
                            dom_committed += cnt
                            continue
                        rep_open, rep_committed = _slots_for_rid(rid, cnt)
                        raw_gap += rep_open
                        dom_committed += rep_committed
                    extra = (max(0, booting_count - raw_gap) + size - 1) // size
                    roundable_k8s = max(0, cur_k8s - unrounded_cnt)
                    committed = max(
                        ((roundable_k8s + size - 1) // size) * size
                        + unrounded_cnt
                        + extra_lid_gap,
                        dom_committed + extra * size + extra_lid_gap,
                    )
                    healthy_raw = max(0, committed - cur_k8s)
                    drain_total = np_draining_in_nodes + unrounded_omit
                    raw_align = (
                        (-(unrounded_cnt + extra_lid_gap + drain_total)) % size
                        if drain_total > 0 or nt in unlab_types
                        else 0
                    )
                    pad_raw = max(0, raw_align - refillable_open_cnt)
                    np_net = max(0, pad_raw + healthy_raw - refill_tl_cnt)
                    omit_base = unrounded_omit if include_omit_base else 0
                    return prov_drain_gap + omit_base + np_net, np_net

                total_gap, total_pad = _compute_gap(
                    k8s_count, dom_counts, draining_dom_counts
                )
                if total_gap > 0:
                    missing[nt] = total_gap
                np_gap, rem_pad = _compute_gap(
                    np_k8s_count,
                    np_dom_counts,
                    np_draining_dom_counts,
                    include_omit_base=False,
                )
                if np_gap > 0:
                    np_missing[nt] = np_gap
                rem_pad = min(rem_pad, total_pad)
                if rem_pad > 0:
                    unconsumed_pad[nt] = rem_pad
            return missing, np_missing, unconsumed_pad

        def get_missing_replica_backfill_slots(self) -> Dict[NodeType, int]:
            return dict(self._missing_replica_backfill)

        def get_headroom(self) -> Optional[int]:
            return self._headroom

        def _make_launch_node(
            self, node_type: NodeType, include_ippr_caps: bool = True
        ) -> SchedulingNode:
            cfg = self._node_type_configs[node_type]
            node = SchedulingNode.from_node_config(
                cfg,
                status=SchedulingNodeStatus.TO_LAUNCH,
                node_kind=NodeKind.WORKER,
            )
            if (
                include_ippr_caps
                and self._ippr_specs
                and node_type in self._ippr_specs.groups
            ):
                capped = self._launch_resources(cfg, self._ippr_specs)
                node.update_total_resources(
                    {"CPU": capped["CPU"], "memory": capped["memory"]}
                )
            return node

        def make_replica_nodes(
            self, node_type: NodeType, include_ippr_caps: bool = True
        ) -> List[SchedulingNode]:
            """Create one atomic TO_LAUNCH replica for `node_type`."""
            cfg = self._node_type_configs[node_type]
            rc = cfg.replica_config
            if rc is None:
                return [self._make_launch_node(node_type, include_ippr_caps)]
            replica = [
                self._make_launch_node(node_type, include_ippr_caps)
                for _ in range(rc.workers_per_replica)
            ]
            _assign_planned_slots(
                replica,
                cfg,
                [],
                self.new_pending_domain(node_type),
                suppressed_runtime_keys=self._suppressed_runtime_keys.get(node_type),
            )
            return replica

        def make_refill_nodes(
            self,
            node_type: NodeType,
            count: int,
            include_ippr_caps: bool = True,
            live_nodes: Optional[List[SchedulingNode]] = None,
        ) -> List[SchedulingNode]:
            """Create up to `count` TO_LAUNCH refill nodes for incomplete unlabeled
            replicas of `node_type` without exceeding per-replica capacity."""
            if count <= 0:
                return []
            if live_nodes is None and node_type in self._prebuilt_refills:
                return copy.deepcopy(self._prebuilt_refills[node_type])[:count]
            cfg = self._node_type_configs[node_type]
            size = cfg.workers_per_replica
            is_exclusive = cfg.is_exclusive_domain
            reps = self._replicas_by_type.get(node_type, {})
            live_by_rid = self._live_nodes_by_rid(
                node_type, live_nodes if live_nodes is not None else self._all_nodes
            )
            pinned_domains = self.peek_pinned_domains()
            eff_idle_ms = self.effective_idle_timeout_ms(cfg)

            def _rep_open(rep: _Replica) -> int:
                if (
                    rep.is_provider
                    or not rep.allow_refill_attach
                    or any(m.is_leaving or m._is_draining_replica for m in rep.members)
                ):
                    return 0
                live_m = live_by_rid.get(rep.rid, [])
                if not live_m:
                    return 0
                return _Replica._slots_for(len(live_m), size, not is_exclusive)[0]

            def _emit_on(
                live_m: List[SchedulingNode],
                take: int,
                rid: str,
                dom_val: Optional[str],
                shared_rt: Optional[Dict[str, str]],
            ) -> List[SchedulingNode]:
                batch = []
                for _ in range(take):
                    node = self._make_launch_node(node_type, include_ippr_caps)
                    node._is_unlabeled_refill = True
                    batch.append(node)
                _assign_planned_slots(
                    batch,
                    cfg,
                    live_m,
                    rid,
                    domain_val=dom_val,
                    shared_runtime_labels=shared_rt,
                    suppressed_runtime_keys=self._suppressed_runtime_keys.get(
                        node_type
                    ),
                )
                return batch

            refill: List[SchedulingNode] = []
            remaining = count
            for rep in sorted(
                (r for r in reps.values() if _rep_open(r) > 0),
                key=lambda r: (
                    _ReplicaGrouper._replica_inc_priority(
                        live_by_rid[r.rid],
                        pinned_domains,
                        cfg.domain_label_key,
                        eff_idle_ms,
                    ),
                    _rep_open(r),
                    r.domain is None or r.domain.startswith(_PENDING_DOMAIN_PREFIX),
                    r.rid,
                ),
            ):
                if remaining <= 0:
                    break
                open_cnt = _rep_open(rep)
                take = min(remaining, open_cnt)
                refill.extend(
                    _emit_on(
                        live_by_rid[rep.rid],
                        take,
                        rep.rid,
                        rep.domain,
                        rep.shared_runtime_labels,
                    )
                )
                remaining -= take
            while remaining > 0:
                take = min(remaining, size)
                rid = self.new_pending_domain(node_type)
                refill.extend(_emit_on([], take, rid, rid, None))
                remaining -= take
            return refill

        @staticmethod
        def _multihost_replica_sizes(
            node_type_configs: Dict[NodeType, NodeTypeConfig],
        ) -> Dict[NodeType, int]:
            return {
                nt: cfg.workers_per_replica
                for nt, cfg in node_type_configs.items()
                if cfg.is_multi_worker_replica
            }

        def get_multihost_replica_sizes(self) -> Dict[NodeType, int]:
            return self._multihost_replica_sizes(self._node_type_configs)

        @classmethod
        def from_schedule_request(
            cls,
            req: SchedulingRequest,
            expired_placeholder_keys: Optional[Set[Tuple[str, str, str]]] = None,
            instance_domains: Optional[Dict[Tuple[NodeType, str], str]] = None,
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Create a schedule context from a schedule request.
            It will populate the context with the existing nodes and the available node
            types from the config.

            Args:
                req: The scheduling request. The caller should make sure the
                    request is valid.
                expired_placeholder_keys: Replica keys whose backfill
                    placeholders have exceeded `pending_idle_timeout_s`.
                instance_domains: Remembered `(node_type, instance_id) -> domain`
                    mappings for workers whose `ray_node` has since exited.

            Returns:
                A schedule context populated from the scheduling request.
            """

            nodes = []
            node_type_configs = req.node_type_configs

            # Pre-scan all IM rows for multi-worker node types before
            # `SchedulingNode.new` filters out non-schedulable statuses so
            # draining/teardown replicas and vanished launch batches stay
            # visible to `_init_replicas`.
            replica_sizes = cls._multihost_replica_sizes(node_type_configs)
            draining_domains: Dict[NodeType, Set[str]] = defaultdict(set)
            torn_down_dom_max_ns: Dict[
                NodeType, Dict[str, Optional[int]]
            ] = defaultdict(dict)
            untagged_td_by_cid: Dict[NodeType, Dict[str, Optional[int]]] = defaultdict(
                dict
            )
            active_draining_lids: Dict[NodeType, Set[str]] = defaultdict(set)
            draining_lid_domains: Dict[NodeType, Set[Tuple[str, str]]] = defaultdict(
                set
            )
            solicited_by_lid: Counter = Counter()
            unsolicited_by_lid: Counter = Counter()
            active_stopping_by_lid: Counter = Counter()
            total_cloud_cids_by_lid: Dict[Tuple[NodeType, str], Set[str]] = defaultdict(
                set
            )
            im_rows_by_lid: Dict[Tuple[NodeType, str], Set[str]] = defaultdict(set)
            im_created_lids: Set[Tuple[NodeType, str]] = set()
            omitted_stopping_cids: Dict[NodeType, Set[str]] = defaultdict(set)
            all_stopping_cids: Dict[NodeType, Set[str]] = defaultdict(set)
            omitted_prov_rids: Dict[NodeType, Set[str]] = defaultdict(set)
            first_fn = req.first_status_transition_time_ns_fn

            def _has_lifetime_status(
                im: Instance,
                statuses: Tuple[Instance.InstanceStatus.ValueType, ...],
            ) -> bool:
                if im.status in statuses:
                    return True
                if first_fn is not None:
                    return any(first_fn(im, s) is not None for s in statuses)
                return any(h.instance_status in statuses for h in im.status_history)

            if replica_sizes:
                active_nt_cids: Set[Tuple[NodeType, str]] = set()
                for instance in req.current_instances:
                    im = instance.im_instance
                    if (
                        im is None
                        or im.node_kind == NodeKind.HEAD
                        or im.instance_type not in replica_sizes
                    ):
                        continue
                    cid = instance.cloud_instance_id or im.cloud_instance_id
                    if cid and im.status in _ACTIVE_CID_STATUSES:
                        active_nt_cids.add((im.instance_type, cid))

                for instance in req.current_instances:
                    im = instance.im_instance
                    if (
                        im is None
                        or im.node_kind == NodeKind.HEAD
                        or im.instance_type not in replica_sizes
                    ):
                        continue
                    nt = im.instance_type
                    cfg = node_type_configs[nt]
                    st = im.status
                    cid = instance.cloud_instance_id or im.cloud_instance_id
                    if _is_shadowed_stopped(nt, st, cid, active_nt_cids):
                        continue
                    dom = (
                        (
                            (instance.ray_node.labels or {}).get(cfg.domain_label_key)
                            if instance.ray_node is not None
                            else None
                        )
                        or (instance_domains or {}).get((nt, im.instance_id))
                        if cfg.domain_label_key
                        else None
                    )
                    is_solic = _is_current_solicited_teardown(im)
                    if im.launch_request_id:
                        lkey = (nt, im.launch_request_id)
                        im_rows_by_lid[lkey].add(cid or f"__iid_{im.instance_id}")
                        if _has_lifetime_status(im, _UNALLOCATED_INSTANCE_STATUSES):
                            im_created_lids.add(lkey)
                        if cid or st == Instance.REQUESTED:
                            total_cloud_cids_by_lid[lkey].add(
                                cid or f"__req_{im.instance_id}"
                            )
                        if is_solic:
                            solicited_by_lid[lkey] += 1
                        elif st in (
                            _TEARDOWN_INSTANCE_STATUSES | _TERMINAL_INSTANCE_STATUSES
                        ):
                            unsolicited_by_lid[lkey] += 1
                        if (
                            st in (Instance.RAY_STOP_REQUESTED, Instance.RAY_STOPPING)
                            and not dom
                        ):
                            active_stopping_by_lid[lkey] += 1
                    if st == Instance.RAY_STOPPING:
                        stop_cid_key = cid or f"__iid_{im.instance_id}"
                        all_stopping_cids[nt].add(stop_cid_key)
                        if instance.cloud_instance_replica_id:
                            omitted_prov_rids[nt].add(
                                instance.cloud_instance_replica_id
                            )
                        else:
                            omitted_stopping_cids[nt].add(stop_cid_key)
                    is_live_or_term_teardown = st in _TEARDOWN_INSTANCE_STATUSES or (
                        st in _TERMINAL_INSTANCE_STATUSES
                        and _has_teardown_history(im)
                        and not (cid and (nt, cid) in active_nt_cids)
                    )
                    had_running = _has_lifetime_status(im, (Instance.RAY_RUNNING,))
                    if (
                        not instance.cloud_instance_replica_id
                        and is_live_or_term_teardown
                        and (not is_solic or (bool(dom) and had_running))
                    ):
                        td_end_ns: Optional[int] = (
                            None
                            if st in _TEARDOWN_INSTANCE_STATUSES
                            else max(
                                (h.timestamp_ns for h in im.status_history),
                                default=0,
                            )
                        )
                        if dom:
                            prev_dom_ns = torn_down_dom_max_ns[nt].get(dom, -1)
                            if prev_dom_ns is not None:
                                torn_down_dom_max_ns[nt][dom] = (
                                    None
                                    if td_end_ns is None
                                    else max(prev_dom_ns, td_end_ns)
                                )
                        elif not is_solic and not im.launch_request_id:
                            td_key = cid or f"__iid_{im.instance_id}"
                            prev_cid_ns = untagged_td_by_cid[nt].get(td_key, -1)
                            if prev_cid_ns is not None:
                                untagged_td_by_cid[nt][td_key] = (
                                    None
                                    if td_end_ns is None
                                    else max(prev_cid_ns, td_end_ns)
                                )
                    is_rep_drain = st in _DRAINING_INSTANCE_STATUSES or (
                        st in _TEARDOWN_INSTANCE_STATUSES
                        and (bool(instance.cloud_instance_replica_id) or is_solic)
                    )
                    if is_rep_drain:
                        if dom:
                            if cfg.domain_exclusive:
                                draining_domains[nt].add(dom)
                            if im.launch_request_id:
                                draining_lid_domains[nt].add(
                                    (im.launch_request_id, dom)
                                )
                        elif im.launch_request_id:
                            active_draining_lids[nt].add(im.launch_request_id)

            # Initialize the scheduling nodes.
            for instance in req.current_instances:
                node = SchedulingNode.new(
                    instance,
                    node_type_configs,
                    req.disable_launch_config_check,
                )
                if node:
                    if instance.im_instance is not None:
                        node._cloud_start_ns = _instance_cloud_start_ns(
                            instance.im_instance
                        )
                    nodes.append(node)
                    if (
                        req.ippr_statuses
                        and req.ippr_specs
                        and instance.cloud_instance_id in req.ippr_statuses
                        and node.node_type in req.ippr_specs.groups
                    ):
                        # Attach the current IPPR state of the node and its IPPR spec for
                        # later resizing.
                        node.ippr_spec = req.ippr_specs.groups[node.node_type]
                        node.ippr_status = req.ippr_statuses[instance.cloud_instance_id]
                        if node.ray_node_id:
                            node.ippr_status.raylet_id = node.ray_node_id

            prescan = (
                ResourceDemandScheduler._ReplicaPreScan(
                    draining_domains={k: set(v) for k, v in draining_domains.items()},
                    torn_down_dom_max_ns={
                        k: dict(v) for k, v in torn_down_dom_max_ns.items()
                    },
                    untagged_td_end_ns={
                        nt: sorted(
                            cids.values(),
                            key=lambda x: (x is not None, -(x or 0)),
                        )
                        for nt, cids in untagged_td_by_cid.items()
                    },
                    active_draining_lids={
                        k: set(v) for k, v in active_draining_lids.items()
                    },
                    draining_lid_domains={
                        k: set(v) for k, v in draining_lid_domains.items()
                    },
                    omitted_stopping_cnt={
                        nt: len(cids) for nt, cids in omitted_stopping_cids.items()
                    },
                    reconciler_stopping_cnt={
                        nt: len(cids) for nt, cids in all_stopping_cids.items()
                    },
                    omitted_prov_rids={k: set(v) for k, v in omitted_prov_rids.items()},
                    solicited_by_lid=dict(solicited_by_lid),
                    unsolicited_by_lid=dict(unsolicited_by_lid),
                    active_stopping_by_lid=dict(active_stopping_by_lid),
                    total_cloud_pods_by_lid={
                        k: len(cids) for k, cids in total_cloud_cids_by_lid.items()
                    },
                    im_total_rows_by_lid={
                        k: len(rows) for k, rows in im_rows_by_lid.items()
                    },
                    im_created_lids=set(im_created_lids),
                )
                if replica_sizes
                else None
            )
            return cls(
                nodes=nodes,
                node_type_configs=node_type_configs,
                cloud_resource_availabilities=req.cloud_resource_availabilities,
                recoverable_resource_availabilities=req.recoverable_resource_availabilities,
                disable_launch_config_check=req.disable_launch_config_check,
                max_num_nodes=req.max_num_nodes,
                idle_timeout_s=req.idle_timeout_s,
                pending_idle_timeout_s=req.pending_idle_timeout_s,
                ippr_specs=req.ippr_specs,
                pinned_domains=ResourceDemandScheduler._pinned_locality_domains(
                    req.gang_resource_requests, node_type_configs
                ),
                expired_placeholder_keys=expired_placeholder_keys,
                replica_view=req.replica_view,
                prescan=prescan,
            )

        @staticmethod
        def _nonprov_active_pod_count(
            nodes: Iterable[SchedulingNode],
            replicas: Dict[str, _Replica],
            node_type: NodeType,
            torn_down_lids: Set[Tuple[NodeType, str]],
        ) -> int:
            return sum(
                1
                for n in nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and not n.provider_replica_id
                and not n.is_leaving
                and n.status
                not in (
                    SchedulingNodeStatus.TO_TERMINATE,
                    SchedulingNodeStatus.TO_LAUNCH,
                )
                and not n._is_backfill_placeholder
                and not (
                    n.im_instance_status == Instance.QUEUED
                    and not (
                        (rep_n := replicas.get(n.replica_id)) is not None
                        and any(
                            m.im_instance_status != Instance.QUEUED
                            for m in rep_n.members
                        )
                    )
                )
                and not (
                    n.im_instance_status == Instance.REQUESTED
                    and (node_type, n.launch_request_id or "") in torn_down_lids
                )
            )

        @staticmethod
        def _is_canceled_unallocated(node: SchedulingNode) -> bool:
            return (
                node.status == SchedulingNodeStatus.TO_TERMINATE
                and node.im_instance_status in (None, Instance.QUEUED)
            )

        @staticmethod
        def _is_doomed_td_requested(
            node: SchedulingNode,
            torn_down_lids: Optional[Set[Tuple[NodeType, str]]],
        ) -> bool:
            return (
                bool(torn_down_lids)
                and node.im_instance_status == Instance.REQUESTED
                and not node.provider_replica_id
                and (node.node_type, node.launch_request_id or "") in torn_down_lids
            )

        @staticmethod
        def _compute_available_node_types(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
            missing_backfill: Dict[NodeType, int],
            torn_down_lids: Optional[Set[Tuple[NodeType, str]]] = None,
        ) -> Dict[NodeType, int]:
            """Compute the number of nodes by node types available for launching based on
            the max number of workers in the config.
            Args:
                nodes: The current existing nodes.
                node_type_configs: The node type configs.
                missing_backfill: Precomputed missing replica backfill slots.
                torn_down_lids: Optional torn-down launch batches whose unallocated
                    REQUESTED rows are doomed.
            Returns:
                A dict of node types and the number of nodes available for launching.
            """
            node_type_available: Dict[NodeType, int] = defaultdict(int)
            node_type_existing: Dict[NodeType, int] = defaultdict(int)
            for node in nodes:
                if ResourceDemandScheduler.ScheduleContext._is_canceled_unallocated(
                    node
                ) or ResourceDemandScheduler.ScheduleContext._is_doomed_td_requested(
                    node, torn_down_lids
                ):
                    continue
                node_type_existing[node.node_type] += 1
            for nt, gap in missing_backfill.items():
                node_type_existing[nt] += gap

            for (
                node_type,
                node_type_config,
            ) in node_type_configs.items():
                node_type_available[
                    node_type
                ] = node_type_config.max_worker_nodes - node_type_existing.get(
                    node_type, 0
                )

            return node_type_available

        def get_nodes(self) -> List[SchedulingNode]:
            """Get the current nodes with filter.

            Returns:
                A list of nodes.
            """
            nodes = copy.deepcopy(self._nodes)
            return nodes

        def peek_nodes(self) -> List[SchedulingNode]:
            """Return a shallow list snapshot of the context's nodes without deepcopy."""
            return list(self._nodes)

        def get_pinned_domains(self) -> Dict[str, Set[str]]:
            return {k: set(v) for k, v in self._pinned_domains.items()}

        def set_pinned_domains(self, pinned_domains: Dict[str, Set[str]]) -> None:
            self._pinned_domains = {k: set(v) for k, v in pinned_domains.items()}

        def is_launch_config_check_disabled(self) -> bool:
            return self._disable_launch_config_check

        def get_node_type_available(self) -> Dict[NodeType, int]:
            return copy.deepcopy(self._node_type_available)

        def get_cluster_shape(self) -> Dict[NodeType, int]:
            cluster_shape = defaultdict(int)
            td_lids = self._replica_view.torn_down_lids
            for node in self._nodes:
                if node.status == SchedulingNodeStatus.TO_TERMINATE or (
                    self._is_doomed_td_requested(node, td_lids)
                ):
                    # Skip the nodes that are to be terminated or doomed REQUESTED rows.
                    continue

                cluster_shape[node.node_type] += 1
            return cluster_shape

        def get_cluster_resources(self) -> Dict[str, float]:
            """Aggregate total cluster resources.

            Sums each node's `total_resources` across the current context,
            excluding nodes marked `TO_TERMINATE`.

            Returns:
                A dict mapping resource names to their summed resources.
            """
            cluster_resources = defaultdict(float)
            for node in self._nodes:
                if (
                    node.status == SchedulingNodeStatus.TO_TERMINATE
                    or node._is_backfill_placeholder
                ):
                    # Skip nodes being terminated and in-memory backfill placeholders.
                    continue

                for key, value in node.total_resources.items():
                    cluster_resources[key] += value
            return cluster_resources

        def get_pending_idle_timeout_s(self) -> Optional[float]:
            return self._pending_idle_timeout_s

        def get_cloud_resource_availabilities(self) -> Dict[NodeType, float]:
            return copy.deepcopy(self._cloud_resource_availabilities)

        def get_recoverable_resource_availabilities(self) -> Dict[NodeType, float]:
            return copy.deepcopy(self._recoverable_resource_availabilities)

        def _pending_termination_deltas(
            self,
        ) -> Tuple[
            Set[int],
            Set[Tuple[NodeType, str]],
            Set[Tuple[NodeType, str]],
            Set[Tuple[NodeType, str]],
        ]:
            """Classify same-tick TO_TERMINATE nodes matching Reconciler transitions."""
            replica_sizes = self.get_multihost_replica_sizes()
            stopping_reps = set(self._replica_view.draining_reps)
            term_cids: Set[Tuple[NodeType, str]] = {
                (n.node_type, n.cloud_instance_id)
                for n in self._nodes
                if n.status == SchedulingNodeStatus.TO_TERMINATE
                and not n._is_backfill_placeholder
                and n.im_instance_status != Instance.QUEUED
                and n.cloud_instance_id
            }
            kept_nodes: List[Tuple[SchedulingNode, Optional[Tuple[NodeType, str]]]] = []
            for n in self._nodes:
                if (
                    n.status != SchedulingNodeStatus.TO_TERMINATE
                    or n._is_backfill_placeholder
                    or n.node_type not in replica_sizes
                ):
                    continue
                rk = (
                    (n.node_type, n.provider_replica_id)
                    if n.provider_replica_id
                    else None
                )
                is_deferred = (
                    rk is not None
                    and n.im_instance_status != Instance.QUEUED
                    and self._replica_view.should_defer_replica_termination(
                        n.node_type,
                        n.provider_replica_id,
                        replica_sizes[n.node_type],
                        term_cids,
                    )
                )
                kept_nodes.append((n, None if not is_deferred else rk))
                if (
                    not is_deferred
                    and rk is not None
                    and n.im_instance_status == Instance.RAY_RUNNING
                ):
                    stopping_reps.add(rk)
            deferred_alloc_ids: Set[int] = set()
            dropped_cids: Set[Tuple[NodeType, str]] = set()
            extra_td_lids: Set[Tuple[NodeType, str]] = set()
            extra_non_fillable_reps: Set[Tuple[NodeType, str]] = set()
            for n, deferred_rk in kept_nodes:
                st = n.im_instance_status
                rk = (
                    (n.node_type, n.provider_replica_id)
                    if n.provider_replica_id
                    else None
                )
                if st == Instance.RAY_RUNNING:
                    if deferred_rk is None and rk is not None:
                        extra_non_fillable_reps.add(rk)
                elif st in (Instance.ALLOCATED, Instance.RAY_INSTALLING):
                    if deferred_rk is not None or (
                        rk is not None and rk in stopping_reps
                    ):
                        deferred_alloc_ids.add(id(n))
                    else:
                        if n.cloud_instance_id:
                            dropped_cids.add((n.node_type, n.cloud_instance_id))
                        if n.launch_request_id:
                            extra_td_lids.add((n.node_type, n.launch_request_id))
                        if rk is not None:
                            extra_non_fillable_reps.add(rk)
            return (
                deferred_alloc_ids,
                dropped_cids,
                extra_td_lids,
                extra_non_fillable_reps,
            )

        def _refresh_capacity_stats(self) -> None:
            deltas = self._pending_termination_deltas()
            (
                deferred_alloc_ids,
                dropped_cids,
                extra_td_lids,
                extra_non_fillable_reps,
            ) = deltas
            (
                self._missing_replica_backfill,
                np_missing_by_type,
                self._unbacked_missing_slots,
            ) = self._missing_replica_backfill_and_nonprov_by_type(
                self._nodes,
                self._node_type_configs,
                self._replicas_by_type,
                self._extra_lid_backfill,
                self._replica_view.torn_down_lids | extra_td_lids,
                self._omitted_stopping_cnt,
                self._unlabeled_view_types,
                deferred_term_node_ids=deferred_alloc_ids,
            )
            vanished_ph_by_lid: Counter = Counter(
                (n.node_type, n.launch_request_id)
                for n in self._nodes
                if n.status != SchedulingNodeStatus.TO_TERMINATE
                and n._is_backfill_placeholder
                and n.launch_request_id
                and not n.provider_replica_id
            )
            unclaimed_van_by_nt: Dict[NodeType, Dict[str, int]] = {
                nt: self._replica_view.unclaimed_vanished_by_lid(
                    nt, extra_td_lids=extra_td_lids
                )
                for nt, cfg in self._node_type_configs.items()
                if cfg.is_multi_worker_replica
            }
            for nt, van_map in unclaimed_van_by_nt.items():
                extra_van = sum(
                    max(0, v_cnt - vanished_ph_by_lid[(nt, v_lid)])
                    for v_lid, v_cnt in van_map.items()
                )
                prov_omit = self._missing_prov_replica_slots(nt)
                if extra_van > 0 or prov_omit > 0:
                    self._missing_replica_backfill[nt] = (
                        self._missing_replica_backfill.get(nt, 0)
                        + extra_van
                        + prov_omit
                    )
            td_lids = self._replica_view.torn_down_lids | extra_td_lids
            self._node_type_available = self._compute_available_node_types(
                self._nodes,
                self._node_type_configs,
                missing_backfill=self._missing_replica_backfill,
                torn_down_lids=td_lids,
            )
            active_nodes = sum(
                1
                for n in self._nodes
                if not (
                    self._is_canceled_unallocated(n)
                    or self._is_doomed_td_requested(n, td_lids)
                )
            )
            self._launching_types = {
                n.node_type
                for n in self._nodes
                if n.status == SchedulingNodeStatus.TO_LAUNCH
            }
            self._headroom: Optional[int] = (
                self._max_num_nodes
                - active_nodes
                - sum(self._missing_replica_backfill.values())
                if self._max_num_nodes is not None
                else None
            )
            refill_gaps: Dict[NodeType, int] = {}
            for nt, cfg in self._node_type_configs.items():
                if not cfg.is_multi_worker_replica:
                    continue
                size = cfg.workers_per_replica
                extra_queued = self._queued_and_to_launch_count(nt)
                raw_gap = self.get_unlabeled_backfill_gap(
                    nt, size, extra_queued=extra_queued, deltas=deltas
                )
                pad_res = self._unbacked_missing_slots.get(nt, 0)
                net_raw = eff_raw = 0
                if nt in self._unlabeled_view_types:
                    non_prov_ph = sum(
                        1
                        for n in self._nodes
                        if n.node_type == nt
                        and n.status != SchedulingNodeStatus.TO_TERMINATE
                        and n._is_backfill_placeholder
                        and not n.launch_request_id
                        and not n.provider_replica_id
                    )
                    np_miss = np_missing_by_type.get(nt, 0)
                    net_raw = raw_gap - non_prov_ph - np_miss
                    eff_raw = raw_gap - non_prov_ph - max(0, np_miss - pad_res)
                mod_gap = max(0, net_raw)
                is_exclusive = cfg.is_exclusive_domain
                live_by_rid = self._live_nodes_by_rid(nt)
                rem_van_reserved: Counter = Counter(
                    {
                        v_lid: max(0, v_cnt - vanished_ph_by_lid[(nt, v_lid)])
                        for v_lid, v_cnt in unclaimed_van_by_nt.get(nt, {}).items()
                    }
                )
                rep_open_sum = 0
                for rep in sorted(
                    self._replicas_by_type.get(nt, {}).values(),
                    key=lambda r: r.rid,
                ):
                    if (
                        rep.is_provider
                        or not rep.no_backfill
                        or not rep.allow_refill_attach
                        or any(m.is_leaving for m in rep.members)
                    ):
                        continue
                    live_cnt = len(live_by_rid.get(rep.rid, ()))
                    if live_cnt <= 0:
                        continue
                    raw_open = _Replica._slots_for(live_cnt, size, not is_exclusive)[0]
                    van_reserved = 0
                    for lid in sorted(rep.lids):
                        take_vr = min(raw_open - van_reserved, rem_van_reserved[lid])
                        if take_vr > 0:
                            van_reserved += take_vr
                            rem_van_reserved[lid] -= take_vr
                    eff_live = live_cnt + van_reserved
                    rep_open_sum += _Replica._slots_for(
                        eff_live, size, not is_exclusive
                    )[0]
                nt_avail = self._node_type_available.get(nt, 0) + pad_res
                if not is_exclusive:
                    cap_open = (
                        mod_gap
                        if nt in self._unlabeled_view_types
                        else (
                            -self._nonprov_active_pod_count(
                                self._nodes,
                                self._replicas_by_type.get(nt, {}),
                                nt,
                                td_lids,
                            )
                        )
                        % size
                    )
                    rep_open_sum = min(rep_open_sum, cap_open)
                if self._headroom is not None:
                    nt_global = self._headroom + self.get_headroom_credit(
                        nt, includes_pad=True
                    )
                    nt_avail = min(nt_avail, max(0, nt_global))

                def _cap_refill(base_gap: int, mod_target: int) -> int:
                    if base_gap <= 0:
                        return 0
                    g = (
                        base_gap + (mod_target - base_gap) % size
                        if nt in self._unlabeled_view_types
                        else base_gap
                    )
                    if 0 <= nt_avail < g:
                        if nt in self._unlabeled_view_types:
                            rem_res = mod_target % size
                            g = (
                                rem_res + ((nt_avail - rem_res) // size) * size
                                if nt_avail >= rem_res
                                else 0
                            )
                        else:
                            g = nt_avail
                    if self._has_replica_view and nt not in self._unlabeled_view_types:
                        rem_res = self._replica_view.unlabeled_backfill_gap(
                            nt,
                            size,
                            extra_queued=extra_queued,
                            dropped_cids=dropped_cids,
                            extra_td_lids=extra_td_lids,
                        )
                        if rem_res > 0:
                            return (
                                rem_res + ((g - rem_res) // size) * size
                                if g >= rem_res
                                else 0
                            )
                        return g - (g % size)
                    return g

                total_gap = _cap_refill(max(mod_gap, rep_open_sum), eff_raw)
                if total_gap > 0:
                    refill_gaps[nt] = total_gap
            self._unbacked_refill_gaps = refill_gaps
            self._reconciler_nt_avail = {}
            self._reconciler_global_avail = None
            if self._has_replica_view and self.get_multihost_replica_sizes():
                reconciler_by_type, reconciler_total = self._reconciler_active_counts(
                    deltas=deltas
                )
                tl_by_type = Counter(
                    n.node_type
                    for n in self._nodes
                    if n.status == SchedulingNodeStatus.TO_LAUNCH
                )
                rem_gap_launched = {
                    nt: self.get_unlabeled_backfill_gap(
                        nt,
                        cfg.workers_per_replica,
                        extra_queued=self._queued_and_to_launch_count(nt),
                        deltas=deltas,
                    )
                    for nt, cfg in self._node_type_configs.items()
                    if nt in self._unlabeled_view_types and tl_by_type[nt] > 0
                }
                self._reconciler_nt_avail = {
                    nt: (
                        cfg.max_worker_nodes
                        - reconciler_by_type.get(nt, 0)
                        - tl_by_type[nt]
                        - rem_gap_launched.get(nt, 0)
                    )
                    for nt, cfg in self._node_type_configs.items()
                }
                for nt, r_avail in self._reconciler_nt_avail.items():
                    sub_pad = (
                        self._unbacked_missing_slots.get(nt, 0)
                        if (
                            nt in self._unlabeled_view_types
                            and nt not in self._launching_types
                        )
                        else 0
                    )
                    self._node_type_available[nt] = min(
                        self._node_type_available.get(nt, 0), r_avail - sub_pad
                    )
                if self._max_num_nodes is not None and self._headroom is not None:
                    self._reconciler_global_avail = (
                        self._max_num_nodes
                        - reconciler_total
                        - sum(tl_by_type.values())
                        - sum(rem_gap_launched.values())
                    )
                    unlab_unbacked = sum(
                        slots
                        for t, slots in self._unbacked_missing_slots.items()
                        if t in self._unlabeled_view_types
                        and t not in self._launching_types
                    )
                    self._headroom = min(
                        self._headroom,
                        self._reconciler_global_avail - unlab_unbacked,
                    )

        def _reconciler_active_counts(
            self,
            deltas: Optional[
                Tuple[
                    Set[int],
                    Set[Tuple[NodeType, str]],
                    Set[Tuple[NodeType, str]],
                    Set[Tuple[NodeType, str]],
                ]
            ] = None,
        ) -> Tuple[Dict[NodeType, int], int]:
            replica_sizes = self.get_multihost_replica_sizes()
            (
                deferred_alloc_ids,
                dropped_cids,
                extra_td_lids,
                extra_non_fillable_reps,
            ) = (
                deltas if deltas is not None else self._pending_termination_deltas()
            )
            td_lids = self._replica_view.torn_down_lids | extra_td_lids
            by_type: Dict[NodeType, int] = defaultdict(int)
            seen_cids: Set[Tuple[NodeType, str]] = set()
            total = 0
            for n in self._nodes:
                if (
                    n.status == SchedulingNodeStatus.TO_LAUNCH
                    or n._is_backfill_placeholder
                    or self._is_doomed_td_requested(n, td_lids)
                ):
                    continue
                if n.status == SchedulingNodeStatus.TO_TERMINATE:
                    st = n.im_instance_status
                    if n.node_type in replica_sizes:
                        counts = st in (
                            Instance.REQUESTED,
                            Instance.RAY_RUNNING,
                            Instance.RAY_STOP_REQUESTED,
                        ) or (
                            st in (Instance.ALLOCATED, Instance.RAY_INSTALLING)
                            and id(n) in deferred_alloc_ids
                        )
                    else:
                        counts = st in (Instance.REQUESTED, Instance.RAY_STOP_REQUESTED)
                    if not counts:
                        continue
                if n.node_type in replica_sizes and n.cloud_instance_id:
                    cid_key = (n.node_type, n.cloud_instance_id)
                    if cid_key in seen_cids:
                        continue
                    seen_cids.add(cid_key)
                total += 1
                if n.node_kind != NodeKind.HEAD:
                    by_type[n.node_type] += 1
            uncovered = self._replica_view.uncovered_replica_slots_by_type(
                replica_sizes,
                dropped_cids=dropped_cids,
                extra_td_lids=extra_td_lids,
                extra_non_fillable_reps=extra_non_fillable_reps,
            )
            for nt in replica_sizes:
                omit = self._reconciler_stopping_base.get(nt, 0) + uncovered.get(nt, 0)
                by_type[nt] += omit
                total += omit
            return by_type, total

        def trim_over_capacity_placeholders(self) -> None:
            """Drop unbacked non-provider placeholders that exceed max-worker limits."""
            deltas = self._pending_termination_deltas()
            reconciler_by_type, reconciler_total = self._reconciler_active_counts(
                deltas=deltas
            )
            tl_global = sum(
                1 for n in self._nodes if n.status == SchedulingNodeStatus.TO_LAUNCH
            )
            drop_ids: Set[int] = set()
            align_by_nt: Dict[NodeType, int] = {}
            for nt, cfg in self._node_type_configs.items():
                if not cfg.is_multi_worker_replica:
                    continue
                tl_nt = sum(
                    1
                    for n in self._nodes
                    if n.node_type == nt and n.status == SchedulingNodeStatus.TO_LAUNCH
                )
                align_nt = (
                    self.get_unlabeled_backfill_gap(
                        nt,
                        cfg.workers_per_replica,
                        extra_queued=self._queued_and_to_launch_count(nt),
                        deltas=deltas,
                    )
                    if tl_nt > 0 and nt in self._unlabeled_view_types
                    else 0
                )
                align_by_nt[nt] = align_nt
                real_nt = reconciler_by_type.get(nt, 0) + tl_nt
                ph_nt = [
                    n
                    for n in self._nodes
                    if n.node_type == nt
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                    and n._is_backfill_placeholder
                    and not n.provider_replica_id
                    and not n.launch_request_id
                ]
                excess_nt = min(
                    len(ph_nt),
                    max(0, real_nt + len(ph_nt) - cfg.max_worker_nodes),
                )
                if excess_nt > 0:
                    drop_ids.update(id(n) for n in ph_nt[-excess_nt:])
            if self._max_num_nodes is not None:
                real_global = reconciler_total + tl_global
                free_ph_global: List[SchedulingNode] = []
                committed_ph_or_align = 0
                for nt, cfg in self._node_type_configs.items():
                    if not cfg.is_multi_worker_replica:
                        continue
                    rem_ph = [
                        n
                        for n in self._nodes
                        if n.node_type == nt
                        and id(n) not in drop_ids
                        and n.status != SchedulingNodeStatus.TO_TERMINATE
                        and n._is_backfill_placeholder
                        and not n.provider_replica_id
                        and not n.launch_request_id
                    ]
                    align_nt = align_by_nt.get(nt, 0)
                    committed_ph_or_align += max(len(rem_ph), align_nt)
                    if len(rem_ph) > align_nt:
                        free_ph_global.extend(rem_ph[align_nt:])
                excess_global = min(
                    len(free_ph_global),
                    max(
                        0,
                        real_global + committed_ph_or_align - self._max_num_nodes,
                    ),
                )
                if excess_global > 0:
                    drop_ids.update(id(n) for n in free_ph_global[-excess_global:])
            if self._has_replica_view and self._unlabeled_view_types:
                for nt in self._unlabeled_view_types:
                    cfg = self._node_type_configs.get(nt)
                    if cfg is None or not cfg.is_multi_worker_replica:
                        continue
                    rem_ph = [
                        n
                        for n in self._nodes
                        if n.node_type == nt
                        and id(n) not in drop_ids
                        and n.status != SchedulingNodeStatus.TO_TERMINATE
                        and n._is_backfill_placeholder
                        and not n.provider_replica_id
                        and not n.launch_request_id
                    ]
                    if not rem_ph:
                        continue
                    tl_nt = sum(
                        1
                        for n in self._nodes
                        if n.node_type == nt
                        and n.status == SchedulingNodeStatus.TO_LAUNCH
                    )
                    gap_needed = self.get_unlabeled_backfill_gap(
                        nt,
                        cfg.workers_per_replica,
                        extra_queued=self._queued_and_to_launch_count(nt),
                        deltas=deltas,
                    )
                    room_nt = max(
                        0,
                        cfg.max_worker_nodes - reconciler_by_type.get(nt, 0) - tl_nt,
                    )
                    if self._max_num_nodes is not None:
                        room_nt = min(
                            room_nt,
                            max(
                                0,
                                self._max_num_nodes - reconciler_total - tl_global,
                            ),
                        )
                    if gap_needed > 0 and room_nt < gap_needed:
                        drop_ids.update(id(n) for n in rem_ph)
            if drop_ids:
                for rmap in self._replicas_by_type.values():
                    for rep in rmap.values():
                        rep.members = [m for m in rep.members if id(m) not in drop_ids]
                self.update([n for n in self._nodes if id(n) not in drop_ids])

        def materialize_unlabeled_launch_gaps(self) -> None:
            """Materialize remaining unlabeled view gaps whenever TO_LAUNCH nodes exist."""
            launch_order = list(
                dict.fromkeys(
                    n.node_type
                    for n in self._nodes
                    if n.status == SchedulingNodeStatus.TO_LAUNCH
                    and n.node_type in self._unlabeled_view_types
                )
            )
            if not launch_order:
                return
            deltas = self._pending_termination_deltas()
            changed = False
            for nt in launch_order:
                cfg = self._node_type_configs[nt]
                size = cfg.workers_per_replica
                queued_and_tl = self._queued_and_to_launch_count(nt)
                rem_gap = self.get_unlabeled_backfill_gap(
                    nt, size, extra_queued=queued_and_tl, deltas=deltas
                )
                if rem_gap > 0:
                    for n in self._nodes:
                        if rem_gap <= 0:
                            break
                        if (
                            n.node_type == nt
                            and n.status != SchedulingNodeStatus.TO_TERMINATE
                            and n._is_backfill_placeholder
                            and not n.launch_request_id
                            and not n.provider_replica_id
                        ):
                            n._is_backfill_placeholder = False
                            n.status = SchedulingNodeStatus.TO_LAUNCH
                            n._is_unlabeled_refill = True
                            rem_gap -= 1
                            changed = True
                    for _ in range(rem_gap):
                        node = self._make_launch_node(nt)
                        node._is_unlabeled_refill = True
                        _assign_node_replica_slot(
                            node,
                            cfg,
                            self.new_pending_domain(nt),
                            1 if size > 1 else 0,
                            keep_head=False,
                            suppressed_runtime_keys=self._suppressed_runtime_keys.get(
                                nt
                            ),
                        )
                        self._nodes.append(node)
                    changed = True
            if changed:
                self._refresh_capacity_stats()

        def rebuild_replicas(self, nodes: List[SchedulingNode]) -> None:
            """Reset grouping state on `nodes` and rebuild multi-worker replicas."""
            kept: List[SchedulingNode] = []
            for n in nodes:
                if n._is_backfill_placeholder:
                    continue
                cfg = self._node_type_configs.get(n.node_type)
                if cfg is not None and cfg.replica_config is not None:
                    n.replica_id = None
                    n._has_runtime_domain = False
                    n._is_draining_replica = False
                    if (
                        n.status != SchedulingNodeStatus.TO_TERMINATE
                        and not n.is_running
                        and n.im_instance_status != Instance.RAY_STOP_REQUESTED
                    ):
                        n.labels = dict(cfg.labels)
                kept.append(n)
            self._nodes = kept
            self._all_nodes = kept
            self._replicas_by_type = {}
            self._active_placeholder_keys = set()
            self._extra_lid_backfill = {}
            self._suppressed_runtime_keys = {}
            self._init_replicas()
            self.update(self._nodes)

        def update(self, new_nodes: List[SchedulingNode]) -> None:
            """Update the context with the new nodes."""
            self._nodes = new_nodes
            self._all_nodes = new_nodes
            self._prebuilt_refills = {}
            self._co_launch_overhead = {}
            self._refresh_capacity_stats()

        def with_ordered_nodes(
            self, nodes: List[SchedulingNode]
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Return a shallow copy of this context with reordered `nodes`."""
            ctx = copy.copy(self)
            ctx._nodes = nodes
            ctx._all_nodes = nodes
            return ctx

        def restricted_to(
            self,
            nodes: List[SchedulingNode],
            node_type_available: Optional[Dict[NodeType, int]] = None,
            refill_gaps: Optional[Dict[NodeType, int]] = None,
            prebuilt_refills: Optional[Dict[NodeType, List[SchedulingNode]]] = None,
            co_launch_overhead: Optional[Dict[NodeType, int]] = None,
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Return a shallow copy of this context restricted to `nodes`."""
            ctx = copy.copy(self)
            ctx._nodes = nodes
            ctx._all_nodes = self._all_nodes
            ctx._node_type_available = (
                dict(node_type_available) if node_type_available is not None else {}
            )
            if refill_gaps is not None:
                ctx._unbacked_refill_gaps = dict(refill_gaps)
            ctx._prebuilt_refills = {
                k: copy.deepcopy(v) for k, v in (prebuilt_refills or {}).items()
            }
            ctx._co_launch_overhead = dict(co_launch_overhead or {})
            return ctx

        def get_max_num_nodes(self) -> Optional[int]:
            """Get the max number of nodes for the entire cluster."""
            return self._max_num_nodes

        def get_node_type_configs(self) -> Dict[NodeType, NodeTypeConfig]:
            return self._node_type_configs

        def __str__(self) -> str:
            return "ScheduleContext({} nodes, node_type_available={})".format(
                len(self._nodes), dict(self._node_type_available)
            )

        def get_ippr_requests(self) -> List[IPPRStatus]:
            """Return IPPR actions to perform this iteration.

            Collects all nodes with an ``IPPRStatus`` that are ready to resize,
            i.e. have a raylet id, have a newly queued status, and a desired
            different from current resources.

            Returns:
                A list of ``IPPRStatus`` to send to the cloud provider for
                in-place pod resize.
            """
            return [
                node.ippr_status
                for node in self._nodes
                if node.ippr_status is not None
                and node.ippr_status.has_resize_request_to_send()
            ]

        def get_launch_requests(self) -> List[LaunchRequest]:
            """Get the launch requests for the nodes that are to be launched."""
            launch_by_type = defaultdict(int)
            for node in self._nodes:
                if node.status == SchedulingNodeStatus.TO_LAUNCH:
                    launch_by_type[node.node_type] += 1

            return [
                LaunchRequest(
                    instance_type=instance_type,
                    count=count,
                    id=str(uuid.uuid4()),
                    request_ts_ms=time.time_ns() // 1000,
                )
                for instance_type, count in launch_by_type.items()
            ]

        def get_terminate_requests(
            self,
            include_sub_replica_queued_canceled: bool = True,
        ) -> List[TerminationRequest]:
            """Get the terminate requests for the nodes that are to be terminated."""
            return [
                node.termination_request
                for node in self._nodes
                if node.termination_request is not None
                and (
                    include_sub_replica_queued_canceled
                    or not node._is_sub_replica_queued_canceled
                )
            ]

    def _build_context_with_placeholder_ttl(
        self, request: SchedulingRequest
    ) -> "ResourceDemandScheduler.ScheduleContext":
        now_s = time.monotonic()
        placeholder_ttl_s = (
            request.backfill_placeholder_ttl_s
            if request.backfill_placeholder_ttl_s is not None
            else float(InstanceReconcileConfig().request_status_timeout_s)
        )
        expired_placeholder_keys = {
            k
            for k, since_s in self._placeholder_since_s.items()
            if now_s - since_s >= placeholder_ttl_s
        }
        live_inst_keys: Set[Tuple[NodeType, str]] = set()
        for inst in request.current_instances:
            im = inst.im_instance
            if im is None or im.node_kind == NodeKind.HEAD or not im.instance_id:
                continue
            nt = im.instance_type
            ikey = (nt, im.instance_id)
            live_inst_keys.add(ikey)
            cfg = request.node_type_configs.get(nt)
            if cfg is not None and cfg.domain_label_key and inst.ray_node is not None:
                dom_val = (inst.ray_node.labels or {}).get(cfg.domain_label_key)
                if dom_val:
                    self._instance_domains[ikey] = dom_val
        if self._instance_domains:
            self._instance_domains = {
                k: v for k, v in self._instance_domains.items() if k in live_inst_keys
            }
        ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
            request,
            expired_placeholder_keys=expired_placeholder_keys,
            instance_domains=self._instance_domains,
        )
        self._placeholder_since_s = {
            k: self._placeholder_since_s.get(k, now_s)
            for k in ctx.get_active_placeholder_keys()
        }
        return ctx

    def schedule(self, request: SchedulingRequest) -> SchedulingReply:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Scheduling for request: resource_request={}, gang_resource_request={}, "
                "cluster_constraint={}".format(
                    ResourceRequestUtil.to_dict_list(request.resource_requests),
                    ProtobufUtil.to_dict_list(request.gang_resource_requests),
                    ProtobufUtil.to_dict_list(request.cluster_resource_constraints),
                )
            )

        ctx = self._build_context_with_placeholder_ttl(request)

        # Enforce outdate nodes.
        ResourceDemandScheduler._terminate_outdated_nodes(ctx)

        # Cancel unlaunchable sub-replica QUEUED remainders for multi-worker types.
        ResourceDemandScheduler._cancel_sub_replica_queued_remainders(ctx)

        # Reclaim per-type excess before min-worker launches so over-cap sibling
        # groups do not starve min-worker refill gaps under global headroom.
        ResourceDemandScheduler._enforce_max_workers_per_type(ctx)

        # Enforce the minimal count of nodes for each worker node type.
        ResourceDemandScheduler._enforce_min_workers_per_type(ctx)

        # Enforce the max worker nodes count per type and globally.
        ResourceDemandScheduler._enforce_max_workers_per_type(ctx)
        ResourceDemandScheduler._enforce_max_workers_global(ctx)

        # Enforce the cluster resource constraints.
        infeasible_constraints = ResourceDemandScheduler._enforce_resource_constraints(
            ctx, request.cluster_resource_constraints
        )

        # Schedule the gang resource requests.
        infeasible_gang_requests = (
            ResourceDemandScheduler._sched_gang_resource_requests(
                ctx, request.gang_resource_requests
            )
        )
        # Recompute pinned domains from only the unscheduled gang requests so
        # max-worker and idle termination protect domains still needed by them.
        ctx.set_pinned_domains(
            ResourceDemandScheduler._pinned_locality_domains(
                infeasible_gang_requests, ctx.get_node_type_configs()
            )
        )

        # Schedule the tasks/actor resource requests
        if request.resource_requests or any(
            n.ippr_status is not None and n.ippr_status.is_in_progress()
            for n in ctx.peek_nodes()
        ):
            infeasible_requests = ResourceDemandScheduler._sched_resource_requests(
                ctx,
                ResourceRequestUtil.ungroup_by_count(request.resource_requests),
            )
        else:
            infeasible_requests = []

        # Pending nodes not needed by the resource demands are considered idle.
        if ctx.get_pending_idle_timeout_s() is not None:
            self._update_pending_nodes_idle_duration(ctx)

        # Shutdown any idle nodes that's not needed (e.g. no resource constraints.
        # not needed by min_worker count, etc.)
        ResourceDemandScheduler._enforce_idle_termination(ctx)
        if ctx.get_pending_idle_timeout_s() is not None:
            self._release_terminated_pending_nodes_idle_duration(ctx)

        ctx.materialize_unlabeled_launch_gaps()
        cluster_resources = ctx.get_cluster_resources()

        # Compute the number of nodes to launch.
        reply = SchedulingReply(
            infeasible_resource_requests=infeasible_requests,
            infeasible_gang_resource_requests=infeasible_gang_requests,
            infeasible_cluster_resource_constraints=infeasible_constraints,
            to_launch=ctx.get_launch_requests(),
            to_ippr=ctx.get_ippr_requests(),
            to_terminate=ctx.get_terminate_requests(),
            cluster_resources=cluster_resources,
        )

        if self._event_logger is not None:
            try:
                self._event_logger.log_cluster_scheduling_update(
                    launch_requests=reply.to_launch,
                    terminate_requests=ctx.get_terminate_requests(
                        include_sub_replica_queued_canceled=False
                    ),
                    infeasible_requests=infeasible_requests,
                    infeasible_gang_requests=infeasible_gang_requests,
                    infeasible_cluster_resource_constraints=infeasible_constraints,
                    cluster_resources=cluster_resources,
                )
            except Exception:
                logger.exception("Failed to emit event logs.")

        return reply

    @staticmethod
    def _enforce_max_workers_per_type(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce the max number of workers for each node type."""
        all_nodes = ctx.peek_nodes()

        non_terminating_nodes_by_type = defaultdict(list)
        terminating_nodes = []
        for node in all_nodes:
            if node.status == SchedulingNodeStatus.TO_TERMINATE:
                terminating_nodes.append(node)
            else:
                non_terminating_nodes_by_type[node.node_type].append(node)

        replica_sizes = ctx.get_multihost_replica_sizes()
        deltas = (
            ctx._pending_termination_deltas()
            if replica_sizes
            else (set(), set(), set(), set())
        )
        reconciler_by_type = (
            ctx._reconciler_active_counts(deltas=deltas)[0] if replica_sizes else {}
        )
        for (
            node_type,
            non_terminate_nodes_of_type,
        ) in non_terminating_nodes_by_type.items():
            limit = ctx.get_node_type_configs()[node_type].max_worker_nodes
            (
                to_terminate,
                remained_nodes,
            ) = ResourceDemandScheduler._terminate_until_within_cap(
                ctx,
                non_terminate_nodes_of_type,
                limit,
                reconciler_by_type.get(node_type, 0),
                (node_type,) if ctx.has_unlabeled_view(node_type) else (),
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                max_num_nodes_per_type=limit,
                deltas=deltas,
            )
            non_terminating_nodes_by_type[node_type] = remained_nodes
            terminating_nodes.extend(to_terminate)

        non_terminating_nodes = []
        for nodes in non_terminating_nodes_by_type.values():
            non_terminating_nodes.extend(nodes)

        assert len(all_nodes) == (
            len(terminating_nodes) + len(non_terminating_nodes)
        ), "The number of nodes should be the same after enforcing max nodes per type."

        ctx.update(terminating_nodes + non_terminating_nodes)
        ctx.trim_over_capacity_placeholders()

        if terminating_nodes:
            logger.debug(
                f"Terminating {len(terminating_nodes)} "
                "nodes for per node type max num node's constraints."
            )

    @staticmethod
    def _enforce_max_workers_global(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce the max number of workers for the entire cluster."""
        all_nodes = ctx.peek_nodes()

        terminating_nodes = []
        non_terminating_nodes = []

        for node in all_nodes:
            if node.status == SchedulingNodeStatus.TO_TERMINATE:
                terminating_nodes.append(node)
            else:
                non_terminating_nodes.append(node)

        num_max_nodes = ctx.get_max_num_nodes()
        if not num_max_nodes:
            ctx.trim_over_capacity_placeholders()
            return

        replica_sizes = ctx.get_multihost_replica_sizes()
        deltas = (
            ctx._pending_termination_deltas()
            if replica_sizes
            else (set(), set(), set(), set())
        )
        reconciler_global = (
            ctx._reconciler_active_counts(deltas=deltas)[1] if replica_sizes else 0
        )
        (
            to_terminate_nodes,
            non_terminating_nodes,
        ) = ResourceDemandScheduler._terminate_until_within_cap(
            ctx,
            non_terminating_nodes,
            num_max_nodes,
            reconciler_global,
            ctx.get_unlabeled_view_types(),
            TerminationRequest.Cause.MAX_NUM_NODES,
            max_num_nodes=num_max_nodes,
            deltas=deltas,
        )

        terminating_nodes.extend(to_terminate_nodes)
        assert len(all_nodes) == (
            len(terminating_nodes) + len(non_terminating_nodes)
        ), "The number of nodes should be the same after enforcing max nodes."

        ctx.update(terminating_nodes + non_terminating_nodes)
        ctx.trim_over_capacity_placeholders()

    @staticmethod
    def _terminate_until_within_cap(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        nodes: List[SchedulingNode],
        limit: int,
        reconciler_base: int,
        unlabeled_types: Iterable[NodeType],
        cause: TerminationRequest.Cause,
        *,
        max_num_nodes: Optional[int] = None,
        max_num_nodes_per_type: Optional[int] = None,
        deltas: Optional[
            Tuple[
                Set[int],
                Set[Tuple[NodeType, str]],
                Set[Tuple[NodeType, str]],
                Set[Tuple[NodeType, str]],
            ]
        ] = None,
    ) -> Tuple[List[SchedulingNode], List[SchedulingNode]]:
        """Terminate ordered units from `nodes` until base real and aligned
        reconciler occupancy both satisfy `limit`, never selecting a head node.
        """
        assert cause in [
            TerminationRequest.Cause.MAX_NUM_NODES,
            TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
        ], "Other termination causes don't have to select nodes for termination."

        replica_sizes = ctx.get_multihost_replica_sizes()
        if deltas is None and replica_sizes:
            deltas = ctx._pending_termination_deltas()
        extra_td_lids = deltas[2] if deltas is not None else set()
        td_lids = ctx.get_replica_view().torn_down_lids | extra_td_lids
        nt_configs = ctx.get_node_type_configs()
        unlabeled_tuple = tuple(unlabeled_types)

        base_real = sum(
            1
            for n in nodes
            if not n._is_backfill_placeholder
            and not ctx._is_doomed_td_requested(n, td_lids)
        )
        tl_by_nt: Counter = Counter(
            n.node_type for n in nodes if n.status == SchedulingNodeStatus.TO_LAUNCH
        )
        q_by_nt: Counter = Counter(
            n.node_type
            for n in nodes
            if n.node_kind != NodeKind.HEAD
            and (
                n.status == SchedulingNodeStatus.TO_LAUNCH
                or n.im_instance_status == Instance.QUEUED
            )
        )
        cancelable_rem = sum(q_by_nt.values())
        reconciler_rem = reconciler_base + sum(tl_by_nt.values())

        def _over_cap() -> bool:
            if base_real > limit:
                return True
            if not replica_sizes or cancelable_rem <= 0:
                return False
            align = sum(
                ctx.get_unlabeled_backfill_gap(
                    nt,
                    nt_configs[nt].workers_per_replica,
                    extra_queued=q_by_nt[nt],
                    deltas=deltas,
                )
                for nt in unlabeled_tuple
                if tl_by_nt[nt] > 0
            )
            return reconciler_rem + align > limit

        if not _over_cap():
            return [], nodes

        nodes = [copy.deepcopy(n) for n in nodes]
        head_node = None
        worker_nodes = list(nodes)
        for i, node in enumerate(worker_nodes):
            if node.node_kind == NodeKind.HEAD:
                head_node = worker_nodes.pop(i)
                break

        units = ResourceDemandScheduler._termination_units(
            worker_nodes,
            replica_sizes or {},
            nt_configs or {},
            ctx.get_pinned_domains() or {},
            ctx.get_replicas_by_type(),
        )
        terminated_nodes: List[SchedulingNode] = []
        remained_nodes: List[SchedulingNode] = []
        for unit in units:
            if _over_cap():
                terminated_nodes.extend(unit)
                for n in unit:
                    if (
                        not n._is_backfill_placeholder
                        and not ctx._is_doomed_td_requested(n, td_lids)
                    ):
                        base_real -= 1
                    if n.status == SchedulingNodeStatus.TO_LAUNCH:
                        tl_by_nt[n.node_type] -= 1
                        q_by_nt[n.node_type] -= 1
                        cancelable_rem -= 1
                        reconciler_rem -= 1
                    elif (
                        n.node_kind != NodeKind.HEAD
                        and n.im_instance_status == Instance.QUEUED
                    ):
                        q_by_nt[n.node_type] -= 1
                        cancelable_rem -= 1
                        reconciler_rem -= 1
            else:
                remained_nodes.extend(unit)
        if head_node:
            remained_nodes.append(head_node)

        assert not _over_cap() or all(
            n.node_kind == NodeKind.HEAD for n in remained_nodes
        ), (
            f"Terminated {len(terminated_nodes)} nodes, failed to satisfy "
            f"{TerminationRequest.Cause.Name(cause)}={limit}"
        )

        for node in terminated_nodes:
            was_to_launch = node.status == SchedulingNodeStatus.TO_LAUNCH
            node.status = SchedulingNodeStatus.TO_TERMINATE
            if was_to_launch or node._is_backfill_placeholder:
                # The launch or placeholder is only cancelled: there is no instance to
                # terminate, and a request without an instance id would fail the
                # instance manager update.
                continue
            node.termination_request = TerminationRequest(
                id=str(uuid.uuid4()),
                instance_id=node.im_instance_id,
                ray_node_id=node.ray_node_id,
                cause=cause,
                instance_type=node.node_type,
                instance_status=node.im_instance_status,
                details=(
                    f"Terminating node due to {TerminationRequest.Cause.Name(cause)}: "
                    f"max_num_nodes={max_num_nodes}, "
                    f"max_num_nodes_per_type={max_num_nodes_per_type}"
                ),
            )
            if cause == TerminationRequest.Cause.MAX_NUM_NODES:
                node.termination_request.max_num_nodes = max_num_nodes
            elif cause == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE:
                node.termination_request.max_num_nodes_per_type = max_num_nodes_per_type

        return terminated_nodes, remained_nodes

    @staticmethod
    def _sort_nodes_for_termination(node: SchedulingNode) -> Tuple:
        """Sort the nodes for termination increasingly by:

            1. First if ray hasn't been started yet
            2. Then if the nodes are idle
            3. Then with lower resources util nodes first.

        Such that nodes sorted earlier will be terminated first.
        """

        running_ray = len(node.ray_node_id) > 0
        # Reverse the idle duration such that the nodes with the largest idle duration
        # will be terminated first.
        idle_dur = -1 * node.idle_duration_ms
        available_resources = node.get_available_resources(
            ResourceRequestSource.PENDING_DEMAND
        )

        utils_per_resources = {}
        for resource, total in node.total_resources.items():
            if total <= 0:
                continue
            utils_per_resources[resource] = (
                total - available_resources.get(resource, 0)
            ) / total

        avg_util = (
            sum(utils_per_resources.values()) / len(utils_per_resources)
            if utils_per_resources
            else 0
        )

        return (running_ray, idle_dur, avg_util)

    @staticmethod
    def _enforce_min_workers_per_type(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce the minimal count of nodes for each worker node type."""

        # Count existing nodes by type (including missing host slots on live
        # incomplete replicas recreated by the cluster manager).
        count_by_node_type = ctx.get_cluster_shape()
        missing_backfill = ctx.get_missing_replica_backfill_slots()
        if not any(
            (
                count_by_node_type.get(nt, 0)
                + missing_backfill.get(nt, 0)
                + ctx.get_unattributed_open_slots(nt)
            )
            < cfg.min_worker_nodes
            for nt, cfg in ctx.get_node_type_configs().items()
        ):
            return
        existing_nodes = ctx.get_nodes()

        new_nodes = []
        # Launch new nodes to satisfy min count for each worker node type.
        for (
            node_type,
            node_type_config,
        ) in ctx.get_node_type_configs().items():
            unattr_open = ctx.get_unattributed_open_slots(node_type)
            cur_count = (
                count_by_node_type.get(node_type, 0)
                + missing_backfill.get(node_type, 0)
                + unattr_open
            )
            min_count = node_type_config.min_worker_nodes
            if cur_count < min_count:
                deficit = min_count - cur_count
                logger.info(
                    f"Adding {deficit} nodes to satisfy min count for "
                    f"node type: {node_type}."
                )
                size = node_type_config.workers_per_replica
                has_unlab = ctx.has_unlabeled_view(node_type)
                if has_unlab:
                    deficit += unattr_open
                    refill_gap = ctx.get_unbacked_refill_gap(node_type)
                else:
                    refill_gap = max(
                        0, ctx.get_unbacked_refill_gap(node_type) - unattr_open
                    )
                if refill_gap > 0:
                    aligned_def = (
                        deficit + ((refill_gap - deficit) % size)
                        if has_unlab
                        else deficit
                    )
                    take_refill = min(refill_gap, aligned_def)
                    new_nodes.extend(
                        ctx.make_refill_nodes(
                            node_type, take_refill, include_ippr_caps=False
                        )
                    )
                    deficit = max(0, deficit - take_refill)
                for _ in range((deficit + size - 1) // size):
                    new_nodes.extend(
                        ctx.make_replica_nodes(node_type, include_ippr_caps=False)
                    )
        # NOTE: we assume the aggregated number of min workers across all node types
        # should not exceed any globally enforced max_num_nodes

        # Add the new nodes to the existing nodes and update the context.
        ctx.update(new_nodes + existing_nodes)

    @staticmethod
    def _enforce_resource_constraints(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        constraints: List[ClusterResourceConstraint],
    ) -> List[ClusterResourceConstraint]:
        """Enforce the cluster resource constraints.

        Args:
            ctx: The schedule context.
            constraints: The cluster resource constraints.

        Returns:
            A list of infeasible constraints.

        Notes:
            It's different from the other scheduling functions since it doesn't actually
        schedule any resource requests. Instead, it asks if the cluster could be
        upscale to a certain shape to fulfill the constraints.
        """

        # NOTE: we currently only have 1 constraint from a cluster, but
        # we may have multiple in the future.
        assert len(constraints) <= 1, "Max 1 cluster resource constraint is supported."
        if len(constraints) == 0:
            # No cluster resource constraints - nothing needs to be done.
            return []

        constraint = constraints[0]
        # Flatten the requests for iterating through.
        requests = ResourceRequestUtil.ungroup_by_count(constraint.resource_requests)

        # Pass the empty nodes to schedule.
        scheduled_nodes, infeasible = ResourceDemandScheduler._try_schedule(
            ctx,
            requests,
            resource_request_source=ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT,
        )

        if infeasible:
            # Unable to satisfy the constraint.
            return [constraint]

        ctx.update(scheduled_nodes)
        return []

    @staticmethod
    def _sched_resource_requests(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        requests: List[ResourceRequest],
    ) -> List[ResourceRequest]:
        """Schedule the resource requests.

        Args:
            ctx: The schedule context.
            requests: The resource requests.

        Returns:
            A list of infeasible resource requests.
        """
        nodes, infeasible = ResourceDemandScheduler._try_schedule(
            ctx, requests, resource_request_source=ResourceRequestSource.PENDING_DEMAND
        )

        # Regardless if there's feasible, we will update the context for schedule nodes.
        ctx.update(nodes)

        return infeasible

    @staticmethod
    def _sched_gang_resource_requests(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        gang_requests: List[GangResourceRequest],
    ) -> List[GangResourceRequest]:
        """Schedule the gang resource requests.

        These requests should be scheduled atomically, i.e. either all of the resources
        requests in a gang request are scheduled or none of them are scheduled.

        For now, the gang resource requests represent Ray's placement groups, while it
        could be more general in the future:
        - For STRICT_PACK placement group requests, we combine them into a single
            request and try to schedule them together.
        - For STRICT_SPREAD placement groups requests, they should be scheduled on
            different nodes by leveraging on the node labels that are associated with
            the placement group.
            If there are requests from rescheduling placement groups due to node
            failures, these requests should not be scheduled on nodes with requests
            from the same placement group.
        - For placement groups with a `topology_strategy`, all bundles are
            scheduled within one value of the topology label.


        Args:
            ctx: The schedule context.
            gang_requests: The gang resource requests.

        Returns:
            A list of infeasible gang resource requests.
        """

        def _sort_gang_resource_requests(req: GangResourceRequest) -> Tuple:
            """Return the sort key for a gang resource request.

            The key sorts the gang resource request by:
                1. the number of label constraints on the locality requirement
                   (set when a rescheduling gang is pinned to its domain).
                2. the number of placement and locality constraints in the gang
                   request, counting a locality requirement once per bundle.
                3. the number of resource requests in the gang request.
            """
            requests, locality_req = ResourceDemandScheduler._unpack_gang_request(req)
            total_placement_constraints = sum(
                len(r.placement_constraints) for r in requests
            )
            locality_label_constraints = 0
            if locality_req is not None:
                total_placement_constraints += len(requests)
                locality_label_constraints = len(
                    locality_req.label_selector.label_constraints
                )
                loc_key = locality_req.locality_constraint.label_name
                if locality_label_constraints == 0 and loc_key:
                    agg = ResourceDemandScheduler._selector_key_filter(
                        locality_req.label_selector, loc_key
                    )
                    for r in requests:
                        agg = ResourceDemandScheduler._intersect_label_filter(
                            agg,
                            ResourceDemandScheduler._request_label_filter(r, loc_key),
                        )
                    if agg[0] is not None:
                        locality_label_constraints = 1

            return (
                locality_label_constraints,
                total_placement_constraints,
                len(requests),
            )

        def _active_label_keys() -> Set[str]:
            keys = {k for n in ctx.peek_nodes() for k in n.labels}
            for cfg in ctx.get_node_type_configs().values():
                keys.update(cfg.labels.keys())
                if cfg.domain_label_key:
                    keys.add(cfg.domain_label_key)
                if cfg.worker_index_label_key:
                    keys.add(cfg.worker_index_label_key)
                keys.update(cfg.runtime_label_keys)
            return keys

        def _gang_dedup_key(
            req: GangResourceRequest, active_keys: Set[str]
        ) -> Optional[Tuple]:
            if not req.bundle_selectors:
                return None
            bs = req.bundle_selectors[0]
            loc_label = (
                bs.locality_requirement.locality_constraint.label_name
                if bs.HasField("locality_requirement")
                and bs.locality_requirement.locality_constraint.label_name
                else None
            )
            fresh_slots: Dict[str, int] = {}
            for r in bs.resource_requests:
                for pc in r.placement_constraints:
                    if pc.HasField("anti_affinity"):
                        key = pc.anti_affinity.label_name
                    elif pc.HasField("affinity"):
                        key = pc.affinity.label_name
                    else:
                        continue
                    if (
                        key not in active_keys
                        and key != loc_label
                        and key not in fresh_slots
                    ):
                        fresh_slots[key] = len(fresh_slots)

            def _norm_sel(sel: LabelSelector) -> Tuple:
                return tuple(
                    (
                        ("f", fresh_slots[c.label_key])
                        if c.label_key in fresh_slots
                        else ("k", c.label_key),
                        c.operator,
                        tuple(c.label_values),
                    )
                    for c in sel.label_constraints
                )

            req_keys = []
            for r in bs.resource_requests:
                pcs = []
                for pc in r.placement_constraints:
                    if pc.HasField("anti_affinity"):
                        aa = pc.anti_affinity
                        k_id = (
                            ("f", fresh_slots[aa.label_name])
                            if aa.label_name in fresh_slots
                            else ("k", aa.label_name)
                        )
                        pcs.append(("anti", k_id, aa.label_value))
                    elif pc.HasField("affinity"):
                        af = pc.affinity
                        k_id = (
                            ("f", fresh_slots[af.label_name])
                            if af.label_name in fresh_slots
                            else ("k", af.label_name)
                        )
                        pcs.append(("aff", k_id, af.label_value))
                    else:
                        pcs.append(("raw", pc.SerializeToString(deterministic=True)))
                req_keys.append(
                    (
                        tuple(sorted(r.resources_bundle.items())),
                        tuple(pcs),
                        tuple(_norm_sel(sel) for sel in r.label_selectors),
                    )
                )
            loc_key: Tuple = ()
            if bs.HasField("locality_requirement"):
                loc = bs.locality_requirement
                loc_key = (
                    loc.locality_constraint.SerializeToString(deterministic=True),
                    _norm_sel(loc.label_selector)
                    if loc.HasField("label_selector")
                    else (),
                )
            return (tuple(req_keys), loc_key)

        infeasible_gang_requests = []
        failed_gang_keys: Set[Tuple] = set()
        known_label_keys = _active_label_keys()
        # Try fulfilling the gang requests one by one.
        for gang_req in sorted(
            gang_requests, key=_sort_gang_resource_requests, reverse=True
        ):
            gang_key = _gang_dedup_key(gang_req, known_label_keys)
            if gang_key is not None and gang_key in failed_gang_keys:
                infeasible_gang_requests.append(gang_req)
                continue

            nodes, infeasible = ResourceDemandScheduler._try_schedule_gang(
                ctx, gang_req
            )

            if infeasible:
                # Unable to satisfy the constraint. We will skip the gang request.
                # Don't update the context.
                if gang_key is not None:
                    failed_gang_keys.add(gang_key)
                infeasible_gang_requests.append(gang_req)
                continue

            # We are able to satisfy the constraint and thus update the context.
            ctx.update(nodes)
            failed_gang_keys.clear()
            known_label_keys = _active_label_keys()

        return infeasible_gang_requests

    @staticmethod
    def _selector_key_filter(sel, key: str) -> Tuple[Optional[Set[str]], Set[str]]:
        """Return `(allowed, forbidden)` for `key` under a single label selector.

        Returns `(set(), set())` when `sel` is globally unsatisfiable on any key
        or contains an unsupported operator; `(F, set())` when `key` is restricted
        to finite set `F`; or `(None, N)` when `key` is co-finite (`U \\ N`).
        """
        in_by_key: Dict[str, List[Set[str]]] = defaultdict(list)
        not_in_by_key: Dict[str, Set[str]] = defaultdict(set)
        for c in sel.label_constraints:
            if c.operator == _LABEL_OPERATOR_IN:
                in_by_key[c.label_key].append(set(c.label_values))
            elif c.operator == _LABEL_OPERATOR_NOT_IN:
                not_in_by_key[c.label_key].update(c.label_values)
            else:
                return set(), set()

        for lk, in_sets in in_by_key.items():
            if not (set.intersection(*in_sets) - not_in_by_key.get(lk, set())):
                return set(), set()

        if key in in_by_key:
            return (
                set.intersection(*in_by_key[key]) - not_in_by_key.get(key, set()),
                set(),
            )
        return None, set(not_in_by_key.get(key, ()))

    @staticmethod
    def _intersect_label_filter(
        a: Tuple[Optional[Set[str]], Set[str]],
        b: Tuple[Optional[Set[str]], Set[str]],
    ) -> Tuple[Optional[Set[str]], Set[str]]:
        """Intersect two `(allowed, forbidden)` label filters."""
        a_allowed, a_forb = a
        b_allowed, b_forb = b
        if a_allowed is None and b_allowed is None:
            return None, a_forb | b_forb
        if a_allowed is None:
            return b_allowed - a_forb, set()
        if b_allowed is None:
            return a_allowed - b_forb, set()
        return (a_allowed & b_allowed) - a_forb - b_forb, set()

    @staticmethod
    def _request_label_filter(
        req: ResourceRequest, key: str
    ) -> Tuple[Optional[Set[str]], Set[str]]:
        """Return `(allowed, forbidden)` for `key` across all satisfiable fallback
        tiers of `req`."""
        if not req.label_selectors:
            return None, set()
        finite_sets: List[Set[str]] = []
        cofinite_forb: List[Set[str]] = []
        for sel in req.label_selectors:
            allowed, forb = ResourceDemandScheduler._selector_key_filter(sel, key)
            if allowed is not None:
                if not allowed:
                    continue
                finite_sets.append(allowed)
            else:
                cofinite_forb.append(forb)
        if not finite_sets and not cofinite_forb:
            return set(), set()
        finite_union = set().union(*finite_sets) if finite_sets else set()
        if not cofinite_forb:
            return finite_union, set()
        return None, set.intersection(*cofinite_forb) - finite_union

    @staticmethod
    def _max_bundles_per_node(requests: List[ResourceRequest]) -> int:
        """Return an upper bound on how many of `requests` can share one node."""
        if not requests:
            return 0
        anti_counts = Counter(
            tag
            for r in requests
            for tag in {
                (c.anti_affinity.label_name, c.anti_affinity.label_value)
                for c in r.placement_constraints
                if c.HasField("anti_affinity")
            }
        )
        candidate_keys = {
            c.label_key
            for r in requests
            for sel in r.label_selectors
            for c in sel.label_constraints
            if c.operator == _LABEL_OPERATOR_IN
        }
        cap = len(requests) - max(anti_counts.values(), default=1) + 1
        for key in candidate_keys:
            unpinned = 0
            by_val: Counter = Counter()
            for r in requests:
                allowed, _ = ResourceDemandScheduler._request_label_filter(r, key)
                if allowed is None:
                    unpinned += 1
                else:
                    by_val.update(allowed)
            cap = min(cap, unpinned + max(by_val.values(), default=0))
        return cap

    @staticmethod
    def _has_domain_spare_capacity(
        domain_nodes: List[SchedulingNode],
        requests: List[ResourceRequest],
        max_bundles_per_node: int,
    ) -> bool:
        """Return whether at least one node in `domain_nodes` can fit at least
        one request in `requests` using its remaining capacity."""
        if max_bundles_per_node <= 0:
            return False
        for node in domain_nodes:
            avail = node.get_ippr_effective_available(
                ResourceRequestSource.PENDING_DEMAND
            )
            for req in requests:
                if node._first_matching_selector(req) is None:
                    continue
                if _fits(avail, dict(req.resources_bundle)):
                    return True
        return False

    @staticmethod
    def _add_label_constraints_to_requests(
        requests: List[ResourceRequest],
        constraints: List[LabelSelectorConstraint],
    ) -> List[ResourceRequest]:
        """Return copies of `requests` with `constraints` added to each request's
        label selectors."""
        updated = []
        for req in requests:
            rc = copy.deepcopy(req)
            if rc.label_selectors:
                for sel in rc.label_selectors:
                    sel.label_constraints.extend(constraints)
            else:
                rc.label_selectors.add(label_constraints=constraints)
            updated.append(rc)
        return updated

    @staticmethod
    def _node_label_score(node: SchedulingNode) -> float:
        return math.fsum(
            (len(r.label_selectors) - ms[0]) / len(r.label_selectors)
            for r in node.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            if len(r.label_selectors) > 1
            and (ms := node._first_matching_selector(r)) is not None
        )

    @staticmethod
    def _unpack_gang_request(
        req: GangResourceRequest,
    ) -> Tuple[List[ResourceRequest], Optional[LocalityRequirement]]:
        """Extract bundle requests and any active STRICT_PACK locality requirement."""
        loc: Optional[LocalityRequirement] = None
        if req.bundle_selectors:
            bs = req.bundle_selectors[0]
            if bs.HasField("locality_requirement"):
                cand = bs.locality_requirement
                if (
                    cand.locality_constraint.placement_strategy
                    == PlacementStrategy.STRICT_PACK
                    and cand.locality_constraint.label_name
                ):
                    loc = cand
        return ResourceRequestUtil.get_gang_requests(req), loc

    @staticmethod
    def _pinned_locality_domains(
        gang_requests: List[GangResourceRequest],
        node_type_configs: Optional[Dict[NodeType, NodeTypeConfig]] = None,
    ) -> Dict[str, Set[str]]:
        pinned_by_key: Dict[str, Set[str]] = defaultdict(set)
        rep_keys = {
            cfg.domain_label_key
            for cfg in (node_type_configs or {}).values()
            if cfg.domain_label_key
        }
        for req in gang_requests:
            reqs, loc = ResourceDemandScheduler._unpack_gang_request(req)
            if not reqs:
                continue
            loc_key = loc.locality_constraint.label_name if loc is not None else None

            gang_pins: Dict[str, Set[str]] = {}
            satisfiable = True
            if loc is not None and loc_key is not None:
                agg = ResourceDemandScheduler._selector_key_filter(
                    loc.label_selector, loc_key
                )
                for r in reqs:
                    agg = ResourceDemandScheduler._intersect_label_filter(
                        agg,
                        ResourceDemandScheduler._request_label_filter(r, loc_key),
                    )
                if agg[0] is not None and not agg[0]:
                    satisfiable = False
                elif agg[0]:
                    gang_pins[loc_key] = set(agg[0])

            if not satisfiable:
                continue

            for key in rep_keys - ({loc_key} if loc_key is not None else set()):
                loc_f = (
                    ResourceDemandScheduler._selector_key_filter(
                        loc.label_selector, key
                    )
                    if loc is not None
                    else (None, set())
                )
                if loc_f[0] is not None and not loc_f[0]:
                    satisfiable = False
                    break
                key_pins: Set[str] = set()
                for r in reqs:
                    rf = ResourceDemandScheduler._intersect_label_filter(
                        loc_f,
                        ResourceDemandScheduler._request_label_filter(r, key),
                    )
                    if rf[0] is not None and not rf[0]:
                        satisfiable = False
                        break
                    if rf[0]:
                        key_pins.update(rf[0])
                if not satisfiable:
                    break
                if key_pins:
                    gang_pins[key] = key_pins

            if satisfiable:
                for key, vals in gang_pins.items():
                    pinned_by_key[key].update(vals)
        return {k: v for k, v in pinned_by_key.items() if v}

    @staticmethod
    def _pending_domain_signature(
        d_nodes: List[SchedulingNode],
        locality_label: str,
    ) -> Tuple:
        """Build a deduplication signature for identical candidate domains."""
        node_sigs = []
        for n in d_nodes:
            sched_reqs = n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            ippr = n.ippr_status
            if ippr is not None:
                in_progress = ippr.is_in_progress()
                can_up = ippr.can_resize_up()
                ippr_sig = (
                    (
                        in_progress,
                        can_up,
                        float(ippr.desired_cpu) if in_progress else None,
                        float(ippr.desired_memory) if in_progress else None,
                        float(ippr.max_cpu()) if can_up else None,
                        float(ippr.max_memory()) if can_up else None,
                    )
                    if (in_progress or can_up)
                    else None
                )
            else:
                ippr_sig = None
            node_sigs.append(
                (
                    n.node_type,
                    n.node_kind,
                    n.status,
                    n.is_running,
                    n._is_backfill_placeholder,
                    tuple(sorted(n.total_resources.items())),
                    tuple(
                        sorted(
                            n.available_resources_for_sched[
                                ResourceRequestSource.PENDING_DEMAND
                            ].items()
                        )
                    ),
                    tuple(
                        sorted(
                            (k, v) for k, v in n.labels.items() if k != locality_label
                        )
                    ),
                    ippr_sig,
                    bool(sched_reqs),
                    any("GPU" in r.resources_bundle for r in sched_reqs),
                    tuple(
                        sorted(
                            {
                                k
                                for r in sched_reqs
                                for k, v in r.resources_bundle.items()
                                if v > 0
                            }
                        )
                    ),
                    tuple(
                        r.SerializeToString(deterministic=True)
                        for r in sched_reqs
                        if len(r.label_selectors) > 1
                    ),
                )
            )
        return tuple(node_sigs)

    @staticmethod
    def _domain_fits_resource_totals(
        pool: List[SchedulingNode],
        req_resource_totals: Dict[str, float],
        resource_shapes: Optional[List[Dict[str, float]]] = None,
        min_nodes_needed: int = 0,
    ) -> bool:
        """Fast aggregate resource and schedulable host-count check before full scheduling."""
        if min_nodes_needed > 0 and len(pool) < min_nodes_needed:
            return False
        if req_resource_totals:

            def _node_max_available(node: SchedulingNode, res_name: str) -> float:
                base = node.available_resources_for_sched[
                    ResourceRequestSource.PENDING_DEMAND
                ].get(
                    res_name,
                    1.0 if res_name.startswith(IMPLICIT_RESOURCE_PREFIX) else 0.0,
                )
                if res_name not in ("CPU", "memory"):
                    return base
                bounds = node.ippr_capacity_bounds()
                if bounds is None:
                    return base
                target_tot = bounds[2] if res_name == "CPU" else bounds[3]
                return max(
                    0.0,
                    base + (target_tot - node.total_resources.get(res_name, 0.0)),
                )

            if any(
                sum(_node_max_available(n, k) for n in pool) + _RESOURCE_EPSILON
                < needed
                for k, needed in req_resource_totals.items()
            ):
                return False
        if min_nodes_needed > 0 and resource_shapes:
            fit_nodes = 0
            for n in pool:
                if _can_fit_any_request(
                    n.get_ippr_effective_available(
                        ResourceRequestSource.PENDING_DEMAND
                    ),
                    resource_shapes,
                ):
                    fit_nodes += 1
                    if fit_nodes >= min_nodes_needed:
                        return True
            return False
        return True

    @staticmethod
    def _try_schedule_gang(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        gang_req: GangResourceRequest,
    ) -> Tuple[List[SchedulingNode], List[ResourceRequest]]:
        """Schedule all bundles of `gang_req` atomically, placing `STRICT_PACK`
        locality gangs within a single value of the locality label."""
        raw_requests, locality_req = ResourceDemandScheduler._unpack_gang_request(
            gang_req
        )
        requests = ResourceRequestUtil.combine_requests_with_affinity(raw_requests)
        if not requests or locality_req is None:
            return ResourceDemandScheduler._try_schedule(
                ctx, requests, ResourceRequestSource.PENDING_DEMAND
            )

        locality_label = locality_req.locality_constraint.label_name
        dom_filter = ResourceDemandScheduler._selector_key_filter(
            locality_req.label_selector, locality_label
        )
        for r in requests:
            dom_filter = ResourceDemandScheduler._intersect_label_filter(
                dom_filter,
                ResourceDemandScheduler._request_label_filter(r, locality_label),
            )
        pinned_opt, excluded_domains = dom_filter
        pinned = pinned_opt is not None
        pinned_domains = pinned_opt or set()
        if locality_req.label_selector.label_constraints:
            requests = ResourceDemandScheduler._add_label_constraints_to_requests(
                requests, locality_req.label_selector.label_constraints
            )

        # Read-only view; candidate trials deepcopy only the nodes they mutate.
        all_nodes = ctx.peek_nodes()
        if pinned and not pinned_domains:
            return all_nodes, requests
        node_type_configs = ctx.get_node_type_configs()
        shapes = _collect_unique_resource_shapes(requests)
        max_bundles_per_node = ResourceDemandScheduler._max_bundles_per_node(requests)

        domains: Dict[str, List[SchedulingNode]] = defaultdict(list)
        for node in all_nodes:
            domain_val = node.labels.get(locality_label)
            if (
                domain_val
                and node.status != SchedulingNodeStatus.TO_TERMINATE
                and not node.is_leaving
            ):
                domains[domain_val].append(node)
        static_domains = {
            cfg.labels[locality_label]
            for cfg in node_type_configs.values()
            if cfg.labels.get(locality_label)
        }
        exclusive_domain_types = [
            nt
            for nt, cfg in node_type_configs.items()
            if cfg.domain_label_key == locality_label
            and cfg.domain_exclusive
            and not cfg.labels.get(locality_label)
        ]
        exclusive_domain_type_set = set(exclusive_domain_types)
        declaring_types = {
            nt
            for nt, cfg in node_type_configs.items()
            if cfg.labels.get(locality_label)
            or cfg.domain_label_key == locality_label
            or locality_label in cfg.runtime_label_keys
        }
        step5_types = [
            nt
            for nt, cfg in node_type_configs.items()
            if not cfg.labels.get(locality_label)
            and nt not in exclusive_domain_type_set
            and (not declaring_types or nt in declaring_types)
            and locality_label not in ctx.get_suppressed_runtime_keys(nt)
        ]

        candidate_domains = (domains.keys() | static_domains) - excluded_domains
        if pinned:
            candidate_domains &= pinned_domains
        gang_ctx = (
            ctx.with_ordered_nodes(sorted(all_nodes, key=lambda n: not n.is_running))
            if candidate_domains & static_domains
            else ctx
        )
        node_type_available = gang_ctx.get_node_type_available()
        headroom = gang_ctx.get_headroom()

        active_pending_domains = {
            dom_val
            for dom_val, d_nodes in domains.items()
            if dom_val.startswith(_PENDING_DOMAIN_PREFIX)
            and any(
                m.is_running
                or m.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
                for m in d_nodes
            )
        }
        active_replicas = {
            (n.node_type, n.replica_id)
            for n in all_nodes
            if n.replica_id is not None
            and n.status != SchedulingNodeStatus.TO_TERMINATE
            and (
                (
                    n.is_running
                    and (
                        bool(n.labels.get(locality_label))
                        or bool(n.provider_replica_id)
                    )
                )
                or n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            )
        }
        unlabeled_pending_by_type: Dict[NodeType, List[SchedulingNode]] = defaultdict(
            list
        )
        for n in all_nodes:
            if (
                n.node_type in exclusive_domain_type_set
                or n.status == SchedulingNodeStatus.TO_TERMINATE
                or n.is_running
                or n.is_leaving
                or n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            ):
                continue
            dom_val = n.labels.get(locality_label)
            rep_active = (
                n.replica_id is not None
                and (n.node_type, n.replica_id) in active_replicas
            )
            if not dom_val:
                unlabeled_pending_by_type[n.node_type].append(n)
            elif (
                dom_val.startswith(_PENDING_DOMAIN_PREFIX)
                and dom_val not in active_pending_domains
                and not rep_active
            ):
                unlabeled_pending_by_type[n.node_type].append(n)

        search = _GangSearch(
            gang_ctx=gang_ctx,
            all_nodes=all_nodes,
            requests=requests,
            locality_label=locality_label,
            pinned=pinned,
            max_bundles_per_node=max_bundles_per_node,
            max_label_score=sum(1.0 for r in requests if len(r.label_selectors) > 1),
            domains=domains,
            static_domains=static_domains,
            exclusive_domain_type_set=exclusive_domain_type_set,
            step5_type_set=set(step5_types),
            unlabeled_pending_by_type=unlabeled_pending_by_type,
            node_type_available=node_type_available,
            node_type_configs=node_type_configs,
            availabilities=gang_ctx.get_cloud_resource_availabilities(),
            recoverable=gang_ctx.get_recoverable_resource_availabilities(),
            headroom=headroom,
        )

        existing_domains = sorted(candidate_domains, key=search.domain_rank)
        launch_candidates: List[
            Tuple[str, List[SchedulingNode], List[SchedulingNode]]
        ] = []
        for nt in sorted(
            exclusive_domain_types,
            key=search.node_type_rank,
            reverse=True,
        ):
            if locality_label in gang_ctx.get_suppressed_runtime_keys(nt):
                continue
            cfg = node_type_configs[nt]
            size = cfg.workers_per_replica
            refill_gap = gang_ctx.get_unbacked_refill_gap(nt)
            launch_res = gang_ctx.get_launch_resources(nt)
            if not _can_fit_any_request(launch_res, shapes):
                continue
            own_pad = gang_ctx.get_own_pad_credit(nt)
            if (
                refill_gap > 0
                and node_type_available.get(nt, 0) + own_pad >= refill_gap
                and (
                    headroom is None
                    or headroom + gang_ctx.get_headroom_credit(nt, includes_pad=True)
                    >= refill_gap
                )
            ):
                refill_by_dom: Dict[str, List[SchedulingNode]] = defaultdict(list)
                for rn in gang_ctx.make_refill_nodes(nt, refill_gap):
                    rn_dom = rn.labels.get(locality_label)
                    if rn_dom:
                        refill_by_dom[rn_dom].append(rn)
                nt_reps = gang_ctx.get_replicas_by_type().get(nt, {})
                for dom_val in existing_domains:
                    dom_nodes = domains.get(dom_val, [])
                    dom_nt_cnt = sum(1 for m in dom_nodes if m.node_type == nt)
                    rep_obj = nt_reps.get(dom_val)
                    dom_refills = refill_by_dom.get(dom_val, [])
                    if (
                        dom_refills
                        and dom_val not in static_domains
                        and (
                            not dom_val.startswith(_PENDING_DOMAIN_PREFIX)
                            or (rep_obj is not None and rep_obj.allow_refill_attach)
                        )
                        and 0 < dom_nt_cnt < size
                    ):
                        launch_candidates.append(
                            (dom_val, dom_nodes + dom_refills, dom_nodes)
                        )
            if not pinned:
                need_fresh = size + refill_gap
                if node_type_available.get(nt, 0) + (
                    own_pad if refill_gap > 0 else 0
                ) >= need_fresh and (
                    headroom is None
                    or headroom
                    + gang_ctx.get_headroom_credit(nt, includes_pad=refill_gap > 0)
                    >= need_fresh
                ):
                    rep_nodes = gang_ctx.make_replica_nodes(nt)
                    launch_candidates.append((rep_nodes[0].replica_id, rep_nodes, []))

        if search.evaluate_domains(existing_domains, launch_candidates):
            return search.best[1], []

        if not pinned:
            for nt in sorted(step5_types, key=search.node_type_rank, reverse=True):
                search.schedule_unlabeled_for_node_type(nt)
            if search.best is None:
                search.evaluate_shared_domains_with_launches(existing_domains)

        return (
            (search.best[1], []) if search.best is not None else (all_nodes, requests)
        )

    @staticmethod
    def _try_schedule(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        requests_to_sched: List[ResourceRequest],
        resource_request_source: ResourceRequestSource,
    ) -> Tuple[List[SchedulingNode], List[ResourceRequest]]:
        """Try to schedule the resource requests on the current context.

        It tries to schedule the requests on the existing nodes first, and
        then try to schedule the requests on new nodes if possible.

        Args:
            ctx: The current scheduling context.
            requests_to_sched: The resource requests to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource
                constraints.

        Returns:
            - List of scheduled nodes to that have part or all of the requests
                scheduled.
            - List of infeasible requests remained that cannot be scheduled.
        """
        # First sort the requests.
        def _sort_resource_request(req: ResourceRequest) -> Tuple:
            """Sort the resource requests by:
                1. The length of its placement constraints.
                2. The length of its first label selector constraints (0 if empty).
                3. Fallback flexibility: requests without an empty fallback first,
                   then fewer reachable constrained selectors first (strict before
                   multi-selector fallback).
                4. The number of resources it requests.
                5. The values of resources it requests.
                6. lexicographically for each resource (for stable ordering)

            This is a legacy sorting function for the autoscaler's binpacking
            algo - we do this so that we could have a deterministic scheduling
            results with reasonable fragmentation.
            """
            num_constrained_selectors = 0
            has_empty_fallback = not req.label_selectors
            for selector in req.label_selectors:
                if not selector.label_constraints:
                    has_empty_fallback = True
                    break
                num_constrained_selectors += 1
            first_constraint_len = (
                len(req.label_selectors[0].label_constraints)
                if req.label_selectors
                else 0
            )
            return (
                len(req.placement_constraints),
                first_constraint_len,
                not has_empty_fallback,
                -num_constrained_selectors,
                len(req.resources_bundle.values()),
                sum(req.resources_bundle.values()),
                sorted(req.resources_bundle.items()),
            )

        requests_to_sched = sorted(
            requests_to_sched, key=_sort_resource_request, reverse=True
        )

        # Precompute serialization keys to avoid redundant SerializeToString
        # calls inside the per-node try_schedule loop.
        shape_keys = {}
        for r in requests_to_sched:
            # Skip re-serializing the same object, e.g. from [r.request] * r.count.
            if id(r) not in shape_keys:
                shape_keys[id(r)] = r.SerializeToString(deterministic=True)

        # Precompute unique resource shapes from all requests for quick
        # feasibility pre-checks (AND within each shape, OR across shapes).
        resource_shapes = _collect_unique_resource_shapes(requests_to_sched)

        existing_nodes = ctx.get_nodes()
        node_type_available = ctx.get_node_type_available()

        # A list of nodes that are either:
        #   1. existing nodes in the cluster. or
        #   2. new nodes that are launched to satisfy the resource requests.
        target_nodes = []

        for node in existing_nodes:
            if node.ippr_status is not None:
                if node.ippr_status.is_in_progress():
                    # While a resize is ongoing or just completed, use desired values
                    # as the node's capacity so binpacking can consider the change.
                    node.update_total_resources(
                        {
                            "CPU": node.ippr_status.desired_cpu,
                            "memory": node.ippr_status.desired_memory,
                        }
                    )

        # Pre-filter: skip TO_TERMINATE, draining multi-worker replicas, and
        # RAY_RUNNING nodes that definitely cannot fit any request, avoiding
        # expensive deepcopy + try_schedule in _sched_best_node.
        exhausted_nodes = []
        schedulable_nodes = []
        for node in existing_nodes:
            if (
                node.status == SchedulingNodeStatus.TO_TERMINATE
                or node._is_draining_replica
                or (
                    node.im_instance_status == Instance.RAY_RUNNING
                    and not _can_fit_any_request(
                        node.get_available_resources(resource_request_source),
                        resource_shapes,
                    )
                )
            ):
                exhausted_nodes.append(node)
            else:
                schedulable_nodes.append(node)
        existing_nodes = schedulable_nodes

        def _can_host_more(node: SchedulingNode) -> bool:
            if not requests_to_sched:
                return False
            avail = node.get_available_resources(resource_request_source)
            return _can_fit_any_request(avail, resource_shapes) and any(
                node._schedulable_selector(r, avail) is not None
                for r in {shape_keys[id(r)]: r for r in requests_to_sched}.values()
            )

        # Try scheduling resource requests with existing nodes first.
        replica_sizes = ctx.get_multihost_replica_sizes()
        committed_replicas = {
            (n.node_type, n.replica_id)
            for n in existing_nodes
            if n.replica_id is not None
            and n.node_type in replica_sizes
            and n.get_sched_requests(resource_request_source)
        }
        attempts: Dict[int, _NodeAttempt] = {}
        while len(requests_to_sched) > 0 and len(existing_nodes) > 0:
            has_running_existing = any(n.is_running for n in existing_nodes)
            preferred_node_ids = (
                {
                    id(n)
                    for n in existing_nodes
                    if n.replica_id is not None
                    and (n.node_type, n.replica_id) in committed_replicas
                    and (n.is_running or not has_running_existing)
                }
                if committed_replicas
                else None
            )
            (
                best_node,
                requests_to_sched,
                existing_nodes,
            ) = ResourceDemandScheduler._sched_best_node(
                requests_to_sched,
                existing_nodes,
                resource_request_source,
                ctx.get_cloud_resource_availabilities(),
                ctx.get_recoverable_resource_availabilities(),
                shape_keys,
                attempts,
                preferred_node_ids=preferred_node_ids,
            )
            if best_node is None:
                # No existing nodes can schedule any more requests.
                break
            if (
                best_node.replica_id is not None
                and best_node.node_type in replica_sizes
            ):
                committed_replicas.add((best_node.node_type, best_node.replica_id))

            # Keep a node that may still host deferred fallback or unconstrained work.
            if _can_host_more(best_node):
                existing_nodes.append(best_node)
            else:
                target_nodes.append(best_node)

        # If there's any existing nodes left, we will add to the target nodes
        target_nodes.extend(existing_nodes)
        target_nodes.extend(exhausted_nodes)

        # Try scheduling remaining requests with IPPR after filling up existing
        # nodes with their current capacity.
        existing_nodes = target_nodes
        target_nodes = []
        unresized_ippr = []

        for node in existing_nodes:
            if (
                node.status != SchedulingNodeStatus.TO_TERMINATE
                and not node._is_draining_replica
                and node.ippr_status is not None
                and node.ippr_status.can_resize_up()
            ):
                unresized_ippr.append(node)
            else:
                target_nodes.append(node)

        original_ippr_candidates = {
            node.ippr_status.cloud_instance_id: copy.deepcopy(node)
            for node in unresized_ippr
        }
        for node in unresized_ippr:
            # Expose per-node maximums so binpacking can evaluate placing more work
            # by upsizing in-place rather than launching new nodes.
            node.update_total_resources(
                {
                    "CPU": node.ippr_status.max_cpu(),
                    "memory": node.ippr_status.max_memory(),
                }
            )

        resized_ippr: List[SchedulingNode] = []
        attempts = {}
        while len(requests_to_sched) > 0 and (resized_ippr or unresized_ippr):
            num_committed = len(resized_ippr)
            (
                best_node,
                requests_to_sched,
                remaining_ippr,
            ) = ResourceDemandScheduler._sched_best_node(
                requests_to_sched,
                resized_ippr + unresized_ippr,
                resource_request_source,
                ctx.get_cloud_resource_availabilities(),
                ctx.get_recoverable_resource_availabilities(),
                shape_keys,
                attempts,
                num_committed=num_committed,
            )
            if best_node is None:
                # No ippr nodes can schedule any more requests.
                break

            picked_resized = num_committed > 0 and (
                not unresized_ippr
                or remaining_ippr[num_committed - 1] is unresized_ippr[0]
            )
            split = num_committed - 1 if picked_resized else num_committed
            resized_ippr = remaining_ippr[:split]
            unresized_ippr = remaining_ippr[split:]
            if not picked_resized:
                # Commit an IPPR action on the selected node to its max effective caps.
                best_node.ippr_status.queue_resize_request(
                    desired_cpu=best_node.ippr_status.max_cpu(),
                    desired_memory=best_node.ippr_status.max_memory(),
                )
            if _can_host_more(best_node):
                resized_ippr.append(best_node)
            else:
                target_nodes.append(best_node)

        target_nodes.extend(resized_ippr)
        # Keep unselected IPPR candidates at their original resources because no
        # resize request was issued for them in this scheduling pass.
        target_nodes.extend(
            original_ippr_candidates[node.ippr_status.cloud_instance_id]
            for node in unresized_ippr
        )

        if not requests_to_sched or not any(
            v
            + (
                ctx.get_own_pad_credit(nt)
                if (
                    ctx.get_unbacked_refill_gap(nt) > 0
                    or ctx.get_co_launch_overhead(nt) > 0
                )
                else 0
            )
            > 0
            for nt, v in node_type_available.items()
        ):
            return target_nodes, requests_to_sched

        replica_sizes = ctx.get_multihost_replica_sizes()
        node_type_configs = ctx.get_node_type_configs()
        head_resource_types = {
            nt for nt, cfg in node_type_configs.items() if cfg.head_resources
        }
        preferred_replica_types = set(replica_sizes) | head_resource_types
        headroom = ctx.get_headroom()
        pending_refills: Dict[NodeType, int] = {
            nt: ctx.get_unbacked_refill_gap(nt)
            for nt in replica_sizes
            if ctx.get_unbacked_refill_gap(nt) > 0
        }
        co_overhead: Dict[NodeType, int] = {
            nt: ctx.get_co_launch_overhead(nt)
            for nt in replica_sizes
            if ctx.get_co_launch_overhead(nt) > 0
        }
        promoted_refills: Set[NodeType] = set()
        task_launched_types: Set[NodeType] = set()

        def _own_pad(nt: NodeType) -> int:
            return (
                ctx.get_own_pad_credit(nt, task_launched_types)
                if (nt in pending_refills or nt in co_overhead)
                else 0
            )

        def _headroom_credit(nt: NodeType) -> int:
            return ctx.get_headroom_credit(
                nt,
                includes_pad=(nt in pending_refills or nt in co_overhead),
                extra_launched_types=task_launched_types,
            )

        def _batch_size(nt: NodeType) -> int:
            overhead = co_overhead.get(nt, 0)
            if nt in pending_refills:
                gap = pending_refills[nt]
                return overhead + (
                    gap + replica_sizes.get(nt, 1) if nt in promoted_refills else gap
                )
            return overhead + replica_sizes.get(nt, 1)

        def _make_batch(nt: NodeType) -> List[SchedulingNode]:
            if nt in pending_refills:
                return ctx.make_refill_nodes(nt, pending_refills[nt])
            return ctx.make_replica_nodes(nt)

        def _can_launch(nt: NodeType, cur_launched: int) -> bool:
            need = _batch_size(nt)
            return node_type_available.get(nt, 0) + _own_pad(nt) >= need and (
                headroom is None
                or cur_launched + need <= headroom + _headroom_credit(nt)
            )

        candidate_replicas: Dict[NodeType, List[SchedulingNode]] = {
            nt: _make_batch(nt) for nt in node_type_available if _can_launch(nt, 0)
        }
        launched_nodes: List[SchedulingNode] = []
        launched_count = 0
        attempts = {}

        while len(requests_to_sched) > 0:
            if pending_refills:
                unique_rem = list(
                    {shape_keys[id(r)]: r for r in requests_to_sched}.values()
                )
                for nt in list(pending_refills):
                    if nt in promoted_refills:
                        continue
                    rep = candidate_replicas.get(nt)
                    if rep is None or not _can_launch(nt, launched_count):
                        continue
                    if any(
                        n._schedulable_selector(
                            r, n.get_available_resources(resource_request_source)
                        )
                        is not None
                        for n in rep
                        for r in unique_rem
                    ):
                        continue
                    full_need = (
                        co_overhead.get(nt, 0)
                        + pending_refills[nt]
                        + replica_sizes.get(nt, 1)
                    )
                    if node_type_available.get(nt, 0) + _own_pad(nt) >= full_need and (
                        headroom is None
                        or launched_count + full_need <= headroom + _headroom_credit(nt)
                    ):
                        full_rep = ctx.make_replica_nodes(nt)
                        if any(
                            n._schedulable_selector(
                                r, n.get_available_resources(resource_request_source)
                            )
                            is not None
                            for n in full_rep
                            for r in unique_rem
                        ):
                            promoted_refills.add(nt)
                            candidate_replicas[nt] = rep + full_rep
                            attempts.clear()

            fresh_nodes = [
                n
                for nt, rep in candidate_replicas.items()
                if _can_launch(nt, launched_count)
                for n in rep
            ]
            num_committed = len(launched_nodes)
            preferred_node_ids = {
                id(n) for n in launched_nodes if n.node_type in preferred_replica_types
            }
            (
                best_node,
                requests_to_sched,
                remaining,
            ) = ResourceDemandScheduler._sched_best_node(
                requests_to_sched,
                launched_nodes + fresh_nodes,
                resource_request_source,
                ctx.get_cloud_resource_availabilities(),
                ctx.get_recoverable_resource_availabilities(),
                shape_keys,
                attempts,
                num_committed=num_committed,
                preferred_node_ids=preferred_node_ids,
            )
            if best_node is None:
                break

            fresh_ids = {id(n) for n in fresh_nodes}
            launched_nodes = [n for n in remaining if id(n) not in fresh_ids]
            rem_fresh = {id(n) for n in remaining if id(n) in fresh_ids}
            if len(rem_fresh) < len(fresh_nodes):
                picked = next(n for n in fresh_nodes if id(n) not in rem_fresh)
                nt = picked.node_type
                for sib in candidate_replicas.pop(nt):
                    if sib is not picked:
                        if _can_host_more(sib):
                            launched_nodes.append(sib)
                        else:
                            target_nodes.append(sib)
                used_size = _batch_size(nt)
                own_credit = _own_pad(nt)
                was_unpromoted_refill = (
                    nt in pending_refills and nt not in promoted_refills
                )
                pending_refills.pop(nt, None)
                promoted_refills.discard(nt)
                co_overhead.pop(nt, None)
                task_launched_types.add(nt)
                launched_count += used_size - own_credit
                node_type_available[nt] -= used_size - own_credit
                if _can_launch(nt, launched_count):
                    candidate_replicas[nt] = _make_batch(nt)
                    if was_unpromoted_refill:
                        attempts.clear()

            if _can_host_more(best_node):
                launched_nodes.append(best_node)
            else:
                target_nodes.append(best_node)

        target_nodes.extend(launched_nodes)

        return target_nodes, requests_to_sched

    @staticmethod
    def _sched_best_node(
        requests: List[ResourceRequest],
        nodes: List[SchedulingNode],
        resource_request_source: ResourceRequestSource,
        cloud_resource_availabilities: Dict[NodeType, float],
        recoverable_resource_availabilities: Dict[NodeType, float],
        shape_keys: Optional[Dict[int, bytes]] = None,
        attempts: Optional[Dict[int, _NodeAttempt]] = None,
        num_committed: int = 0,
        preferred_node_ids: Optional[Set[int]] = None,
    ) -> Tuple[Optional[SchedulingNode], List[ResourceRequest], List[SchedulingNode]]:
        """Schedule the requests on the best node.
        A simple greedy algorithm is used to schedule the requests:
            1. Try to schedule the requests on each node.
            2. Sort the nodes by a multi-level score:
                2.1. UtilizationScore: to maximize resource utilization.
                2.2. Recoverable Availability: prioritize node types that have
                never failed or have recovered from failures.
                2.3. Priority: prioritize node types with higher user-defined
                priority.
                2.4. Cloud resource availabilities: prioritize node types with
                the most available cloud resources, in order to minimize allocation
                failures.
            3. Return the node with the highest score.

        The highest score node is updated with the scheduled requests, and the node is
        removed from the node list.

        Args:
            requests: The resource requests to be scheduled.
            nodes: The node candidates to be scheduled on. The nodes will be updated
                after the scheduling attempt, i.e. the node that is scheduled will be
                removed from the list.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.
            cloud_resource_availabilities: The cloud resource availability score. A low
                score indicates that allocation for this node type has recently failed.
            recoverable_resource_availabilities: The recoverable cloud resource
                availability score. Similar to cloud_resource_availabilities, but it
                recovers from 0.0 to 1.0 linearly over
                RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S seconds.
            shape_keys: Precomputed serialization keys for each request. Avoids
                redundant SerializeToString calls inside try_schedule. If None,
                keys are computed on the fly (fallback for tests and external callers).
            attempts: Scheduling attempts by node id, shared across calls within a
                phase where `active_selectors[k]` never decreases (candidates only
                lose capacity and no candidate offering a lower selector index is
                appended). An attempt is reused while the node's constrained and
                unconstrained active-selector request streams and any fallback
                blockers stay valid. When any label selector reads an anti-affinity
                label or `attempts` is None, every candidate node is simulated.
            num_committed: Prefix length of `nodes` representing already-committed
                capacity (e.g., already-launched nodes or already-resized IPPR pods)
                preferred over uncommitted candidates at equal `score[0]`.
            preferred_node_ids: Optional set of Python object ids for candidate
                nodes belonging to already-committed multi-host replicas that
                should be preferred over launching a new replica of the same
                label-selector tier.

        Returns:
            best_node: The best node to schedule the requests.
            infeasible: The infeasible requests that cannot be scheduled on the best
                node.
            nodes: Remaining nodes after the best node is removed.
        """

        keys = [_shape_key(r, shape_keys) for r in requests]
        runs = _coalesce_runs((k, 1) for k in keys)
        runs_by_key: Dict[bytes, List[Tuple[int, int]]] = defaultdict(list)
        for pos, (k, c) in enumerate(runs):
            runs_by_key[k].append((pos, c))
        requests_by_key = dict(zip(keys, requests))
        results = []

        node_cache = NodeStateCache(resource_request_source)
        candidates = [
            (idx, node)
            for idx, node in enumerate(nodes)
            if not node_cache.was_seen_or_mark(
                node,
                is_preferred=bool(
                    preferred_node_ids and id(node) in preferred_node_ids
                ),
                is_committed=idx < num_committed,
            )
        ]

        dynamic_overlap = _has_dynamic_selector_overlap(requests_by_key.values())
        use_attempt_cache = attempts is not None and not dynamic_overlap
        cache: Dict[int, _NodeAttempt] = attempts if attempts is not None else {}

        for _, node in candidates:
            if id(node) not in cache:
                avail = node.get_available_resources(resource_request_source)
                matched = {
                    k: ms
                    for k, r in requests_by_key.items()
                    if (ms := node._schedulable_selector(r, avail)) is not None
                }
                cache[id(node)] = _NodeAttempt.from_matched(matched)

        active_selectors: Dict[bytes, int] = {}
        for _, n in candidates:
            for k, (idx, _) in cache[id(n)].matched.items():
                if k in requests_by_key and idx < active_selectors.get(k, idx + 1):
                    active_selectors[k] = idx
        if dynamic_overlap:
            for k, r in requests_by_key.items():
                if k not in active_selectors:
                    active_selectors[k] = len(r.label_selectors)

        stream_cache: Dict[
            Tuple[Tuple[bytes, int], ...], Tuple[Tuple[bytes, int], ...]
        ] = {(): ()}

        def stream_for(
            sub_matched: Tuple[Tuple[bytes, int], ...],
        ) -> Tuple[Tuple[bytes, int], ...]:
            cached = stream_cache.get(sub_matched)
            if cached is not None:
                return cached
            hits = [
                (pos, (k, c))
                for k, m_idx in sub_matched
                if active_selectors.get(k) == m_idx
                for pos, c in runs_by_key.get(k, ())
            ]
            hits.sort()
            out = _coalesce_runs(kc for _, kc in hits)
            stream_cache[sub_matched] = out
            return out

        # Prune score[0] == 0 candidates early when a score[0] == 1 candidate exists.
        pass_has_constrained = use_attempt_cache and any(
            cache[id(n)].has_constrained(active_selectors) for _, n in candidates
        )

        for idx, node in candidates:
            result = cache[id(node)]
            if use_attempt_cache:
                if pass_has_constrained and not result.has_constrained(
                    active_selectors
                ):
                    continue
                if result.matches(stream_for, active_selectors):
                    if result.node is not None:
                        results.append((idx, result.node, result))
                    continue

            node_requests = (
                [r for r, k in zip(requests, keys) if k in result.matched]
                if use_attempt_cache
                else requests
            )
            if not node_requests:
                if use_attempt_cache:
                    cache[id(node)] = _NodeAttempt(
                        result.matched, result.c_matched, result.u_matched
                    )
                continue

            node_copy = copy.deepcopy(node)
            num_sched = len(node_copy.get_sched_requests(resource_request_source))
            _, score = node_copy.try_schedule(
                node_requests,
                resource_request_source,
                shape_keys,
                active_selectors,
                result.matched,
            )
            new_sched = node_copy.get_sched_requests(resource_request_source)[
                num_sched:
            ]
            if not new_sched:
                continue

            if use_attempt_cache:
                attempt = result.with_schedule(
                    active_selectors,
                    node_copy,
                    new_sched,
                    score,
                    stream_for,
                    shape_keys,
                    requests_by_key,
                    resource_request_source,
                )
                cache[id(node)] = attempt
            else:
                attempt = _NodeAttempt(
                    matched=result.matched,
                    node=node_copy,
                    scheduled_requests=new_sched,
                    score=score,
                )
            results.append((idx, node_copy, attempt))

        # No nodes can schedule any of the requests.
        if len(results) == 0:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "No nodes can schedule the requests: {}, for nodes: {}".format(
                        ResourceRequestUtil.to_dict_list(requests), nodes
                    )
                )
            return None, requests, nodes

        # Pick the result with the best score. Like a stable sort, max keeps the
        # first of equally scored results.
        best_idx, best_node, best_result = max(
            results,
            key=lambda item: (
                item[2].score[0],
                item[0] < num_committed,
                item[2].score[1],
                not item[1]._is_backfill_placeholder,
                bool(preferred_node_ids and id(nodes[item[0]]) in preferred_node_ids),
                item[2].score[2:],
                recoverable_resource_availabilities.get(item[1].node_type, 1.0),
                item[1].priority,
                cloud_resource_availabilities.get(item[1].node_type, 1.0),
            ),
        )
        # Remove the best node from the nodes.
        cache.pop(id(nodes.pop(best_idx)), None)

        # Keep the requests that the best node did not schedule, in order.
        to_drop = Counter(
            _shape_key(r, shape_keys) for r in best_result.scheduled_requests
        )
        infeasible = []
        for r, k in zip(requests, keys):
            if to_drop[k] > 0:
                to_drop[k] -= 1
            else:
                infeasible.append(r)
        assert not +to_drop, "cached attempt scheduled requests that are gone"

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Best node: {}, score: {}, remaining requests: {}".format(
                    best_node,
                    best_result.score,
                    ResourceRequestUtil.to_dict_list(infeasible),
                )
            )
        return best_node, infeasible, nodes

    @staticmethod
    def _terminate_outdated_nodes(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Terminate the nodes that are outdated, i.e. the node type config has been
        updated or the node's launch config hash is outdated.

        Args:
            ctx: The schedule context.
        """
        if ctx.is_launch_config_check_disabled():
            # Outdated nodes check through launch config check is disabled.
            return

        nt_configs = ctx.get_node_type_configs()
        has_outdated_worker = any(
            n.status == SchedulingNodeStatus.SCHEDULABLE
            and not n._is_backfill_placeholder
            and n.node_kind != NodeKind.HEAD
            and (
                nt_configs.get(n.node_type) is None
                or (
                    n.is_running
                    and bool(nt_configs[n.node_type].launch_config_hash)
                    and nt_configs[n.node_type].launch_config_hash
                    != n.launch_config_hash
                )
            )
            for n in ctx.peek_nodes()
        )
        if not has_outdated_worker:
            for n in ctx.peek_nodes():
                if (
                    n.status == SchedulingNodeStatus.SCHEDULABLE
                    and not n._is_backfill_placeholder
                    and n.node_kind == NodeKind.HEAD
                ):
                    logger.warning(
                        f"Head node {n.im_instance_id}(ray={n.ray_node_id}) is "
                        "outdated with node config changes. "
                        "Please check the node's config or restart the cluster or restart "
                        "the head node. Autoscaler is not able to shutdown the outdated "
                        "head node"
                    )
            return

        nodes = ctx.get_nodes()

        def _mark_outdated(target: SchedulingNode) -> None:
            target.status = SchedulingNodeStatus.TO_TERMINATE
            target.termination_request = TerminationRequest(
                id=str(uuid.uuid4()),
                instance_id=target.im_instance_id,
                ray_node_id=target.ray_node_id,
                instance_type=target.node_type,
                instance_status=target.im_instance_status,
                cause=TerminationRequest.Cause.OUTDATED,
                details=f"node from {target.node_type} has outdated config",
            )

        outdated_replicas: Set[Tuple[NodeType, str]] = set()
        for node in nodes:
            if (
                node.status != SchedulingNodeStatus.SCHEDULABLE
                or node._is_backfill_placeholder
            ):
                # We don't need to care about non-schedulable or placeholder nodes.
                continue

            if node.node_kind == NodeKind.HEAD:
                # We should not be terminating the head node even if it's outdated.
                logger.warning(
                    f"Head node {node.im_instance_id}(ray={node.ray_node_id}) is "
                    "outdated with node config changes. "
                    "Please check the node's config or restart the cluster or restart "
                    "the head node. Autoscaler is not able to shutdown the outdated "
                    "head node"
                )
                continue
            node_type = node.node_type
            node_type_config = ctx.get_node_type_configs().get(node_type)
            if node_type_config is None or (
                node.is_running
                and node_type_config.launch_config_hash
                and node_type_config.launch_config_hash != node.launch_config_hash
            ):
                # The node type config has been updated, and the node's launch config
                # hash is outdated.
                _mark_outdated(node)
                if node.replica_id is not None:
                    outdated_replicas.add((node.node_type, node.replica_id))

        if outdated_replicas:
            replica_sizes = ctx.get_multihost_replica_sizes()
            for node in nodes:
                if (
                    node.status != SchedulingNodeStatus.SCHEDULABLE
                    or node.node_kind == NodeKind.HEAD
                    or node.replica_id is None
                    or (node.node_type, node.replica_id) not in outdated_replicas
                ):
                    continue
                if node._is_backfill_placeholder:
                    node.status = SchedulingNodeStatus.TO_TERMINATE
                elif node.node_type in replica_sizes:
                    _mark_outdated(node)

        ctx.update(nodes)

    @staticmethod
    def _cancel_sub_replica_queued_remainders(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Cancel unlaunchable `1..size-1` `QUEUED` remainders on multi-worker types.

        The V2 reconciler rounds multi-worker `QUEUED -> REQUESTED` launches down to
        whole `workers_per_replica` multiples, so any trailing sub-replica `QUEUED`
        remainder can never launch and would otherwise deadlock scheduling or hold
        `min_worker_nodes` / `max_worker_nodes` slots indefinitely.
        """
        replica_sizes = ctx.get_multihost_replica_sizes()
        if not replica_sizes:
            return
        if not any(
            n.node_type in replica_sizes
            and n.node_kind != NodeKind.HEAD
            and n.status == SchedulingNodeStatus.SCHEDULABLE
            and n.im_instance_status == Instance.QUEUED
            for n in ctx.peek_nodes()
        ):
            return
        nodes = ctx.get_nodes()
        canceled = False
        for node_type, size in replica_sizes.items():
            queued_nodes = sorted(
                (
                    n
                    for n in nodes
                    if n.node_type == node_type
                    and n.node_kind != NodeKind.HEAD
                    and n.status == SchedulingNodeStatus.SCHEDULABLE
                    and n.im_instance_status == Instance.QUEUED
                ),
                key=lambda n: n.im_instance_id or "",
            )
            rem = ctx.doomed_queued_count(node_type, size, len(queued_nodes))
            if rem <= 0:
                continue
            for node in queued_nodes[-rem:]:
                node.status = SchedulingNodeStatus.TO_TERMINATE
                node._is_sub_replica_queued_canceled = True
                node.termination_request = TerminationRequest(
                    id=str(uuid.uuid4()),
                    instance_id=node.im_instance_id,
                    ray_node_id=node.ray_node_id,
                    cause=TerminationRequest.Cause.IDLE,
                    instance_type=node.node_type,
                    instance_status=node.im_instance_status,
                    details=(
                        f"canceling sub-replica QUEUED remainder "
                        f"({rem} < workers_per_replica={size})"
                    ),
                )
                canceled = True
        if canceled:
            ctx.rebuild_replicas(nodes)

    def _update_pending_nodes_idle_duration(
        self,
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Set the idle duration of the pending nodes not needed by any resource
        demands or cluster resource constraints, so that they are subject to idle
        termination like the running nodes.

        The durations are tracked by the number of unneeded pending nodes per node
        type rather than by instance, so they don't depend on which of the
        interchangeable pending nodes the requests happen to be scheduled on. With
        N unneeded pending nodes of a type, the k-th (k < N) longest duration is
        how long at least k + 1 of them have been continuously not needed.

        Args:
            ctx: The schedule context, after all resource demands are scheduled.
        """
        now_s = time.monotonic()
        peek_nodes = ctx.peek_nodes()
        if not any(
            n.status == SchedulingNodeStatus.SCHEDULABLE
            and not n._is_backfill_placeholder
            and n.node_kind != NodeKind.HEAD
            and n.im_instance_status
            in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
            and not any(n.sched_requests.values())
            for n in peek_nodes
        ):
            self._pending_unneeded_since_s = {}
            self._pending_node_since_s = {}
            return
        nodes = ctx.get_nodes()
        node_type_configs = ctx.get_node_type_configs()
        replica_sizes = ctx.get_multihost_replica_sizes()
        pinned_domains = ctx.get_pinned_domains()
        replicas_by_type: Dict[NodeType, Dict[str, List[SchedulingNode]]] = defaultdict(
            lambda: defaultdict(list)
        )
        active_replicas_by_type: Dict[NodeType, Set[str]] = defaultdict(set)
        for node in nodes:
            if (
                node.node_type in replica_sizes
                and node.node_kind != NodeKind.HEAD
                and node.status != SchedulingNodeStatus.TO_TERMINATE
                and node.replica_id is not None
            ):
                replicas_by_type[node.node_type][node.replica_id].append(node)
                avail = node.get_available_resources(
                    ResourceRequestSource.PENDING_DEMAND
                )
                if any(node.sched_requests.values()) or (
                    node.is_running
                    and any(
                        total - avail.get(res, 0.0) > _RESOURCE_EPSILON
                        for res, total in node.total_resources.items()
                    )
                ):
                    active_replicas_by_type[node.node_type].add(node.replica_id)
        pending_active_pinned: Set[Tuple[str, str]] = set()
        if pinned_domains:
            for node in nodes:
                if (
                    node.node_kind == NodeKind.HEAD
                    or node.status == SchedulingNodeStatus.TO_TERMINATE
                    or node.is_leaving
                ):
                    continue
                cfg = node_type_configs.get(node.node_type)
                is_active_node = node.status == SchedulingNodeStatus.TO_LAUNCH or any(
                    node.sched_requests.values()
                )
                if not is_active_node and node.is_running:
                    eff_idle_ms = ctx.effective_idle_timeout_ms(cfg)
                    avail = node.get_available_resources(
                        ResourceRequestSource.PENDING_DEMAND
                    )
                    is_busy = any(
                        total - avail.get(res, 0.0) > _RESOURCE_EPSILON
                        for res, total in node.total_resources.items()
                    )
                    is_active_node = (
                        is_busy
                        or eff_idle_ms is None
                        or node.idle_duration_ms <= eff_idle_ms
                    )
                elif (
                    not is_active_node
                    and not node._is_backfill_placeholder
                    and node.im_instance_status
                    not in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
                ):
                    is_active_node = True
                if is_active_node:
                    pending_active_pinned.update(
                        ResourceDemandScheduler._shared_pinned_pairs(
                            [node], cfg, pinned_domains
                        )
                    )
            for nt, by_rep in replicas_by_type.items():
                cfg = node_type_configs[nt]
                size = replica_sizes[nt]
                for rid, r_nodes in by_rep.items():
                    if any(m.is_leaving for m in r_nodes):
                        continue
                    real_nodes = [m for m in r_nodes if not m._is_backfill_placeholder]
                    if len(real_nodes) % size != 0:
                        pending_active_pinned.update(
                            ResourceDemandScheduler._shared_pinned_pairs(
                                r_nodes, cfg, pinned_domains
                            )
                        )
            for nt, by_rep in replicas_by_type.items():
                cfg = node_type_configs[nt]
                size = replica_sizes[nt]
                dom_key = cfg.domain_label_key
                for rid, r_nodes in by_rep.items():
                    if any(m.is_leaving for m in r_nodes):
                        continue
                    real_nodes = [m for m in r_nodes if not m._is_backfill_placeholder]
                    is_incomplete = len(real_nodes) % size != 0
                    if (
                        is_incomplete
                        and ResourceDemandScheduler._is_replica_pinned(
                            r_nodes, pinned_domains, dom_key
                        )
                    ) or (
                        ResourceDemandScheduler._shared_pinned_pairs(
                            r_nodes, cfg, pinned_domains
                        )
                        & pending_active_pinned
                    ):
                        active_replicas_by_type[nt].add(rid)

        unneeded_nodes_by_type: Dict[NodeType, List[SchedulingNode]] = defaultdict(list)
        for node in nodes:
            if (
                node.status == SchedulingNodeStatus.SCHEDULABLE
                and not node._is_backfill_placeholder
                and node.node_kind != NodeKind.HEAD
                and node.im_instance_status
                in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
                and not any(node.sched_requests.values())
                and not (
                    node.node_type in replica_sizes
                    and node.replica_id in active_replicas_by_type[node.node_type]
                )
                and not (
                    node.node_type not in replica_sizes
                    and bool(
                        ResourceDemandScheduler._shared_pinned_pairs(
                            [node],
                            node_type_configs.get(node.node_type),
                            pinned_domains,
                        )
                        & pending_active_pinned
                    )
                )
            ):
                unneeded_nodes_by_type[node.node_type].append(node)

        unneeded_since_s: Dict[NodeType, List[float]] = {}
        node_since_by_key: Dict[Tuple[NodeType, str], float] = {}
        for node_type, unneeded_nodes in unneeded_nodes_by_type.items():
            prev_since_s = self._pending_unneeded_since_s.get(node_type, [])
            since_s = prev_since_s[: len(unneeded_nodes)]
            since_s += [now_s] * (len(unneeded_nodes) - len(since_s))
            unneeded_since_s[node_type] = since_s
            size = replica_sizes.get(node_type, 1)
            by_rep = replicas_by_type[node_type]
            real_by_rep = {
                rid: [m for m in rn if not m._is_backfill_placeholder]
                for rid, rn in by_rep.items()
            }
            status_rank = ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES.index
            known_rep_rank = {
                rid: max(
                    (
                        status_rank(m.im_instance_status)
                        for m in rn
                        if m.im_instance_status
                        in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
                    ),
                    default=0,
                )
                for rid, rn in real_by_rep.items()
                if rn and all(m._has_runtime_domain for m in rn)
            }
            unneeded_nodes.sort(
                key=lambda n: (
                    size > 1
                    and not (
                        len(real_by_rep.get(n.replica_id or "", ())) == size
                        and all(
                            m.im_instance_status == Instance.QUEUED
                            for m in real_by_rep[n.replica_id]
                        )
                    ),
                    known_rep_rank[n.replica_id]
                    if size > 1 and n.replica_id in known_rep_rank
                    else status_rank(n.im_instance_status),
                    n.replica_id or "" if size > 1 else "",
                    status_rank(n.im_instance_status),
                )
            )
            for node, node_since_s in zip(unneeded_nodes, since_s):
                if node.im_instance_id:
                    node_since_by_key[(node_type, node.im_instance_id)] = node_since_s
                node.idle_duration_ms = int((now_s - node_since_s) * 1000)

        self._pending_unneeded_since_s = unneeded_since_s
        self._pending_node_since_s = node_since_by_key
        ctx.update(nodes)

    def _release_terminated_pending_nodes_idle_duration(
        self,
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Drop the idle durations taken by the pending nodes being idle terminated."""
        for node in ctx.peek_nodes():
            if (
                not node._is_sub_replica_queued_canceled
                and node.termination_request is not None
                and node.termination_request.cause == TerminationRequest.Cause.IDLE
                and node.im_instance_status
                in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
            ):
                since_s = self._pending_unneeded_since_s.get(node.node_type)
                node_since = self._pending_node_since_s.pop(
                    (node.node_type, node.im_instance_id or ""), None
                )
                if since_s:
                    if node_since in since_s:
                        since_s.remove(node_since)
                    elif not node.im_instance_id:
                        since_s.pop(0)

    @staticmethod
    def _termination_units(
        nodes: List[SchedulingNode],
        replica_sizes: Dict[NodeType, int],
        node_type_configs: Dict[NodeType, NodeTypeConfig],
        pinned_domains: Dict[str, Set[str]],
        replicas_by_type: Optional[Dict[NodeType, Dict[str, _Replica]]] = None,
    ) -> List[List[SchedulingNode]]:
        """Group worker nodes into ordered termination units, keeping multi-worker
        replicas whole so replica-atomic providers never lose active sibling hosts."""
        sort_key = ResourceDemandScheduler._sort_nodes_for_termination
        term_order = ResourceDemandScheduler._PENDING_TERMINATION_ORDER
        base = {id(n): sort_key(n) for n in nodes}

        def _term_rank(m: SchedulingNode) -> int:
            return term_order.get(m.im_instance_status, 4)

        def _prog_key(
            u: List[SchedulingNode], is_refill_on_live: bool = False
        ) -> Tuple[int, int, float]:
            real = [m for m in u if not m._is_backfill_placeholder] or u
            ranks = [_term_rank(m) for m in real]
            size = replica_sizes.get(real[0].node_type, 1)
            tier = 2 if is_refill_on_live else int(len(real) >= size)
            return (
                max(ranks),
                tier,
                sum(ranks) / len(ranks),
            )

        def _worst_key(
            real: List[SchedulingNode], is_pinned: bool
        ) -> Tuple[bool, bool, Tuple]:
            worst = max(base[id(m)] for m in real)
            is_unalloc = all(
                m.status == SchedulingNodeStatus.TO_LAUNCH
                or m.im_instance_status == Instance.QUEUED
                for m in real
            )
            if is_unalloc:
                return (False, is_pinned, worst)
            return (True, False, max(worst, (True, 0, 0.0)) if is_pinned else worst)

        ranked_units: List[
            Tuple[Tuple, Tuple[int, int, float], str, List[SchedulingNode]]
        ] = []
        grouped: Dict[Tuple[NodeType, str], List[SchedulingNode]] = defaultdict(list)
        for n in nodes:
            cfg = node_type_configs.get(n.node_type)
            dk = cfg.domain_label_key if cfg is not None else None
            is_exclusive = cfg.is_exclusive_domain if cfg is not None else False
            rid = (
                n.replica_id or (n.labels.get(dk) if is_exclusive else None)
                if n.node_type in replica_sizes
                else None
            )
            if rid is None:
                is_pinned = ResourceDemandScheduler._is_node_pinned(
                    n, pinned_domains, dk
                )
                ranked_units.append(
                    (_worst_key([n], is_pinned), _prog_key([n]), "", [n])
                )
            else:
                grouped[(n.node_type, rid)].append(n)

        for (nt, rid), unit in grouped.items():
            cfg = node_type_configs.get(nt)
            dk = cfg.domain_label_key if cfg is not None else None
            size = replica_sizes.get(nt, 1)
            rep = (replicas_by_type or {}).get(nt, {}).get(rid)
            allow_multi = (
                rep.allow_multi_replica
                if rep is not None
                else not any(m.provider_replica_id for m in unit)
            )
            u_sorted = sorted(
                unit,
                key=lambda m: (base[id(m)], _term_rank(m)),
            )
            unalloc = [
                m
                for m in u_sorted
                if m.status == SchedulingNodeStatus.TO_LAUNCH
                or m.im_instance_status == Instance.QUEUED
            ]
            alloc = [
                m
                for m in u_sorted
                if m.status != SchedulingNodeStatus.TO_LAUNCH
                and m.im_instance_status != Instance.QUEUED
            ]
            alloc_real = [m for m in alloc if not m._is_backfill_placeholder]
            if unalloc and alloc_real:
                chunks = [(unalloc, True), (alloc, False)]
            elif allow_multi and len(u_sorted) > size and len(u_sorted) % size == 0:
                chunks = [
                    (u_sorted[i : i + size], False)
                    for i in range(0, len(u_sorted), size)
                ]
            else:
                chunks = [(u_sorted, False)]
            for chunk, is_refill_on_live in chunks:
                real = [m for m in chunk if not m._is_backfill_placeholder] or chunk
                is_pinned = ResourceDemandScheduler._is_replica_pinned(
                    chunk, pinned_domains, dk
                )
                ranked_units.append(
                    (
                        _worst_key(real, is_pinned),
                        _prog_key(chunk, is_refill_on_live),
                        rid,
                        chunk,
                    )
                )

        ranked_units.sort(key=lambda item: item[:3])
        return [unit for _, _, _, unit in ranked_units]

    @staticmethod
    def _idle_timeout_for_node(
        node: SchedulingNode,
        ctx: "ResourceDemandScheduler.ScheduleContext",
        node_type_configs: Dict[NodeType, NodeTypeConfig],
    ) -> Optional[float]:
        """Return `node`'s expired idle timeout in seconds, or None if not eligible."""
        if (
            node.status != SchedulingNodeStatus.SCHEDULABLE
            or node._is_backfill_placeholder
            or node.node_kind == NodeKind.HEAD
            or any(node.sched_requests.values())
        ):
            return None
        if (
            node.im_instance_status
            in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
        ):
            timeout_s = ctx.get_pending_idle_timeout_s()
        else:
            timeout_s = ctx.effective_idle_timeout_s(
                node_type_configs.get(node.node_type)
            )
        return (
            timeout_s
            if timeout_s is not None and node.idle_duration_ms > timeout_s * 1000
            else None
        )

    @dataclass(frozen=True)
    class _KnownIdleReplica:
        rid: str
        real_nodes: List[SchedulingNode]
        placeholders: List[SchedulingNode]
        is_incomplete: bool
        may_pair: bool
        all_expired: bool

    @dataclass(frozen=True)
    class _IdleReplicaTypeState:
        cfg: NodeTypeConfig
        size: int
        idle_queued: List[List[SchedulingNode]]
        known: List["ResourceDemandScheduler._KnownIdleReplica"]
        flat: List[SchedulingNode]

    @staticmethod
    def _collect_multihost_idle_state(
        cfg: NodeTypeConfig,
        size: int,
        by_rid: Dict[str, List[SchedulingNode]],
        reps_for_type: Dict[str, _Replica],
        idle_timeouts: Dict[int, float],
        pinned_domains: Dict[str, Set[str]],
        active_pinned_domains: Set[Tuple[str, str]],
    ) -> "ResourceDemandScheduler._IdleReplicaTypeState":
        idle_queued: List[List[SchedulingNode]] = []
        known_raw: List[Tuple[str, List[SchedulingNode], List[SchedulingNode]]] = []
        unknown: List[List[SchedulingNode]] = []
        has_running_attached_req = False
        active_booting_lids = {
            m.launch_request_id
            for r_nodes in by_rid.values()
            for m in r_nodes
            if m.launch_request_id
            and not m.is_running
            and not m._is_backfill_placeholder
            and not m.is_leaving
            and (m.progress_rank < 3 or m.im_instance_status == Instance.QUEUED)
            and id(m) not in idle_timeouts
        }
        for rid, r_nodes in sorted(by_rid.items()):
            if all(m.im_instance_status == Instance.QUEUED for m in r_nodes):
                if all(id(m) in idle_timeouts for m in r_nodes):
                    idle_queued.append(r_nodes)
                elif pinned_domains and not any(m.is_leaving for m in r_nodes):
                    active_pinned_domains.update(
                        ResourceDemandScheduler._shared_pinned_pairs(
                            r_nodes, cfg, pinned_domains
                        )
                    )
            elif any(m.is_leaving for m in r_nodes):
                continue
            else:
                if pinned_domains and any(
                    m.status == SchedulingNodeStatus.TO_LAUNCH
                    or (
                        m.im_instance_status == Instance.QUEUED
                        and id(m) not in idle_timeouts
                    )
                    for m in r_nodes
                ):
                    active_pinned_domains.update(
                        ResourceDemandScheduler._shared_pinned_pairs(
                            r_nodes, cfg, pinned_domains
                        )
                    )
                real_known_m = [
                    m
                    for m in r_nodes
                    if m._has_runtime_domain and not m._is_backfill_placeholder
                ]
                ph_m = [m for m in r_nodes if m._is_backfill_placeholder]
                unknown_m = [
                    m
                    for m in r_nodes
                    if not m._has_runtime_domain
                    and not m._is_backfill_placeholder
                    and (m.progress_rank < 3 or m.im_instance_status == Instance.QUEUED)
                ]
                if real_known_m:
                    known_raw.append((rid, real_known_m, ph_m))
                    if any(m.is_running for m in real_known_m) and any(
                        m.progress_rank == 2 for m in unknown_m
                    ):
                        has_running_attached_req = True
                if unknown_m:
                    unknown.append(unknown_m)

        flat = [m for rn in unknown for m in rn]
        has_unknown_cloud_pod = any(m.progress_rank < 2 for m in flat)
        has_unknown_req = any(m.progress_rank == 2 for m in flat)
        known: List[ResourceDemandScheduler._KnownIdleReplica] = []
        for rid, rn, ph_m in known_raw:
            has_running = any(m.is_running for m in rn)
            is_prov = all(m.provider_replica_id for m in rn)
            has_ph_demand = any(any(m.sched_requests.values()) for m in ph_m)
            is_inc = len(rn) % size != 0
            rep = reps_for_type.get(rid)
            rep_lids = rep.lids if rep is not None else set()
            has_boot_lid = not is_prov and (
                bool(rep_lids & active_booting_lids)
                or any(m.launch_request_id in active_booting_lids for m in rn)
            )
            may_pair = is_inc and (
                (not is_prov and has_unknown_cloud_pod)
                or (has_unknown_req and (not has_running or has_running_attached_req))
            )
            all_exp = (
                not has_ph_demand
                and not has_boot_lid
                and all(id(m) in idle_timeouts for m in rn)
            )
            known.append(
                ResourceDemandScheduler._KnownIdleReplica(
                    rid=rid,
                    real_nodes=rn,
                    placeholders=ph_m,
                    is_incomplete=is_inc,
                    may_pair=may_pair,
                    all_expired=all_exp,
                )
            )

        if pinned_domains:
            flat_all_expired = bool(flat) and all(id(m) in idle_timeouts for m in flat)
            paired_inc: List[SchedulingNode] = []
            has_active_pair_peer = False
            for kr in known:
                unit_m = kr.real_nodes + kr.placeholders
                if kr.may_pair:
                    if kr.all_expired and flat_all_expired:
                        paired_inc.extend(unit_m)
                    else:
                        has_active_pair_peer = True
                        active_pinned_domains.update(
                            ResourceDemandScheduler._shared_pinned_pairs(
                                unit_m, cfg, pinned_domains
                            )
                        )
                elif kr.is_incomplete or not kr.all_expired:
                    active_pinned_domains.update(
                        ResourceDemandScheduler._shared_pinned_pairs(
                            unit_m, cfg, pinned_domains
                        )
                    )
            if flat:
                if not flat_all_expired or has_active_pair_peer:
                    if len(flat) % size != 0 or has_active_pair_peer:
                        active_pinned_domains.update(
                            ResourceDemandScheduler._shared_pinned_pairs(
                                flat, cfg, pinned_domains
                            )
                        )
                    else:
                        active_pinned_domains.update(
                            ResourceDemandScheduler._shared_pinned_pairs(
                                (m for m in flat if id(m) not in idle_timeouts),
                                cfg,
                                pinned_domains,
                            )
                        )
                else:
                    comb = paired_inc + flat
                    real_c = [m for m in comb if not m._is_backfill_placeholder]
                    if len(real_c) % size != 0:
                        active_pinned_domains.update(
                            ResourceDemandScheduler._shared_pinned_pairs(
                                comb, cfg, pinned_domains
                            )
                        )
        return ResourceDemandScheduler._IdleReplicaTypeState(
            cfg=cfg,
            size=size,
            idle_queued=idle_queued,
            known=known,
            flat=flat,
        )

    @staticmethod
    def _emit_multihost_idle_units(
        state: "ResourceDemandScheduler._IdleReplicaTypeState",
        reps_for_type: Dict[str, _Replica],
        idle_timeouts: Dict[int, float],
        is_pinned_protected,
        units: List[List[SchedulingNode]],
    ) -> None:
        cfg = state.cfg
        size = state.size
        flat = state.flat
        for q_nodes in state.idle_queued:
            if not is_pinned_protected(
                q_nodes, cfg, is_incomplete=len(q_nodes) % size != 0
            ):
                units.append(q_nodes)
        idle_inc_known: List[List[SchedulingNode]] = []
        has_active_inc = False
        for kr in state.known:
            rn = kr.real_nodes
            ph_m = kr.placeholders
            full_unit = rn + ph_m
            rep = reps_for_type.get(kr.rid)
            allow_multi = (
                rep.allow_multi_replica
                if rep is not None
                else not any(m.provider_replica_id for m in rn)
            )
            if not kr.may_pair:
                if kr.all_expired and not is_pinned_protected(
                    full_unit, cfg, is_incomplete=kr.is_incomplete
                ):
                    if (
                        allow_multi
                        and len(rn) > size
                        and len(rn) % size == 0
                        and not ph_m
                    ):
                        rn_sorted = sorted(
                            rn,
                            key=ResourceDemandScheduler._sort_nodes_for_termination,
                        )
                        units.extend(
                            rn_sorted[i : i + size]
                            for i in range(0, len(rn_sorted), size)
                        )
                    else:
                        units.append(full_unit)
            elif kr.all_expired and not is_pinned_protected(
                full_unit, cfg, is_incomplete=False
            ):
                idle_inc_known.append(full_unit)
            else:
                has_active_inc = True

        if not flat:
            units.extend(
                u
                for u in idle_inc_known
                if not is_pinned_protected(u, cfg, is_incomplete=True)
            )
        elif not has_active_inc and all(id(m) in idle_timeouts for m in flat):
            flat.sort(
                key=lambda m: (m.progress_rank, m.idle_duration_ms),
                reverse=True,
            )
            if idle_inc_known:
                combined = [m for u in idle_inc_known for m in u] + flat
                real_comb = [m for m in combined if not m._is_backfill_placeholder]
                if not is_pinned_protected(
                    combined, cfg, is_incomplete=len(real_comb) % size != 0
                ):
                    if (
                        len(real_comb) > size
                        and (not cfg.domain_label_key or len(real_comb) % size == 0)
                        and len(real_comb) == len(combined)
                        and not any(m.provider_replica_id for m in real_comb)
                    ):
                        comb_sorted = sorted(
                            real_comb,
                            key=ResourceDemandScheduler._sort_nodes_for_termination,
                        )
                        units.extend(
                            comb_sorted[i : i + size]
                            for i in range(0, len(comb_sorted), size)
                        )
                    else:
                        units.append(combined)
            elif not is_pinned_protected(
                flat, cfg, is_incomplete=len(flat) % size != 0
            ):
                if not cfg.domain_label_key or len(flat) % size == 0:
                    units.extend(flat[i : i + size] for i in range(0, len(flat), size))
                else:
                    units.append(flat)

    @staticmethod
    def _enforce_idle_termination(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce idle termination for nodes not needed by demands or constraints.
        Multi-worker replica node types terminate only as whole idle replicas."""
        node_type_configs = ctx.get_node_type_configs()
        if not any(
            ResourceDemandScheduler._idle_timeout_for_node(n, ctx, node_type_configs)
            is not None
            for n in ctx.peek_nodes()
        ):
            return
        count_by_node_type = ctx.get_cluster_shape()
        replica_sizes = ctx.get_multihost_replica_sizes()
        pinned_domains = ctx.get_pinned_domains()
        terminate_nodes_by_type: Dict[NodeType, int] = defaultdict(int)
        terminate_non_queued_by_type: Dict[NodeType, int] = defaultdict(int)
        nodes = ctx.get_nodes()
        non_queued_count_by_type: Counter = Counter(
            n.node_type
            for n in nodes
            if n.node_kind != NodeKind.HEAD
            and n.status == SchedulingNodeStatus.SCHEDULABLE
            and n.im_instance_status != Instance.QUEUED
        )
        # Nodes whose Ray stop is requested or whose multi-worker replica is
        # draining are leaving, so they must not count toward min_worker_nodes
        # when choosing additional replicas to idle terminate.
        for n in nodes:
            if n.status == SchedulingNodeStatus.SCHEDULABLE and n.is_leaving:
                count_by_node_type[n.node_type] -= 1
                if (
                    n.node_kind != NodeKind.HEAD
                    and n.im_instance_status != Instance.QUEUED
                ):
                    non_queued_count_by_type[n.node_type] -= 1
        idle_timeouts: Dict[int, float] = {}
        for n in nodes:
            t = ResourceDemandScheduler._idle_timeout_for_node(
                n, ctx, node_type_configs
            )
            if t is not None:
                idle_timeouts[id(n)] = t

        replica_nodes_by_type: Dict[
            NodeType, Dict[str, List[SchedulingNode]]
        ] = defaultdict(lambda: defaultdict(list))
        for n in nodes:
            if (
                n.node_type in replica_sizes
                and n.status != SchedulingNodeStatus.TO_TERMINATE
                and n.node_kind != NodeKind.HEAD
            ):
                replica_nodes_by_type[n.node_type][
                    n.replica_id or f"\x00singleton:{id(n)}"
                ].append(n)

        units: List[List[SchedulingNode]] = []
        active_pinned_domains: Set[Tuple[str, str]] = set()

        def _is_pinned_protected(
            target_nodes: List[SchedulingNode],
            cfg: Optional[NodeTypeConfig],
            is_incomplete: bool = False,
        ) -> bool:
            if not pinned_domains:
                return False
            if (
                ResourceDemandScheduler._shared_pinned_pairs(
                    target_nodes, cfg, pinned_domains
                )
                & active_pinned_domains
            ):
                return True
            if not is_incomplete:
                return False
            return ResourceDemandScheduler._is_replica_pinned(
                target_nodes,
                pinned_domains,
                cfg.domain_label_key if cfg is not None else None,
            )

        if pinned_domains:
            for n in nodes:
                if (
                    n.node_type not in replica_sizes
                    and n.node_kind != NodeKind.HEAD
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                    and not n.is_leaving
                    and id(n) not in idle_timeouts
                ):
                    active_pinned_domains.update(
                        ResourceDemandScheduler._shared_pinned_pairs(
                            [n], node_type_configs.get(n.node_type), pinned_domains
                        )
                    )

        replicas_by_type = ctx.get_replicas_by_type()
        per_type_state = [
            ResourceDemandScheduler._collect_multihost_idle_state(
                node_type_configs[node_type],
                size,
                replica_nodes_by_type.get(node_type, {}),
                replicas_by_type.get(node_type, {}),
                idle_timeouts,
                pinned_domains,
                active_pinned_domains,
            )
            for node_type, size in replica_sizes.items()
        ]

        for n in nodes:
            if n.node_type not in replica_sizes and id(n) in idle_timeouts:
                if _is_pinned_protected([n], node_type_configs.get(n.node_type)):
                    continue
                units.append([n])

        for state in per_type_state:
            ResourceDemandScheduler._emit_multihost_idle_units(
                state,
                replicas_by_type.get(state.cfg.name, {}),
                idle_timeouts,
                _is_pinned_protected,
                units,
            )

        def _idle_unit_sort_key(
            u: List[SchedulingNode],
        ) -> Tuple[bool, bool, int, int]:
            real = [n for n in u if not n._is_backfill_placeholder] or u
            size = replica_sizes.get(real[0].node_type, 1)
            is_degraded = (
                any(n._is_backfill_placeholder for n in u) or len(real) % size != 0
            )
            return (
                any(len(n.ray_node_id) > 0 for n in real),
                not is_degraded,
                -min(n.idle_duration_ms for n in real),
                -min(n.progress_rank for n in real),
            )

        units.sort(key=_idle_unit_sort_key)
        for unit in units:
            nt = unit[0].node_type
            cfg = node_type_configs.get(nt)
            min_count = cfg.min_worker_nodes if cfg is not None else 0
            non_queued = sum(
                n.node_kind != NodeKind.HEAD
                and n.status == SchedulingNodeStatus.SCHEDULABLE
                and n.im_instance_status != Instance.QUEUED
                for n in unit
            )
            rem = count_by_node_type.get(nt, 0) - terminate_nodes_by_type[nt]
            nq_rem = non_queued_count_by_type[nt] - terminate_non_queued_by_type[nt]
            if rem - len(unit) < min_count or (
                non_queued > 0 and nq_rem - non_queued < min_count
            ):
                continue

            terminate_nodes_by_type[nt] += len(unit)
            terminate_non_queued_by_type[nt] += non_queued
            for node in sorted(
                unit, key=ResourceDemandScheduler._sort_nodes_for_termination
            ):
                node.status = SchedulingNodeStatus.TO_TERMINATE
                if node._is_backfill_placeholder:
                    continue
                timeout_s = idle_timeouts[id(node)]
                node.termination_request = TerminationRequest(
                    id=str(uuid.uuid4()),
                    instance_id=node.im_instance_id,
                    ray_node_id=node.ray_node_id,
                    cause=TerminationRequest.Cause.IDLE,
                    instance_type=node.node_type,
                    instance_status=node.im_instance_status,
                    idle_duration_ms=node.idle_duration_ms,
                    details=(
                        f"{'idle' if node.ray_node_id else 'pending and not needed'} "
                        f"for {node.idle_duration_ms / 1000} secs > "
                        f"timeout={timeout_s} secs"
                    ),
                )

        ctx.update(nodes)
