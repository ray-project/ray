import copy
import itertools
import logging
import math
import time
import uuid
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from ray._private.protobuf_compat import message_to_dict
from ray._raylet import IMPLICIT_RESOURCE_PREFIX
from ray.autoscaler._private.constants import AUTOSCALER_CONSERVE_GPU_NODES
from ray.autoscaler._private.resource_demand_scheduler import (
    UtilizationScore,
    _fits,
    _inplace_subtract,
)
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.common import (
    _ACTIVE_CID_STATUSES,
    _DRAINING_INSTANCE_STATUSES,
    _TEARDOWN_INSTANCE_STATUSES,
    InstanceUtil,
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


def _has_label_constraints(request: ResourceRequest) -> bool:
    """Return True if `request` specifies at least one label constraint."""
    for selector in request.label_selectors:
        if selector.label_constraints:
            return True
    return False


def _has_dynamic_selector_overlap(requests: Iterable[ResourceRequest]) -> bool:
    """Return True if any label selector reads a label added by anti-affinity."""
    anti_affinity_keys = set()
    for r in requests:
        for c in r.placement_constraints:
            if c.HasField("anti_affinity"):
                anti_affinity_keys.add(c.anti_affinity.label_name)
    if not anti_affinity_keys:
        return False
    for r in requests:
        for selector in r.label_selectors:
            for c in selector.label_constraints:
                if c.label_key in anti_affinity_keys:
                    return True
    return False


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
        self, node: "SchedulingNode", is_preferred: bool = False
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
        )

        if state_key in self.seen_states:
            return True

        self.seen_states.add(state_key)
        return False


class UnschedulableRequestCache:
    """Caches resource request shape keys that have failed to schedule on a node."""

    def __init__(self):
        self.shapes = set()

    def contains(self, shape_key: bytes) -> bool:
        return shape_key in self.shapes

    def add(self, shape_key: bytes) -> None:
        self.shapes.add(shape_key)

    def clear(self) -> None:
        self.shapes.clear()


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
    # Optional IM launch_request_id for exact launch-batch replica attachment.
    launch_request_id: Optional[str] = None
    # Whether the node belongs to an authoritative runtime/provider replica group.
    _has_runtime_domain: bool = False
    # Whether the node belongs to a draining/teardown multi-worker replica.
    _is_draining_replica: bool = False
    # Whether the node is an in-memory backfill placeholder for a committed slot.
    _is_backfill_placeholder: bool = False
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
        self.launch_request_id: Optional[str] = launch_request_id or None
        self._has_runtime_domain: bool = False
        self._is_draining_replica: bool = False
        self._is_backfill_placeholder: bool = False
        self._is_unlabeled_refill: bool = False
        self._is_sub_replica_queued_canceled: bool = False
        self._saved_locality_label: Optional[Tuple[bool, Optional[str]]] = None

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
            )

        node = SchedulingNode.from_node_config(
            node_config,
            SchedulingNodeStatus.SCHEDULABLE,
            node_kind=instance.im_instance.node_kind,
            im_instance_id=instance.im_instance.instance_id,
            im_instance_status=instance.im_instance.status,
            provider_replica_id=instance.cloud_instance_replica_id,
            launch_request_id=instance.im_instance.launch_request_id or None,
        )
        if (
            instance.im_instance.status == Instance.RAY_STOP_REQUESTED
            and instance.ray_node is not None
            and node_config.domain_label_key
        ):
            dom_val = (instance.ray_node.labels or {}).get(node_config.domain_label_key)
            if dom_val:
                node.labels[node_config.domain_label_key] = dom_val
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
        )

    def __post_init__(self):
        assert self.node_type, "node_type should be set"

    def try_schedule(
        self,
        requests: List[ResourceRequest],
        resource_request_source: ResourceRequestSource,
        shape_keys: Optional[Dict[int, bytes]] = None,
        best_label_scores: Optional[Dict[bytes, float]] = None,
        precomputed_label_scores: Optional[Dict[bytes, float]] = None,
    ) -> Tuple[List[ResourceRequest], UtilizationScore]:
        """Try to schedule the resource requests on this node.

        This modifies the node's available resources if the requests are schedulable.
        Requests are tiered by the label selector they match on this node: the
        primary selector first, then each fallback selector, then requests without
        label selectors. Once a labeled tier is placed, lower labeled tiers are
        skipped so one pass never mixes primary and fallback placements; requests
        without label selectors still fill the remaining capacity unless a skipped
        labeled tier can fit on this node. Within each tier, the requests are
        scheduled one by one in the sorted order, and no backtracking is done.

        Args:
            requests: The resource requests to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.
            shape_keys: Precomputed serialization keys for each request. If None,
                keys are computed on the fly (fallback).
            best_label_scores: The best label selector score that any candidate
                node gives each request, by shape key. Scores are tiered relative
                to it, so selectors that no node can satisfy do not hold a request
                back. If None, scores are tiered as is.
            precomputed_label_scores: Optional precomputed `_satisfies_label_constraints`
                scores by shape key valid for this node's initial `labels`.

        Returns:
            A tuple of:
                - list of remaining requests that cannot be scheduled on this node.
                - the utilization score for this node with respect to the current
                resource requests being scheduled.
        """
        keyed_requests = []
        for r in requests:
            sk = shape_keys.get(id(r)) if shape_keys is not None else None
            if sk is None:
                sk = r.SerializeToString(deterministic=True)
            keyed_requests.append((r, sk))

        # Label tier of each request: its label selector score on this node, 0 if
        # it has no label selectors, or None if the node satisfies none of them.
        # Labels only grow while scheduling (anti-affinity), so their count
        # versions the memo.
        tiers: Dict[Tuple[bytes, int], Optional[float]] = {}
        initial_num_labels = len(self.labels)

        def compute_tiers() -> List[Optional[float]]:
            num_labels = len(self.labels)
            use_precomputed = (
                precomputed_label_scores is not None
                and num_labels == initial_num_labels
            )
            out: List[Optional[float]] = []
            for r, sk in keyed_requests:
                key = (sk, num_labels)
                if key not in tiers:
                    if use_precomputed and sk in precomputed_label_scores:
                        score = precomputed_label_scores[sk]
                        if score == 0:
                            tier: Optional[float] = 0
                        else:
                            best = (best_label_scores or {}).get(sk)
                            tier = (min(score / best, 1.0) if best else score) or None
                    elif not _has_label_constraints(r):
                        tier = 0
                    else:
                        score = self._satisfies_label_constraints(r)
                        best = (best_label_scores or {}).get(sk)
                        tier = (min(score / best, 1.0) if best else score) or None
                    tiers[key] = tier
                out.append(tiers[key])
            return out

        # Cache to prevent O(N^2 * M) iteration by returning early for identical
        # requests.
        unfittable_cache = UnschedulableRequestCache()
        label_score = 0
        done_tiers = set()
        request_tiers: Optional[List[Optional[float]]] = None
        available_resources = self.get_available_resources(resource_request_source)

        while True:
            if request_tiers is None:
                request_tiers = compute_tiers()
            available_tiers = set(request_tiers) - {None}
            next_tiers = sorted(available_tiers - done_tiers, reverse=True)
            if not next_tiers:
                break
            tier = next_tiers[0]
            done_tiers.add(tier)

            if label_score and (
                (0 < tier < label_score)
                or (
                    tier == 0
                    and any(
                        0 < (rt or 0) < label_score
                        and self._can_schedule_one(r, resource_request_source)
                        for (r, _), rt in zip(keyed_requests, request_tiers)
                    )
                )
            ):
                continue

            num_labels_before = len(self.labels)
            remaining = []
            remaining_tiers: List[Optional[float]] = []
            labels_grew = False
            # Try schedule the requests of this tier one by one in the sorted order.
            # If an anti-affinity placement grows self.labels, restart tier
            # selection so earlier requests whose tier promoted are revisited.
            for idx, ((r, sk), req_tier) in enumerate(
                zip(keyed_requests, request_tiers)
            ):
                if req_tier != tier or unfittable_cache.contains(sk):
                    remaining.append((r, sk))
                    remaining_tiers.append(req_tier)
                    continue

                if not self._try_schedule_one(
                    r,
                    resource_request_source,
                    check_labels=False,
                    available_resources=available_resources,
                ):
                    unfittable_cache.add(sk)
                    remaining.append((r, sk))
                    remaining_tiers.append(req_tier)
                else:
                    label_score = max(label_score, tier)
                    if len(self.labels) > num_labels_before:
                        remaining.extend(keyed_requests[idx + 1 :])
                        done_tiers.clear()
                        labels_grew = True
                        break

            keyed_requests = remaining
            request_tiers = None if labels_grew else remaining_tiers

        unschedulable_requests = [r for r, _ in keyed_requests]
        score = self._compute_score(resource_request_source, label_score)

        return unschedulable_requests, score

    def _compute_score(
        self,
        resource_request_source: ResourceRequestSource,
        label_score: float,
    ) -> UtilizationScore:
        """Compute the utilization score for this node with respect to the current resource
        request being scheduled.

        A "higher" score means that this node is more suitable for scheduling the
        current scheduled resource requests.

        The score is a tuple of 5 values:
            1. Whether this node has labels matching the current resource request's
                label_selector requirements:
                    0: if this node does not satisfy any label selector requirements or
                       no label selectors are provided.
                    (len(label_selectors) - i) / len(label_selectors): a normalized
                        score in (0.0, 1.0] based on the priority index i of the label
                        selector in the resource request that this node satisfies.
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
            label_score: The label selector tier of the requests scheduled
                during the current `try_schedule` pass, so requests placed in
                earlier passes do not affect it.

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

    def _satisfies_label_constraints(self, request: ResourceRequest) -> float:
        """Returns a normalized value in (0.0, 1.0] based on the priority of the label
        selector this node satisfies (1.0 for the first selector, decreasing
        proportionally for fallback selectors), or 0 if none apply or none have
        constraints."""
        if not _has_label_constraints(request):
            return 0
        num_selectors = len(request.label_selectors)
        for i, selector in enumerate(request.label_selectors):
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
                return (num_selectors - i) / num_selectors
        return 0

    def _schedulable_label_score(
        self,
        request: ResourceRequest,
        resource_request_source: ResourceRequestSource,
        check_labels: bool = True,
        available_resources: Optional[Dict[str, float]] = None,
    ) -> Optional[float]:
        """Return the label selector score if `request` fits on this node, else None."""
        if check_labels and request.label_selectors:
            score = self._satisfies_label_constraints(request)
            if score == 0 and _has_label_constraints(request):
                return None
        else:
            score = 0

        for constraint in request.placement_constraints:
            if constraint.HasField("anti_affinity"):
                anti_affinity = constraint.anti_affinity
                if (
                    self.labels.get(anti_affinity.label_name)
                    == anti_affinity.label_value
                ):
                    return None

        avail = (
            available_resources
            if available_resources is not None
            else self.get_available_resources(resource_request_source)
        )
        if not _fits(avail, request.resources_bundle):
            return None
        return score

    def _can_schedule_one(
        self, request: ResourceRequest, resource_request_source: ResourceRequestSource
    ) -> bool:
        return (
            self._schedulable_label_score(request, resource_request_source) is not None
        )

    def _try_schedule_one(
        self,
        request: ResourceRequest,
        resource_request_source: ResourceRequestSource,
        check_labels: bool = True,
        available_resources: Optional[Dict[str, float]] = None,
    ) -> bool:
        """Try to schedule one resource request on this node. The request could be from
        various sources, specified by `resource_request_source`.

        Args:
            request: The resource request to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.
            check_labels: Whether to re-evaluate label selectors (False when the
                caller has already verified them for the current node labels).
            available_resources: Optional pre-fetched available resources dict for
                `resource_request_source`.

        Returns:
            True if the resource request is scheduled on this node.
        """
        available_resources_dict = (
            available_resources
            if available_resources is not None
            else self.get_available_resources(resource_request_source)
        )
        if (
            self._schedulable_label_score(
                request,
                resource_request_source,
                check_labels=check_labels,
                available_resources=available_resources_dict,
            )
            is None
        ):
            return False

        # Schedule the request, update resources
        _inplace_subtract(available_resources_dict, dict(request.resources_bundle))

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


def _has_prefix(tier_keys: Iterable[bytes], prefix: Tuple[bytes, ...]) -> bool:
    return tuple(itertools.islice(tier_keys, len(prefix))) == prefix


def _accepted_prefix(
    tier_keys: Iterable[bytes], remaining_counts: Dict[bytes, int]
) -> Tuple[bytes, ...]:
    prefix = []
    last = 0
    for k in tier_keys:
        prefix.append(k)
        n = remaining_counts.get(k, 0)
        if n > 0:
            remaining_counts[k] = n - 1
            last = len(prefix)
    return tuple(prefix[:last])


@dataclass
class _NodeAttempt:
    """Cached scheduling attempt for one candidate node in `_sched_best_node`."""

    # Label selector score by shape key for each request this node can fit.
    label_scores: Dict[bytes, float]
    # Best label selector scores (> this node's own score) when simulated.
    suboptimal_best_scores: Optional[Dict[bytes, float]] = None
    # Top label tier when simulated (None before simulation).
    top: Optional[float] = None
    # Prefix through the last accepted shape in this node's top label tier,
    # and in tier 0 when co-packed below top > 0.
    top_prefix: Tuple[bytes, ...] = ()
    zero_prefix: Tuple[bytes, ...] = ()
    # Keys in 0 < tier < top that blocked tier 0 from co-packing when simulated.
    tier_zero_blockers: Tuple[bytes, ...] = ()
    # Simulated node candidate (None if unsimulated or nothing scheduled).
    node: Optional[SchedulingNode] = None
    # Resource requests scheduled on the node during this attempt.
    scheduled_requests: List[ResourceRequest] = field(default_factory=list)
    # Utilization score of the simulated node.
    score: Optional[UtilizationScore] = None

    def node_tier(self, key: bytes, best_label_scores: Dict[bytes, float]) -> float:
        score = self.label_scores[key]
        if score == 0:
            return 0.0
        best = best_label_scores.get(key)
        return min(score / best, 1.0) if best else score

    def top_tier(
        self,
        requests_by_key: Dict[bytes, ResourceRequest],
        best_label_scores: Dict[bytes, float],
        pass_top: float,
    ) -> float:
        top = -1.0
        for k in self.label_scores:
            if k not in requests_by_key:
                continue
            tier = self.node_tier(k, best_label_scores)
            if tier > top:
                top = tier
                if top >= pass_top:
                    return top
        return top

    def matches(
        self,
        keys: List[bytes],
        requests_by_key: Dict[bytes, ResourceRequest],
        best_label_scores: Dict[bytes, float],
        top: float,
    ) -> bool:
        if self.top is None:
            return False
        if self.node is None:
            return True
        if self.top != top:
            return False
        if self.suboptimal_best_scores and not all(
            best_label_scores.get(k) == v
            for k, v in self.suboptimal_best_scores.items()
            if k in requests_by_key
        ):
            return False
        if not _has_prefix(
            (
                k
                for k in keys
                if k in self.label_scores
                and self.node_tier(k, best_label_scores) == top
            ),
            self.top_prefix,
        ):
            return False
        if self.zero_prefix and not _has_prefix(
            (k for k in keys if self.label_scores.get(k) == 0),
            self.zero_prefix,
        ):
            return False
        if self.tier_zero_blockers and not any(
            k in requests_by_key for k in self.tier_zero_blockers
        ):
            return False
        return True

    @classmethod
    def from_schedule(
        cls,
        label_scores: Dict[bytes, float],
        best_label_scores: Dict[bytes, float],
        node_copy: SchedulingNode,
        new_sched: List[ResourceRequest],
        score: UtilizationScore,
        keys: Optional[List[bytes]],
        shape_key_fn: Callable[[ResourceRequest], bytes],
        top: Optional[float],
        requests_by_key: Optional[Dict[bytes, ResourceRequest]],
        resource_request_source: Optional[ResourceRequestSource],
    ) -> "_NodeAttempt":
        if keys is None:
            return cls(
                label_scores=label_scores,
                node=node_copy,
                scheduled_requests=new_sched,
                score=score,
            )

        sched_counts = Counter(map(shape_key_fn, new_sched))
        node_keys = [k for k in keys if k in label_scores]
        attempt = cls(
            label_scores=label_scores,
            suboptimal_best_scores={
                k: best_label_scores[k]
                for k in label_scores
                if k in best_label_scores and label_scores[k] < best_label_scores[k]
            },
            top=top,
            node=node_copy,
            scheduled_requests=new_sched,
            score=score,
        )
        attempt.top_prefix = _accepted_prefix(
            (k for k in node_keys if attempt.node_tier(k, best_label_scores) == top),
            sched_counts,
        )
        if any(sched_counts.values()):
            attempt.zero_prefix = _accepted_prefix(
                (k for k in node_keys if label_scores[k] == 0),
                sched_counts,
            )
        elif (
            top > 0
            and requests_by_key is not None
            and resource_request_source is not None
            and any(label_scores[k] == 0 for k in node_keys)
        ):
            attempt.tier_zero_blockers = tuple(
                k
                for k in dict.fromkeys(node_keys)
                if 0 < attempt.node_tier(k, best_label_scores) < top
                and node_copy._can_schedule_one(
                    requests_by_key[k], resource_request_source
                )
            )
        return attempt


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
        self, nodes: List[SchedulingNode], target_domain: str, prior_score: float = 0.0
    ) -> bool:
        label_score_fn = ResourceDemandScheduler._node_label_score
        domain_nodes = [
            n
            for n in nodes
            if n.labels.get(self.locality_label) == target_domain
            and n.status != SchedulingNodeStatus.TO_TERMINATE
        ]
        num_to_launch = len(nodes) - len(self.all_nodes)
        used_pending = (
            sum(
                not n.is_running
                and bool(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
                for n in nodes
            )
            - self.orig_used_pending
        )
        label_score = (
            math.fsum(label_score_fn(n) for n in domain_nodes) - prior_score
            if self.max_label_score
            else 0.0
        )
        has_running = any(n.is_running for n in domain_nodes)
        launch_neg_rank = (0.0, 0, 0.0)
        if num_to_launch > 0:
            launched = (
                Counter(
                    n.node_type
                    for n in nodes
                    if n.status == SchedulingNodeStatus.TO_LAUNCH
                )
                - self.orig_to_launch
            )
            launch_neg_rank = tuple(-x for x in min(map(self.node_type_rank, launched)))
        used_placeholders = (
            sum(
                n._is_backfill_placeholder
                and bool(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
                for n in nodes
            )
            - self.orig_used_placeholders
        )
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
    def schedule_in_domain(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        all_nodes: List[SchedulingNode],
        domain_nodes: List[SchedulingNode],
        replaced: List[SchedulingNode],
        pending_siblings: List[SchedulingNode],
        requests: List[ResourceRequest],
        locality_label: str,
        domain_val: str,
    ) -> Optional[List[SchedulingNode]]:
        """Schedule `requests` within a single dynamic domain and any booting
        `pending_siblings` without launching new nodes. Returns the updated node
        list, or None if the gang does not fit."""
        label_score_fn = ResourceDemandScheduler._node_label_score
        running = [n for n in domain_nodes if n.is_running]
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
        launch_avail: Dict[NodeType, int] = {}
        for nt in dict.fromkeys(dn.node_type for dn in domain_nodes):
            if nt not in self.step5_type_set:
                continue
            need = self.gang_ctx.min_launch_batch_size(nt)
            avail = self.node_type_available.get(nt, 0)
            if avail >= need and (self.headroom is None or self.headroom >= need):
                launch_avail[nt] = avail
        if not launch_avail:
            return None
        siblings = (
            self._prep_siblings(pending_siblings, self.locality_label, domain_val)
            if pending_siblings
            else []
        )
        pool = domain_nodes + siblings
        before_orig_reqs = sum(
            len(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in domain_nodes
            if n._saved_locality_label is None
        )
        scheduled, infeasible = ResourceDemandScheduler._try_schedule(
            self.gang_ctx.restricted_to(pool, node_type_available=launch_avail),
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
        replaced = list(originals) + list(pending_siblings)
        rep_ids = {id(n) for n in replaced}
        return [n for n in self.all_nodes if id(n) not in rep_ids] + scheduled

    def evaluate_domains(
        self,
        existing_domains: List[str],
        launch_candidates: List[Tuple[str, List[SchedulingNode], List[SchedulingNode]]],
    ) -> bool:
        """Evaluate existing domains and launch candidates; return True on optimal hit."""
        label_score_fn = ResourceDemandScheduler._node_label_score
        seen_pending_domain_sigs: Set[Tuple] = set()
        failed_dynamic_sigs: Set[Tuple] = set()

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
                is_pending_domain = not cand_nodes and domain_val.startswith(
                    _PENDING_DOMAIN_PREFIX
                )
                if is_pending_domain:
                    pending_sig = ResourceDemandScheduler._pending_domain_signature(
                        domain_nodes, self.locality_label
                    )
                    if pending_sig in seen_pending_domain_sigs:
                        continue
                    seen_pending_domain_sigs.add(pending_sig)
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
                dynamic_sig: Optional[Tuple] = None
                if (
                    not cand_nodes
                    and not is_pending_domain
                    and not self.pinned
                    and not pending_siblings
                    and not self.requests_ref_locality
                ):
                    dynamic_sig = ResourceDemandScheduler._pending_domain_signature(
                        domain_nodes, self.locality_label
                    )
                    if dynamic_sig in failed_dynamic_sigs:
                        continue
                prior_score = (
                    math.fsum(
                        label_score_fn(n)
                        for n in (cand_originals if cand_nodes else domain_nodes)
                    )
                    if self.max_label_score
                    else 0.0
                )
                nodes = _GangSearch.schedule_in_domain(
                    self.gang_ctx,
                    self.all_nodes,
                    domain_nodes,
                    originals,
                    pending_siblings,
                    self.requests,
                    self.locality_label,
                    domain_val,
                )
                if nodes is None and dynamic_sig is not None:
                    failed_dynamic_sigs.add(dynamic_sig)
                if (
                    nodes is not None
                    and cand_nodes
                    and not cand_originals
                    and not any(n._is_unlabeled_refill for n in cand_nodes)
                ):
                    cand_nt = cand_nodes[0].node_type
                    rg = self.gang_ctx.get_unbacked_refill_gap(cand_nt)
                    if rg > 0:
                        nodes = nodes + self.gang_ctx.make_refill_nodes(cand_nt, rg)
            else:
                prior_score = (
                    math.fsum(
                        label_score_fn(n)
                        for n in self.all_nodes
                        if n.labels.get(self.locality_label) == domain_val
                        and n.status != SchedulingNodeStatus.TO_TERMINATE
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
                if infeasible:
                    nodes = None

            if nodes is not None and self._consider(nodes, domain_val, prior_score):
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
        refill_gap = self.gang_ctx.get_unbacked_refill_gap(nt)
        avail = max(0, self.node_type_available.get(nt, 0) - refill_gap)
        if self.headroom is not None and self.headroom < refill_gap:
            avail = 0
        if not pool and avail <= 0:
            return
        nt_ctx = self.gang_ctx.restricted_to(
            pool,
            node_type_available={nt: avail} if avail > 0 else None,
            clear_refill_gaps=True,
            headroom_delta=-refill_gap if refill_gap > 0 else 0,
        )
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
        launched_new = False
        for n in scheduled:
            if (
                n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
                or n.replica_id in sched_rids
            ):
                n.labels[self.locality_label] = pending_domain
            if n.status == SchedulingNodeStatus.TO_LAUNCH and n.node_type == nt:
                launched_new = True
        pool_ids = {id(n) for n in pool}
        nodes = [n for n in self.all_nodes if id(n) not in pool_ids] + scheduled
        if launched_new and refill_gap > 0:
            nodes = nodes + self.gang_ctx.make_refill_nodes(nt, refill_gap)
        self._consider(nodes, pending_domain, 0.0)


def _assign_node_replica_slot(
    node: SchedulingNode,
    cfg: Optional[NodeTypeConfig],
    replica_id: str,
    host_index: int,
    keep_head: Optional[bool] = None,
    domain_val: Optional[str] = None,
    shared_runtime_labels: Optional[Dict[str, str]] = None,
) -> None:
    """Assign `replica_id`, slot/domain/runtime labels, and head resources."""
    node.replica_id = replica_id
    rc = cfg.replica_config if cfg is not None else None
    if rc is None:
        return
    if rc.domain_label_key and not cfg.labels.get(rc.domain_label_key):
        node.labels[rc.domain_label_key] = (
            domain_val if domain_val is not None else replica_id
        )
    if rc.worker_index_label_key and not cfg.labels.get(rc.worker_index_label_key):
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
        if count <= 0:
            return size, size
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
        )
        if keep_head:
            heads_needed -= 1


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
        self.is_exclusive_domain = bool(self.dom_key) and self.rc.domain_exclusive
        self.is_shared_domain = bool(self.dom_key) and not self.rc.domain_exclusive
        self.wid_key = self.rc.worker_index_label_key
        self.head_keys = set(self.rc.head_resources)
        self.type_nodes = type_nodes
        self.replicas: Dict[str, _Replica] = {}
        self.active_placeholder_keys: Set[Tuple[str, str, str]] = set()
        self.extra_lid_backfill: int = 0
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
        self.active_draining_lids = ctx.get_active_draining_lids(node_type)
        self.non_stopping_by_lid: Counter = Counter(
            n.launch_request_id
            for n in type_nodes
            if n.launch_request_id
            and n.im_instance_status != Instance.RAY_STOP_REQUESTED
            and not n.provider_replica_id
        )
        self.draining_doms = (
            ctx.get_draining_domains(node_type) if self.is_exclusive_domain else set()
        )

    @staticmethod
    def _sort_by_termination(nodes: List[SchedulingNode]) -> None:
        nodes.sort(
            key=lambda n: (
                ResourceDemandScheduler._sort_nodes_for_termination(n),
                n.im_instance_id or "",
            )
        )

    def _is_nonprov_lid_draining(self, lid: str) -> bool:
        return (
            lid in self.active_draining_lids
            and self.non_stopping_by_lid[lid] % self.size != 0
        )

    def _placeholder_key(
        self, rep: _Replica, prov_rid: Optional[str]
    ) -> Tuple[str, str, str]:
        if prov_rid:
            return (self.node_type, "prov", prov_rid)
        if (
            self.is_exclusive_domain
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
        self, rep: _Replica, allow_refill: bool = False
    ) -> int:
        if rep.no_backfill and not (allow_refill and rep.allow_refill_attach):
            return 0
        count = len(rep.members)
        if count <= 0:
            return self.size
        if rep.allow_multi_replica:
            min_reps = (count + self.size - 1) // self.size
            if not self.is_exclusive_domain:
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
    ) -> int:
        take = min(
            self._open_slots_for_placement(rep, allow_refill=allow_refill),
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
    ) -> None:
        if count <= 0:
            return
        phs: List[SchedulingNode] = []
        for _ in range(count):
            ph = SchedulingNode.from_node_config(
                self.cfg,
                status=SchedulingNodeStatus.SCHEDULABLE,
                node_kind=NodeKind.WORKER,
                im_instance_id="",
                im_instance_status=None,
                provider_replica_id=prov_rid,
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

    def _group_shared_domain_replicas(
        self, shared_running_by_dom: Dict[str, List[SchedulingNode]]
    ) -> None:
        """Group running nodes in non-exclusive domains without provider replica IDs."""
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
            for lid, raw_nodes in groups:
                self._sort_by_termination(raw_nodes)
                for group_nodes in self._split_stopping_chunks(raw_nodes):
                    rep = self._new_replica(
                        domain=dom_val,
                        lids={lid} if lid else None,
                        allow_multi_replica=True,
                        no_backfill=lid is None,
                    )
                    for m in group_nodes:
                        m.replica_id = rep.rid
                        m._has_runtime_domain = True
                        rep.members.append(m)

    def _tag_draining_and_lids(self) -> None:
        for rep in self.replicas.values():
            for m in rep.members:
                if m.launch_request_id:
                    rep.lids.add(m.launch_request_id)
            is_td = not rep.is_provider and bool(rep.lids & self.torn_down_lids)
            if is_td:
                rep.no_backfill = True
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
                    and any(self._is_nonprov_lid_draining(lid) for lid in rep.lids)
                )
            ):
                for m in rep.members:
                    m._is_draining_replica = True
            elif is_td:
                rep.allow_refill_attach = True

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
                    ) or self._is_nonprov_lid_draining(lid)
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
            if len(lid_nodes) % self.size == 0 or has_booting or not self.dom_key:
                for grp in self._split_stopping_chunks(lid_nodes):
                    is_draining = self._is_nonprov_lid_draining(lid) or any(
                        m.im_instance_status == Instance.RAY_STOP_REQUESTED for m in grp
                    )
                    is_td = lid in self.torn_down_lids
                    rep = self._new_replica(
                        lids={lid},
                        shared_runtime_labels=self._compute_shared_runtime_labels(
                            [m for m in grp if m.is_running]
                        ),
                        allow_multi_replica=(
                            not self.is_exclusive_domain
                            or (len(grp) > self.size and len(grp) % self.size == 0)
                        ),
                        no_backfill=is_td,
                        allow_refill_attach=is_td and not is_draining,
                    )
                    for m in grp:
                        m.replica_id = rep.rid
                        m._has_runtime_domain = True
                        if is_draining:
                            m._is_draining_replica = True
                        rep.members.append(m)
            else:
                unmatched.extend(lid_nodes)

        if unmatched:
            self._sort_by_termination(unmatched)
            chunk = self.size if not self.dom_key else 1
            for i in range(0, len(unmatched), chunk):
                slice_nodes = unmatched[i : i + chunk]
                rep = self._new_replica(no_backfill=True)
                is_draining = any(
                    m.im_instance_status == Instance.RAY_STOP_REQUESTED
                    or (
                        m.launch_request_id
                        and self._is_nonprov_lid_draining(m.launch_request_id)
                    )
                    for m in slice_nodes
                )
                for m in slice_nodes:
                    m.replica_id = rep.rid
                    if is_draining:
                        m._is_draining_replica = True
                    rep.members.append(m)

    def _inc_priority(
        self,
        members: List[SchedulingNode],
        pinned_domains: Dict[str, Set[str]],
        eff_idle_ms: Optional[float],
    ) -> int:
        if ResourceDemandScheduler._is_replica_pinned(
            members, pinned_domains, self.dom_key
        ):
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

    def _attach_refill_remainder(self, src_nodes: List[SchedulingNode]) -> None:
        """Attach sub-replica refill pods (`len(src_nodes) % size`) to torn-down
        incomplete non-provider replicas."""
        rem = len(src_nodes) % self.size
        if rem <= 0 or not self.replicas:
            return
        pinned_domains = self._ctx.peek_pinned_domains()
        eff_idle_s = (
            self.cfg.idle_timeout_s
            if self.cfg.idle_timeout_s is not None
            else self._ctx.get_idle_timeout_s()
        )
        eff_idle_ms = eff_idle_s * 1000 if eff_idle_s is not None else None
        candidates = sorted(
            (
                rep
                for rep in self.replicas.values()
                if not rep.is_provider
                and rep.allow_refill_attach
                and self._open_slots_for_placement(rep, allow_refill=True) > 0
                and not any(m.is_leaving or m._is_draining_replica for m in rep.members)
            ),
            key=lambda rep: (
                self._inc_priority(rep.members, pinned_domains, eff_idle_ms),
                self._open_slots_for_placement(rep, allow_refill=True),
                rep.rid,
            ),
        )
        for rep in candidates:
            if rem <= 0 or not src_nodes:
                break
            open_cnt = self._open_slots_for_placement(rep, allow_refill=True)
            placed_lids = {
                n.launch_request_id
                for n in src_nodes[: min(rem, open_cnt)]
                if n.launch_request_id
            }
            took = self._place(
                rep, src_nodes, rem, known=True, draining=False, allow_refill=True
            )
            if took > 0:
                rep.lids.update(placed_lids)
                rem -= took

    def _attach_attributed_booting(
        self, booting_with_lid: Dict[str, List[SchedulingNode]]
    ) -> Tuple[List[Tuple[_Replica, int]], List[SchedulingNode]]:
        """Attach booting nodes with a launch_request_id to matching replicas."""
        full_slots = list(range(1, self.size)) + [0]
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
        has_cross_slice_lid = self.is_exclusive_domain and (
            any(cnt > 1 for cnt in inc_lids.values())
            or any(len(r.lids) > 1 for r in nonprov_reps)
        )

        for lid, lid_nodes in sorted(booting_with_lid.items()):
            matching_reps = sorted(
                (rep for rep in self.replicas.values() if lid in rep.lids),
                key=lambda rep: (
                    self._open_slots_for_placement(rep) == 0,
                    not ResourceDemandScheduler._is_replica_pinned(
                        rep.members, pinned_domains, self.dom_key
                    ),
                    self._open_slots_for_placement(rep),
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
                    rep, lid_nodes, len(lid_nodes), known=True, draining=rep_draining
                )
            if not lid_nodes:
                continue
            is_lid_torn_down = lid in self.torn_down_lids
            if not is_lid_torn_down and len(lid_nodes) % self.size != 0:
                self._attach_refill_remainder(lid_nodes)
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
                rep = _Replica(
                    rid=self._ctx.new_pending_domain(self.node_type),
                    lids={lid},
                )
                rep.domain = rep.rid
                self._place(rep, lid_nodes, len(lid_nodes), known=True, draining=False)
                vanished_batches.append(
                    (rep, min(self.size - len(rep.members), len(vanished_cids)))
                )
                continue
            total_nonprov_open = sum(
                self._open_slots_for_placement(r)
                for r in self.replicas.values()
                if not r.is_provider and not any(m.is_leaving for m in r.members)
            )
            if (
                has_cross_slice_lid
                and not is_lid_torn_down
                and (len(lid_nodes) % self.size != 0 or total_nonprov_open >= self.size)
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
                is_chunk_draining = self._is_nonprov_lid_draining(lid) or (
                    is_lid_torn_down and all(m.progress_rank >= 2 for m in chunk_nodes)
                )
                td_rep = (
                    self._new_replica(
                        rid=rid,
                        lids={lid},
                        no_backfill=True,
                        allow_refill_attach=not is_chunk_draining,
                    )
                    if is_lid_torn_down and len(chunk_nodes) < self.size
                    else None
                )
                for node, h in zip(chunk_nodes, full_slots):
                    _assign_node_replica_slot(node, self.cfg, rid, h)
                    if is_chunk_draining:
                        node._is_draining_replica = True
                    if td_rep is not None:
                        td_rep.members.append(node)
        return vanished_batches, scrambled_cloud_booting

    def _attach_unattributed_booting(
        self,
        booting_no_lid: List[SchedulingNode],
        fill_all_open: bool = False,
    ) -> List[SchedulingNode]:
        """Attach unattributed or cross-slice cloud-booting nodes to incomplete replicas."""
        if not (self.replicas and booting_no_lid):
            return booting_no_lid
        pinned_domains = self._ctx.peek_pinned_domains()
        eff_idle_s = (
            self.cfg.idle_timeout_s
            if self.cfg.idle_timeout_s is not None
            else self._ctx.get_idle_timeout_s()
        )
        eff_idle_ms = eff_idle_s * 1000 if eff_idle_s is not None else None
        incomplete = sorted(
            (
                rep
                for rep in self.replicas.values()
                if self._open_slots_for_placement(rep) > 0
                and not any(m.is_leaving for m in rep.members)
            ),
            key=lambda rep: (
                self._inc_priority(rep.members, pinned_domains, eff_idle_ms),
                self._open_slots_for_placement(rep),
                rep.rid,
            ),
        )
        cloud_booting = list(booting_no_lid)
        prov_rids = {rep.rid for rep in incomplete if rep.is_provider}
        for rep in incomplete:
            if rep.rid in prov_rids:
                continue
            if ResourceDemandScheduler._is_replica_pinned(
                rep.members, pinned_domains, self.dom_key
            ) or not any(m.is_running for m in rep.members):
                self._place(
                    rep,
                    cloud_booting,
                    self._open_slots_for_placement(rep),
                    known=False,
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
            spare = len(cloud_booting) % self.size
        for rep in incomplete:
            open_cnt = self._open_slots_for_placement(rep)
            if open_cnt == 0 or spare <= 0 or rep.rid in prov_rids:
                continue
            spare -= self._place(rep, cloud_booting, spare, known=False)
        return cloud_booting

    def _populate_backfill_placeholders(
        self, vanished_batches: List[Tuple[_Replica, int]]
    ) -> List[SchedulingNode]:
        """Add committed missing-slot backfill placeholders for incomplete replicas."""
        placeholders: List[SchedulingNode] = []
        real_active = sum(
            1
            for n in self.type_nodes
            if not n.is_leaving
            and not n._is_draining_replica
            and not (
                n.status == SchedulingNodeStatus.TO_TERMINATE
                and n.im_instance_status == Instance.QUEUED
            )
        )
        remaining_ph = max(0, self.cfg.max_worker_nodes - real_active)
        if self.is_shared_domain:
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
                if len(l_reps) <= 1:
                    continue
                total_cnt = sum(len(r.members) for r in l_reps)
                lid_deficit = (-total_cnt) % self.size
                sum_open = sum(self._open_slots_for_placement(r) for r in l_reps)
                if sum_open > lid_deficit:
                    for r in l_reps:
                        r.no_backfill = True
                    if lid_deficit > 0 and lid not in self.torn_down_lids:
                        pkey = (self.node_type, "lid", lid)
                        self.active_placeholder_keys.add(pkey)
                        if pkey not in self.expired_placeholder_keys:
                            self.extra_lid_backfill += min(lid_deficit, remaining_ph)
                            remaining_ph = max(0, remaining_ph - lid_deficit)

        for rep in sorted(self.replicas.values(), key=lambda r: r.rid):
            if (
                rep.no_backfill
                or any(m.is_leaving for m in rep.members)
                or not any(
                    m.provider_replica_id or m.launch_request_id for m in rep.members
                )
            ):
                continue
            open_slots = self._open_slots_for_placement(rep)
            if open_slots == 0:
                continue
            prov_rid = next(
                (m.provider_replica_id for m in rep.members if m.provider_replica_id),
                None,
            )
            pkey = self._placeholder_key(rep, prov_rid)
            self.active_placeholder_keys.add(pkey)
            if pkey in self.expired_placeholder_keys:
                rep.no_backfill = True
                continue
            emit_cnt = min(open_slots, remaining_ph)
            if emit_cnt <= 0:
                continue
            self._add_placeholders(rep, emit_cnt, prov_rid, placeholders)
            remaining_ph -= emit_cnt
        for rep, missing_cnt in vanished_batches:
            self.replicas[rep.rid] = rep
            pkey = self._placeholder_key(rep, None)
            self.active_placeholder_keys.add(pkey)
            if pkey in self.expired_placeholder_keys:
                rep.no_backfill = True
                continue
            emit_cnt = min(missing_cnt, remaining_ph)
            if emit_cnt <= 0:
                continue
            self._add_placeholders(rep, emit_cnt, None, placeholders)
            remaining_ph -= emit_cnt
        return placeholders

    def group(self) -> _ReplicaGroupResult:
        """Group all worker nodes of `self.node_type` and return backfill metadata."""
        if self.size == 1:
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
                        n, self.cfg, self._ctx.new_pending_domain(self.node_type), 0
                    )
            if running_unlabeled:
                self._sort_by_termination(running_unlabeled)
                for m in running_unlabeled:
                    m.replica_id = self._ctx.new_pending_domain(self.node_type)
            return _ReplicaGroupResult(
                [], self.replicas, self.active_placeholder_keys, 0
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

        # Stage 1: Authoritative physical/runtime replica grouping.
        self._group_provider_replicas(by_prov)
        self._group_shared_domain_replicas(shared_running_by_dom)
        self._tag_draining_and_lids()

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

        self._group_unlabeled_running(running_unlabeled, booting_with_lid)

        # Stage 2: Exact launch_request_id attachment for booting pods.
        vanished_batches, scrambled_cloud = self._attach_attributed_booting(
            booting_with_lid
        )
        if queued_nodes and len(queued_nodes) % self.size != 0:
            if not any(
                p.launch_request_id and p.launch_request_id in self.torn_down_lids
                for p in queued_nodes
            ):
                self._attach_refill_remainder(queued_nodes)

        # Stage 3: Unattributed and cross-slice cloud-booting attachment.
        if scrambled_cloud:
            scrambled_cloud = self._attach_unattributed_booting(
                scrambled_cloud, fill_all_open=True
            )
        booting_no_lid = scrambled_cloud + self._attach_unattributed_booting(
            booting_no_lid
        )
        full_slots = list(range(1, self.size)) + [0]
        for group in (booting_no_lid, queued_nodes, other_pending):
            for i in range(0, len(group), self.size):
                rid = self._ctx.new_pending_domain(self.node_type)
                chunk_nodes = group[i : i + self.size]
                for p, h in zip(chunk_nodes, full_slots):
                    _assign_node_replica_slot(p, self.cfg, rid, h)
                    if p.im_instance_status == Instance.RAY_STOP_REQUESTED or (
                        p.launch_request_id
                        and (
                            self._is_nonprov_lid_draining(p.launch_request_id)
                            or (
                                p.progress_rank >= 2
                                and p.launch_request_id in self.torn_down_lids
                            )
                        )
                    ):
                        p._is_draining_replica = True

        # Stage 4: Committed missing-slot backfill placeholders.
        placeholders = self._populate_backfill_placeholders(vanished_batches)
        return _ReplicaGroupResult(
            placeholders=placeholders,
            replicas=self.replicas,
            active_placeholder_keys=self.active_placeholder_keys,
            extra_lid_backfill=self.extra_lid_backfill,
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
        is_exclusive = (
            bool(cfg.domain_label_key and cfg.domain_exclusive)
            if cfg is not None
            else False
        )
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
            draining_domains: Optional[Dict[NodeType, Set[str]]] = None,
            active_draining_lids: Optional[Dict[NodeType, Set[str]]] = None,
            expired_placeholder_keys: Optional[Set[Tuple[str, str, str]]] = None,
            replica_view: Optional[_MultiHostReplicaView] = None,
        ):
            self._nodes = nodes
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
            self._draining_domains: Dict[NodeType, Set[str]] = {
                k: set(v) for k, v in (draining_domains or {}).items()
            }
            self._active_draining_lids: Dict[NodeType, Set[str]] = {
                k: set(v) for k, v in (active_draining_lids or {}).items()
            }
            self._expired_placeholder_keys: Set[Tuple[str, str, str]] = set(
                expired_placeholder_keys or ()
            )
            self._active_placeholder_keys: Set[Tuple[str, str, str]] = set()
            self._replicas_by_type: Dict[NodeType, Dict[str, _Replica]] = {}
            self._extra_lid_backfill: Dict[NodeType, int] = {}
            self._unbacked_refill_gaps: Dict[NodeType, int] = {}
            self._replica_view = replica_view or _MultiHostReplicaView()
            self._init_unlabeled_aligned: Set[NodeType] = {
                nt
                for nt, cfg in self._node_type_configs.items()
                if cfg.is_multi_worker_replica
                and self._is_unlabeled_view_aligned_at_init(nt, cfg.workers_per_replica)
            }
            self._init_replicas()
            self._refresh_capacity_stats()

        def get_active_placeholder_keys(self) -> Set[Tuple[str, str, str]]:
            return set(self._active_placeholder_keys)

        def get_expired_placeholder_keys(self) -> Set[Tuple[str, str, str]]:
            return self._expired_placeholder_keys

        def get_replica_view(self) -> _MultiHostReplicaView:
            return self._replica_view

        def get_draining_domains(self, node_type: NodeType) -> Set[str]:
            return self._draining_domains.get(node_type, set())

        def get_active_draining_lids(self, node_type: NodeType) -> Set[str]:
            return self._active_draining_lids.get(node_type, set())

        def peek_pinned_domains(self) -> Dict[str, Set[str]]:
            return self._pinned_domains

        def get_replicas_by_type(self) -> Dict[NodeType, Dict[str, _Replica]]:
            return self._replicas_by_type

        def get_unbacked_refill_gap(self, node_type: NodeType) -> int:
            return self._unbacked_refill_gaps.get(node_type, 0)

        def min_launch_batch_size(self, node_type: NodeType) -> int:
            """Return minimum launchable batch size for `node_type`."""
            gap = self._unbacked_refill_gaps.get(node_type, 0)
            if gap > 0:
                return gap
            cfg = self._node_type_configs.get(node_type)
            return cfg.workers_per_replica if cfg is not None else 1

        def _is_unlabeled_view_aligned_at_init(
            self, node_type: NodeType, size: int
        ) -> bool:
            if not self._replica_view.unlabeled_occupying_cids.get(node_type):
                return False
            view_gap = self._replica_view.unlabeled_backfill_gap(node_type, size)
            non_prov_occ = sum(
                1
                for n in self._nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and n.status
                not in (
                    SchedulingNodeStatus.TO_TERMINATE,
                    SchedulingNodeStatus.TO_LAUNCH,
                )
                and n.im_instance_status != Instance.QUEUED
                and not n._is_backfill_placeholder
                and not n.provider_replica_id
            )
            return view_gap == (-non_prov_occ) % size

        def _effective_unlabeled_gap(
            self, node_type: NodeType, size: int, extra_queued: int = 0
        ) -> int:
            if node_type not in self._init_unlabeled_aligned:
                return 0
            term_pend = sum(
                1
                for n in self._nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and not n._is_backfill_placeholder
                and not n.provider_replica_id
                and n.status == SchedulingNodeStatus.TO_TERMINATE
                and n.im_instance_status
                in (Instance.ALLOCATED, Instance.RAY_INSTALLING)
            )
            return self._replica_view.unlabeled_backfill_gap(
                node_type, size, extra_queued=extra_queued - term_pend
            )

        def get_unlabeled_backfill_gap(self, node_type: NodeType, size: int) -> int:
            return self._effective_unlabeled_gap(node_type, size)

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
            if cfg.replica_config is None:
                return
            type_nodes = [
                n
                for n in self._nodes
                if n.node_type == node_type
                and n.node_kind != NodeKind.HEAD
                and n.status != SchedulingNodeStatus.TO_TERMINATE
            ]
            res = _ReplicaGrouper(self, node_type, cfg, type_nodes).group()
            self._replicas_by_type[node_type] = res.replicas
            self._active_placeholder_keys.update(res.active_placeholder_keys)
            if res.extra_lid_backfill > 0:
                self._extra_lid_backfill[node_type] = res.extra_lid_backfill
            if res.placeholders:
                self._nodes.extend(res.placeholders)

        def _init_replicas(self) -> None:
            """Group running and pending worker nodes into canonical fixed replicas."""
            for node_type, cfg in self._node_type_configs.items():
                self._init_replicas_for_node_type(node_type, cfg)

        @staticmethod
        def _missing_replica_backfill_by_type(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
            replicas_by_type: Optional[Dict[NodeType, Dict[str, _Replica]]] = None,
            extra_lid_backfill: Optional[Dict[NodeType, int]] = None,
            torn_down_lids: Optional[Set[Tuple[NodeType, str]]] = None,
        ) -> Dict[NodeType, int]:
            """Count missing host slots across live incomplete replicas whose
            replacement pods have not yet appeared in `nodes`."""
            missing: Dict[NodeType, int] = {}
            td_lids = torn_down_lids or set()
            for nt, cfg in node_type_configs.items():
                if not cfg.is_multi_worker_replica:
                    continue
                size = cfg.workers_per_replica
                reps = (replicas_by_type or {}).get(nt, {})
                extra_lid_gap = (extra_lid_backfill or {}).get(nt, 0)
                k8s_count = booting_count = 0
                dom_counts: Counter = Counter()
                draining_dom_counts: Counter = Counter()
                draining_unassigned = 0
                for n in nodes:
                    if (
                        n.node_type != nt
                        or n.node_kind == NodeKind.HEAD
                        or n.status
                        in (
                            SchedulingNodeStatus.TO_TERMINATE,
                            SchedulingNodeStatus.TO_LAUNCH,
                        )
                        or n.im_instance_status == Instance.QUEUED
                    ):
                        continue
                    if n._is_draining_replica:
                        if n.replica_id:
                            draining_dom_counts[n.replica_id] += 1
                        else:
                            draining_unassigned += 1
                        continue
                    k8s_count += 1
                    if n.replica_id and (
                        n._has_runtime_domain
                        or (
                            n.launch_request_id
                            and (nt, n.launch_request_id) in td_lids
                            and n.replica_id in reps
                        )
                    ):
                        dom_counts[n.replica_id] += 1
                    elif not n.is_running:
                        booting_count += 1

                def _slots_for_rid(rid: str, cnt: int) -> Tuple[int, int]:
                    rep = reps.get(rid)
                    if rep is not None:
                        if rep.no_backfill:
                            return 0, cnt
                        return _Replica._slots_for(cnt, size, rep.allow_multi_replica)
                    return _Replica._slots_for(
                        cnt,
                        size,
                        not (cfg.domain_label_key and cfg.domain_exclusive)
                        or rid.startswith(_PENDING_DOMAIN_PREFIX),
                    )

                draining_gap = sum(
                    _slots_for_rid(rid, cnt)[0]
                    for rid, cnt in draining_dom_counts.items()
                )
                if draining_unassigned:
                    draining_gap += (size - draining_unassigned % size) % size
                dom_committed = 0
                raw_gap = 0
                unrounded_cnt = 0
                for rid, cnt in dom_counts.items():
                    rep = reps.get(rid)
                    if rep is not None and rep.no_backfill:
                        unrounded_cnt += cnt
                        dom_committed += cnt
                        continue
                    rep_open, rep_committed = _slots_for_rid(rid, cnt)
                    raw_gap += rep_open
                    dom_committed += rep_committed
                extra = (max(0, booting_count - raw_gap) + size - 1) // size
                roundable_k8s = max(0, k8s_count - unrounded_cnt)
                committed = max(
                    ((roundable_k8s + size - 1) // size) * size
                    + unrounded_cnt
                    + extra_lid_gap,
                    dom_committed + extra * size + extra_lid_gap,
                )
                healthy_gap = max(0, committed - k8s_count)
                total_gap = draining_gap + healthy_gap
                if total_gap > 0:
                    missing[nt] = total_gap
            return missing

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
            size = rc.workers_per_replica
            rid = self.new_pending_domain(node_type)
            replica = []
            for h in list(range(1, size)) + [0]:
                node = self._make_launch_node(node_type, include_ippr_caps)
                _assign_node_replica_slot(node, cfg, rid, h)
                replica.append(node)
            return replica

        def make_refill_nodes(
            self,
            node_type: NodeType,
            count: int,
            target_domain: Optional[str] = None,
            include_ippr_caps: bool = True,
        ) -> List[SchedulingNode]:
            """Create up to `count` TO_LAUNCH refill nodes for incomplete unlabeled
            replicas of `node_type` without exceeding per-replica capacity."""
            if count <= 0:
                return []
            cfg = self._node_type_configs[node_type]
            size = cfg.workers_per_replica
            is_exclusive = bool(cfg.domain_label_key and cfg.domain_exclusive)
            reps = self._replicas_by_type.get(node_type, {})
            live_by_rid: Dict[str, List[SchedulingNode]] = defaultdict(list)
            for n in self._nodes:
                if (
                    n.node_type == node_type
                    and n.node_kind != NodeKind.HEAD
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                    and not n.is_leaving
                    and not n._is_draining_replica
                    and n.replica_id
                ):
                    live_by_rid[n.replica_id].append(n)

            def _rep_open(rep: _Replica) -> int:
                if rep.is_provider or any(
                    m.is_leaving or m._is_draining_replica for m in rep.members
                ):
                    return 0
                live_m = live_by_rid.get(rep.rid, [])
                if not live_m:
                    return 0
                if is_exclusive:
                    return max(0, size - len(live_m))
                return (-len(live_m)) % size

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
                )
                return batch

            if target_domain is not None:
                cand = reps.get(target_domain)
                if cand is None:
                    return []
                take = min(count, _rep_open(cand))
                if take <= 0:
                    return []
                return _emit_on(
                    live_by_rid[cand.rid],
                    take,
                    cand.rid,
                    cand.domain,
                    cand.shared_runtime_labels,
                )

            refill: List[SchedulingNode] = []
            remaining = count
            for rep in sorted(
                reps.values(),
                key=lambda r: (
                    not r.no_backfill,
                    r.domain is None or r.domain.startswith(_PENDING_DOMAIN_PREFIX),
                    _rep_open(r) == 0,
                    r.rid,
                ),
            ):
                if remaining <= 0:
                    break
                open_cnt = _rep_open(rep)
                if open_cnt <= 0:
                    continue
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
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Create a schedule context from a schedule request.
            It will populate the context with the existing nodes and the available node
            types from the config.

            Args:
                req: The scheduling request. The caller should make sure the
                    request is valid.
                expired_placeholder_keys: Replica keys whose backfill
                    placeholders have exceeded `pending_idle_timeout_s`.

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
            active_draining_lids: Dict[NodeType, Set[str]] = defaultdict(set)
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
                    cid = im.cloud_instance_id
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
                    cid = im.cloud_instance_id
                    if not _is_shadowed_stopped(nt, st, cid, active_nt_cids) and st in (
                        _DRAINING_INSTANCE_STATUSES | _TEARDOWN_INSTANCE_STATUSES
                    ):
                        if im.launch_request_id:
                            active_draining_lids[nt].add(im.launch_request_id)
                        if (
                            cfg.domain_exclusive
                            and cfg.domain_label_key
                            and instance.ray_node is not None
                        ):
                            dom = (instance.ray_node.labels or {}).get(
                                cfg.domain_label_key
                            )
                            if dom:
                                draining_domains[nt].add(dom)

            # Initialize the scheduling nodes.
            for instance in req.current_instances:
                node = SchedulingNode.new(
                    instance,
                    node_type_configs,
                    req.disable_launch_config_check,
                )
                if node:
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
                draining_domains=draining_domains,
                active_draining_lids=active_draining_lids,
                expired_placeholder_keys=expired_placeholder_keys,
                replica_view=req.replica_view,
            )

        @staticmethod
        def _compute_available_node_types(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
            missing_backfill: Dict[NodeType, int],
        ) -> Dict[NodeType, int]:
            """Compute the number of nodes by node types available for launching based on
            the max number of workers in the config.
            Args:
                nodes: The current existing nodes.
                node_type_configs: The node type configs.
                missing_backfill: Precomputed missing replica backfill slots.
            Returns:
                A dict of node types and the number of nodes available for launching.
            """
            node_type_available: Dict[NodeType, int] = defaultdict(int)
            node_type_existing: Dict[NodeType, int] = defaultdict(int)
            for node in nodes:
                if (
                    node.status == SchedulingNodeStatus.TO_TERMINATE
                    and node.im_instance_status == Instance.QUEUED
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
            for node in self._nodes:
                if node.status == SchedulingNodeStatus.TO_TERMINATE:
                    # Skip the nodes that are to be terminated.
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

        def get_idle_timeout_s(self) -> Optional[float]:
            return self._idle_timeout_s

        def get_pending_idle_timeout_s(self) -> Optional[float]:
            return self._pending_idle_timeout_s

        def get_cloud_resource_availabilities(self) -> Dict[NodeType, float]:
            return copy.deepcopy(self._cloud_resource_availabilities)

        def get_recoverable_resource_availabilities(self) -> Dict[NodeType, float]:
            return copy.deepcopy(self._recoverable_resource_availabilities)

        def _refresh_capacity_stats(self) -> None:
            self._missing_replica_backfill: Dict[
                NodeType, int
            ] = self._missing_replica_backfill_by_type(
                self._nodes,
                self._node_type_configs,
                self._replicas_by_type,
                self._extra_lid_backfill,
                self._replica_view.torn_down_lids,
            )
            self._node_type_available = self._compute_available_node_types(
                self._nodes,
                self._node_type_configs,
                missing_backfill=self._missing_replica_backfill,
            )
            active_nodes = sum(
                1
                for n in self._nodes
                if not (
                    n.status == SchedulingNodeStatus.TO_TERMINATE
                    and n.im_instance_status == Instance.QUEUED
                )
            )
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
                extra_queued = sum(
                    1
                    for n in self._nodes
                    if n.node_type == nt
                    and n.node_kind != NodeKind.HEAD
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                    and (
                        n.im_instance_status == Instance.QUEUED
                        or n.status == SchedulingNodeStatus.TO_LAUNCH
                    )
                )
                raw_gap = self._effective_unlabeled_gap(
                    nt, size, extra_queued=extra_queued
                )
                if raw_gap <= 0:
                    continue
                non_prov_ph = sum(
                    1
                    for n in self._nodes
                    if n.node_type == nt
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                    and n._is_backfill_placeholder
                    and not n.provider_replica_id
                )
                gap = max(
                    0,
                    raw_gap - non_prov_ph - self._missing_replica_backfill.get(nt, 0),
                )
                if 0 < gap < size:
                    refill_gaps[nt] = gap
            self._unbacked_refill_gaps = refill_gaps

        def update(self, new_nodes: List[SchedulingNode]) -> None:
            """Update the context with the new nodes."""
            self._nodes = new_nodes
            self._refresh_capacity_stats()

        def with_ordered_nodes(
            self, nodes: List[SchedulingNode]
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Return a shallow copy of this context with reordered `nodes`."""
            ctx = copy.copy(self)
            ctx._nodes = nodes
            return ctx

        def restricted_to(
            self,
            nodes: List[SchedulingNode],
            node_type_available: Optional[Dict[NodeType, int]] = None,
            clear_refill_gaps: bool = False,
            headroom_delta: int = 0,
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Return a shallow copy of this context restricted to `nodes`."""
            ctx = copy.copy(self)
            ctx._nodes = nodes
            ctx._node_type_available = (
                dict(node_type_available) if node_type_available is not None else {}
            )
            if clear_refill_gaps:
                ctx._unbacked_refill_gaps = {}
            if headroom_delta and ctx._headroom is not None:
                ctx._headroom = max(0, ctx._headroom + headroom_delta)
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

        def get_ippr_specs(self) -> Optional[IPPRSpecs]:
            """Return typed IPPR specs if present on the scheduling request."""
            return self._ippr_specs

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
            refill_by_type = defaultdict(int)
            for node in self._nodes:
                if node.status == SchedulingNodeStatus.TO_LAUNCH:
                    launch_by_type[node.node_type] += 1
                    if node._is_unlabeled_refill:
                        refill_by_type[node.node_type] += 1

            launch_requests = []
            for instance_type, count in launch_by_type.items():
                refill_cnt = refill_by_type.get(instance_type, 0)
                if refill_cnt > 0:
                    size = self._node_type_configs[instance_type].workers_per_replica
                    rem = refill_cnt % size
                    if rem > 0:
                        count += size - rem
                launch_requests.append(
                    LaunchRequest(
                        instance_type=instance_type,
                        count=count,
                        id=str(uuid.uuid4()),
                        request_ts_ms=time.time_ns() // 1000,
                    )
                )
            return launch_requests

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
            request.pending_idle_timeout_s
            if request.pending_idle_timeout_s is not None
            else float(InstanceReconcileConfig().request_status_timeout_s)
        )
        expired_placeholder_keys = {
            k
            for k, since_s in self._placeholder_since_s.items()
            if now_s - since_s >= placeholder_ttl_s
        }
        ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(
            request,
            expired_placeholder_keys=expired_placeholder_keys,
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

        # Enforce the minimal count of nodes for each worker node type.
        ResourceDemandScheduler._enforce_min_workers_per_type(ctx)

        # Enforce the max worker nodes count.
        ResourceDemandScheduler._enforce_max_workers_per_type(ctx)

        # Enforce the max worker nodes count globally.
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
        infeasible_requests = ResourceDemandScheduler._sched_resource_requests(
            ctx,
            ResourceRequestUtil.ungroup_by_count(request.resource_requests),
        )

        # Pending nodes not needed by the resource demands are considered idle.
        if ctx.get_pending_idle_timeout_s() is not None:
            self._update_pending_nodes_idle_duration(ctx)

        # Shutdown any idle nodes that's not needed (e.g. no resource constraints.
        # not needed by min_worker count, etc.)
        ResourceDemandScheduler._enforce_idle_termination(ctx)
        if ctx.get_pending_idle_timeout_s() is not None:
            self._release_terminated_pending_nodes_idle_duration(ctx)

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

        # Get all the nodes by type
        all_nodes = ctx.get_nodes()

        non_terminating_nodes_by_type = defaultdict(list)
        terminating_nodes = []
        for node in all_nodes:
            if node.status == SchedulingNodeStatus.TO_TERMINATE:
                terminating_nodes.append(node)
            else:
                non_terminating_nodes_by_type[node.node_type].append(node)

        replica_sizes = ctx.get_multihost_replica_sizes()
        pinned_domains = ctx.get_pinned_domains()
        replicas_by_type = ctx.get_replicas_by_type()
        # Step 1. Enforce the max number of workers for each node type.
        for node_type in non_terminating_nodes_by_type.keys():
            non_terminate_nodes_of_type = non_terminating_nodes_by_type[node_type]
            node_config = ctx.get_node_type_configs()[node_type]
            num_max_nodes_per_type = node_config.max_worker_nodes
            real_count = sum(
                1 for n in non_terminate_nodes_of_type if not n._is_backfill_placeholder
            )
            num_extra_nodes = real_count - num_max_nodes_per_type

            if num_extra_nodes <= 0:
                # No extra nodes for this type, continue.
                continue

            # Terminate the nodes
            (
                to_terminate,
                remained_nodes,
            ) = ResourceDemandScheduler._select_nodes_to_terminate(
                non_terminate_nodes_of_type,
                num_extra_nodes,
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                max_num_nodes_per_type=num_max_nodes_per_type,
                replica_sizes=replica_sizes,
                node_type_configs=ctx.get_node_type_configs(),
                pinned_domains=pinned_domains,
                replicas_by_type=replicas_by_type,
            )

            non_terminating_nodes_by_type[node_type] = remained_nodes
            terminating_nodes.extend(to_terminate)

        non_terminating_nodes = []
        for nodes in non_terminating_nodes_by_type.values():
            non_terminating_nodes.extend(nodes)

        # Update the context
        assert len(all_nodes) == (
            len(terminating_nodes) + len(non_terminating_nodes)
        ), "The number of nodes should be the same after enforcing max nodes per type."

        ctx.update(terminating_nodes + non_terminating_nodes)

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
        all_nodes = ctx.get_nodes()

        terminating_nodes = []
        non_terminating_nodes = []

        for node in all_nodes:
            if node.status == SchedulingNodeStatus.TO_TERMINATE:
                terminating_nodes.append(node)
            else:
                non_terminating_nodes.append(node)

        num_max_nodes = ctx.get_max_num_nodes()
        real_non_terminating = sum(
            1 for n in non_terminating_nodes if not n._is_backfill_placeholder
        )

        num_to_terminate = (
            max(real_non_terminating - num_max_nodes, 0) if num_max_nodes else 0
        )

        if num_to_terminate <= 0:
            # No extra nodes needed to terminate.
            return

        # Terminate the nodes
        (
            to_terminate_nodes,
            non_terminating_nodes,
        ) = ResourceDemandScheduler._select_nodes_to_terminate(
            non_terminating_nodes,
            num_to_terminate,
            TerminationRequest.Cause.MAX_NUM_NODES,
            max_num_nodes=num_max_nodes,
            replica_sizes=ctx.get_multihost_replica_sizes(),
            node_type_configs=ctx.get_node_type_configs(),
            pinned_domains=ctx.get_pinned_domains(),
            replicas_by_type=ctx.get_replicas_by_type(),
        )

        # Multi-worker replica groups terminate in whole replica units, so total
        # terminated nodes may exceed `num_to_terminate`.
        terminated_real = sum(
            1 for n in to_terminate_nodes if not n._is_backfill_placeholder
        )
        assert terminated_real >= num_to_terminate, (
            "Terminating {} real nodes, failed to terminate {} nodes to "
            "satisfy max_num_nodes={}".format(
                terminated_real,
                num_to_terminate - terminated_real,
                num_max_nodes,
            )
        )

        # Update the context
        terminating_nodes.extend(to_terminate_nodes)
        assert len(all_nodes) == (
            len(terminating_nodes) + len(non_terminating_nodes)
        ), "The number of nodes should be the same after enforcing max nodes."

        ctx.update(terminating_nodes + non_terminating_nodes)

    @staticmethod
    def _select_nodes_to_terminate(
        nodes: List[SchedulingNode],
        num_to_terminate: int,
        cause: TerminationRequest.Cause,
        max_num_nodes: Optional[int] = None,
        max_num_nodes_per_type: Optional[int] = None,
        replica_sizes: Optional[Dict[NodeType, int]] = None,
        node_type_configs: Optional[Dict[NodeType, NodeTypeConfig]] = None,
        pinned_domains: Optional[Dict[str, Set[str]]] = None,
        replicas_by_type: Optional[Dict[NodeType, Dict[str, _Replica]]] = None,
    ) -> Tuple[List[SchedulingNode], List[SchedulingNode]]:
        """Select 'num_to_terminate' of nodes to be terminated
        from the 'nodes' list. It should never select a head node.

        Args:
            nodes: The nodes to be terminated.
            num_to_terminate: The number of nodes to be terminated.
            cause: The cause of the termination. Should be one of
                TerminationRequest.Cause.MAX_NUM_NODES or
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE.

            max_num_nodes: The max number of nodes for the entire cluster only
                used when the cause is TerminationRequest.Cause.MAX_NUM_NODES.
            max_num_nodes_per_type: The max number of nodes for each node type.
                Only used when the cause is
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE.
            replica_sizes: Worker count per replica for multi-worker node types,
                so idle/pending replicas are terminated whole before active ones.
            node_type_configs: Node type configs used to look up domain labels.
            pinned_domains: Locality domains currently pinned by rescheduling gangs.
            replicas_by_type: Canonical grouped replica map per node type.

        Returns:
            A tuple of:
                - The terminated nodes.
                - The remained nodes.
        """
        # Remove the head node before grouping/sorting worker termination units.
        head_node = None
        for i, node in enumerate(nodes):
            if node.node_kind == NodeKind.HEAD:
                head_node = nodes.pop(i)
                break

        units = ResourceDemandScheduler._termination_units(
            nodes,
            replica_sizes or {},
            node_type_configs or {},
            pinned_domains or {},
            replicas_by_type,
        )
        terminated_nodes: List[SchedulingNode] = []
        terminated_real = 0
        remained_nodes: List[SchedulingNode] = []
        for unit in units:
            if terminated_real < num_to_terminate:
                terminated_nodes.extend(unit)
                terminated_real += sum(
                    1 for n in unit if not n._is_backfill_placeholder
                )
            else:
                remained_nodes.extend(unit)
        if head_node:
            remained_nodes.append(head_node)

        assert cause in [
            TerminationRequest.Cause.MAX_NUM_NODES,
            TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
        ], "Other termination causes don't have to select nodes for termination."

        for node in terminated_nodes:
            was_to_launch = node.status == SchedulingNodeStatus.TO_LAUNCH
            node.status = SchedulingNodeStatus.TO_TERMINATE
            if was_to_launch or node._is_backfill_placeholder:
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
            else:
                raise ValueError("Unknown termination cause: {}".format(cause))

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
        existing_nodes = ctx.get_nodes()

        new_nodes = []
        # Launch new nodes to satisfy min count for each worker node type.
        for (
            node_type,
            node_type_config,
        ) in ctx.get_node_type_configs().items():
            cur_count = count_by_node_type.get(node_type, 0) + missing_backfill.get(
                node_type, 0
            )
            min_count = node_type_config.min_worker_nodes
            if cur_count < min_count:
                deficit = min_count - cur_count
                logger.info(
                    f"Adding {deficit} nodes to satisfy min count for "
                    f"node type: {node_type}."
                )
                size = node_type_config.workers_per_replica
                refill_gap = ctx.get_unbacked_refill_gap(node_type)
                if 0 < refill_gap < size:
                    new_nodes.extend(
                        ctx.make_refill_nodes(
                            node_type, refill_gap, include_ippr_caps=False
                        )
                    )
                    deficit = max(0, deficit - refill_gap)
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
                if (
                    _has_label_constraints(req)
                    and node._satisfies_label_constraints(req) <= 0
                ):
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
            node._satisfies_label_constraints(r)
            for r in node.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            if len(r.label_selectors) > 1
        )

    @staticmethod
    def _unpack_gang_request(
        req: GangResourceRequest,
    ) -> Tuple[List[ResourceRequest], Optional[LocalityRequirement]]:
        """Extract bundle requests and any active STRICT_PACK locality requirement."""
        loc: Optional[LocalityRequirement] = None
        if req.bundle_selectors:
            bs = req.bundle_selectors[0]
            requests = list(bs.resource_requests)
            if bs.HasField("locality_requirement"):
                cand = bs.locality_requirement
                if (
                    cand.locality_constraint.placement_strategy
                    == PlacementStrategy.STRICT_PACK
                    and cand.locality_constraint.label_name
                ):
                    loc = cand
        else:
            requests = list(req.requests)
        return requests, loc

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
                n.is_running
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
            exclusive_domain_types if not pinned else [],
            key=search.node_type_rank,
            reverse=True,
        ):
            cfg = node_type_configs[nt]
            size = cfg.workers_per_replica
            refill_gap = gang_ctx.get_unbacked_refill_gap(nt)
            launch_res = gang_ctx.get_launch_resources(nt)
            if not _can_fit_any_request(launch_res, shapes):
                continue
            if (
                0 < refill_gap < size
                and node_type_available.get(nt, 0) >= refill_gap
                and (headroom is None or headroom >= refill_gap)
            ):
                for dom_val in existing_domains:
                    dom_nodes = domains.get(dom_val, [])
                    dom_nt_cnt = sum(1 for m in dom_nodes if m.node_type == nt)
                    if (
                        dom_val not in static_domains
                        and not dom_val.startswith(_PENDING_DOMAIN_PREFIX)
                        and 0 < dom_nt_cnt < size
                    ):
                        dom_gap = min(refill_gap, size - dom_nt_cnt)
                        refill_nodes = gang_ctx.make_refill_nodes(
                            nt, dom_gap, target_domain=dom_val
                        )
                        if refill_nodes:
                            launch_candidates.append(
                                (dom_val, dom_nodes + refill_nodes, dom_nodes)
                            )
            need_fresh = size + (refill_gap if 0 < refill_gap < size else 0)
            if node_type_available.get(nt, 0) >= need_fresh and (
                headroom is None or headroom >= need_fresh
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
                2. The length of its first label selector constraints (if any).
                3. The number of resources it requests.
                4. The values of resources it requests.
                5. lexicographically for each resource (for stable ordering)

            This is a legacy sorting function for the autoscaler's binpacking
            algo - we do this so that we could have a deterministic scheduling
            results with reasonable fragmentation.
            """
            label_constraint_len = (
                len(req.label_selectors[0].label_constraints)
                if req.label_selectors
                else 0
            )
            return (
                len(req.placement_constraints),
                label_constraint_len,
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

        # Anti-affinity adds labels while scheduling. Cache scheduling attempts
        # only if no label selector reads those labels.
        unique_requests = {shape_keys[id(r)]: r for r in requests_to_sched}.values()
        static_labels = not _has_dynamic_selector_overlap(unique_requests)

        def _can_host_more(node: SchedulingNode) -> bool:
            if not requests_to_sched:
                return False
            avail = node.get_available_resources(resource_request_source)
            if not _can_fit_any_request(avail, resource_shapes):
                return False
            return any(
                node._can_schedule_one(r, resource_request_source)
                for r in {shape_keys[id(r)]: r for r in requests_to_sched}.values()
            )

        # Try scheduling resource requests with existing nodes first.
        attempts = {} if static_labels else None
        while len(requests_to_sched) > 0 and len(existing_nodes) > 0:
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
            )
            if best_node is None:
                # No existing nodes can schedule any more requests.
                break

            # A node that may host lower label selector tiers stays a candidate.
            if _can_host_more(best_node):
                existing_nodes.append(best_node)
            else:
                target_nodes.append(best_node)

        # If there's any existing nodes left, we will add to the target nodes
        target_nodes.extend(existing_nodes)
        target_nodes.extend(exhausted_nodes)

        # Try scheduling remaining requests with IPPR after filling up existing nodes with their current capacity.
        existing_nodes = target_nodes
        target_nodes = []
        ippr_candidates = []

        for node in existing_nodes:
            if (
                node.status != SchedulingNodeStatus.TO_TERMINATE
                and not node._is_draining_replica
                and node.ippr_status is not None
                and node.ippr_status.can_resize_up()
            ):
                ippr_candidates.append(node)
            else:
                target_nodes.append(node)

        original_ippr_candidates = {
            node.ippr_status.cloud_instance_id: copy.deepcopy(node)
            for node in ippr_candidates
        }
        for node in ippr_candidates:
            # Expose per-node maximums so binpacking can evaluate placing more work
            # by upsizing in-place rather than launching new nodes.
            node.update_total_resources(
                {
                    "CPU": node.ippr_status.max_cpu(),
                    "memory": node.ippr_status.max_memory(),
                }
            )

        attempts = {} if static_labels else None
        while len(requests_to_sched) > 0 and len(ippr_candidates) > 0:
            (
                best_node,
                requests_to_sched,
                ippr_candidates,
            ) = ResourceDemandScheduler._sched_best_node(
                requests_to_sched,
                ippr_candidates,
                resource_request_source,
                ctx.get_cloud_resource_availabilities(),
                ctx.get_recoverable_resource_availabilities(),
                shape_keys,
                attempts,
            )
            if best_node is None:
                # No ippr nodes can schedule any more requests.
                break

            # Commit an IPPR action on the selected node to its max effective caps.
            best_node.ippr_status.queue_resize_request(
                desired_cpu=best_node.ippr_status.max_cpu(),
                desired_memory=best_node.ippr_status.max_memory(),
            )
            original_ippr_candidates.pop(best_node.ippr_status.cloud_instance_id, None)
            if _can_host_more(best_node):
                ippr_candidates.append(best_node)
            else:
                target_nodes.append(best_node)

        # Keep unselected IPPR candidates at their original resources because no
        # resize request was issued for them in this scheduling pass.
        target_nodes.extend(
            original_ippr_candidates.get(node.ippr_status.cloud_instance_id, node)
            for node in ippr_candidates
        )

        if not requests_to_sched or not any(
            v > 0 for v in node_type_available.values()
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
        promoted_refills: Set[NodeType] = set()

        def _batch_size(nt: NodeType) -> int:
            if nt in pending_refills:
                gap = pending_refills[nt]
                return gap + replica_sizes.get(nt, 1) if nt in promoted_refills else gap
            return replica_sizes.get(nt, 1)

        def _make_batch(nt: NodeType) -> List[SchedulingNode]:
            if nt in pending_refills:
                return ctx.make_refill_nodes(nt, pending_refills[nt])
            return ctx.make_replica_nodes(nt)

        def _can_launch(nt: NodeType, cur_launched: int) -> bool:
            need = _batch_size(nt)
            return node_type_available.get(nt, 0) >= need and (
                headroom is None or cur_launched + need <= headroom
            )

        candidate_replicas: Dict[NodeType, List[SchedulingNode]] = {
            nt: _make_batch(nt) for nt in node_type_available if _can_launch(nt, 0)
        }
        launched_nodes: List[SchedulingNode] = []
        launched_count = 0
        attempts = {} if static_labels else None

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
                        n._can_schedule_one(r, resource_request_source)
                        for n in rep
                        for r in unique_rem
                    ):
                        continue
                    full_need = pending_refills[nt] + replica_sizes.get(nt, 1)
                    if node_type_available.get(nt, 0) >= full_need and (
                        headroom is None or launched_count + full_need <= headroom
                    ):
                        full_rep = ctx.make_replica_nodes(nt)
                        if any(
                            n._can_schedule_one(r, resource_request_source)
                            for n in full_rep
                            for r in unique_rem
                        ):
                            promoted_refills.add(nt)
                            candidate_replicas[nt] = rep + full_rep

            fresh_nodes = [
                n
                for nt, rep in candidate_replicas.items()
                if _can_launch(nt, launched_count)
                for n in rep
            ]
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
                pending_refills.pop(nt, None)
                promoted_refills.discard(nt)
                launched_count += used_size
                node_type_available[nt] -= used_size
                if node_type_available[nt] >= _batch_size(nt):
                    candidate_replicas[nt] = _make_batch(nt)

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
        attempts: Optional[Dict[int, object]] = None,
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
            recoverable_resource_availabilities: The recoverable cloud resource availability
                score. Similar to cloud_resource_availabilities, but it will recover from
                0.0 to 1.0 linearly over RAY_AUTOSCALER_AVAILABILITY_RECOVERY_S seconds.
            shape_keys: Precomputed serialization keys for each request. Avoids
                redundant SerializeToString calls inside try_schedule. If None,
                keys are computed on the fly (fallback for tests and external callers).
            attempts: Scheduling attempts by node id, shared across calls that
                schedule the remaining requests on the same candidate nodes. An
                attempt is reused while the node's label tiers and accepted
                request prefixes in its top and unlabeled tiers stay unchanged,
                so each call only simulates nodes affected by the previous
                selection (e.g. one host per call when placing TPU slice bundles).
                When any label selector reads a label added by anti-affinity,
                simulated attempt reuse and pruning are bypassed while keeping
                initial label scores cached. If None, every candidate node is
                simulated with every request.
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

        def shape_key(r: ResourceRequest) -> bytes:
            sk = shape_keys.get(id(r)) if shape_keys is not None else None
            return sk if sk is not None else r.SerializeToString(deterministic=True)

        keys = [shape_key(r) for r in requests]
        requests_by_key = dict(zip(keys, requests))
        results = []

        # Deduplicate identical non-running nodes before computing label scores,
        # best_label_scores, and pass_top.
        node_cache = NodeStateCache(resource_request_source)
        candidates = [
            (idx, node)
            for idx, node in enumerate(nodes)
            if not node_cache.was_seen_or_mark(
                node,
                is_preferred=bool(
                    preferred_node_ids and id(node) in preferred_node_ids
                ),
            )
        ]

        # Without a shared cache, label scores are still needed to normalize
        # tiers, but nothing is reused across calls.
        cache: Dict[int, _NodeAttempt] = attempts if attempts is not None else {}
        for _, node in candidates:
            if id(node) not in cache:
                avail = node.get_available_resources(resource_request_source)
                label_scores = {}
                for k, r in requests_by_key.items():
                    score = node._schedulable_label_score(
                        r, resource_request_source, available_resources=avail
                    )
                    if score is not None:
                        label_scores[k] = score
                cache[id(node)] = _NodeAttempt(label_scores)

        best_label_scores: Dict[bytes, float] = {}
        if candidates:
            for k, r in requests_by_key.items():
                if len(r.label_selectors) > 1 and _has_label_constraints(r):
                    best = 0.0
                    for _, n in candidates:
                        s = cache[id(n)].label_scores.get(k, 0.0)
                        if s > best:
                            best = s
                            if best == 1.0:
                                break
                    best_label_scores[k] = best
        # Any node whose best schedulable tier is below `pass_top` loses to a
        # `pass_top` candidate on `score[0]`, so skip simulating it until needed.
        # Bypass simulated attempt reuse and tier pruning when a label selector
        # reads an anti-affinity label that can grow mid-simulation.
        use_attempt_cache = attempts is not None and not _has_dynamic_selector_overlap(
            requests_by_key.values()
        )
        pass_top = 0.0
        if use_attempt_cache:
            has_any_candidate_score = False
            for _, n in candidates:
                for k, score in cache[id(n)].label_scores.items():
                    if k in requests_by_key:
                        has_any_candidate_score = True
                        if score > 0:
                            pass_top = 1.0
                            break
                if pass_top == 1.0:
                    break
            if not has_any_candidate_score:
                pass_top = 1.0

        for idx, node in candidates:
            result = cache[id(node)]
            top: Optional[float] = None
            if use_attempt_cache:
                top = result.top_tier(requests_by_key, best_label_scores, pass_top)
                if top < pass_top:
                    continue
                if result.matches(keys, requests_by_key, best_label_scores, top):
                    if result.node is not None:
                        results.append((idx, result))
                    continue

            node_requests = (
                [r for r, k in zip(requests, keys) if k in result.label_scores]
                if use_attempt_cache
                else requests
            )
            if not node_requests:
                if use_attempt_cache:
                    cache[id(node)] = _NodeAttempt(result.label_scores, top=top)
                continue

            node_copy = copy.deepcopy(node)
            num_sched = len(node_copy.get_sched_requests(resource_request_source))
            _, score = node_copy.try_schedule(
                node_requests,
                resource_request_source,
                shape_keys,
                best_label_scores,
                result.label_scores,
            )
            new_sched = node_copy.get_sched_requests(resource_request_source)[
                num_sched:
            ]
            if not new_sched:
                if use_attempt_cache:
                    cache[id(node)] = _NodeAttempt(result.label_scores, top=top)
                continue

            attempt = _NodeAttempt.from_schedule(
                result.label_scores,
                best_label_scores,
                node_copy,
                new_sched,
                score,
                keys if use_attempt_cache else None,
                shape_key,
                top,
                requests_by_key if use_attempt_cache else None,
                resource_request_source if use_attempt_cache else None,
            )
            if use_attempt_cache:
                cache[id(node)] = attempt
            results.append((idx, attempt))

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
        best_idx, best_result = max(
            results,
            key=lambda item: (
                item[1].score[0],
                item[1].score[1],
                not item[1].node._is_backfill_placeholder,
                bool(preferred_node_ids and id(nodes[item[0]]) in preferred_node_ids),
                item[1].score[2:],
                recoverable_resource_availabilities.get(item[1].node.node_type, 1.0),
                item[1].node.priority,
                cloud_resource_availabilities.get(item[1].node.node_type, 1.0),
            ),
        )
        # Remove the best node from the nodes.
        cache.pop(id(nodes.pop(best_idx)))

        # Keep the requests that the best node did not schedule, in order.
        to_drop = Counter(map(shape_key, best_result.scheduled_requests))
        infeasible = []
        for r, k in zip(requests, keys):
            if to_drop[k] > 0:
                to_drop[k] -= 1
            else:
                infeasible.append(r)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Best node: {}, score: {}, remaining requests: {}".format(
                    best_result.node,
                    best_result.score,
                    ResourceRequestUtil.to_dict_list(infeasible),
                )
            )
        return best_result.node, infeasible, nodes

    @staticmethod
    def _terminate_outdated_nodes(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Terminate the nodes that are outdated, i.e. the node type config has been
        updated or the node's launch config hash is outdated.

        Args:
            ctx: The schedule context.
        """
        nodes = ctx.get_nodes()

        if ctx.is_launch_config_check_disabled():
            # Outdated nodes check through launch config check is disabled.
            return

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
            gap = ctx.get_unlabeled_backfill_gap(node_type, size)
            rem = (
                len(queued_nodes)
                if (gap > 0 and len(queued_nodes) < gap)
                else (len(queued_nodes) - gap) % size
            )
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
            ctx.update(nodes)

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
                    eff_idle_s = (
                        cfg.idle_timeout_s
                        if cfg is not None and cfg.idle_timeout_s is not None
                        else ctx.get_idle_timeout_s()
                    )
                    avail = node.get_available_resources(
                        ResourceRequestSource.PENDING_DEMAND
                    )
                    is_busy = any(
                        total - avail.get(res, 0.0) > _RESOURCE_EPSILON
                        for res, total in node.total_resources.items()
                    )
                    is_active_node = (
                        is_busy
                        or eff_idle_s is None
                        or node.idle_duration_ms <= eff_idle_s * 1000
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
        for node in ctx.get_nodes():
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

        def _prog_key(u: List[SchedulingNode]) -> Tuple[int, bool, float]:
            real = [m for m in u if not m._is_backfill_placeholder] or u
            ranks = [_term_rank(m) for m in real]
            size = replica_sizes.get(real[0].node_type, 1)
            return (
                max(ranks),
                len(real) >= size,
                sum(ranks) / len(ranks),
            )

        ranked_units: List[
            Tuple[Tuple, Tuple[int, bool, float], str, List[SchedulingNode]]
        ] = []
        grouped: Dict[Tuple[NodeType, str], List[SchedulingNode]] = defaultdict(list)
        for n in nodes:
            cfg = node_type_configs.get(n.node_type)
            dk = cfg.domain_label_key if cfg is not None else None
            is_exclusive = (
                bool(dk and cfg.domain_exclusive) if cfg is not None else False
            )
            rid = (
                n.replica_id or (n.labels.get(dk) if is_exclusive else None)
                if n.node_type in replica_sizes
                else None
            )
            if rid is None:
                worst = base[id(n)]
                if ResourceDemandScheduler._is_node_pinned(n, pinned_domains, dk):
                    worst = max(worst, (True, 0, 0.0))
                ranked_units.append((worst, _prog_key([n]), "", [n]))
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
            chunks = (
                [u_sorted[i : i + size] for i in range(0, len(u_sorted), size)]
                if allow_multi and len(u_sorted) > size and len(u_sorted) % size == 0
                else [u_sorted]
            )
            for chunk in chunks:
                real = [m for m in chunk if not m._is_backfill_placeholder] or chunk
                worst = max(base[id(m)] for m in real)
                if ResourceDemandScheduler._is_replica_pinned(
                    chunk, pinned_domains, dk
                ):
                    worst = max(worst, (True, 0, 0.0))
                ranked_units.append((worst, _prog_key(chunk), rid, chunk))

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
            cfg = node_type_configs.get(node.node_type)
            timeout_s = (
                cfg.idle_timeout_s
                if cfg is not None and cfg.idle_timeout_s is not None
                else ctx.get_idle_timeout_s()
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
                    and m.progress_rank < 3
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
                        and len(real_comb) % size == 0
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
                if len(flat) % size == 0:
                    units.extend(flat[i : i + size] for i in range(0, len(flat), size))
                else:
                    units.append(flat)

    @staticmethod
    def _enforce_idle_termination(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce idle termination for nodes not needed by demands or constraints.
        Multi-worker replica node types terminate only as whole idle replicas."""
        count_by_node_type = ctx.get_cluster_shape()
        node_type_configs = ctx.get_node_type_configs()
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
            dom_key = cfg.domain_label_key if cfg is not None else None
            for key, pinned_vals in pinned_domains.items():
                for m in target_nodes:
                    if m.labels.get(key) in pinned_vals or (
                        dom_key and key == dom_key and m.replica_id in pinned_vals
                    ):
                        return True
            return False

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
