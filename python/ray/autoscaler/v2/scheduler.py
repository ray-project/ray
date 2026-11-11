import copy
import itertools
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Set, Tuple

from ray._private.protobuf_compat import message_to_dict
from ray._raylet import IMPLICIT_RESOURCE_PREFIX
from ray.autoscaler._private.constants import AUTOSCALER_CONSERVE_GPU_NODES
from ray.autoscaler._private.resource_demand_scheduler import (
    UtilizationScore,
    _fits,
    _inplace_subtract,
)
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
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
    ResourceRequest,
    ResourceRequestByCount,
)
from ray.core.generated.common_pb2 import (
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

    def was_seen_or_mark(self, node: "SchedulingNode") -> bool:
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
            frozenset(node.total_resources.items()),
            frozenset(avail_res.items()),
            frozenset(node.labels.items()),
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
    # Whether the node belongs to an authoritative runtime/provider replica group.
    _has_runtime_domain: bool = False
    # Whether the IM instance has already been assigned a cloud launch_request_id.
    _has_launch_request: bool = False

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
        self._has_runtime_domain: bool = False
        self._has_launch_request: bool = False

    def get_available_resources(self, resource_request_source: ResourceRequestSource):
        """Get the available resources for the given resource request source."""
        return self.available_resources_for_sched[resource_request_source]

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
            )

        node = SchedulingNode.from_node_config(
            node_config,
            SchedulingNodeStatus.SCHEDULABLE,
            node_kind=instance.im_instance.node_kind,
            im_instance_id=instance.im_instance.instance_id,
            im_instance_status=instance.im_instance.status,
            provider_replica_id=instance.cloud_instance_replica_id,
        )
        node._has_launch_request = bool(instance.im_instance.launch_request_id)
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
    ) -> "SchedulingNode":
        """Create a scheduling node from a node config.

        Args:
            node_config: The node config.
            status: The status of the node.
            node_kind: The node kind.
            im_instance_id: The instance id of the im instance.
            im_instance_status: The instance status of the im instance.
            provider_replica_id: Optional provider replica group identifier.

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
        )

    def __post_init__(self):
        assert self.node_type, "node_type should be set"

    def try_schedule(
        self,
        requests: List[ResourceRequest],
        resource_request_source: ResourceRequestSource,
        shape_keys: Optional[Dict[int, bytes]] = None,
        best_label_scores: Optional[Dict[bytes, float]] = None,
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
        tiers = {}

        def label_tier(r: ResourceRequest, sk: bytes) -> Optional[float]:
            if (sk, len(self.labels)) not in tiers:
                if not r.label_selectors:
                    tier = 0
                else:
                    score = self._satisfies_label_constraints([r])
                    best = (best_label_scores or {}).get(sk)
                    tier = (min(score / best, 1.0) if best else score) or None
                tiers[(sk, len(self.labels))] = tier
            return tiers[(sk, len(self.labels))]

        # Cache to prevent O(N^2 * M) iteration by returning early for identical
        # requests.
        unfittable_cache = UnschedulableRequestCache()
        label_score = 0

        for tier in sorted(
            {label_tier(r, sk) for r, sk in keyed_requests} - {None}, reverse=True
        ):
            if label_score and (
                tier > 0
                or any(
                    0 < (label_tier(r, sk) or 0) < label_score
                    and self._can_schedule_one(r, resource_request_source)
                    for r, sk in keyed_requests
                )
            ):
                continue

            remaining = []
            # Try schedule the requests of this tier one by one in the sorted order.
            for r, sk in keyed_requests:
                if label_tier(r, sk) != tier or unfittable_cache.contains(sk):
                    remaining.append((r, sk))
                    continue

                if not self._try_schedule_one(r, resource_request_source):
                    unfittable_cache.add(sk)
                    remaining.append((r, sk))
                else:
                    label_score = label_score or tier

            keyed_requests = remaining

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

    def _satisfies_label_constraints(
        self, sched_requests: List[ResourceRequest]
    ) -> float:
        """Returns a normalized value in (0.0, 1.0] based on the priority of the label
        selector this node satisfies (1.0 for the first selector, decreasing
        proportionally for fallback selectors), or 0 if none apply."""
        for req in sched_requests:
            num_selectors = len(req.label_selectors)
            for i, selector in enumerate(req.label_selectors):
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

    def _can_schedule_one(
        self, request: ResourceRequest, resource_request_source: ResourceRequestSource
    ) -> bool:
        if (
            request.label_selectors
            and self._satisfies_label_constraints([request]) == 0
        ):
            return False

        for constraint in request.placement_constraints:
            if constraint.HasField("anti_affinity"):
                anti_affinity = constraint.anti_affinity
                if (
                    self.labels.get(anti_affinity.label_name)
                    == anti_affinity.label_value
                ):
                    return False

        return _fits(
            self.get_available_resources(resource_request_source),
            dict(request.resources_bundle),
        )

    def _try_schedule_one(
        self, request: ResourceRequest, resource_request_source: ResourceRequestSource
    ) -> bool:
        """Try to schedule one resource request on this node. The request could be from
        various sources, specified by `resource_request_source`.

        Args:
            request: The resource request to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.

        Returns:
            True if the resource request is scheduled on this node.
        """
        if not self._can_schedule_one(request, resource_request_source):
            return False

        available_resources_dict = self.get_available_resources(resource_request_source)

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

    @staticmethod
    def _is_unrequested_queued(node: SchedulingNode) -> bool:
        return (
            node.im_instance_status == Instance.QUEUED and not node._has_launch_request
        )

    @staticmethod
    def _progress_rank(node: SchedulingNode) -> int:
        if ResourceDemandScheduler._is_running_node(node):
            return -1
        if node.status == SchedulingNodeStatus.TO_LAUNCH:
            return 5
        if node.im_instance_status == Instance.QUEUED and node._has_launch_request:
            return ResourceDemandScheduler._PENDING_PROGRESSION[Instance.REQUESTED]
        return ResourceDemandScheduler._PENDING_PROGRESSION.get(
            node.im_instance_status, 4
        )

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
            self._init_replicas()
            self._node_type_available = self._compute_available_node_types(
                self._nodes, self._node_type_configs
            )

        def _new_pending_domain(self, tag: str = "") -> str:
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

        def _assign_replica_slot(
            self,
            node: SchedulingNode,
            replica_id: str,
            host_index: int,
            keep_head: Optional[bool] = None,
        ) -> None:
            node.replica_id = replica_id
            cfg = self._node_type_configs.get(node.node_type)
            rc = cfg.replica_config if cfg is not None else None
            if rc is None:
                return
            if rc.domain_label_key and rc.domain_label_key not in cfg.labels:
                node.labels[rc.domain_label_key] = replica_id
            if (
                rc.worker_index_label_key
                and rc.worker_index_label_key not in cfg.labels
            ):
                node.labels[rc.worker_index_label_key] = str(host_index)
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

        def _init_replicas(self) -> None:
            """Group running and pending worker nodes into canonical fixed replicas."""
            scarce_skip = {"CPU", "memory", "object_store_memory"}
            for node_type, cfg in self._node_type_configs.items():
                rc = cfg.replica_config
                if rc is None:
                    continue
                size, dom_key, wid_key = (
                    rc.workers_per_replica,
                    rc.domain_label_key,
                    rc.worker_index_label_key,
                )
                head_keys = set(rc.head_resources)
                pinned = self._pinned_domains.get(dom_key, set()) if dom_key else set()
                type_nodes = [
                    n
                    for n in self._nodes
                    if n.node_type == node_type
                    and n.node_kind != NodeKind.HEAD
                    and n.status != SchedulingNodeStatus.TO_TERMINATE
                ]
                active_count = len(type_nodes)
                running_by_dom: Dict[str, List[SchedulingNode]] = defaultdict(list)
                by_prov: Dict[str, List[SchedulingNode]] = defaultdict(list)
                running_unlabeled: List[SchedulingNode] = []
                pending: List[SchedulingNode] = []

                def _has_head(dn: List[SchedulingNode]) -> bool:
                    return bool(head_keys) and any(
                        m.total_resources.get(hk, 0) > 0 for m in dn for hk in head_keys
                    )

                def _place_into(
                    d: str,
                    dn: List[SchedulingNode],
                    src: List[SchedulingNode],
                    take: int,
                    known: bool = False,
                ) -> int:
                    take = min(max(0, size - len(dn)), take, len(src))
                    if take <= 0:
                        return 0
                    if not dn:
                        slots = list(range(1, take)) + [0]
                        has_h = False
                    else:
                        present = (
                            {m.labels.get(wid_key) for m in dn}
                            if wid_key
                            else {str(i) for i in range(len(dn))}
                        )
                        has_h = _has_head(dn)
                        slots = [
                            h for h in range(size) if str(h) not in present
                        ] or list(range(size))
                        if has_h and 0 in slots:
                            slots.remove(0)
                            slots.append(0)
                    for h in slots[:take]:
                        p = src.pop(0)
                        self._assign_replica_slot(
                            p,
                            d,
                            h,
                            keep_head=bool(head_keys) and h == 0 and not has_h,
                        )
                        if known:
                            p._has_runtime_domain = True
                        has_h = has_h or (h == 0 and bool(head_keys))
                        dn.append(p)
                    return take

                for n in type_nodes:
                    if size > 1 and n.provider_replica_id:
                        by_prov[n.provider_replica_id].append(n)
                    elif ResourceDemandScheduler._is_running_node(n):
                        d = n.labels.get(dom_key) if dom_key else None
                        if d:
                            n.replica_id = d
                            n._has_runtime_domain = True
                            running_by_dom[d].append(n)
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
                        pending.append(n)

                for _, p_nodes in sorted(by_prov.items()):
                    p_run = [
                        m
                        for m in p_nodes
                        if ResourceDemandScheduler._is_running_node(m)
                    ]
                    p_pend = sorted(
                        (
                            m
                            for m in p_nodes
                            if not ResourceDemandScheduler._is_running_node(m)
                            and m.status
                            in (
                                SchedulingNodeStatus.SCHEDULABLE,
                                SchedulingNodeStatus.TO_LAUNCH,
                            )
                        ),
                        key=lambda m: (
                            ResourceDemandScheduler._progress_rank(m),
                            m.im_instance_id or "",
                        ),
                    )
                    valid_doms = [
                        m.labels[dom_key]
                        for m in p_run
                        if dom_key and m.labels.get(dom_key)
                    ]
                    rid = (
                        valid_doms[0]
                        if valid_doms
                        else self._new_pending_domain(node_type)
                    )
                    for m in p_run:
                        d_m = m.labels.get(dom_key) if dom_key else None
                        m._has_runtime_domain = True
                        m.replica_id = d_m or rid
                        running_by_dom[m.replica_id].append(m)
                    if p_pend:
                        _place_into(
                            rid,
                            running_by_dom[rid],
                            p_pend,
                            len(p_pend),
                            known=True,
                        )
                        while p_pend:
                            extra_rid = self._new_pending_domain(node_type)
                            _place_into(
                                extra_rid,
                                running_by_dom[extra_rid],
                                p_pend,
                                size,
                                known=True,
                            )

                pending.sort(
                    key=lambda n: (
                        ResourceDemandScheduler._progress_rank(n),
                        n.im_instance_id or "",
                    )
                )
                booting_pods = [
                    p for p in pending if ResourceDemandScheduler._progress_rank(p) < 3
                ]
                uncreated_pending = [
                    p for p in pending if ResourceDemandScheduler._progress_rank(p) >= 3
                ]

                if running_unlabeled:
                    running_unlabeled.sort(
                        key=lambda n: (
                            ResourceDemandScheduler._sort_nodes_for_termination(n),
                            n.im_instance_id or "",
                        )
                    )
                    chunk = max(1, size if not dom_key else 1)
                    for i in range(0, len(running_unlabeled), chunk):
                        rid = self._new_pending_domain(node_type)
                        grp = running_unlabeled[i : i + chunk]
                        for m in grp:
                            m.replica_id = rid
                        for h_idx in range(
                            len(grp),
                            len(grp)
                            + (
                                min(chunk - len(grp), len(booting_pods))
                                if not dom_key
                                else 0
                            ),
                        ):
                            self._assign_replica_slot(
                                booting_pods.pop(0),
                                rid,
                                h_idx,
                                keep_head=bool(head_keys) and h_idx == 0,
                            )

                if size == 1:
                    for n in pending:
                        self._assign_replica_slot(
                            n, self._new_pending_domain(node_type), 0, keep_head=True
                        )
                    continue

                if running_by_dom and booting_pods:
                    scarce_keys = [
                        k for k in cfg.resources if k not in scarce_skip | head_keys
                    ] or [k for k in cfg.resources if k not in head_keys]

                    def _scarce_idle(dn: List[SchedulingNode]) -> bool:
                        return all(
                            m.get_available_resources(
                                ResourceRequestSource.PENDING_DEMAND
                            ).get(k, 0.0)
                            >= min(
                                m.total_resources.get(k, 0.0), cfg.resources.get(k, 0.0)
                            )
                            - 1e-9
                            for m in dn
                            for k in scarce_keys
                        )

                    def _fill(d: str, dn: List[SchedulingNode], limit: int) -> int:
                        return _place_into(d, dn, booting_pods, limit, known=False)

                    def _budget(rem: int) -> int:
                        if active_count > cfg.max_worker_nodes:
                            return rem % size
                        max_new = max(
                            0,
                            (
                                cfg.max_worker_nodes
                                - len(running_by_dom) * size
                                - len(uncreated_pending)
                            )
                            // size,
                        )
                        return rem - min(rem // size, max_new) * size

                    incomplete = sorted(
                        ((d, dn) for d, dn in running_by_dom.items() if len(dn) < size),
                        key=lambda x: (
                            (0, False, 0)
                            if x[0] in pinned
                            else (1, False, max(m.idle_duration_ms for m in x[1]))
                            if _scarce_idle(x[1])
                            else (2, _has_head(x[1]), 0),
                            size - len(x[1]),
                            x[0],
                        ),
                    )
                    for d, dn in incomplete:
                        if d in pinned:
                            _fill(d, dn, size - len(dn))

                    spare = _budget(len(booting_pods))
                    idle_inc = [
                        (d, dn)
                        for d, dn in incomplete
                        if d not in pinned and len(dn) < size and _scarce_idle(dn)
                    ]
                    batch_end = need_sum = 0
                    for idx, (_, dn) in enumerate(idle_inc, start=1):
                        need_sum += size - len(dn)
                        if need_sum > len(booting_pods):
                            break
                        if (
                            need_sum <= spare
                            or (need_sum - spare) % size == 0
                            or need_sum % size == 0
                        ):
                            batch_end = idx
                    for d, dn in idle_inc[:batch_end]:
                        _fill(d, dn, size - len(dn))

                    spare = _budget(len(booting_pods))
                    for d, dn in incomplete:
                        if len(dn) < size and spare > 0:
                            spare -= _fill(d, dn, spare)

                for group in (booting_pods, uncreated_pending):
                    for i in range(0, len(group), size):
                        rid = self._new_pending_domain(node_type)
                        chunk_nodes = group[i : i + size]
                        slots = list(range(1, len(chunk_nodes))) + [0]
                        for p, h in zip(chunk_nodes, slots):
                            self._assign_replica_slot(
                                p, rid, h, keep_head=bool(head_keys) and h == 0
                            )

        @staticmethod
        def _missing_replica_backfill_by_type(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
        ) -> Dict[NodeType, int]:
            """Count missing host slots across live incomplete replicas whose
            replacement pods have not yet appeared in `nodes`."""
            missing: Dict[NodeType, int] = {}
            for nt, cfg in node_type_configs.items():
                if (
                    cfg.replica_config is None
                    or cfg.replica_config.workers_per_replica <= 1
                ):
                    continue
                size = cfg.replica_config.workers_per_replica
                k8s_count = booting_count = 0
                dom_running: Counter = Counter()
                paired_known: Counter = Counter()
                for n in nodes:
                    if (
                        n.node_type != nt
                        or n.node_kind == NodeKind.HEAD
                        or n.status
                        in (
                            SchedulingNodeStatus.TO_TERMINATE,
                            SchedulingNodeStatus.TO_LAUNCH,
                        )
                        or ResourceDemandScheduler._is_unrequested_queued(n)
                    ):
                        continue
                    is_running = ResourceDemandScheduler._is_running_node(n)
                    k8s_count += 1
                    if n._has_runtime_domain and n.replica_id:
                        dom_running[n.replica_id] += 1
                    elif not is_running:
                        booting_count += 1
                        if n.replica_id:
                            paired_known[n.replica_id] += 1
                dom_committed = sum(
                    max(size, cnt + paired_known.get(rid, 0))
                    for rid, cnt in dom_running.items()
                )
                raw_gap = sum(max(0, size - cnt) for cnt in dom_running.values())
                extra = (max(0, booting_count - raw_gap) + size - 1) // size
                committed = max(
                    ((k8s_count + size - 1) // size) * size,
                    dom_committed + extra * size,
                )
                if committed > k8s_count:
                    missing[nt] = committed - k8s_count
            return missing

        def get_missing_replica_backfill_slots(self) -> Dict[NodeType, int]:
            return self._missing_replica_backfill_by_type(
                self._nodes, self._node_type_configs
            )

        def make_replica_nodes(self, node_type: NodeType) -> List[SchedulingNode]:
            """Create one atomic TO_LAUNCH replica for `node_type`."""
            cfg = self._node_type_configs[node_type]
            rc = cfg.replica_config
            capped = (
                self._launch_resources(cfg, self._ippr_specs)
                if self._ippr_specs and node_type in self._ippr_specs.groups
                else None
            )

            def _new_node() -> SchedulingNode:
                node = SchedulingNode.from_node_config(
                    cfg,
                    status=SchedulingNodeStatus.TO_LAUNCH,
                    node_kind=NodeKind.WORKER,
                )
                if capped is not None:
                    node.update_total_resources(
                        {"CPU": capped["CPU"], "memory": capped["memory"]}
                    )
                return node

            if rc is None:
                return [_new_node()]
            size = rc.workers_per_replica
            rid = self._new_pending_domain(node_type)
            replica = []
            for h in list(range(1, size)) + [0]:
                node = _new_node()
                self._assign_replica_slot(
                    node, rid, h, keep_head=bool(rc.head_resources) and h == 0
                )
                replica.append(node)
            return replica

        def get_multihost_replica_sizes(self) -> Dict[NodeType, int]:
            return {
                nt: cfg.replica_config.workers_per_replica
                for nt, cfg in self._node_type_configs.items()
                if cfg.replica_config is not None
                and cfg.replica_config.workers_per_replica > 1
            }

        def get_pinned_domains_for_type(self, node_type: NodeType) -> Set[str]:
            cfg = self._node_type_configs.get(node_type)
            dom_key = (
                cfg.replica_config.domain_label_key
                if cfg is not None and cfg.replica_config is not None
                else None
            )
            return set(self._pinned_domains.get(dom_key, ())) if dom_key else set()

        @classmethod
        def from_schedule_request(
            cls, req: SchedulingRequest
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Create a schedule context from a schedule request.
            It will populate the context with the existing nodes and the available node
            types from the config.

            Args:
                req: The scheduling request. The caller should make sure the
                    request is valid.

            Returns:
                A schedule context populated from the scheduling request.
            """

            nodes = []
            node_type_configs = req.node_type_configs

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
            )

        @staticmethod
        def _compute_available_node_types(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
        ) -> Dict[NodeType, int]:
            """Compute the number of nodes by node types available for launching based on
            the max number of workers in the config.
            Args:
                nodes: The current existing nodes.
                node_type_configs: The node type configs.
            Returns:
                A dict of node types and the number of nodes available for launching.
            """
            node_type_available: Dict[NodeType, int] = defaultdict(int)
            node_type_existing: Dict[NodeType, int] = defaultdict(int)
            for node in nodes:
                node_type_existing[node.node_type] += 1
            for (
                nt,
                gap,
            ) in ResourceDemandScheduler.ScheduleContext._missing_replica_backfill_by_type(
                nodes, node_type_configs
            ).items():
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
                if node.status == SchedulingNodeStatus.TO_TERMINATE:
                    # Skip the nodes that are to be terminated.
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

        def update(self, new_nodes: List[SchedulingNode]) -> None:
            """Update the context with the new nodes."""
            self._nodes = new_nodes
            self._node_type_available = self._compute_available_node_types(
                self._nodes,
                self._node_type_configs,
            )

        def restricted_to(
            self, nodes: List[SchedulingNode]
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """Return a shallow copy of this context restricted to `nodes` with
            node launches disabled."""
            ctx = copy.copy(self)
            ctx._nodes = nodes
            ctx._node_type_available = {}
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
            for node in self._nodes:
                if node.status == SchedulingNodeStatus.TO_LAUNCH:
                    launch_by_type[node.node_type] += 1

            launch_requests = []
            for instance_type, count in launch_by_type.items():
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
        ) -> List[TerminationRequest]:
            """Get the terminate requests for the nodes that are to be terminated."""
            return [
                node.termination_request
                for node in self._nodes
                if node.termination_request is not None
            ]

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

        ctx = ResourceDemandScheduler.ScheduleContext.from_schedule_request(request)

        # Enforce outdate nodes.
        ResourceDemandScheduler._terminate_outdated_nodes(ctx)

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
        ctx._pinned_domains = ResourceDemandScheduler._pinned_locality_domains(
            infeasible_gang_requests, ctx.get_node_type_configs()
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
                    terminate_requests=reply.to_terminate,
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
        pinned_domains = ctx._pinned_domains
        # Step 1. Enforce the max number of workers for each node type.
        for node_type in non_terminating_nodes_by_type.keys():
            non_terminate_nodes_of_type = non_terminating_nodes_by_type[node_type]
            node_config = ctx.get_node_type_configs()[node_type]
            num_max_nodes_per_type = node_config.max_worker_nodes
            num_extra_nodes = len(non_terminate_nodes_of_type) - num_max_nodes_per_type

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

        num_to_terminate = (
            max(len(non_terminating_nodes) - num_max_nodes, 0) if num_max_nodes else 0
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
            pinned_domains=ctx._pinned_domains,
        )

        # Multi-worker replica groups terminate in whole replica units, so total
        # terminated nodes may exceed `num_to_terminate`.
        assert len(to_terminate_nodes) >= num_to_terminate, (
            "Terminating {} nodes, failed to terminate {} nodes to "
            "satisfy max_num_nodes={}".format(
                len(to_terminate_nodes),
                num_to_terminate - len(to_terminate_nodes),
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

        if replica_sizes and any(n.node_type in replica_sizes for n in nodes):
            units = ResourceDemandScheduler._termination_units(
                nodes,
                replica_sizes,
                node_type_configs or {},
                pinned_domains or {},
            )
            terminated_nodes: List[SchedulingNode] = []
            remained_nodes: List[SchedulingNode] = []
            for unit in units:
                if len(terminated_nodes) < num_to_terminate:
                    terminated_nodes.extend(unit)
                else:
                    remained_nodes.extend(unit)
            if head_node:
                remained_nodes.append(head_node)
        else:
            nodes.sort(
                key=lambda n: (
                    ResourceDemandScheduler._sort_nodes_for_termination(n),
                    ResourceDemandScheduler._PENDING_TERMINATION_ORDER.get(
                        n.im_instance_status, 4
                    ),
                )
            )
            terminated_nodes, remained_nodes = (
                nodes[:num_to_terminate],
                # The head could be None if there's no head node being reported yet
                # from the ray cluster.
                nodes[num_to_terminate:] + ([head_node] if head_node else []),
            )

        assert cause in [
            TerminationRequest.Cause.MAX_NUM_NODES,
            TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
        ], "Other termination causes don't have to select nodes for termination."

        for node in terminated_nodes:
            was_to_launch = node.status == SchedulingNodeStatus.TO_LAUNCH
            node.status = SchedulingNodeStatus.TO_TERMINATE
            if was_to_launch:
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
                rc = node_type_config.replica_config
                size = rc.workers_per_replica if rc is not None else 1
                for _ in range((deficit + size - 1) // size):
                    new_nodes.extend(ctx.make_replica_nodes(node_type))
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
            requests = (
                req.bundle_selectors[0].resource_requests
                if req.bundle_selectors
                else req.requests
            )
            total_placement_constraints = sum(
                len(r.placement_constraints) for r in requests
            )
            locality_label_constraints = 0
            if req.bundle_selectors and req.bundle_selectors[0].HasField(
                "locality_requirement"
            ):
                locality_req = req.bundle_selectors[0].locality_requirement
                total_placement_constraints += len(requests)
                locality_label_constraints = len(
                    locality_req.label_selector.label_constraints
                )

            return (
                locality_label_constraints,
                total_placement_constraints,
                len(requests),
            )

        infeasible_gang_requests = []
        # Try fulfilling the gang requests one by one.
        for gang_req in sorted(
            gang_requests, key=_sort_gang_resource_requests, reverse=True
        ):
            nodes, infeasible = ResourceDemandScheduler._try_schedule_gang(
                ctx, gang_req
            )

            if infeasible:
                # Unable to satisfy the constraint. We will skip the gang request.
                # Don't update the context.
                infeasible_gang_requests.append(gang_req)
                continue

            # We are able to satisfy the constraint and thus update the context.
            ctx.update(nodes)

        return infeasible_gang_requests

    @staticmethod
    def _required_label_values(req: ResourceRequest, key: str) -> Optional[Set[str]]:
        """Return values of `key` permitted by every fallback tier of `req`, or
        None if `req` leaves `key` unconstrained on at least one tier."""
        if not req.label_selectors:
            return None
        tier_vals = []
        for sel in req.label_selectors:
            in_c = [
                set(c.label_values)
                for c in sel.label_constraints
                if c.label_key == key and c.operator == _LABEL_OPERATOR_IN
            ]
            if not in_c:
                return None
            not_in = {
                v
                for c in sel.label_constraints
                if c.label_key == key and c.operator == _LABEL_OPERATOR_NOT_IN
                for v in c.label_values
            }
            tier_vals.append(set.intersection(*in_c) - not_in)
        return set.union(*tier_vals)

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
            if r.label_selectors
            for c in r.label_selectors[0].label_constraints
            if c.operator == _LABEL_OPERATOR_IN
        }
        cap = len(requests) - max(anti_counts.values(), default=1) + 1
        for key in candidate_keys:
            unpinned = 0
            by_val: Counter = Counter()
            for r in requests:
                allowed = ResourceDemandScheduler._required_label_values(r, key)
                if allowed is None:
                    unpinned += 1
                else:
                    by_val.update(allowed)
            cap = min(cap, unpinned + max(by_val.values(), default=0))
        return cap

    @staticmethod
    def _is_domain_idle(
        domain_nodes: List[SchedulingNode],
        requests: List[ResourceRequest],
        shapes: List[Dict[str, float]],
        max_bundles_per_node: int,
    ) -> bool:
        """Return whether compatible nodes in `domain_nodes` retain full bundle
        capacity for `requests`."""
        if max_bundles_per_node <= 0:
            return False
        has_label_reqs = any(r.label_selectors for r in requests)

        def _cap(res: Dict[str, float]) -> List[float]:
            return [
                min(
                    max_bundles_per_node,
                    *(
                        res.get(k, 0) // v
                        for k, v in s.items()
                        if not k.startswith(IMPLICIT_RESOURCE_PREFIX)
                    ),
                )
                for s in shapes
                if any(not k.startswith(IMPLICIT_RESOURCE_PREFIX) for k in s)
            ]

        matching = [
            n
            for n in domain_nodes
            if _can_fit_any_request(n.total_resources, shapes)
            and (
                not has_label_reqs
                or any(n._satisfies_label_constraints([r]) > 0 for r in requests)
            )
        ]
        return bool(matching) and all(
            _cap(n.get_available_resources(ResourceRequestSource.PENDING_DEMAND))
            == _cap(n.total_resources)
            for n in matching
        )

    @staticmethod
    def _is_running_node(node: SchedulingNode) -> bool:
        return bool(node.ray_node_id) or node.im_instance_status == Instance.RAY_RUNNING

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
        return sum(
            node._satisfies_label_constraints([r])
            for r in node.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            if len(r.label_selectors) > 1
        )

    @staticmethod
    def _schedule_in_domain(
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
        _ls = ResourceDemandScheduler._node_label_score
        running = [
            n for n in domain_nodes if ResourceDemandScheduler._is_running_node(n)
        ]
        non_running = [
            n for n in domain_nodes if not ResourceDemandScheduler._is_running_node(n)
        ]
        max_ls = sum(1.0 for r in requests if len(r.label_selectors) > 1)
        prior_ls = round(sum(map(_ls, domain_nodes)), 9) if max_ls else 0.0
        run_fit: Optional[Tuple[List[SchedulingNode], float]] = None
        if running and non_running:
            sched_run, inf_run = ResourceDemandScheduler._try_schedule(
                ctx.restricted_to(copy.deepcopy(running)),
                requests,
                ResourceRequestSource.PENDING_DEMAND,
            )
            if not inf_run:
                cand_run = sched_run + copy.deepcopy(non_running)
                ls_run = round(sum(map(_ls, cand_run)) - prior_ls, 9) if max_ls else 0.0
                if ls_run >= max_ls - 1e-9:
                    rep_ids = {id(n) for n in replaced}
                    return [n for n in all_nodes if id(n) not in rep_ids] + cand_run
                run_fit = (cand_run, ls_run)

        scheduled, infeasible = ResourceDemandScheduler._try_schedule(
            ctx.restricted_to(copy.deepcopy(domain_nodes)),
            requests,
            ResourceRequestSource.PENDING_DEMAND,
        )
        if not infeasible and run_fit is not None:
            if round(sum(map(_ls, scheduled)) - prior_ls, 9) <= run_fit[1] + 1e-9:
                scheduled = run_fit[0]
        if infeasible and pending_siblings and len(infeasible) < len(requests):
            siblings = copy.deepcopy(pending_siblings)
            for sib in siblings:
                sib.labels[locality_label] = domain_val
            scheduled, infeasible = ResourceDemandScheduler._try_schedule(
                ctx.restricted_to(scheduled + siblings),
                infeasible,
                ResourceRequestSource.PENDING_DEMAND,
            )
            replaced = list(replaced) + list(pending_siblings)
            if not infeasible:
                for n in scheduled:
                    if (
                        not ResourceDemandScheduler._is_running_node(n)
                        and n.replica_id != domain_val
                        and not n.get_sched_requests(
                            ResourceRequestSource.PENDING_DEMAND
                        )
                    ):
                        n.labels.pop(locality_label, None)
        if infeasible:
            return None
        rep_ids = {id(n) for n in replaced}
        return [n for n in all_nodes if id(n) not in rep_ids] + scheduled

    @staticmethod
    def _locality_domain_filters(
        locality_req, locality_label: str
    ) -> Tuple[Optional[Set[str]], Set[str]]:
        """Return `(pinned_domains, excluded_domains)` from `locality_req`."""
        pinned = [
            set(c.label_values)
            for c in locality_req.label_selector.label_constraints
            if c.label_key == locality_label and c.operator == _LABEL_OPERATOR_IN
        ]
        excluded = {
            v
            for c in locality_req.label_selector.label_constraints
            if c.label_key == locality_label and c.operator == _LABEL_OPERATOR_NOT_IN
            for v in c.label_values
        }
        return (set.intersection(*pinned) - excluded if pinned else None), excluded

    @staticmethod
    def _pinned_locality_domains(
        gang_requests: List[GangResourceRequest],
        node_type_configs: Optional[Dict[NodeType, NodeTypeConfig]] = None,
    ) -> Dict[str, Set[str]]:
        pinned_by_key: Dict[str, Set[str]] = defaultdict(set)
        rep_keys = {
            cfg.replica_config.domain_label_key
            for cfg in (node_type_configs or {}).values()
            if cfg.replica_config is not None and cfg.replica_config.domain_label_key
        }
        for req in gang_requests:
            reqs = (
                req.bundle_selectors[0].resource_requests
                if req.bundle_selectors
                else req.requests
            )
            check_keys = set(rep_keys)
            excluded_by_key: Dict[str, Set[str]] = {}
            if req.bundle_selectors and req.bundle_selectors[0].HasField(
                "locality_requirement"
            ):
                loc = req.bundle_selectors[0].locality_requirement
                if (
                    loc.locality_constraint.placement_strategy
                    == PlacementStrategy.STRICT_PACK
                ):
                    key = loc.locality_constraint.label_name
                    check_keys.add(key)
                    pinned, excl = ResourceDemandScheduler._locality_domain_filters(
                        loc, key
                    )
                    if excl:
                        excluded_by_key[key] = excl
                    if pinned:
                        pinned_by_key[key].update(pinned)
            for key in check_keys:
                excl_vals = excluded_by_key.get(key, set())
                for r in reqs:
                    allowed = ResourceDemandScheduler._required_label_values(r, key)
                    if allowed:
                        pinned_by_key[key].update(allowed - excl_vals)
        return {k: v for k, v in pinned_by_key.items() if v}

    @staticmethod
    def _try_schedule_gang(
        ctx: "ResourceDemandScheduler.ScheduleContext",
        gang_req: GangResourceRequest,
    ) -> Tuple[List[SchedulingNode], List[ResourceRequest]]:
        """Schedule all bundles of `gang_req` atomically, placing `STRICT_PACK`
        locality gangs within a single value of the locality label."""
        locality_req = None
        if gang_req.bundle_selectors:
            bundle_selector = gang_req.bundle_selectors[0]
            requests = bundle_selector.resource_requests
            if bundle_selector.HasField("locality_requirement"):
                locality_req = bundle_selector.locality_requirement
        else:
            requests = gang_req.requests

        requests = ResourceRequestUtil.combine_requests_with_affinity(requests)
        if (
            not requests
            or locality_req is None
            or locality_req.locality_constraint.placement_strategy
            != PlacementStrategy.STRICT_PACK
        ):
            return ResourceDemandScheduler._try_schedule(
                ctx, requests, ResourceRequestSource.PENDING_DEMAND
            )

        locality_label = locality_req.locality_constraint.label_name
        pinned_opt, excluded_domains = ResourceDemandScheduler._locality_domain_filters(
            locality_req, locality_label
        )
        pinned = pinned_opt is not None
        pinned_domains = pinned_opt or set()
        if locality_req.label_selector.label_constraints:
            requests = ResourceDemandScheduler._add_label_constraints_to_requests(
                requests, locality_req.label_selector.label_constraints
            )

        all_nodes = ctx.get_nodes()
        if pinned and not pinned_domains:
            return all_nodes, requests
        node_type_configs = ctx.get_node_type_configs()
        shapes = _collect_unique_resource_shapes(requests)
        max_bundles_per_node = ResourceDemandScheduler._max_bundles_per_node(requests)

        domains: Dict[str, List[SchedulingNode]] = defaultdict(list)
        for node in all_nodes:
            domain_val = node.labels.get(locality_label)
            if domain_val and node.status != SchedulingNodeStatus.TO_TERMINATE:
                domains[domain_val].append(node)
        static_domains = {
            cfg.labels[locality_label]
            for cfg in node_type_configs.values()
            if cfg.labels.get(locality_label)
        }
        replica_domain_types = [
            nt
            for nt, cfg in node_type_configs.items()
            if cfg.replica_config is not None
            and cfg.replica_config.domain_label_key == locality_label
        ]

        gang_ctx = copy.copy(ctx)
        gang_ctx.update(
            sorted(
                all_nodes,
                key=lambda n: not ResourceDemandScheduler._is_running_node(n),
            )
        )
        candidate_domains = (domains.keys() | static_domains) - excluded_domains
        if pinned:
            candidate_domains &= pinned_domains
        node_type_available = gang_ctx.get_node_type_available()
        max_num_nodes = gang_ctx.get_max_num_nodes()
        recoverable = gang_ctx.get_recoverable_resource_availabilities()
        availabilities = gang_ctx.get_cloud_resource_availabilities()
        ippr_specs = gang_ctx.get_ippr_specs()

        def _node_type_rank(nt: NodeType) -> Tuple[float, int, float]:
            cfg = node_type_configs.get(nt)
            return (
                recoverable.get(nt, 1.0),
                cfg.priority if cfg else 0,
                availabilities.get(nt, 1.0),
            )

        def _domain_rank(domain_val: str) -> Tuple:
            d_nodes = domains.get(domain_val, [])
            nts = {n.node_type for n in d_nodes} | {
                nt
                for nt, cfg in node_type_configs.items()
                if cfg.labels.get(locality_label) == domain_val
            }
            best_prog = min(
                (
                    ResourceDemandScheduler._progress_rank(n)
                    for n in d_nodes
                    if not ResourceDemandScheduler._is_running_node(n)
                ),
                default=5,
            )
            return (
                not any(ResourceDemandScheduler._is_running_node(n) for n in d_nodes),
                not any(n.status == SchedulingNodeStatus.SCHEDULABLE for n in d_nodes),
                best_prog,
                tuple(-x for x in max(map(_node_type_rank, nts))),
                domain_val,
            )

        existing_domains = sorted(candidate_domains, key=_domain_rank)
        unlabeled_pending_by_type: Dict[NodeType, List[SchedulingNode]] = defaultdict(
            list
        )
        for n in all_nodes:
            if (
                n.node_type not in replica_domain_types
                and n.status != SchedulingNodeStatus.TO_TERMINATE
                and not ResourceDemandScheduler._is_running_node(n)
                and not n.labels.get(locality_label)
                and not n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND)
            ):
                unlabeled_pending_by_type[n.node_type].append(n)

        missing_backfill_total = sum(
            gang_ctx.get_missing_replica_backfill_slots().values()
        )
        new_replicas: List[Tuple[str, List[SchedulingNode]]] = []
        for nt in sorted(
            replica_domain_types if not pinned else [],
            key=_node_type_rank,
            reverse=True,
        ):
            cfg = node_type_configs[nt]
            size = cfg.replica_config.workers_per_replica
            launch_res = ResourceDemandScheduler.ScheduleContext._launch_resources(
                cfg, ippr_specs
            )
            if (
                _can_fit_any_request(launch_res, shapes)
                and node_type_available.get(nt, 0) >= size
                and (
                    max_num_nodes is None
                    or len(all_nodes) + missing_backfill_total + size <= max_num_nodes
                )
            ):
                rep_nodes = gang_ctx.make_replica_nodes(nt)
                new_replicas.append((rep_nodes[0].replica_id, rep_nodes))

        _ls = ResourceDemandScheduler._node_label_score
        max_label_score = sum(1.0 for r in requests if len(r.label_selectors) > 1)
        orig_to_launch = Counter(
            n.node_type for n in all_nodes if n.status == SchedulingNodeStatus.TO_LAUNCH
        )
        orig_used_pending = sum(
            not ResourceDemandScheduler._is_running_node(n)
            and bool(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
            for n in all_nodes
        )
        visit_idx = itertools.count()
        best: Optional[Tuple[Tuple, List[SchedulingNode]]] = None

        def _consider(
            nodes: List[SchedulingNode], target_domain: str, prior_score: float = 0.0
        ) -> bool:
            nonlocal best
            d_nodes = [
                n
                for n in nodes
                if n.labels.get(locality_label) == target_domain
                and n.status != SchedulingNodeStatus.TO_TERMINATE
            ]
            num_to_launch = len(nodes) - len(all_nodes)
            used_pending = (
                sum(
                    not ResourceDemandScheduler._is_running_node(n)
                    and bool(n.get_sched_requests(ResourceRequestSource.PENDING_DEMAND))
                    for n in nodes
                )
                - orig_used_pending
            )
            label_score = (
                round(sum(map(_ls, d_nodes)) - prior_score, 9)
                if max_label_score
                else 0.0
            )
            has_running = any(
                ResourceDemandScheduler._is_running_node(n) for n in d_nodes
            )
            launch_neg_rank = (0.0, 0, 0.0)
            if num_to_launch > 0:
                launched = (
                    Counter(
                        n.node_type
                        for n in nodes
                        if n.status == SchedulingNodeStatus.TO_LAUNCH
                    )
                    - orig_to_launch
                )
                launch_neg_rank = tuple(-x for x in min(map(_node_type_rank, launched)))
            key = (
                num_to_launch,
                -label_score,
                launch_neg_rank,
                not has_running,
                used_pending,
                next(visit_idx),
            )
            if best is None or key < best[0]:
                best = (key, nodes)
            return (
                num_to_launch == 0
                and used_pending == 0
                and has_running
                and label_score == max_label_score
            )

        for domain_val, replica in [(d, []) for d in existing_domains] + new_replicas:
            if domain_val not in static_domains:
                d_nodes, originals = (
                    (replica, [])
                    if replica
                    else (domains[domain_val], domains[domain_val])
                )
                pending_siblings: List[SchedulingNode] = []
                if (
                    d_nodes
                    and not domain_val.startswith(_PENDING_DOMAIN_PREFIX)
                    and ResourceDemandScheduler._is_domain_idle(
                        d_nodes, requests, shapes, max_bundles_per_node
                    )
                ):
                    for nt in dict.fromkeys(dn.node_type for dn in d_nodes):
                        pending_siblings.extend(
                            pn
                            for pn in unlabeled_pending_by_type.get(nt, ())
                            if pn.status == SchedulingNodeStatus.SCHEDULABLE
                        )
                prior_score = sum(map(_ls, d_nodes)) if max_label_score else 0.0
                nodes = ResourceDemandScheduler._schedule_in_domain(
                    gang_ctx,
                    all_nodes,
                    d_nodes,
                    originals,
                    pending_siblings,
                    requests,
                    locality_label,
                    domain_val,
                )
            else:
                prior_score = (
                    sum(
                        _ls(n)
                        for n in all_nodes
                        if n.labels.get(locality_label) == domain_val
                        and n.status != SchedulingNodeStatus.TO_TERMINATE
                    )
                    if max_label_score
                    else 0.0
                )
                domain_reqs = (
                    ResourceDemandScheduler._add_label_constraints_to_requests(
                        requests,
                        [
                            LabelSelectorConstraint(
                                label_key=locality_label,
                                operator=_LABEL_OPERATOR_IN,
                                label_values=[domain_val],
                            )
                        ],
                    )
                )
                nodes, infeasible = ResourceDemandScheduler._try_schedule(
                    gang_ctx, domain_reqs, ResourceRequestSource.PENDING_DEMAND
                )
                if infeasible:
                    nodes = None

            if nodes is not None and _consider(nodes, domain_val, prior_score):
                return best[1], []

        if not replica_domain_types and not pinned:
            for nt in sorted(
                (
                    nt
                    for nt, cfg in node_type_configs.items()
                    if not cfg.labels.get(locality_label)
                ),
                key=_node_type_rank,
                reverse=True,
            ):
                pool = unlabeled_pending_by_type.get(nt, [])
                avail = node_type_available.get(nt, 0)
                if not pool and avail <= 0:
                    continue
                nt_ctx = gang_ctx.restricted_to(pool)
                if avail > 0:
                    nt_ctx._node_type_available = {nt: avail}
                if max_num_nodes is not None:
                    nt_ctx._max_num_nodes = (
                        max_num_nodes
                        - (len(all_nodes) - len(pool))
                        - missing_backfill_total
                    )
                scheduled, infeasible = ResourceDemandScheduler._try_schedule(
                    nt_ctx, requests, ResourceRequestSource.PENDING_DEMAND
                )
                if infeasible:
                    continue
                pending_domain = gang_ctx._new_pending_domain(nt)
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
                        n.labels[locality_label] = pending_domain
                pool_ids = {id(n) for n in pool}
                nodes = [n for n in all_nodes if id(n) not in pool_ids] + scheduled
                _consider(nodes, pending_domain)

        return (best[1], []) if best is not None else (all_nodes, requests)

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

        # Pre-filter: skip RAY_RUNNING nodes that definitely cannot fit any
        # request, avoiding expensive deepcopy + try_schedule in _sched_best_node.
        exhausted_nodes = []
        schedulable_nodes = []
        for node in existing_nodes:
            if node.status == SchedulingNodeStatus.TO_TERMINATE or (
                node.im_instance_status == Instance.RAY_RUNNING
                and not _can_fit_any_request(
                    node.get_available_resources(resource_request_source),
                    resource_shapes,
                )
            ):
                exhausted_nodes.append(node)
            else:
                schedulable_nodes.append(node)
        existing_nodes = schedulable_nodes

        # Anti-affinity adds labels while scheduling. Cache scheduling attempts
        # only if no label selector reads those labels.
        unique_requests = {shape_keys[id(r)]: r for r in requests_to_sched}.values()
        anti_affinity_labels = {
            c.anti_affinity.label_name
            for r in unique_requests
            for c in r.placement_constraints
            if c.HasField("anti_affinity")
        }
        static_labels = not any(
            c.label_key in anti_affinity_labels
            for r in unique_requests
            for selector in r.label_selectors
            for c in selector.label_constraints
        )

        def _can_host_more(node: SchedulingNode) -> bool:
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

        replica_sizes = ctx.get_multihost_replica_sizes()
        missing_backfill_total = sum(ctx.get_missing_replica_backfill_slots().values())
        max_num_nodes = ctx.get_max_num_nodes()

        def _can_launch(nt: NodeType, cur_nodes: int) -> bool:
            size = replica_sizes.get(nt, 1)
            return node_type_available.get(nt, 0) >= size and (
                max_num_nodes is None
                or cur_nodes + missing_backfill_total + size <= max_num_nodes
            )

        candidate_replicas: Dict[NodeType, List[SchedulingNode]] = {
            nt: ctx.make_replica_nodes(nt)
            for nt in node_type_available
            if _can_launch(nt, len(target_nodes))
        }
        launched_nodes: List[SchedulingNode] = []
        attempts = {} if static_labels else None

        while len(requests_to_sched) > 0:
            num_nodes = len(target_nodes) + len(launched_nodes)
            fresh_nodes = [
                n
                for nt, rep in candidate_replicas.items()
                if _can_launch(nt, num_nodes)
                for n in rep
            ]
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
                            if attempts is not None:
                                attempts.pop(id(sib), None)
                            target_nodes.append(sib)
                size = replica_sizes.get(nt, 1)
                node_type_available[nt] -= size
                if node_type_available[nt] >= size:
                    candidate_replicas[nt] = ctx.make_replica_nodes(nt)

            (launched_nodes if _can_host_more(best_node) else target_nodes).append(
                best_node
            )

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
                Requires that no label selector reads a label that anti-affinity
                adds. If None, every node is simulated with every request.

        Returns:
            best_node: The best node to schedule the requests.
            infeasible: The infeasible requests that cannot be scheduled on the best
                node.
            nodes: Remaining nodes after the best node is removed.
        """

        def shape_key(r: ResourceRequest) -> bytes:
            sk = shape_keys.get(id(r)) if shape_keys is not None else None
            return sk if sk is not None else r.SerializeToString(deterministic=True)

        def has_prefix(tier_keys: Iterable[bytes], prefix: Tuple[bytes, ...]) -> bool:
            return tuple(itertools.islice(tier_keys, len(prefix))) == prefix

        def accepted_prefix(
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

        keys = [shape_key(r) for r in requests]
        requests_by_key = dict(zip(keys, requests))
        results = []

        # A temporary data class to store the scheduling result.
        @dataclass
        class ScheduleResult:
            # Label selector score by shape key for each request this node can fit.
            label_scores: Dict[bytes, float] = field(default_factory=dict)
            # Best label selector scores when this result was simulated.
            best_label_scores: Optional[Dict[bytes, float]] = None
            # Prefix through the last accepted shape in this node's top label tier
            # (None before simulation), and in tier 0 when co-packed below top > 0.
            top_prefix: Optional[Tuple[bytes, ...]] = None
            zero_prefix: Tuple[bytes, ...] = ()
            # The node candidate after a scheduling attempt, or None if it could
            # not schedule any of the requests.
            node: Optional[SchedulingNode] = None
            # The resource requests scheduled on the node.
            scheduled_requests: List[ResourceRequest] = field(default_factory=list)
            # the score of the scheduling node to compare with others.
            score: Optional[UtilizationScore] = None

        # Without a shared cache, label scores are still needed to normalize
        # tiers, but nothing is reused across calls.
        cache = attempts if attempts is not None else {}
        for node in nodes:
            if id(node) not in cache:
                cache[id(node)] = ScheduleResult(
                    {
                        k: node._satisfies_label_constraints([r])
                        for k, r in requests_by_key.items()
                        if node._can_schedule_one(r, resource_request_source)
                    }
                )
        best_label_scores = {
            k: max((cache[id(n)].label_scores.get(k, 0) for n in nodes), default=0)
            for k, r in requests_by_key.items()
            if len(r.label_selectors) > 1
        }
        # Any node whose best schedulable tier is below `pass_top` loses to a
        # `pass_top` candidate on `score[0]`, so skip simulating it until needed.
        pass_top = max(
            (
                1.0 if score > 0 else 0.0
                for n in nodes
                for k, score in cache[id(n)].label_scores.items()
                if k in requests_by_key
            ),
            default=1.0,
        )

        # Track node states we've already simulated in this pass. Since we only
        # select one best node to return, we skip the heavy deepcopy/simulation
        # overhead for duplicates
        node_cache = NodeStateCache(resource_request_source)

        # Iterate through each node and modify the node's available resources
        # if the requests are schedulable.
        for idx, node in enumerate(nodes):
            # Skip this node if we've already evaluated its exact state.
            if node_cache.was_seen_or_mark(node):
                continue

            result = cache[id(node)]
            node_best_scores = {
                k: best_label_scores[k]
                for k in result.label_scores
                if k in best_label_scores
            }

            def node_tier(k: bytes) -> float:
                score = result.label_scores[k]
                best = node_best_scores.get(k)
                return min(score / best, 1.0) if best else score

            top = -1.0
            if attempts is not None:
                top = max(
                    (node_tier(k) for k in result.label_scores if k in requests_by_key),
                    default=-1.0,
                )
                if top < pass_top:
                    continue

            # `try_schedule` runs this node's top tier, then tier 0 unless a fitting
            # fallback tier blocks it (any such blocker has >1 selectors, so
            # `node_best_scores` invalidates when it leaves or promotes). Since calls
            # only drop requests, matching accepted prefixes leave only rejected tail
            # shapes and keep the cached schedule and score exact.
            if not (
                attempts is not None
                and result.top_prefix
                and result.best_label_scores == node_best_scores
                and has_prefix(
                    (
                        k
                        for k in keys
                        if k in result.label_scores and node_tier(k) == top
                    ),
                    result.top_prefix,
                )
                and (
                    not result.zero_prefix
                    or has_prefix(
                        (k for k in keys if result.label_scores.get(k) == 0),
                        result.zero_prefix,
                    )
                )
            ):
                # With an attempt cache, only simulate the requests that this node
                # can schedule, and skip the deepcopy if none do.
                node_requests = (
                    [r for r, k in zip(requests, keys) if k in result.label_scores]
                    if attempts is not None
                    else requests
                )
                result = ScheduleResult(
                    result.label_scores, node_best_scores, top_prefix=()
                )
                if node_requests:
                    node_copy = copy.deepcopy(node)
                    num_sched = len(
                        node_copy.get_sched_requests(resource_request_source)
                    )
                    _, score = node_copy.try_schedule(
                        node_requests,
                        resource_request_source,
                        shape_keys,
                        node_best_scores,
                    )
                    new_sched = node_copy.get_sched_requests(resource_request_source)[
                        num_sched:
                    ]
                    if new_sched:
                        top_prefix: Tuple[bytes, ...] = ()
                        zero_prefix: Tuple[bytes, ...] = ()
                        if attempts is not None:
                            sched_counts = Counter(map(shape_key, new_sched))
                            node_keys = [k for k in keys if k in result.label_scores]
                            top_prefix = accepted_prefix(
                                (k for k in node_keys if node_tier(k) == top),
                                sched_counts,
                            )
                            if any(sched_counts.values()):
                                zero_prefix = accepted_prefix(
                                    (
                                        k
                                        for k in node_keys
                                        if result.label_scores[k] == 0
                                    ),
                                    sched_counts,
                                )
                        result = ScheduleResult(
                            result.label_scores,
                            node_best_scores,
                            top_prefix,
                            zero_prefix,
                            node_copy,
                            new_sched,
                            score=score,
                        )
                cache[id(node)] = result

            if result.node is not None:
                results.append((idx, result))

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
                item[1].score,
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

        if ctx._disable_launch_config_check:
            # Outdated nodes check through launch config check is disabled.
            return

        for node in nodes:
            if node.status != SchedulingNodeStatus.SCHEDULABLE:
                # We don't need to care about the non-running nodes.
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
                ResourceDemandScheduler._is_running_node(node)
                and node_type_config.launch_config_hash
                and node_type_config.launch_config_hash != node.launch_config_hash
            ):
                # The node type config has been updated, and the node's launch config
                # hash is outdated.
                node.status = SchedulingNodeStatus.TO_TERMINATE
                node.termination_request = TerminationRequest(
                    id=str(time.time_ns()),
                    instance_id=node.im_instance_id,
                    ray_node_id=node.ray_node_id,
                    instance_type=node.node_type,
                    instance_status=node.im_instance_status,
                    cause=TerminationRequest.Cause.OUTDATED,
                    details=f"node from {node.node_type} has outdated config",
                )

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
        replica_sizes = ctx.get_multihost_replica_sizes()
        replicas_by_type: Dict[NodeType, Dict[str, List[SchedulingNode]]] = defaultdict(
            lambda: defaultdict(list)
        )
        active_replicas_by_type: Dict[NodeType, Set[str]] = {
            nt: ctx.get_pinned_domains_for_type(nt) for nt in replica_sizes
        }
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
                    ResourceDemandScheduler._is_running_node(node)
                    and any(
                        total - avail.get(res, 0.0) > 1e-9
                        for res, total in node.total_resources.items()
                    )
                ):
                    active_replicas_by_type[node.node_type].add(node.replica_id)

        unneeded_nodes_by_type: Dict[NodeType, List[SchedulingNode]] = defaultdict(list)
        for node in nodes:
            if (
                node.status == SchedulingNodeStatus.SCHEDULABLE
                and node.node_kind != NodeKind.HEAD
                and node.im_instance_status
                in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
                and not any(node.sched_requests.values())
                and not (
                    node.node_type in replica_sizes
                    and node.replica_id in active_replicas_by_type[node.node_type]
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
                for rid, rn in by_rep.items()
                if rn and all(m._has_runtime_domain for m in rn)
            }
            unneeded_nodes.sort(
                key=lambda n: (
                    size > 1
                    and not (
                        len(by_rep.get(n.replica_id or "", ())) == size
                        and all(
                            ResourceDemandScheduler._is_unrequested_queued(m)
                            for m in by_rep[n.replica_id]
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
                node.termination_request is not None
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
                    else:
                        since_s.pop(0)

    @staticmethod
    def _termination_units(
        nodes: List[SchedulingNode],
        replica_sizes: Dict[NodeType, int],
        node_type_configs: Dict[NodeType, NodeTypeConfig],
        pinned_domains: Dict[str, Set[str]],
    ) -> List[List[SchedulingNode]]:
        """Group worker nodes into ordered termination units, keeping multi-worker
        replicas whole so replica-atomic providers never lose active sibling hosts."""
        sort_key = ResourceDemandScheduler._sort_nodes_for_termination
        term_order = ResourceDemandScheduler._PENDING_TERMINATION_ORDER
        base = {id(n): sort_key(n) for n in nodes}

        def _dom_key(nt: NodeType) -> Optional[str]:
            cfg = node_type_configs.get(nt)
            return (
                cfg.replica_config.domain_label_key
                if cfg is not None and cfg.replica_config is not None
                else None
            )

        def _term_rank(m: SchedulingNode) -> int:
            if m.im_instance_status == Instance.QUEUED and m._has_launch_request:
                return term_order[Instance.REQUESTED]
            return term_order.get(m.im_instance_status, 4)

        def _prog_key(u: List[SchedulingNode]) -> Tuple[bool, int, float]:
            ranks = [_term_rank(m) for m in u]
            return (
                len(u) < replica_sizes.get(u[0].node_type, 1),
                max(ranks),
                sum(ranks) / len(ranks),
            )

        ranked_units: List[
            Tuple[Tuple, Tuple[bool, int, float], str, List[SchedulingNode]]
        ] = []
        grouped: Dict[Tuple[NodeType, str], List[SchedulingNode]] = defaultdict(list)
        for n in nodes:
            dk = _dom_key(n.node_type)
            rid = (
                n.replica_id or (n.labels.get(dk) if dk else None)
                if n.node_type in replica_sizes
                else None
            )
            if rid is None:
                ranked_units.append((base[id(n)], _prog_key([n]), "", [n]))
            else:
                grouped[(n.node_type, rid)].append(n)

        for (nt, rid), unit in grouped.items():
            dk = _dom_key(nt)
            u_sorted = sorted(
                unit,
                key=lambda m: (base[id(m)], _term_rank(m)),
            )
            worst = max(base[id(m)] for m in u_sorted)
            if dk and rid in pinned_domains.get(dk, ()):
                worst = max(worst, (True, 0, 0.0))
            ranked_units.append((worst, _prog_key(u_sorted), rid, u_sorted))

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

    @staticmethod
    def _enforce_idle_termination(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce idle termination for nodes not needed by demands or constraints.
        Multi-worker replica node types terminate only as whole idle replicas."""
        count_by_node_type = ctx.get_cluster_shape()
        node_type_configs = ctx.get_node_type_configs()
        replica_sizes = ctx.get_multihost_replica_sizes()
        terminate_nodes_by_type: Dict[NodeType, int] = defaultdict(int)
        terminate_non_queued_by_type: Dict[NodeType, int] = defaultdict(int)
        nodes = ctx.get_nodes()
        non_queued_count_by_type: Counter = Counter(
            n.node_type
            for n in nodes
            if n.node_kind != NodeKind.HEAD
            and n.status == SchedulingNodeStatus.SCHEDULABLE
            and not ResourceDemandScheduler._is_unrequested_queued(n)
        )
        # A node whose Ray stop is already requested is leaving, so it must not
        # count toward min_worker_nodes when choosing more nodes to idle terminate.
        for n in nodes:
            if (
                n.status == SchedulingNodeStatus.SCHEDULABLE
                and n.im_instance_status == Instance.RAY_STOP_REQUESTED
            ):
                count_by_node_type[n.node_type] -= 1
                non_queued_count_by_type[n.node_type] -= 1
        idle_timeouts: Dict[int, float] = {}
        for n in nodes:
            t = ResourceDemandScheduler._idle_timeout_for_node(
                n, ctx, node_type_configs
            )
            if t is not None:
                idle_timeouts[id(n)] = t
        units: List[List[SchedulingNode]] = [
            [n]
            for n in nodes
            if n.node_type not in replica_sizes and id(n) in idle_timeouts
        ]

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

        for node_type, size in replica_sizes.items():
            pinned_for_type = ctx.get_pinned_domains_for_type(node_type)
            known: List[Tuple[str, List[SchedulingNode]]] = []
            unknown: List[List[SchedulingNode]] = []
            for rid, r_nodes in sorted(
                replica_nodes_by_type.get(node_type, {}).items()
            ):
                if all(
                    ResourceDemandScheduler._is_unrequested_queued(m) for m in r_nodes
                ):
                    if all(id(m) in idle_timeouts for m in r_nodes):
                        units.append(r_nodes)
                elif all(m._has_runtime_domain for m in r_nodes):
                    known.append((rid, r_nodes))
                elif all(
                    ResourceDemandScheduler._progress_rank(m) < 3 for m in r_nodes
                ):
                    unknown.append(r_nodes)

            has_active_inc = False
            for rid, rn in known:
                if all(id(m) in idle_timeouts for m in rn) and not (
                    rid in pinned_for_type and len(rn) < size
                ):
                    units.append(rn)
                elif len(rn) < size:
                    has_active_inc = True

            flat = [m for rn in unknown for m in rn]
            if (
                flat
                and not has_active_inc
                and all(
                    ResourceDemandScheduler._progress_rank(m) < 3
                    and id(m) in idle_timeouts
                    for m in flat
                )
            ):
                flat.sort(
                    key=lambda m: (
                        ResourceDemandScheduler._progress_rank(m),
                        m.idle_duration_ms,
                    ),
                    reverse=True,
                )
                units.extend(
                    flat[i : i + size]
                    for i in range(0, len(flat), size)
                    if len(flat[i : i + size]) == size or not known
                )

        units.sort(
            key=lambda u: (
                any(len(n.ray_node_id) > 0 for n in u),
                -min(n.idle_duration_ms for n in u),
                -min(ResourceDemandScheduler._progress_rank(n) for n in u),
            )
        )
        for unit in units:
            nt = unit[0].node_type
            cfg = node_type_configs.get(nt)
            min_count = cfg.min_worker_nodes if cfg is not None else 0
            non_queued = sum(
                n.node_kind != NodeKind.HEAD
                and n.status == SchedulingNodeStatus.SCHEDULABLE
                and not ResourceDemandScheduler._is_unrequested_queued(n)
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
                timeout_s = idle_timeouts[id(node)]
                node.status = SchedulingNodeStatus.TO_TERMINATE
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
