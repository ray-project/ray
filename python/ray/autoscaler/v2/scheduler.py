import copy
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Callable,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
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
from ray.core.generated.common_pb2 import LabelSelectorOperator
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
            )

        return SchedulingNode.from_node_config(
            node_config,
            SchedulingNodeStatus.SCHEDULABLE,
            node_kind=instance.im_instance.node_kind,
            im_instance_id=instance.im_instance.instance_id,
            im_instance_status=instance.im_instance.status,
        )

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
    ) -> "SchedulingNode":
        """Create a scheduling node from a node config.

        Args:
            node_config: The node config.
            status: The status of the node.
            node_kind: The node kind.
            im_instance_id: The instance id of the im instance.
            im_instance_status: The instance status of the im instance.

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

    def __init__(self, event_logger: Optional[AutoscalerEventLogger] = None):
        self._event_logger = event_logger
        # Pending instances have no idle duration reported by GCS. For each node
        # type, the k-th entry is the monotonic time since which at least k + 1
        # pending nodes of that type have been continuously not needed by any
        # resource demands. It's tracked by counts rather than by instances, since
        # pending nodes of the same type are interchangeable for scheduling.
        self._pending_unneeded_since_s: Dict[NodeType, List[float]] = {}

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
        ):
            self._nodes = nodes
            self._node_type_configs = node_type_configs
            self._node_type_available = self._compute_available_node_types(
                nodes, node_type_configs
            )
            self._max_num_nodes = max_num_nodes
            self._idle_timeout_s = idle_timeout_s
            self._pending_idle_timeout_s = pending_idle_timeout_s
            self._disable_launch_config_check = disable_launch_config_check
            self._ippr_specs = ippr_specs
            self._cloud_resource_availabilities = cloud_resource_availabilities
            self._recoverable_resource_availabilities = (
                recoverable_resource_availabilities
            )

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

            # Update the available node types.
            self._node_type_available = self._compute_available_node_types(
                self._nodes, self._node_type_configs
            )

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
        )

        assert len(to_terminate_nodes) == num_to_terminate, (
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

        Returns:
            A tuple of:
                - The terminated nodes.
                - The remained nodes.
        """

        # Sort the nodes for termination.
        nodes.sort(key=ResourceDemandScheduler._sort_nodes_for_termination)

        # Remove the head node from the list.
        head_node = None
        for i, node in enumerate(nodes):
            if node.node_kind == NodeKind.HEAD:
                # Remove the head node from the list.
                head_node = nodes.pop(i)
                break

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
                # The launch is only cancelled: there is no instance to terminate, and a
                # request without an instance id would fail the instance manager update.
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

        # Count the existing nodes by type
        count_by_node_type = ctx.get_cluster_shape()

        new_nodes = []
        # Launch new nodes to satisfy min count for each node type.
        for (
            node_type,
            node_type_config,
        ) in ctx.get_node_type_configs().items():
            cur_count = count_by_node_type.get(node_type, 0)
            min_count = node_type_config.min_worker_nodes
            if cur_count < min_count:
                logger.info(
                    f"Adding {min_count - cur_count} nodes to satisfy min count for "
                    f"node type: {node_type}."
                )
                # One node per launch: a list repeating a single node would make every
                # launch the same object, so cancelling one would cancel them all.
                new_nodes.extend(
                    SchedulingNode.from_node_config(
                        copy.deepcopy(node_type_config),
                        status=SchedulingNodeStatus.TO_LAUNCH,
                        node_kind=NodeKind.WORKER,
                    )
                    for _ in range(min_count - cur_count)
                )
        # NOTE: we assume the aggregated number of min workers across all node types
        # should not exceed any globally enforced max_num_nodes

        # Add the new nodes to the existing nodes and update the context.
        ctx.update(new_nodes + ctx.get_nodes())

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


        Args:
            ctx: The schedule context.
            gang_requests: The gang resource requests.

        Returns:
            A list of infeasible gang resource requests.
        """

        def _sort_gang_resource_requests(req: GangResourceRequest) -> Tuple:
            """Return the sort key for a gang resource request.

            The key sorts the gang resource request by:
                1. the number of placement constraints in the gang request.
                2. the number of resource requests in the gang request.
            """
            total_placement_constraints = 0
            for resource_request in req.requests:
                total_placement_constraints += len(
                    resource_request.placement_constraints
                )

            return (total_placement_constraints, len(req.requests))

        infeasible_gang_requests = []
        # Try fulfilling the gang requests one by one.
        for gang_req in sorted(
            gang_requests, key=_sort_gang_resource_requests, reverse=True
        ):
            if gang_req.bundle_selectors:
                # TODO: @ryanaoleary multiple `bundle_selectors` will be supported
                # for `fallback_strategy`.
                requests = gang_req.bundle_selectors[0].resource_requests
            else:
                # Use legacy field if `bundle_selectors` not provided.
                requests = gang_req.requests
            # Try to combine requests with affinity constraints into the same request.
            requests = ResourceRequestUtil.combine_requests_with_affinity(requests)

            nodes, infeasible = ResourceDemandScheduler._try_schedule(
                ctx, requests, ResourceRequestSource.PENDING_DEMAND
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

        # Pre-filter: skip RAY_RUNNING nodes that definitely cannot fit any
        # request, avoiding expensive deepcopy + try_schedule in _sched_best_node.
        exhausted_nodes = []
        schedulable_nodes = []
        for node in existing_nodes:
            if (
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

        def _can_host_more(node: SchedulingNode) -> bool:
            avail = node.get_available_resources(resource_request_source)
            return _can_fit_any_request(avail, resource_shapes) and any(
                node._schedulable_selector(r, avail) is not None
                for r in {shape_keys[id(r)]: r for r in requests_to_sched}.values()
            )

        # Try scheduling resource requests with existing nodes first.
        attempts: Dict[int, _NodeAttempt] = {}
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
            if node.ippr_status is not None and node.ippr_status.can_resize_up():
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

        ippr_specs = ctx.get_ippr_specs()

        def _to_launch_node_with_ippr_caps(node_type: NodeType) -> SchedulingNode:
            node = SchedulingNode.from_node_config(
                ctx.get_node_type_configs()[node_type],
                status=SchedulingNodeStatus.TO_LAUNCH,
                node_kind=NodeKind.WORKER,
            )
            # If the new node can be resized, consider its maximum IPPR capacity.
            if ippr_specs and node_type in ippr_specs.groups:
                group = ippr_specs.groups[node_type]
                node.update_total_resources(
                    {
                        "CPU": float(
                            max(group.max_cpu, node.total_resources.get("CPU", 0))
                        ),
                        "memory": float(
                            max(group.max_memory, node.total_resources.get("memory", 0))
                        ),
                    }
                )
            return node

        # Try scheduling resource requests with new nodes.
        node_pools = [
            _to_launch_node_with_ippr_caps(node_type)
            for node_type, num_available in node_type_available.items()
            if num_available > 0
        ]
        # New nodes that may still host deferred fallback or unconstrained requests.
        launched_nodes: List[SchedulingNode] = []
        max_num_nodes = ctx.get_max_num_nodes()
        attempts = {}

        while len(requests_to_sched) > 0:
            num_committed = len(launched_nodes)
            num_nodes = len(target_nodes) + num_committed
            can_launch_new = max_num_nodes is None or num_nodes < max_num_nodes
            fresh_nodes = node_pools if can_launch_new else []
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
            )
            if best_node is None:
                if not can_launch_new:
                    logger.debug(
                        "Max number of nodes reached: {}, "
                        "cannot launch more nodes.".format(max_num_nodes)
                    )
                break

            picked_committed = num_committed > 0 and (
                not fresh_nodes or remaining[num_committed - 1] is fresh_nodes[0]
            )
            split = num_committed - 1 if picked_committed else num_committed
            launched_nodes = remaining[:split]
            if not picked_committed:
                remaining_fresh = remaining[split:]
                # Update the node pool if a node with the same node type of the
                # added node can be launched.
                node_type_available[best_node.node_type] -= 1
                if node_type_available[best_node.node_type] > 0:
                    remaining_fresh.append(
                        _to_launch_node_with_ippr_caps(best_node.node_type)
                    )
                node_pools = remaining_fresh

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
            if not node_cache.was_seen_or_mark(node)
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
                item[2].score[1:],
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
                node_type_config.launch_config_hash
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
        unneeded_nodes_by_type: Dict[NodeType, List[SchedulingNode]] = defaultdict(list)
        for node in nodes:
            if (
                node.status != SchedulingNodeStatus.SCHEDULABLE
                or node.node_kind == NodeKind.HEAD
                or node.im_instance_status
                not in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
                or any(node.sched_requests.values())
            ):
                continue
            unneeded_nodes_by_type[node.node_type].append(node)

        unneeded_since_s: Dict[NodeType, List[float]] = {}
        for node_type, unneeded_nodes in unneeded_nodes_by_type.items():
            # Keep the oldest timestamps when the count drops, and start new ones
            # when it grows. A node type without any unneeded pending nodes is
            # dropped, which resets its durations.
            prev_since_s = self._pending_unneeded_since_s.get(node_type, [])
            since_s = prev_since_s[: len(unneeded_nodes)]
            since_s += [now_s] * (len(unneeded_nodes) - len(since_s))
            unneeded_since_s[node_type] = since_s

            # Assign the longest durations to the least progressed nodes, which are
            # the cheapest to terminate (e.g. QUEUED ones have no cloud instance).
            unneeded_nodes.sort(
                key=lambda n: ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES.index(
                    n.im_instance_status
                )
            )
            for node, node_since_s in zip(unneeded_nodes, since_s):
                node.idle_duration_ms = int((now_s - node_since_s) * 1000)

        self._pending_unneeded_since_s = unneeded_since_s
        ctx.update(nodes)

    def _release_terminated_pending_nodes_idle_duration(
        self,
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Drop the idle durations taken by the pending nodes being idle terminated.

        Otherwise, once the terminated nodes are gone, their (expired) durations
        would be inherited by the remaining unneeded pending nodes of the same
        type, which may have been unneeded for much shorter.

        Only the idle terminations are released: nodes terminated for other causes
        (e.g. outdated, max_worker_nodes) are terminated before the unneeded pending
        nodes are counted, so they never take a duration in this round.

        Args:
            ctx: The schedule context, after idle termination is enforced.
        """
        for node in ctx.get_nodes():
            if (
                node.termination_request is None
                or node.termination_request.cause != TerminationRequest.Cause.IDLE
                or node.im_instance_status
                not in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
            ):
                continue
            # Idle termination visits the longest idle nodes first, so the ones
            # terminated always hold the oldest timestamps of their type.
            since_s = self._pending_unneeded_since_s.get(node.node_type)
            if since_s:
                since_s.pop(0)

    @staticmethod
    def _enforce_idle_termination(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """Enforce the idle termination for the nodes that are not needed by the cluster
        resource constraints and idle for too long.

        Args:
            ctx: The schedule context.
        """
        count_by_node_type = ctx.get_cluster_shape()
        node_type_configs = ctx.get_node_type_configs()
        terminate_nodes_by_type: Dict[NodeType, int] = defaultdict(int)

        nodes = ctx.get_nodes()
        s_to_ms = 1000
        # Visit the nodes in termination preference order, i.e. pending nodes and
        # nodes idle for longer first, so that the nodes kept for min_worker_nodes
        # are the most useful ones.
        for node in sorted(
            nodes, key=ResourceDemandScheduler._sort_nodes_for_termination
        ):
            if node.status != SchedulingNodeStatus.SCHEDULABLE:
                # We don't need to care about the non-running nodes.
                continue

            if node.node_kind == NodeKind.HEAD:
                # The head node is not subject to idle termination.
                continue

            node_type = node.node_type
            if (
                node.im_instance_status
                in ResourceDemandScheduler.PENDING_TERMINABLE_STATUSES
            ):
                # Pending nodes have their own idle timeout.
                idle_timeout_s = ctx.get_pending_idle_timeout_s()
            else:
                idle_timeout_s = ctx.get_idle_timeout_s()
                # Override the scheduler idle_timeout_s if set for this node_type.
                if node_type in node_type_configs:
                    if node_type_configs[node_type].idle_timeout_s is not None:
                        idle_timeout_s = node_type_configs[node_type].idle_timeout_s
            if idle_timeout_s is None:
                # No idle timeout is set, skip the idle termination.
                continue

            if node.idle_duration_ms <= idle_timeout_s * s_to_ms:
                # The node is not idle for too long, skip it.
                continue

            if node.sched_requests[ResourceRequestSource.PENDING_DEMAND]:
                # The node is needed by the pending requests.
                # Skip it.
                logger.debug(
                    "Node {} (idle for {} secs) is needed by the pending requests, "
                    "skip idle termination.".format(
                        node.ray_node_id, node.idle_duration_ms / s_to_ms
                    )
                )
                continue

            if node.sched_requests[ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT]:
                # The node is needed by the resource constraints.
                # Skip it.
                logger.debug(
                    "Node {} (idle for {} secs) is needed by the cluster resource "
                    "constraints, skip idle termination.".format(
                        node.ray_node_id, node.idle_duration_ms / s_to_ms
                    )
                )
                continue

            # Honor the min_worker_nodes setting for the node type.
            min_count = 0
            if node_type in node_type_configs:
                min_count = node_type_configs[node_type].min_worker_nodes
            if (
                count_by_node_type.get(node_type, 0)
                - terminate_nodes_by_type[node_type]
                <= min_count
            ):
                logger.info(
                    "Node {} (idle for {} secs) belongs to node_type {} and is "
                    "required by min_worker_nodes, skipping idle termination.".format(
                        node.ray_node_id, node.idle_duration_ms / s_to_ms, node_type
                    )
                )
                continue

            terminate_nodes_by_type[node.node_type] += 1
            # The node is idle for too long, terminate it.
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
                    f"for {node.idle_duration_ms/s_to_ms} secs > "
                    f"timeout={idle_timeout_s} secs"
                ),
            )

        ctx.update(nodes)
