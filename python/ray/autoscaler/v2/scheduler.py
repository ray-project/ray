import copy
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

from ray._private.protobuf_compat import message_to_dict
from ray.autoscaler._private.constants import AUTOSCALER_CONSERVE_GPU_NODES
from ray.autoscaler._private.resource_demand_scheduler import (
    UtilizationScore,
    _fits,
    _inplace_subtract,
)
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.schema import AutoscalerInstance, NodeType
from ray.autoscaler.v2.utils import ProtobufUtil, ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    ResourceRequest,
    ResourceRequestByCount,
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


@dataclass
class SchedulingReply:
    # Instances to launch.
    to_launch: List[LaunchRequest] = field(default_factory=list)
    # To terminate.
    to_terminate: List[TerminationRequest] = field(default_factory=list)
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
    """
    Interface for a resource scheduler.

    Implements the `instance_manager.proto ResourceSchedulerService` interface.
    """

    @abstractmethod
    def schedule(self, request: SchedulingRequest) -> SchedulingReply:
        """
        Given the resource requests and the current cluster state, calculate the
        target cluster shape by trying to schedule the resource requests on the
        nodes.
        """
        pass


class SchedulingNodeStatus(Enum):
    """
    The status of a scheduling node (`SchedulingNode`)
    """

    # The node is added by the ResourceDemandScheduler.
    TO_LAUNCH = "TO_LAUNCH"
    # The node is pending, i.e. there's already an autoscaler instance being launched
    # The node is schedulable. It could be running ray or pending to run ray. Either
    # Way, it should be able to accept new resource requests/resource constraints.
    SCHEDULABLE = "SCHEDULABLE"
    # The node is to be terminated by the ResourceDemandScheduler
    TO_TERMINATE = "TO_TERMINATE"


class ResourceRequestSource(Enum):
    """
    The source of the resource request.
    """

    # The resource request is from demand, e.g. ray tasks/actors,
    # placement groups, etc.
    PENDING_DEMAND = "PENDING_DEMAND"
    # The resource request is from the cluster resource constraints, i.e.
    # from ray.autoscaler.sdk.request_resources().
    CLUSTER_RESOURCE_CONSTRAINT = "CLUSTER_RESOURCE_CONSTRAINT"


@dataclass
class SchedulingNode:
    """
    A abstraction of a node that can be scheduled on by the resource scheduler.

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
    # Node's labels, including static or dynamic labels.
    labels: Dict[str, str] = field(default_factory=dict)
    # Observability descriptive message for why the node was launched in the
    # first place.
    launch_reason: Optional[str] = None
    # Termination request, none when the node is not being terminated.
    termination_request: Optional[TerminationRequest] = None
    # The instance id of the IM(Instance Manager) instance. None if the node
    # is not yet in IM.
    im_instance_id: Optional[str] = None
    # The ray node id of the ray node. None if the node is not included in
    # ray cluster's GCS report yet (not running ray yet).
    ray_node_id: Optional[str] = None
    # Idle duration in ms. Default not idle.
    idle_duration_ms: int = 0
    # Launch config hash.
    launch_config_hash: Optional[str] = None
    # node kind.
    node_kind: NodeKind = NodeKind.WORKER

    def __init__(
        self,
        node_type: NodeType,
        total_resources: Dict[str, float],
        available_resources: Dict[str, float],
        labels: Dict[str, str],
        status: SchedulingNodeStatus,
        im_instance_id: str = "",
        ray_node_id: str = "",
        idle_duration_ms: int = 0,
        launch_config_hash: str = "",
        node_kind: NodeKind = NodeKind.WORKER,
        termination_request: Optional[TerminationRequest] = None,
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
        self.ray_node_id = ray_node_id
        self.idle_duration_ms = idle_duration_ms
        self.launch_config_hash = launch_config_hash
        self.node_kind = node_kind
        self.termination_request = termination_request

    def get_available_resources(self, resource_request_source: ResourceRequestSource):
        """Get the available resources for the given resource request source."""
        return self.available_resources_for_sched[resource_request_source]

    def get_sched_requests(self, resource_request_source: ResourceRequestSource):
        """Get the resource requests for the given resource request source."""
        return self.sched_requests[resource_request_source]

    def add_sched_request(
        self,
        request: ResourceRequest,
        resource_request_source: ResourceRequestSource,
    ):
        """
        Add the resource requests to the node.

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
        """
        Create a new scheduling node from an autoscaler instance.

        It creates:
            - None if the instance is not schedulable by IM.
            - A schedulable node if the instance is running ray or pending to run ray,
              so it should be considered in the scheduling process.

        Args:
            instance: The instance.
            node_type_configs: The node type configs.
            disable_launch_config_check: If outdated node check through launch config is
                disabled.

        """
        if not SchedulingNode.is_schedulable(instance):
            return None

        if instance.im_instance.status == Instance.RAY_RUNNING:
            assert instance.ray_node is not None, (
                "ray node should not be None "
                f"when the instance is running ray: instance={instance}"
            )
            # An running ray node
            return SchedulingNode(
                node_type=instance.im_instance.instance_type,
                total_resources=dict(instance.ray_node.total_resources),
                # Available resources for scheduling requests of different
                # sources.
                available_resources=dict(instance.ray_node.available_resources),
                # Use ray node's dynamic labels.
                labels=dict(instance.ray_node.dynamic_labels),
                status=SchedulingNodeStatus.SCHEDULABLE,
                im_instance_id=instance.im_instance.instance_id,
                ray_node_id=instance.im_instance.node_id,
                idle_duration_ms=instance.ray_node.idle_duration_ms,
                launch_config_hash=instance.im_instance.launch_config_hash,
                node_kind=instance.im_instance.node_kind,
            )

        # This is an instance pending to run ray. Initialize a schedulable node
        # from the node type config.
        node_config = node_type_configs.get(instance.im_instance.instance_type, None)
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
                termination_request=TerminationRequest(
                    id=str(uuid.uuid4()),
                    instance_id=instance.im_instance.instance_id,
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
        )

    @staticmethod
    def is_schedulable(instance: AutoscalerInstance) -> bool:
        """
        Check if the instance is schedulable by IM.

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
    ) -> "SchedulingNode":
        """
        Create a scheduling node from a node config.

        Args:
            node_config: The node config.
            status: The status of the node.
            node_kind: The node kind.
            im_instance_id: The instance id of the im instance.
            node_kind: The node kind.
        """
        return SchedulingNode(
            node_type=node_config.name,
            total_resources=dict(node_config.resources),
            available_resources=dict(node_config.resources),
            labels=dict(node_config.labels),
            status=status,
            im_instance_id=im_instance_id,
            node_kind=node_kind,
        )

    def __post_init__(self):
        assert self.node_type, "node_type should be set"

    def try_schedule(
        self,
        requests: List[ResourceRequest],
        resource_request_source: ResourceRequestSource,
    ) -> Tuple[List[ResourceRequest], UtilizationScore]:
        """
        Try to schedule the resource requests on this node.

        This modifies the node's available resources if the requests are schedulable.
        The requests are scheduled one by one in the sorted order, and no
        backtracking is done.

        Args:
            requests: The resource requests to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.

        Returns:
            A tuple of:
                - list of remaining requests that cannot be scheduled on this node.
                - the utilization score for this node with respect to the current
                resource requests being scheduled.
        """
        # Track the resource requests that cannot be scheduled on this node.
        unschedulable_requests = []

        # Sort the requests and try schedule them one by one.
        for r in requests:
            if not self._try_schedule_one(r, resource_request_source):
                unschedulable_requests.append(r)

        score = self._compute_score(resource_request_source)

        return unschedulable_requests, score

    def _compute_score(
        self, resource_request_source: ResourceRequestSource
    ) -> UtilizationScore:
        """
        Compute the utilization score for this node with respect to the current resource
        request being scheduled.

        A "higher" score means that this node is more suitable for scheduling the
        current scheduled resource requests.

        The score is a tuple of 4 values:
            1. Whether this node is a GPU node and the current resource request has
                GPU requirements:
                    0: if this node is a GPU node and the current resource request
                    placed onto the node has no GPU requirements.
                    1: if this node is not a GPU node or the current resource request
                    placed onto the node has GPU requirements.
            2. The number of resource types being scheduled.
            3. The minimum utilization rate across all resource types.
            4. The average utilization rate across all resource types.

        NOTE:
            This function is adapted from  _resource_based_utilization_scorer from
            autoscaler v1.

        TODO(rickyx,jjyao):  We should also consider node labels for
            scoring. For example, if a node has a label that matches the affinity
            label of the resource request, we should give it a higher score.

        TODO(rickyx): add pluggable scoring functions here.

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
            gpu_ok,
            num_matching_resource_types,
            min(util_by_resources) if util_by_resources else 0,
            float(sum(util_by_resources)) / len(util_by_resources)
            if util_by_resources
            else 0,
        )

    def _try_schedule_one(
        self, request: ResourceRequest, resource_request_source: ResourceRequestSource
    ) -> bool:
        """
        Try to schedule one resource request on this node. The request could be from
        various sources, specified by `resource_request_source`.

        Args:
            request: The resource request to be scheduled.
            resource_request_source: The source of the resource request, i.e.
                pending demands from ray actors/tasks or cluster resource constraints.

        Returns:
            True if the resource request is scheduled on this node.
        """

        # Check if there's placement constraints that are not satisfied.
        for constraint in request.placement_constraints:
            if constraint.HasField("anti_affinity"):
                anti_affinity = constraint.anti_affinity
                if (
                    anti_affinity.label_name in self.labels
                    and anti_affinity.label_value
                    == self.labels[anti_affinity.label_name]
                ):
                    # The node already has a label that matches the anti-affinity
                    return False

            # We don't need to check for affinity constraints here since
            # we have already combined resource requests with the affinity
            # constraints into the same request at `combine_requests_with_affinity`.
            pass

        available_resources_dict = self.get_available_resources(resource_request_source)

        # Check if there's enough resources to schedule the request.
        if not _fits(available_resources_dict, dict(request.resources_bundle)):
            return False

        # Schedule the request, update resources
        _inplace_subtract(available_resources_dict, dict(request.resources_bundle))

        # Add the request to the node.
        self.add_sched_request(request, resource_request_source)

        # Update the dynamic labels if there's any
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
        """
        Add a label to the node.
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
    """
    A resource demand scheduler that schedules resource requests based on the
    following rules:
        1. Enforce the minimal count of nodes for each worker node type.
        2. Enforce the cluster resource constraints.
        3. Schedule the gang resource requests.
        4. Schedule the tasks/actor resource requests
    """

    def __init__(self, event_logger: Optional[AutoscalerEventLogger] = None):
        self._event_logger = event_logger

    @dataclass
    class ScheduleContext:
        """
        Encapsulates the context for processing one scheduling request.

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
        # The current schedulable nodes (including pending nodes and pending requests).
        _nodes: List[SchedulingNode] = field(default_factory=list)
        # The number of nodes by node types available for launching based on the max
        # number of workers in the config. This takes into account any pending/running
        # nodes.
        _node_type_available: Dict[NodeType, int] = field(default_factory=dict)

        def __init__(
            self,
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
            disable_launch_config_check: bool,
            max_num_nodes: Optional[int] = None,
            idle_timeout_s: Optional[float] = None,
        ):
            self._nodes = nodes
            self._node_type_configs = node_type_configs
            self._node_type_available = self._compute_available_node_types(
                nodes, node_type_configs
            )
            self._max_num_nodes = max_num_nodes
            self._idle_timeout_s = idle_timeout_s
            self._disable_launch_config_check = disable_launch_config_check

        @classmethod
        def from_schedule_request(
            cls, req: SchedulingRequest
        ) -> "ResourceDemandScheduler.ScheduleContext":
            """
            Create a schedule context from a schedule request.
            It will populate the context with the existing nodes and the available node
            types from the config.

            Args:
                req: The scheduling request. The caller should make sure the
                    request is valid.
            """

            nodes = []
            node_type_configs = req.node_type_configs

            # Initialize the scheduling nodes.
            for instance in req.current_instances:
                node = SchedulingNode.new(
                    instance, node_type_configs, req.disable_launch_config_check
                )
                if node:
                    nodes.append(node)

            return cls(
                nodes=nodes,
                node_type_configs=node_type_configs,
                disable_launch_config_check=req.disable_launch_config_check,
                max_num_nodes=req.max_num_nodes,
                idle_timeout_s=req.idle_timeout_s,
            )

        @staticmethod
        def _compute_available_node_types(
            nodes: List[SchedulingNode],
            node_type_configs: Dict[NodeType, NodeTypeConfig],
        ) -> Dict[NodeType, int]:
            """
            Compute the number of nodes by node types available for launching based on
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
            """
            Get the current nodes with filter.

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

        def get_idle_timeout_s(self) -> Optional[float]:
            return self._idle_timeout_s

        def update(self, new_nodes: List[SchedulingNode]) -> None:
            """
            Update the context with the new nodes.
            """
            self._nodes = new_nodes

            # Update the available node types.
            self._node_type_available = self._compute_available_node_types(
                self._nodes, self._node_type_configs
            )

        def get_max_num_nodes(self) -> Optional[int]:
            """
            Get the max number of nodes for the entire cluster.
            """
            return self._max_num_nodes

        def get_node_type_configs(self) -> Dict[NodeType, NodeTypeConfig]:
            return self._node_type_configs

        def __str__(self) -> str:
            return "ScheduleContext({} nodes, node_type_available={})".format(
                len(self._nodes), dict(self._node_type_available)
            )

        def get_launch_requests(self) -> List[LaunchRequest]:
            """
            Get the launch requests for the nodes that are to be launched.
            """
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
            """
            Get the terminate requests for the nodes that are to be terminated.
            """
            return [
                node.termination_request
                for node in self._nodes
                if node.termination_request is not None
            ]

    def schedule(self, request: SchedulingRequest) -> SchedulingReply:
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

        # Shutdown any idle nodes that's not needed (e.g. no resource constraints.
        # not needed by min_worker count, etc.)
        ResourceDemandScheduler._enforce_idle_termination(ctx)

        # Compute the number of nodes to launch.
        reply = SchedulingReply(
            infeasible_resource_requests=infeasible_requests,
            infeasible_gang_resource_requests=infeasible_gang_requests,
            infeasible_cluster_resource_constraints=infeasible_constraints,
            to_launch=ctx.get_launch_requests(),
            to_terminate=ctx.get_terminate_requests(),
        )

        if self._event_logger is not None:
            try:
                self._event_logger.log_cluster_scheduling_update(
                    launch_requests=reply.to_launch,
                    terminate_requests=reply.to_terminate,
                    infeasible_requests=infeasible_requests,
                    infeasible_gang_requests=infeasible_gang_requests,
                    infeasible_cluster_resource_constraints=infeasible_constraints,
                    cluster_shape=ctx.get_cluster_shape(),
                    node_type_configs=ctx.get_node_type_configs(),
                )
            except Exception:
                logger.exception("Failed to emit event logs.")

        return reply

    @staticmethod
    def _enforce_max_workers_per_type(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """
        Enforce the max number of workers for each node type.
        """

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
        assert len(all_nodes) == len(
            terminating_nodes + non_terminating_nodes
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
        """
        Enforce the max number of workers for the entire cluster.
        """
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
        assert len(all_nodes) == len(
            terminating_nodes + non_terminating_nodes
        ), "The number of nodes should be the same after enforcing max nodes."

        all_nodes = terminating_nodes + non_terminating_nodes
        ctx.update(all_nodes)

    @staticmethod
    def _select_nodes_to_terminate(
        nodes: List[SchedulingNode],
        num_to_terminate: int,
        cause: TerminationRequest.Cause,
        max_num_nodes: Optional[int] = None,
        max_num_nodes_per_type: Optional[int] = None,
    ) -> Tuple[List[SchedulingNode], List[SchedulingNode]]:
        """
        Select 'num_to_terminate' of nodes to be terminated
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
            node.status = SchedulingNodeStatus.TO_TERMINATE
            node.termination_request = TerminationRequest(
                id=str(uuid.uuid4()),
                instance_id=node.im_instance_id,
                ray_node_id=node.ray_node_id,
                cause=cause,
                instance_type=node.node_type,
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
        """
        Sort the nodes for termination increasingly by:

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
        """
        Enforce the minimal count of nodes for each worker node type.
        """

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
                new_nodes.extend(
                    [
                        SchedulingNode.from_node_config(
                            copy.deepcopy(node_type_config),
                            status=SchedulingNodeStatus.TO_LAUNCH,
                            node_kind=NodeKind.WORKER,
                        )
                    ]
                    * (min_count - cur_count)
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
        """
        Enforce the cluster resource constraints.

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
        min_bundles = constraint.min_bundles
        # Flatten the requests for iterating through.
        requests = ResourceRequestUtil.ungroup_by_count(min_bundles)

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
        """
        Schedule the resource requests.

        Args:
            ctx: The schedule context.
            requests_by_count: The resource requests.

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
        """
        Schedule the gang resource requests.

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
            """
            Key function for sorting the gang resource request by:
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
        """
        Try to schedule the resource requests on the current context.

        It tries to schedule the requests on the existing nodes first, and
        then try to schedule the requests on new nodes if possible.

        Args:
            requests_to_sched: The resource requests to be scheduled.
            ctx: The current scheduling context.
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
            """
            Sort the resource requests by:
                1. The length of it's placement constraints.
                2. The number of resources it requests.
                3. The values of resources it requests.
                4. lexicographically for each resource (for stable ordering)

            This is a legacy sorting function for the autoscaler's binpacking
            algo - we do this so that we could have a deterministic scheduling
            results with reasonable fragmentation.
            """
            return (
                len(req.placement_constraints),
                len(req.resources_bundle.values()),
                sum(req.resources_bundle.values()),
                sorted(req.resources_bundle.items()),
            )

        requests_to_sched = sorted(
            requests_to_sched, key=_sort_resource_request, reverse=True
        )

        existing_nodes = ctx.get_nodes()
        node_type_available = ctx.get_node_type_available()

        # A list of nodes that are either:
        #   1. existing nodes in the cluster. or
        #   2. new nodes that are launched to satisfy the resource requests.
        target_nodes = []

        # Try scheduling resource requests with existing nodes first.
        while len(requests_to_sched) > 0 and len(existing_nodes) > 0:
            (
                best_node,
                requests_to_sched,
                existing_nodes,
            ) = ResourceDemandScheduler._sched_best_node(
                requests_to_sched, existing_nodes, resource_request_source
            )
            if best_node is None:
                # No existing nodes can schedule any more requests.
                break

            target_nodes.append(best_node)

        # If there's any existing nodes left, we will add to the target nodes
        target_nodes.extend(existing_nodes)

        # Try scheduling resource requests with new nodes.
        node_pools = [
            SchedulingNode.from_node_config(
                ctx.get_node_type_configs()[node_type],
                status=SchedulingNodeStatus.TO_LAUNCH,
                node_kind=NodeKind.WORKER,
            )
            for node_type, num_available in node_type_available.items()
            if num_available > 0
        ]
        while len(requests_to_sched) > 0 and len(node_pools) > 0:
            # Max number of nodes reached.
            max_num_nodes = ctx.get_max_num_nodes()
            if max_num_nodes is not None and len(target_nodes) >= max_num_nodes:
                logger.debug(
                    "Max number of nodes reached: {}, "
                    "cannot launch more nodes.".format(max_num_nodes)
                )
                break

            (
                best_node,
                requests_to_sched,
                node_pools,
            ) = ResourceDemandScheduler._sched_best_node(
                requests_to_sched, node_pools, resource_request_source
            )
            if best_node is None:
                break

            target_nodes.append(best_node)
            # Update the node pool if a node with the same node type of the
            # added node can be launched.
            node_type_available[best_node.node_type] -= 1
            if node_type_available[best_node.node_type] > 0:
                node_pools.append(
                    SchedulingNode.from_node_config(
                        ctx.get_node_type_configs()[best_node.node_type],
                        status=SchedulingNodeStatus.TO_LAUNCH,
                        node_kind=NodeKind.WORKER,
                    )
                )

        return target_nodes, requests_to_sched

    @staticmethod
    def _sched_best_node(
        requests: List[ResourceRequest],
        nodes: List[SchedulingNode],
        resource_request_source: ResourceRequestSource,
    ) -> Tuple[SchedulingNode, List[ResourceRequest], List[SchedulingNode]]:
        """
        Schedule the requests on the best node.
        A simple greedy algorithm is used to schedule the requests:
            1. Try to schedule the requests on each node.
            2. Sort the nodes by a score
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

        Returns:
            best_node: The best node to schedule the requests.
            infeasible: The infeasible requests that cannot be scheduled on the best
                node.
            nodes: Remaining nodes after the best node is removed.
        """
        results = []

        # A temporary data class to store the scheduling result.
        @dataclass
        class ScheduleResult:
            # The node candidate after a scheduling attempt.
            node: SchedulingNode
            # The infeasible resource requests that are not scheduled.
            infeasible_requests: List[ResourceRequest]
            # The index of the node in the original node list.
            idx: int
            # the score of the scheduling node to compare with others.
            score: UtilizationScore

        nodes_copy = copy.deepcopy(nodes)

        # Iterate through each node and modify the node's available resources
        # if the requests are schedulable.
        for idx, node in enumerate(nodes_copy):
            remaining, score = node.try_schedule(requests, resource_request_source)

            if len(remaining) == len(requests):
                # The node cannot schedule any of the requests.
                continue

            results.append(ScheduleResult(node, remaining, idx, score))

        # No nodes can schedule any of the requests.
        if len(results) == 0:
            logger.debug(
                "No nodes can schedule the requests: {}, for nodes: {}".format(
                    ResourceRequestUtil.to_dict_list(requests), nodes
                )
            )
            return None, requests, nodes

        # Sort the results by score.
        results = sorted(results, key=lambda r: r.score, reverse=True)
        best_result = results[0]

        # Remove the best node from the nodes.
        nodes.pop(best_result.idx)
        logger.debug(
            "Best node: {}, score: {}, remaining requests: {}".format(
                best_result.node,
                best_result.score,
                ResourceRequestUtil.to_dict_list(best_result.infeasible_requests),
            )
        )
        return best_result.node, best_result.infeasible_requests, nodes

    @staticmethod
    def _terminate_outdated_nodes(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """
        Terminate the nodes that are outdated, i.e. the node type config has been
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
                    cause=TerminationRequest.Cause.OUTDATED,
                    details=f"node from {node.node_type} has outdated config",
                )

        ctx.update(nodes)

    @staticmethod
    def _enforce_idle_termination(
        ctx: "ResourceDemandScheduler.ScheduleContext",
    ) -> None:
        """
        Enforce the idle termination for the nodes that are not needed by the cluster
        resource constraints and idle for too long.

        Args:
            ctx: The schedule context.
        """
        count_by_node_type = ctx.get_cluster_shape()
        node_type_configs = ctx.get_node_type_configs()
        terminate_nodes_by_type: Dict[NodeType, int] = defaultdict(int)

        nodes = ctx.get_nodes()
        s_to_ms = 1000
        for node in nodes:
            if node.status != SchedulingNodeStatus.SCHEDULABLE:
                # We don't need to care about the non-running nodes.
                continue

            if node.node_kind == NodeKind.HEAD:
                # The head node is not subject to idle termination.
                continue

            idle_timeout_s = ctx.get_idle_timeout_s()
            # Override the scheduler idle_timeout_s if set for this node_type.
            node_type = node.node_type
            if node_type in node_type_configs:
                if node_type_configs[node_type].idle_timeout_s is not None:
                    idle_timeout_s = node_type_configs[node_type].idle_timeout_s
            if idle_timeout_s is None:
                # No idle timeout is set, skip the idle termination.
                continue

            if node.idle_duration_ms <= idle_timeout_s * s_to_ms:
                # The node is not idle for too long, skip it.
                continue

            if node.sched_requests[ResourceRequestSource.CLUSTER_RESOURCE_CONSTRAINT]:
                # The node is needed by the resource constraints.
                # Skip it.
                if node.idle_duration_ms > ctx.get_idle_timeout_s() * s_to_ms:
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
                idle_duration_ms=node.idle_duration_ms,
                details=f"idle for {node.idle_duration_ms/s_to_ms} secs > "
                f"timeout={idle_timeout_s} secs",
            )

        ctx.update(nodes)
