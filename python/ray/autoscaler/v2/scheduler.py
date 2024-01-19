import copy
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Callable, Dict, List, Optional, Tuple

from google.protobuf.json_format import MessageToDict

from ray.autoscaler._private.resource_demand_scheduler import UtilizationScore
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.schema import AutoscalerInstance, NodeType
from ray.autoscaler.v2.utils import resource_requests_by_count
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    ResourceRequest,
    ResourceRequestByCount,
)
from ray.core.generated.instance_manager_pb2 import LaunchRequest, TerminationRequest

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
    # Available node type configs
    node_type_configs: Dict[NodeType, NodeTypeConfig] = field(default_factory=dict)
    # Max number of worker nodes.
    max_num_nodes: Optional[int] = None
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
    infeasible_resource_requests: List[ResourceRequestByCount] = field(
        default_factory=list
    )
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

    # The node is to be launched.
    TO_LAUNCH = "TO_LAUNCH"
    # The node is pending, i.e. there's already an autoscaler instance being launched
    PENDING = "PENDING"
    # The node is running.
    RUNNING = "RUNNING"
    # The node is to be terminated.
    TO_TERMINATE = "TO_TERMINATE"


@dataclass
class SchedulingNode:
    """
    A abstraction of a node that can be scheduled on by the resource scheduler.

    A scheduling node is expected to be used as:

        node  = SchedulingNode.from_node_config(node_config)
        remaining, score = node.try_schedule(requests)

        .... do something with the score ....

    NOTE:
        One could also extend the scheduling behavior by overriding `try_schedule`
    """

    @classmethod
    def from_node_config(
        cls,
        node_config: NodeTypeConfig,
        status: SchedulingNodeStatus,
        im_instance_id: Optional[str] = None,
    ) -> "SchedulingNode":
        """
        Create a scheduling node from a node config.
        """
        return cls(
            node_type=node_config.name,
            total_resources=dict(node_config.resources),
            available_resources=dict(node_config.resources),
            labels=dict(node_config.labels),
            status=status,
            im_instance_id=im_instance_id,
        )

    # Node type name.
    node_type: NodeType
    # Requests committed to be placed on this node.
    sched_requests: List[ResourceRequest] = field(default_factory=list)
    # The node's current resource capacity.
    total_resources: Dict[str, float] = field(default_factory=dict)
    # The node's available resources.
    available_resources: Dict[str, float] = field(default_factory=dict)
    # Node's labels, including static or dynamic labels.
    labels: Dict[str, str] = field(default_factory=dict)
    # Status
    status: SchedulingNodeStatus = SchedulingNodeStatus.TO_LAUNCH
    # Observability descriptive message for why the node was launched in the
    # first place.
    launch_reason: Optional[str] = None
    # Termination request, none when the node is not being terminated.
    termination_request: Optional[TerminationRequest] = None
    # The instance id of the im instance. None if the node is not yet
    # from IM.
    im_instance_id: Optional[str] = None
    # The ray node id of the ray node. None if the node is not yet
    # running ray.
    ray_node_id: Optional[str] = None
    # Idle duration in ms. Default not idle.
    idle_duration_ms: int = 0

    def try_schedule(
        self, requests: List[ResourceRequest]
    ) -> Tuple[List[ResourceRequest], UtilizationScore]:
        """
        Try to schedule the resource requests on this node.

        This modifies the node's available resources if the requests are schedulable.
        When iterating through the requests, the requests are sorted by the
        `_sort_resource_request` function. The requests are scheduled one by one in
        the sorted order, and no backtracking is done.

        Args:
            requests: The resource requests to be scheduled.

        Returns:
            A tuple of:
                - list of remaining requests that cannot be scheduled on this node.
                - the utilization score for this node with respect to the current
                resource requests being scheduled.
        """
        pass

    def _compute_score(self) -> UtilizationScore:
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
        pass

    def __repr__(self) -> str:
        return (
            "SchedulingNode(node_type={node_type}, "
            "instance_id={instance_id},"
            "ray_node_id={ray_node_id},"
            "idle_duration_ms={idle_duration_ms},"
            "termination_request={termination_request},"
            "status={status}, "
            "total_resources={total_resources}, "
            "available_resources={available_resources}, "
            "labels={labels}, launch_reason={launch_reason}), "
            "sched_requests={sched_requests})"
        ).format(
            node_type=self.node_type,
            instance_id=self.im_instance_id,
            ray_node_id=self.ray_node_id,
            idle_duration_ms=self.idle_duration_ms,
            termination_request=str(MessageToDict(self.termination_request))
            if self.termination_request
            else None,
            status=self.status,
            total_resources=self.total_resources,
            available_resources=self.available_resources,
            labels=self.labels,
            launch_reason=self.launch_reason,
            sched_requests="|".join(str(MessageToDict(r)) for r in self.sched_requests),
        )


class ResourceDemandScheduler(IResourceScheduler):
    """
    A "simple" resource scheduler that schedules resource requests based on the
    following rules:
        1. Enforce the minimal count of nodes for each worker node type.
        2. Enforce the cluster resource constraints.
        3. Schedule the gang resource requests.
        4. Schedule the tasks/actor resource requests
    """

    @dataclass
    class ScheduleContext:
        """
        Encapsulates the context for processing one scheduling request.

        This exposes functions to read and write the scheduling nodes, to prevent
        accidental modification of the internal state.
        """

        # The node type configs for this scheduling request.
        _node_type_configs: Dict[NodeType, NodeTypeConfig]
        # The max number of nodes for the entire cluster.
        _max_num_nodes: Optional[int] = None
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
            max_num_nodes: Optional[int] = None,
        ):
            self._nodes = nodes
            self._node_type_configs = node_type_configs
            self._node_type_available = self._compute_available_node_types(
                nodes, node_type_configs
            )
            self._max_num_nodes = max_num_nodes

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

            # Initialize the scheduling nodes needed.
            for instance in req.current_instances:
                if instance.ray_node is not None:
                    nodes.append(
                        SchedulingNode(
                            node_type=instance.ray_node.ray_node_type_name,
                            total_resources=dict(instance.ray_node.total_resources),
                            available_resources=dict(
                                instance.ray_node.available_resources
                            ),
                            labels=dict(instance.ray_node.dynamic_labels),
                            status=SchedulingNodeStatus.RUNNING,
                            im_instance_id=instance.im_instance.instance_id
                            if instance.im_instance
                            else None,
                            ray_node_id=instance.ray_node.node_id.decode("utf-8"),
                            idle_duration_ms=instance.ray_node.idle_duration_ms,
                        )
                    )
                elif (
                    instance.im_instance is not None
                    and InstanceUtil.is_ray_running_reachable(
                        instance.im_instance.status
                    )
                ):
                    # This is an im instance that's pending to run ray:
                    # e.g. allocated, or being requested, or ray is installing.
                    node_config = node_type_configs.get(
                        instance.im_instance.instance_type, None
                    )

                    if node_config is None:
                        # Configs might have been updated, and no more
                        # node_type_configs for this node type.
                        logger.info(
                            "Skipping instance {} since no node config found for {}".format(
                                instance.im_instance.instance_id,
                                instance.im_instance.instance_type,
                            )
                        )
                        continue
                    nodes.append(
                        SchedulingNode.from_node_config(
                            node_config,
                            status=SchedulingNodeStatus.PENDING,
                            im_instance_id=instance.im_instance.instance_id,
                        )
                    )
                else:
                    logger.info(
                        "Skipping instance {} since it's not pending/running".format(
                            instance
                        )
                    )

            return cls(
                nodes=nodes,
                node_type_configs=node_type_configs,
                max_num_nodes=req.max_num_nodes,
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

        def get_nodes(
            self, filter_fn: Optional[Callable] = None
        ) -> List[SchedulingNode]:
            nodes = copy.deepcopy(self._nodes)
            if filter_fn is not None:
                return list(filter(filter_fn, nodes))
            return nodes

        def get_cluster_shape(self) -> Dict[NodeType, int]:
            cluster_shape = defaultdict(int)
            for node in self._nodes:
                cluster_shape[node.node_type] += 1
            return cluster_shape

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
            return "ScheduleContext({} nodes, node_type_available={}): {}".format(
                len(self._nodes), self._node_type_available, self._nodes
            )

        def get_to_launch(self) -> List[LaunchRequest]:
            """
            Get new instances to launch.

            Returns:
                A list of launch requests.
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
                        id=str(time.time_ns()),
                        request_ts_ms=time.time_ns() // 1000,
                    )
                )
            return launch_requests

        def get_to_terminate(
            self,
        ) -> List[TerminationRequest]:
            """
            Get instances to terminate.

            Returns:
                A dict of instances to terminate and the termination reason.
            """
            return [
                node.termination_request
                for node in self._nodes
                if node.termination_request is not None
            ]

    def schedule(self, request: SchedulingRequest) -> SchedulingReply:
        self._init_context(request)

        # Enforce the minimal count of nodes for each worker node type.
        self._enforce_min_workers()

        # Enforce the max worker nodes count.
        self._enforce_max_workers()

        # Enforce the cluster resource constraints.
        infeasible_constraints = self._enforce_resource_constraints(
            request.cluster_resource_constraints
        )

        # Schedule the gang resource requests.
        infeasible_gang_requests = self._sched_gang_resource_requests(
            request.gang_resource_requests
        )

        # Schedule the tasks/actor resource requests
        infeasible_requests = self._sched_resource_requests(
            request.resource_requests,
        )

        # Compute the number of nodes to launch.
        reply = SchedulingReply(
            infeasible_resource_requests=resource_requests_by_count(
                infeasible_requests
            ),
            infeasible_gang_resource_requests=infeasible_gang_requests,
            infeasible_cluster_resource_constraints=infeasible_constraints,
            to_launch=self._ctx.get_to_launch(),
            to_terminate=self._ctx.get_to_terminate(),
        )

        return reply

    def _init_context(self, request: SchedulingRequest) -> None:
        self._ctx = self.ScheduleContext.from_schedule_request(request)

    def _enforce_max_workers(self) -> None:
        """
        Enforce the max number of workers for the entire cluster.

        This checks for the below 2 constraints:
            1. The max number of workers for each node type.
            2. The max number of workers for the entire cluster.
        """

        # Get all the nodes by type
        nodes = self._ctx.get_nodes(
            filter_fn=lambda n: n.status != SchedulingNodeStatus.TO_TERMINATE
        )
        num_nodes_total = len(nodes)

        nodes_by_type = defaultdict(list)
        for node in nodes:
            nodes_by_type[node.node_type].append(node)

        all_terminated_nodes = []
        # Enforce the max number of workers for each node type.
        for node_type, nodes in nodes_by_type.items():
            node_config = self._ctx.get_node_type_configs().get(node_type, None)
            num_max_nodes_per_type = node_config.max_worker_nodes if node_config else 0
            num_extra_nodes = len(nodes) - num_max_nodes_per_type

            if num_extra_nodes <= 0:
                # No extra nodes for this type, continue.
                continue

            # Sort the instances for termination.
            nodes.sort(key=self._sort_nodes_for_termination)

            # Terminate the nodes
            terminated_nodes, nodes = self._terminate_nodes(
                nodes,
                num_extra_nodes,
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                max_num_nodes_per_type=num_max_nodes_per_type,
            )
            nodes_by_type[node_type] = nodes

            all_terminated_nodes.extend(terminated_nodes)

        # Enforce the max number of workers for the entire cluster.
        remained_nodes = []
        for nodes in nodes_by_type.values():
            remained_nodes.extend(nodes)

        num_max_nodes = self._ctx.get_max_num_nodes()

        num_to_terminate = (
            max(len(remained_nodes) - num_max_nodes, 0) if num_max_nodes else 0
        )

        remained_nodes.sort(key=self._sort_nodes_for_termination)
        # Terminate the nodes
        terminated_nodes, remained_nodes = self._terminate_nodes(
            remained_nodes,
            num_to_terminate,
            TerminationRequest.Cause.MAX_NUM_NODES,
            max_num_nodes=num_max_nodes,
        )

        all_terminated_nodes.extend(terminated_nodes)

        if len(terminated_nodes) < num_to_terminate:
            logger.warning(
                "Failed to terminate {} nodes to satisfy max_num_nodes={}".format(
                    num_to_terminate - len(terminated_nodes), num_max_nodes
                )
            )

        # Update the context
        all_nodes = all_terminated_nodes + remained_nodes
        logger.debug(
            f"Enforced max nodes: terminating {len(all_terminated_nodes)} "
            f"nodes: {all_terminated_nodes}"
        )
        assert len(all_nodes) == num_nodes_total
        self._ctx.update(all_nodes)
        logger.debug("After enforced max nodes: {}".format(self._ctx))
        return

    @staticmethod
    def _terminate_nodes(
        nodes: List[SchedulingNode],
        num_to_terminate: int,
        cause: TerminationRequest.Cause,
        max_num_nodes: Optional[int] = None,
        max_num_nodes_per_type: Optional[int] = None,
        idle_duration_ms: Optional[int] = None,
    ) -> Tuple[List[SchedulingNode], List[SchedulingNode]]:
        """
        Terminate the nodes with the lowest idle duration.

        This assumes the nodes have been sorted by the
        `_sort_nodes_for_termination` function.

        Args:
            nodes: The nodes to terminate.
            num_to_terminate: The number of nodes to terminate.
            cause: The termination cause.

        Returns:
            The terminated nodes.
            The remained nodes.

        """

        terminated_nodes, remained_nodes = (
            nodes[:num_to_terminate],
            nodes[num_to_terminate:],
        )

        for node in terminated_nodes:
            node.status = SchedulingNodeStatus.TO_TERMINATE
            node.termination_request = TerminationRequest(
                id=str(time.time_ns()),
                instance_id=node.im_instance_id,
                ray_node_id=node.ray_node_id,
                cause=cause,
            )
            if cause == TerminationRequest.Cause.MAX_NUM_NODES:
                node.termination_request.max_num_nodes = max_num_nodes
            elif cause == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE:
                node.termination_request.max_num_nodes_per_type = max_num_nodes_per_type
            elif cause == TerminationRequest.Cause.IDLE:
                node.termination_request.idle_duration_ms = idle_duration_ms
            else:
                raise ValueError("Unknown termination cause: {}".format(cause))

        return terminated_nodes, remained_nodes

    @staticmethod
    def _sort_nodes_for_termination(node: SchedulingNode) -> Tuple:
        """
        Sort the nodes for termination increasingly by:

            1. First if it doesn't have a ray node.
            2. Then if the nodes are idle
            3. Then with lower resources util nodes first.

        Such that nodes sorted earlier will be terminated first.
        """

        running_ray = node.status == SchedulingNodeStatus.RUNNING
        # Reverse the idle duration such that the nodes with the largest idle duration
        # will be terminated first.
        idle_dur = -1 * node.idle_duration_ms

        utils_per_resources = {}
        for resource, total in node.total_resources.items():
            if total <= 0:
                continue
            utils_per_resources[resource] = (
                total - node.available_resources.get(resource, 0)
            ) / total

        avg_util = (
            sum(utils_per_resources.values()) / len(utils_per_resources)
            if utils_per_resources
            else 0
        )

        return (running_ray, idle_dur, avg_util)

    def _enforce_min_workers(self) -> None:
        """
        Enforce the minimal count of nodes for each worker node type.
        """

        # Count the existing nodes by type
        count_by_node_type = self._ctx.get_cluster_shape()
        logger.debug("Enforcing min workers: {}".format(self._ctx))

        new_nodes = []
        # Launch new nodes to satisfy min count for each node type.
        for (
            node_type,
            node_type_config,
        ) in self._ctx.get_node_type_configs().items():
            cur_count = count_by_node_type.get(node_type, 0)
            min_count = node_type_config.min_worker_nodes
            if cur_count < min_count:
                new_nodes.extend(
                    [
                        SchedulingNode.from_node_config(
                            copy.deepcopy(node_type_config),
                            status=SchedulingNodeStatus.TO_LAUNCH,
                        )
                    ]
                    * (min_count - cur_count)
                )
        # NOTE: we assume the aggregated number of min workers across all node types
        # should not exceed any globally enforced max_num_nodes

        # Add the new nodes to the existing nodes and update the context.
        self._ctx.update(new_nodes + self._ctx.get_nodes())
        logger.debug("After enforced min workers: {}".format(self._ctx))

    def _enforce_resource_constraints(
        self,
        constraints: List[ClusterResourceConstraint],
    ) -> List[ClusterResourceConstraint]:
        """
        Enforce the cluster resource constraints.

        Args:
            constraints: The cluster resource constraints.

        Returns:
            A list of infeasible constraints.

        Notes:
            It's different from the other scheduling functions since it doesn't actually
        schedule any resource requests. Instead, it asks if the cluster could be
        upscale to a certain shape to fulfill the constraints.
        """
        return []

    def _sched_resource_requests(
        self,
        requests_by_count: List[ResourceRequestByCount],
    ) -> List[ResourceRequest]:
        """
        Schedule the resource requests.

        Args:
            requests_by_count: The resource requests.

        Returns:
            A list of infeasible resource requests.
        """
        return []

    def _sched_gang_resource_requests(
        self,
        gang_requests: List[GangResourceRequest],
    ) -> List[GangResourceRequest]:
        """
        Schedule the gang resource requests.

        These requests should be scheduled atomically, i.e. either all of the resources
        requests in a gang request are scheduled or none of them are scheduled.

        Args:
            gang_requests: The gang resource requests.

        Returns:
            A list of infeasible gang resource requests.
        """
        return []
