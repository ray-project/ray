from abc import ABCMeta, abstractmethod
from collections import defaultdict
import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from ray.autoscaler.v2.schema import CloudInstanceId, InstanceId
from ray.autoscaler._private.constants import AUTOSCALER_CONSERVE_GPU_NODES
from google.protobuf.json_format import MessageToDict
from ray.autoscaler._private.resource_demand_scheduler import (
    UtilizationScore,
)
from ray.autoscaler._private.util import NodeType
from ray.autoscaler.v2.utils import (
    flatten_requests_by_count,
    resource_requests_by_count,
)

from ray.core.generated.instance_manager_pb2 import (
    Instance,
    ScheduleResourceBundlesRequest,
    ScheduleResourceBundlesReply,
    ResourceScheduleConfig,
    NodeTypeConfig,
)

from ray.core.generated.autoscaler_pb2 import (
    NodeState,
    ResourceRequest,
    ClusterResourceConstraint,
    GangResourceRequest,
    ResourceRequestByCount,
    AffinityConstraint,
    PlacementConstraint,
)


from ray.autoscaler.v2.logger import logger


class SchedulingNodeStatus(Enum):
    """
    The status of a scheduling node (`SchedulingNode`)
    """

    # The node is to be launched.
    TO_LAUNCH = "TO_LAUNCH"
    # The node is pending to be launched (a pending request, or a pending instance)
    PENDING = "PENDING"
    # The node is running.
    RUNNING = "RUNNING"


@dataclass
class SchedulingNode:
    """
    A abstraction of a node that can be scheduled on by the resource scheduler.

    A scheduling node is expected to be used as:

        node  = SchedulingNode.from_node_config(node_config)
        infeasible = node.try_schedule(requests)
        score = node.compute_score()

        .... do something with the score ....

    NOTE:
        One could also extend the scheduling behavior by overriding:
            1. try_schedule()
            2. compute_score()

    """

    # Class level node id counter.
    _next_node_id = 0

    # A unique id simply for differentiating nodes. Not the same as a normal ray node
    # id.
    node_id: int
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
    # TODO
    launch_reason: Optional[str] = None

    @classmethod
    def from_node_config(
        cls,
        node_config: NodeTypeConfig,
        status: SchedulingNodeStatus = SchedulingNodeStatus.TO_LAUNCH,
    ) -> "SchedulingNode":
        """
        Create a scheduling node from a node config.
        """
        return cls(
            node_id=cls.next_node_id(),
            node_type=node_config.name,
            total_resources=dict(node_config.resources),
            available_resources=dict(node_config.resources),
            labels=dict(node_config.labels),
            status=status,
        )

    def try_schedule(self, requests: List[ResourceRequest]) -> List[ResourceRequest]:
        """
        Try to schedule the resource requests on this node.

        This modifies the node's available resources if the requests are schedulable.
        When iterating through the requests, the requests are sorted by the
        `_sort_resource_request` function. The requests are scheduled one by one in
        the sorted order, and no backtracking is done.

        Args:
            requests: The resource requests to be scheduled.

        Returns:
            A list of infeasible requests that cannot be scheduled on this node.
        """
        # Track the fittable resource requests
        infeasible_requests = []

        # Sort the requests and try schedule them one by one.
        for r in sorted(requests, key=self._sort_resource_request, reverse=True):
            if not self._try_schedule_one(r):
                infeasible_requests.append(r)

        return infeasible_requests

    def compute_score(self) -> UtilizationScore:
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

        TODO(rickyx): node availability

        Returns:
            A utilization score for this node.
        """

        # Compute the number of resource types being scheduled.
        num_matching_resource_types = 0
        for req in self.sched_requests:
            for resource_name in req.resources_bundle.keys():
                if resource_name in self.total_resources:
                    num_matching_resource_types += 1

        # Compute the utilization rate for each resource type
        util_by_resources = []
        for k, v in self.total_resources.items():
            if v == 0:
                # Skip any zero values.
                continue
            if k in self.available_resources:
                util = (v - self.available_resources.get(k, 0)) / v
                assert util >= 0 and util <= 1, f"Invalid utilization: {util}"
                util_by_resources.append(util)

        # Prefer not to launch a GPU node if there aren't any GPU requirements in the
        # resource bundle.
        gpu_ok = True
        if AUTOSCALER_CONSERVE_GPU_NODES:
            is_gpu_node = self.total_resources.get("GPU", 0) > 0
            any_gpu_requests = any(
                "GPU" in r.resources_bundle for r in self.sched_requests
            )
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

    @classmethod
    def next_node_id(cls) -> int:
        """
        Return the next scheduling node id.
        """
        cls._next_node_id += 1
        return cls._next_node_id

    def __repr__(self) -> str:
        return (
            "SchedulingNode(id={node_id},node_type={node_type}, "
            "status={status}, "
            "total_resources={total_resources}, "
            "available_resources={available_resources}, "
            "labels={labels}, launch_reason={launch_reason}), "
            "sched_requests={sched_requests})"
        ).format(
            node_id=self.node_id,
            node_type=self.node_type,
            status=self.status,
            total_resources=self.total_resources,
            available_resources=self.available_resources,
            labels=self.labels,
            launch_reason=self.launch_reason,
            sched_requests="|".join(str(MessageToDict(r)) for r in self.sched_requests),
        )

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

    @staticmethod
    def _sort_resource_request(req: ResourceRequest) -> Tuple:
        """
        Sort the resource requests by:
            1. The length of it's placement constraints.
            2. The number of resources it requests.
            3. The values of resources it requests.
            4. lexicographically for each resource (for stable ordering)
        """
        return (
            len(req.placement_constraints),
            len(req.resources_bundle.values()),
            sum(req.resources_bundle.values()),
            sorted(req.resources_bundle.items()),
        )

    def _try_schedule_one(self, request: ResourceRequest) -> bool:
        """
        Try to schedule one resource request on this node.

        If the resource request is schedulable, the node's available resources will be
        updated, as well as the dynamic labels.

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
            # affinity doesn't affect if a request can be scheduled on a node.
            # Affinity constraints are only used for scoring.

        # Check if there's enough resources to schedule the request.
        for k, v in request.resources_bundle.items():
            if self.available_resources.get(k, 0) < v:
                return False

        # Schedule the request, update resources
        for k, v in request.resources_bundle.items():
            self.available_resources[k] -= v

        # Update the dynamic labels.
        for constraint in request.placement_constraints:
            if constraint.HasField("affinity"):
                affinity = constraint.affinity
                self._add_label(affinity.label_name, affinity.label_value)

            if constraint.HasField("anti_affinity"):
                anti_affinity = constraint.anti_affinity
                self._add_label(anti_affinity.label_name, anti_affinity.label_value)

        # Add the request to the node.
        self.sched_requests.append(request)

        return True


# TODO(rickyx): Move interface to a separate file.
class IResourceScheduler(metaclass=ABCMeta):
    """
    Interface for a resource scheduler.

    Implements the `instance_manager.proto ResourceSchedulerService` interface.
    """

    @abstractmethod
    def schedule_resource_bundles(
        self, request: ScheduleResourceBundlesRequest
    ) -> ScheduleResourceBundlesReply:
        pass


@dataclass
class ScheduleContext:
    """
    A context object that holds the current scheduling context.
    It contains:
        1. existing nodes (including pending nodes and pending requests).
        2. the number of nodes by node types available for launching based on the max
            number of workers in the config. This takes into account any pending/running
            nodes.
        3. the resource schedule config.

    The context should be updated during the scheduling process:
    ```
        ctx.update(new_nodes)
    ```

    Most of the getters return a copy of the internal state to prevent accidental
    modification of the internal state.

    """

    # The current resource schedule config.
    config_: ResourceScheduleConfig
    # The current schedulable nodes (including pending nodes and pending requests).
    nodes_: List[SchedulingNode] = field(default_factory=list)
    # The number of nodes by node types available for launching based on the max
    # number of workers in the config. This takes into account any pending/running
    # nodes.
    node_type_available_: Dict[NodeType, int] = field(default_factory=dict)

    def __init__(
        self,
        nodes: List[SchedulingNode],
        node_type_available: Dict[NodeType, int],
        config: ResourceScheduleConfig,
    ):
        self._nodes = nodes
        self._node_type_available = node_type_available
        self._config = config

    @classmethod
    def from_schedule_request(
        cls, req: ScheduleResourceBundlesRequest
    ) -> "ScheduleContext":
        """
        Create a schedule context from a schedule request.

        It will populate the context with the existing nodes and the available node
        types from the config.
        """
        nodes: List[SchedulingNode] = []

        ray_nodes_by_cloud_instance_id: Dict[CloudInstanceId, NodeState] = {
            node.instance_id: node for node in req.node_states
        }

        logger.info(
            f"ray cluster nodes: {[(node.instance_id, node.ray_node_type_name) for node in req.node_states]}"
        )
        logger.info(f"ray cluster current cluster: {req.current_instances}")

        # Add all the running ray nodes first.
        # NOTE: there could be ray nodes (e.g. head node) whose lifecycle is
        # not managed by the instance manager. We will need to use information
        # from the ray cluster state to populate the scheduling node candidate.

        for node in req.node_states:
            nodes.append(
                SchedulingNode(
                    node_id=SchedulingNode.next_node_id(),
                    node_type=node.ray_node_type_name,
                    total_resources=dict(node.total_resources),
                    available_resources=dict(node.available_resources),
                    labels=dict(node.dynamic_labels),
                    status=SchedulingNodeStatus.RUNNING,
                )
            )

        for instance in req.current_instances:
            if instance.cloud_instance_id in ray_nodes_by_cloud_instance_id:
                continue
            # No corresponding ray node yet. Any such instance will
            # be marked as the state.
            nodes.append(
                SchedulingNode(
                    node_id=SchedulingNode.next_node_id(),
                    node_type=instance.instance_type,
                    total_resources=dict(instance.total_resources),
                    available_resources=dict(instance.total_resources),
                    labels={},
                    status=SchedulingNodeStatus.PENDING,
                )
            )

        # Get the available node types.
        node_type_available = cls._compute_available_node_types(
            nodes, req.schedule_config
        )

        return cls(
            nodes=nodes,
            node_type_available=node_type_available,
            config=req.schedule_config,
        )

    @staticmethod
    def _compute_available_node_types(
        nodes: List[SchedulingNode], schedule_config: ResourceScheduleConfig
    ) -> Dict[NodeType, int]:
        """
        Compute the number of nodes by node types available for launching based on the
        max number of workers in the config.

        Args:
            nodes: The current existing nodes.
            schedule_config: The current resource schedule config.

        Returns:
            A dict of node types and the number of nodes available for launching.
        """
        node_type_available = defaultdict(int)
        node_type_existing = defaultdict(int)
        for node in nodes:
            node_type_existing[node.node_type] += 1

        for (
            node_type,
            node_type_config,
        ) in schedule_config.node_type_configs.items():
            node_type_available[
                node_type
            ] = node_type_config.max_workers - node_type_existing.get(node_type, 0)

        return node_type_available

    def get_nodes(self) -> List[SchedulingNode]:
        # NOTE: We do the deep copy here since the nodes are usually being updated
        # during the scheduling process. To prevent accidental modification of the
        # nodes, we return a copy, and callers should modify the context explicitly
        # with `ctx.update()`
        return copy.deepcopy(self._nodes)

    def get_node_config(self, node_type: NodeType) -> Optional[NodeTypeConfig]:
        c = self._config.node_type_configs.get(node_type, None)
        if c:
            return copy.deepcopy(c)
        return None

    def get_sched_config(self) -> ResourceScheduleConfig:
        return copy.deepcopy(self._config)

    def update(self, new_nodes: List[SchedulingNode]) -> None:
        """
        Update the context with the new nodes.
        """
        self._nodes = new_nodes

        # Update the available node types.
        self._node_type_available = self._compute_available_node_types(
            self._nodes, self._config
        )

        # This should not happen since we check the max number of nodes in the
        # scheduler.
        max_num_nodes = self.get_max_num_nodes()
        assert max_num_nodes is None or max_num_nodes >= len(self._nodes), (
            f"Number of nodes {len(self._nodes)} exceeds the max number of nodes "
            f"{max_num_nodes}"
        )

    def get_node_type_available(self) -> Dict[NodeType, int]:
        return copy.deepcopy(self._node_type_available)

    def get_cluster_shape(self) -> Dict[NodeType, int]:
        """
        Return the current cluster shape.

        The cluster shape is a dict of node types and the number of nodes of that type.
        """
        cluster_shape = defaultdict(int)
        for node in self._nodes:
            cluster_shape[node.node_type] += 1
        return cluster_shape

    def get_max_num_nodes(self) -> Optional[int]:
        """
        Return the max number of nodes allowed in the cluster.
        """
        return (
            self._config.max_num_nodes
            if self._config.HasField("max_num_nodes")
            else None
        )


class SimpleResourceScheduler(IResourceScheduler):
    """
    A "simple" resource scheduler that schedules resource requests based on the
    following rules:
        1. Enforce the minimal count of nodes for each worker node type.
        2. Enforce the cluster resource constraints.
        3. Schedule the gang resource requests.
        4. Schedule the tasks/actor resource requests

    """

    def schedule_resource_bundles(
        self, request: ScheduleResourceBundlesRequest
    ) -> ScheduleResourceBundlesReply:
        # These including pending nodes and pending requests.
        ctx = ScheduleContext.from_schedule_request(request)
        cur_shape = ctx.get_cluster_shape()
        logger.info("Current cluster shape: {}".format(cur_shape))

        # 1. Enforce the minimal count of nodes for each worker node type.
        self._enforce_min_workers(ctx)
        logger.info(
            "After enforcing min workers: {}".format(dict(ctx.get_cluster_shape()))
        )

        # 2. Enforce the cluster resource constraints.
        infeasible_constraints = self._enforce_resource_constraints(
            request.cluster_resource_constraints, ctx
        )
        logger.info(
            "After enforcing cluster resource constraints: {}".format(
                dict(ctx.get_cluster_shape())
            )
        )
        logger.info(
            "infeasible_constraints: {}".format(
                ";".join(str(MessageToDict(r)) for r in infeasible_constraints)
            )
        )

        # 3. Schedule the gang resource requests.
        infeasible_gang_requests = self._sched_gang_resource_requests(
            request.gang_resource_requests, ctx
        )
        logger.info(
            "After scheduling gang resource requests: {}".format(
                dict(ctx.get_cluster_shape())
            )
        )
        logger.info(
            "infeasible_gang_requests: {}".format(
                ";".join(str(MessageToDict(r)) for r in infeasible_gang_requests)
            )
        )

        # 4. Schedule the tasks/actor resource requests
        infeasible_requests = self._sched_resource_requests(
            request.resource_requests, ctx
        )
        logger.info(
            "After scheduling resource requests: {}".format(
                dict(ctx.get_cluster_shape())
            )
        )
        logger.info(
            "infeasible_requests: {}".format(
                ";".join(str(MessageToDict(r)) for r in infeasible_requests)
            )
        )

        reply = ScheduleResourceBundlesReply(
            infeasible_resource_requests=resource_requests_by_count(
                infeasible_requests
            ),
            infeasible_gang_resource_requests=infeasible_gang_requests,
            infeasible_cluster_resource_constraints=infeasible_constraints,
            target_cluster_shape=ctx.get_cluster_shape(),
            current_cluster_shape=cur_shape,
        )

        return reply

    def _enforce_min_workers(self, ctx: ScheduleContext) -> None:
        """
        Enforce the minimal count of nodes for each worker node type.
        """
        sched_config = ctx.get_sched_config()

        # Count the existing nodes by type
        count_by_node_type = ctx.get_cluster_shape()

        new_nodes = []
        # Launch new nodes to satisfy min count for each node type.
        for node_type, node_type_config in sched_config.node_type_configs.items():
            cur_count = count_by_node_type.get(node_type, 0)
            min_count = node_type_config.min_workers
            if cur_count < min_count:
                new_nodes.extend(
                    [
                        SchedulingNode.from_node_config(
                            node_type_config, status=SchedulingNodeStatus.TO_LAUNCH
                        )
                    ]
                    * (min_count - cur_count)
                )
        max_num_nodes = ctx.get_max_num_nodes()
        if max_num_nodes and len(new_nodes) + len(ctx.get_nodes()) > max_num_nodes:
            raise ValueError(
                "Number of nodes {} exceeds the max number of nodes {}".format(
                    len(new_nodes) + len(ctx.get_nodes()), max_num_nodes
                )
            )
        # Add the new nodes to the existing nodes and update the context.
        ctx.update(new_nodes + ctx.get_nodes())

    def _sched_resource_requests(
        self,
        requests_by_count: List[ResourceRequestByCount],
        ctx: ScheduleContext,
    ) -> List[ResourceRequest]:
        """
        Schedule the resource requests.
        """
        requests = flatten_requests_by_count(requests_by_count)
        nodes, infeasible = self._try_schedule(requests, ctx)

        # Regardless if there's feasible, we will update the context for schedule nodes.
        ctx.update(nodes)

        return infeasible

    @staticmethod
    def _sort_gang_resource_requests(req: GangResourceRequest) -> Tuple:
        """
        Key function for sorting the gang resource request by:
            1. the number of placement constraints in the gang request.
            2. the number of resource requests in the gang request.
        """
        total_placement_constraints = 0
        for rr in req.requests:
            total_placement_constraints += len(rr.placement_constraints)

        return (total_placement_constraints, len(req.requests))

    def _sched_gang_resource_requests(
        self,
        gang_requests: List[GangResourceRequest],
        ctx: ScheduleContext,
    ) -> List[GangResourceRequest]:
        # TODO: add unit testing?
        def _combine_requests_with_affinity(
            rs: List[ResourceRequest],
        ) -> List[ResourceRequest]:
            """
            Combine the resource requests with affinity constraints
            into the same request. This is needed since the scheduler
            only schedules one request at a time, and we need to make sure
            that the affinity constraints are satisfied for multiple requests.

            Args:
                rs: The list of resource requests to be combined.

            Returns:
                A list of combined resource requests.
            """
            requests_by_affinity: Dict[str, List[ResourceRequest]] = defaultdict(list)
            combined_requests: List[ResourceRequest] = []
            for r in rs:
                other_constraints = []
                has_affinity = False
                # Check if there's affinity constraints, and group the requests
                # by the affinity label name and value.
                for constraint in r.placement_constraints:
                    if constraint.HasField("affinity"):
                        affinity = constraint.affinity
                        requests_by_affinity[affinity.SerializeToString()].append(r)
                        has_affinity = True
                    else:
                        # We will remove the affinity constraint, but reserve others.
                        other_constraints.append(constraint)

                if not has_affinity:
                    # No affinity constraints, just add to the combined requests.
                    combined_requests.append(r)
                else:
                    # Has affinity constraints, it will be combined later.
                    # We also update the placement constraints so that the affinity
                    # constraints are removed to avoid double counting the same
                    # affinity constraints when merging multiple resource requests
                    # later.
                    del r.placement_constraints[:]
                    r.placement_constraints.extend(other_constraints)

            for serialized_affinity_constraint, rs in requests_by_affinity.items():
                combined_request = ResourceRequest()
                for r in rs:
                    # Merge the resource bundles with the same affinity constraint.
                    for k, v in r.resources_bundle.items():
                        combined_request.resources_bundle[k] = (
                            combined_request.resources_bundle.get(k, 0) + v
                        )
                    combined_request.placement_constraints.extend(
                        r.placement_constraints
                    )
                # Adds the affinity constraint back.
                affinity_constraint = AffinityConstraint()
                affinity_constraint.ParseFromString(serialized_affinity_constraint)
                combined_request.placement_constraints.append(
                    PlacementConstraint(affinity=affinity_constraint)
                )

                combined_requests.append(combined_request)
            logger.info(
                "combined_requests: {}".format(
                    ";".join(str(MessageToDict(r)) for r in combined_requests)
                )
            )
            return combined_requests

        infeasible_gang_requests = []
        # Try fulfilling the gang requests one by one.
        for gang_req in sorted(
            gang_requests, key=self._sort_gang_resource_requests, reverse=True
        ):
            requests = gang_req.requests
            # Try to combine requests with affinity constraints into the same request.
            requests = _combine_requests_with_affinity(requests)

            nodes, infeasible = self._try_schedule(requests, ctx)

            if infeasible:
                infeasible_gang_requests.append(gang_req)
                continue

            # We are able to satisfy the constraint and thus update the context.
            ctx.update(nodes)

        return infeasible_gang_requests

    def _enforce_resource_constraints(
        self,
        constraints: List[ClusterResourceConstraint],
        ctx: ScheduleContext,
    ) -> List[ClusterResourceConstraint]:
        """
        Enforce the cluster resource constraints.

        It's different from the other scheduling functions since it doesn't actually
        schedule any resource requests. Instead, it asks if the cluster could be
        upscale to a certain shape to fulfill the constraints.
        This is implemented by updating the context with the nodes having available
        resources temporarily cleared, and try to see if all the constraint is
        satisfied.
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
        requests = flatten_requests_by_count(min_bundles)

        # We will make all existing nodes available for scheduling. This is needed since
        # resource constraints are not actual pending resource requests: only the nodes'
        # capacity are needed to check if the constraint is satisfied.
        cur_nodes = ctx.get_nodes()
        cur_nodes_with_resources_cleared = []
        for node in copy.deepcopy(cur_nodes):
            node.available_resources = dict(node.total_resources)
            cur_nodes_with_resources_cleared.append(node)

        # Update the context with the nodes having available resources temporarily
        # cleared.
        ctx.update(cur_nodes_with_resources_cleared)

        # Pass the empty nodes to the scheduler.
        scheduled_nodes, infeasible = self._try_schedule(requests, ctx)

        if infeasible:
            # Unable to satisfy the constraint.
            return [constraint]

        # We are able to satisfy the constraint and thus update the context.
        # We will merge the newly launched nodes if any in the scheduled nodes with
        # the existing nodes with available resources correctly set.
        cur_nodes_id_map = {n.node_id: n for n in cur_nodes}
        for n in scheduled_nodes:
            if cur_nodes_id_map.get(n.node_id) is None:
                # This is a newly launched node.
                cur_nodes.append(n)

        ctx.update(cur_nodes)
        return []

    def _try_schedule(
        self,
        requests_to_sched: List[ResourceRequest],
        ctx: ScheduleContext,
    ) -> Tuple[List[SchedulingNode], List[ResourceRequest]]:
        """
        Try to schedule the resource requests on the current context.

        It tries to schedule the requests on the existing nodes first, and
        then try to schedule the requests on new nodes if possible.

        Args:
            requests_to_sched: The resource requests to be scheduled.
            ctx: The current scheduling context.

        Returns:
            - List of scheduled nodes to that have part or all of the requests
                scheduled.
            - List of infeasible requests remained that cannot be scheduled.
        """
        existing_nodes = ctx.get_nodes()
        node_type_available = ctx.get_node_type_available()

        # A list of node candidates that can be scheduled to be returned.
        scheduled_nodes = []

        # try_schedule_existing
        while len(requests_to_sched) > 0:
            node, infeasible, existing_nodes = self._sched_best_node(
                requests_to_sched, existing_nodes
            )
            if node is None:
                break
            else:
                # Add the node to the scheduled nodes.
                scheduled_nodes.append(node)

            # Update the remaining requests to be scheduled.
            requests_to_sched = infeasible

        # If there's any existing nodes left, we just add to the scheduled nodes.
        scheduled_nodes.extend(existing_nodes)

        # try_schedule_new
        node_pools = [
            SchedulingNode.from_node_config(ctx.get_node_config(node_type))
            for node_type, num_available in node_type_available.items()
            if num_available > 0
        ]
        while len(requests_to_sched) > 0:
            # No more available new nodes possible.
            if len(node_pools) == 0:
                break

            # Max number of nodes reached.
            max_num_nodes = ctx.get_max_num_nodes()
            if max_num_nodes is not None and len(scheduled_nodes) >= max_num_nodes:
                break

            node, infeasible, node_pools = self._sched_best_node(
                requests_to_sched, node_pools
            )
            if node is None:
                break
            else:
                scheduled_nodes.append(node)

            requests_to_sched = infeasible

            # Update the node pool if another node with the same node type of the
            # added node can be launched.
            node_type_available[node.node_type] -= 1
            if node_type_available[node.node_type] > 0:
                node_pools.append(
                    SchedulingNode.from_node_config(ctx.get_node_config(node.node_type))
                )

        return scheduled_nodes, requests_to_sched

    def _sched_best_node(
        self, requests: List[ResourceRequest], nodes: List[SchedulingNode]
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

        Returns:
            best_node: The best node to schedule the requests.
            infeasible: The infeasible requests that cannot be scheduled on the best
                node.
            remaining_nodes: Remaining nodes after the best node is removed.
        """
        results = []

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

        logger.debug("=========Scheduling========")
        logger.debug("nodes : \n{}".format("\n".join(repr(n) for n in nodes)))
        logger.debug(
            "requests: {} ".format(";".join(str(MessageToDict(r)) for r in requests))
        )

        nodes_copy = copy.deepcopy(nodes)

        for idx, node in enumerate(nodes):
            infeasible = node.try_schedule(requests)

            if len(infeasible) == len(requests):
                # The node cannot schedule any of the requests.
                continue
            score = node.compute_score()
            results.append(ScheduleResult(node, infeasible, idx, score))

        if len(results) == 0:
            logger.debug("No feasible nodes.")
            return None, requests, nodes_copy

        # Sort the results by score.
        results = sorted(results, key=lambda r: r.score, reverse=True)
        best_result = results[0]

        logger.debug(f"best_node: {best_result.node} ")
        nodes_copy.pop(best_result.idx)

        return best_result.node, best_result.infeasible_requests, nodes_copy
