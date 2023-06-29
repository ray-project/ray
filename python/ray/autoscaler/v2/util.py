from collections import defaultdict
from typing import Dict, List, Tuple

from ray.autoscaler.v2.schema import (
    NODE_DEATH_CAUSE_RAYLET_DIED,
    ClusterConstraintDemand,
    ClusterStatus,
    NodeInfo,
    NodeUsage,
    PendingNode,
    PlacementGroupResourceDemand,
    RayTaskActorDemand,
    ResourceDemand,
    ResourceRequestByCount,
    ResourceUsage,
    Stats,
)
from ray.core.generated.experimental.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    GetClusterStatusReply,
    NodeState,
    NodeStatus,
    ResourceRequest,
)


class ClusterStatusParser:
    @classmethod
    def from_get_cluster_status_reply(
        cls, proto: GetClusterStatusReply, stats: Stats
    ) -> ClusterStatus:
        # parse healthy nodes info
        healthy_nodes, failed_nodes = cls._parse_nodes(proto.cluster_resource_state)

        # parse pending nodes info
        pending_nodes = cls._parse_pending_requests(proto.autoscaling_state)

        # parse cluster resource usage
        cluster_resource_usage = cls._parse_cluster_resource_usage(
            proto.cluster_resource_state
        )

        # parse resource demands
        resource_demands = cls._parse_resource_demands(proto.cluster_resource_state)

        # modify the states
        stats = cls._parse_stats(
            proto.cluster_resource_state, proto.autoscaling_state, stats
        )

        return cls(
            healthy_nodes=healthy_nodes,
            pending_nodes=pending_nodes,
            failed_nodes=failed_nodes,
            cluster_resource_usage=cluster_resource_usage,
            resource_demands=resource_demands,
            stats=stats,
        )

    @classmethod
    def _parse_resource_demands(
        cls, state: ClusterResourceState
    ) -> List[ResourceDemand]:
        """
        Parse the resource demands from the cluster resource state.

        Args:
            state: the cluster resource state

        Returns:
            resource_demands: the resource demands
        """
        resource_demands = []

        for request_count in state.pending_resource_request:
            # TODO(rickyx): constraints?
            demand = RayTaskActorDemand(
                bundles=[
                    ResourceRequestByCount(
                        request_count.request.resources_bundle, request_count.count
                    )
                ],
            )
            resource_demands.append(demand)

        for gang_request in state.pending_gang_resource_requests:
            demand = PlacementGroupResourceDemand(
                bundles=cls._aggregate_resource_requests_by_shape(
                    gang_request.requests
                ),
                strategy=gang_request.strategy,
            )
            resource_demands.append(demand)

        for constraint_request in state.pending_cluster_resource_constraints:
            demand = ClusterConstraintDemand(
                bundles=cls._aggregate_resource_requests_by_shape(
                    constraint_request.min_bundles
                ),
            )
            resource_demands.append(demand)

        return resource_demands

    @classmethod
    def _aggregate_resource_requests_by_shape(
        requests: List[ResourceRequest],
    ) -> List[ResourceRequestByCount]:
        """
        Aggregate resource requests by shape.

        TODO:

        Args:
            requests: the list of resource requests

        Returns:
            resource_requests_by_count: the aggregated resource requests by count
        """

        resource_requests_by_count = defaultdict(int)
        for request in requests:
            resource_requests_by_count[request.resources_bundle] += 1

        return [
            ResourceRequestByCount(bundle, count)
            for bundle, count in resource_requests_by_count.items()
        ]

    @classmethod
    def _parse_node_resource_usage(
        node_state: NodeState, usage: Dict[str, ResourceUsage]
    ):
        for resource_name, resource_total in node_state.total_resources.items():
            usage[resource_name].total += resource_total
            # Will be subtracted from available later.
            usage[resource_name].used += resource_total

        for (
            resource_name,
            resource_available,
        ) in node_state.available_resources.items():
            usage[resource_name].used -= resource_available

    @classmethod
    def _parse_cluster_resource_usage(
        cls,
        state: ClusterResourceState,
    ) -> List[ResourceUsage]:
        """
        Parse the cluster resource usage from the cluster resource state.

        Args:
            state: the cluster resource state

        Returns:
            cluster_resource_usage: the cluster resource usage
        """

        cluster_resource_usage = defaultdict(ResourceUsage)

        for node_state in state.node_states:
            cls._parse_node_resource_usage(node_state, cluster_resource_usage)

        return list(cluster_resource_usage.values())

    @classmethod
    def _parse_nodes(
        cls,
        state: ClusterResourceState,
    ) -> Tuple[List[NodeInfo], List[NodeInfo]]:
        """
        Parse the node info from the cluster resource state.

        Args:
            state: the cluster resource state

        Returns:
            healthy_nodes: the list of healthy nodes (both idle and none-idle)
            dead_nodes: the list of dead nodes
        """
        healthy_nodes = []
        dead_nodes = []
        for node_state in state.node_states:
            # Basic node info.
            node_info = NodeInfo(
                instance_type_name=node_state.instance_type_name,
                node_status=NodeStatus.Value(node_state.status),
                node_id=node_state.node_id,
                ip_address=node_state.node_ip_address,
                ray_node_type_name=node_state.ray_node_type_name,
                instance_id=node_state.instance_id,
            )

            # Parse the resource usage
            usage = defaultdict(ResourceUsage)
            cls._parse_node_resource_usage(node_state, usage)
            node_resource_usage = NodeUsage(
                usage=usage.values(),
                idle_time_ms=node_state.time_since_last_status_change_ms
                if node_state.status == NodeStatus.IDLE
                else 0,
            )
            node_info.resource_usage = node_resource_usage

            if node_state.status == NodeStatus.DEAD:
                node_info.failure_detail = NODE_DEATH_CAUSE_RAYLET_DIED
                dead_nodes.append(node_info)
            else:
                healthy_nodes.append(node_info)

        return healthy_nodes, dead_nodes

    @classmethod
    def _parse_pending_requests(cls, state: AutoscalingState) -> List[PendingNode]:
        """
        Parse the pending requests from the autoscaling state.

        Args:
            state: the autoscaling state
        Returns:
            pending_nodes: the list of pending nodes
        """
        pending_nodes = []
        for pending_request in state.pending_instance_requests:
            pending_node = PendingNode(
                instance_type_name=pending_request.instance_type_name,
                node_type_name=pending_request.ray_node_type_name,
            )

            pending_nodes += [pending_node] * pending_request.target_count

        return pending_nodes
