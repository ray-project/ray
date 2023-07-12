from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Tuple

from ray._private.utils import binary_to_hex
from ray.autoscaler.v2.schema import (
    NODE_DEATH_CAUSE_RAYLET_DIED,
    ClusterConstraintDemand,
    ClusterStatus,
    NodeInfo,
    NodeUsage,
    PendingLaunchRequest,
    PlacementGroupResourceDemand,
    RayTaskActorDemand,
    ResourceDemand,
    ResourceDemandSummary,
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
        pending_launches, pending_nodes = cls._parse_pending(proto.autoscaling_state)

        # parse cluster resource usage
        cluster_resource_usage = cls._parse_cluster_resource_usage(
            proto.cluster_resource_state
        )

        # parse resource demands
        resource_demands = cls._parse_resource_demands(proto.cluster_resource_state)

        # parse stats
        stats = cls._parse_stats(proto, stats)

        return ClusterStatus(
            healthy_nodes=healthy_nodes,
            pending_launches=pending_launches,
            pending_nodes=pending_nodes,
            failed_nodes=failed_nodes,
            cluster_resource_usage=cluster_resource_usage,
            resource_demands=resource_demands,
            stats=stats,
            node_availability=None,
        )

    @classmethod
    def _parse_stats(cls, reply: GetClusterStatusReply, stats: Stats) -> Stats:
        """
        Parse the stats from the get cluster status reply.
        Args:
            reply: the get cluster status reply
            stats: the stats
        Returns:
            stats: the parsed stats
        """
        stats = deepcopy(stats)

        stats.gcs_request_time_s = stats.gcs_request_time_s
        # TODO(rickyx): Populate other autoscaler stats once available.
        stats.autoscaler_version = str(reply.autoscaling_state.autoscaler_state_version)
        stats.cluster_resource_state_version = str(
            reply.cluster_resource_state.cluster_resource_state_version
        )

        return stats

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
        task_actor_demand = []
        pg_demand = []
        constraint_demand = []

        for request_count in state.pending_resource_requests:
            # TODO(rickyx): constraints?
            demand = RayTaskActorDemand(
                bundles_by_count=[
                    ResourceRequestByCount(
                        request_count.request.resources_bundle, request_count.count
                    )
                ],
            )
            task_actor_demand.append(demand)

        for gang_request in state.pending_gang_resource_requests:
            demand = PlacementGroupResourceDemand(
                bundles_by_count=cls._aggregate_resource_requests_by_shape(
                    gang_request.requests
                ),
                details=gang_request.details,
            )
            pg_demand.append(demand)

        for constraint_request in state.cluster_resource_constraints:
            demand = ClusterConstraintDemand(
                bundles_by_count=cls._aggregate_resource_requests_by_shape(
                    constraint_request.min_bundles
                ),
            )
            constraint_demand.append(demand)

        return ResourceDemandSummary(
            ray_task_actor_demand=task_actor_demand,
            placement_group_demand=pg_demand,
            cluster_constraint_demand=constraint_demand,
        )

    @classmethod
    def _aggregate_resource_requests_by_shape(
        cls,
        requests: List[ResourceRequest],
    ) -> List[ResourceRequestByCount]:
        """
        Aggregate resource requests by shape.
        Args:
            requests: the list of resource requests
        Returns:
            resource_requests_by_count: the aggregated resource requests by count
        """

        resource_requests_by_count = defaultdict(int)
        for request in requests:
            bundle = frozenset(request.resources_bundle.items())
            resource_requests_by_count[bundle] += 1

        return [
            ResourceRequestByCount(dict(bundle), count)
            for bundle, count in resource_requests_by_count.items()
        ]

    @classmethod
    def _parse_node_resource_usage(
        cls, node_state: NodeState, usage: Dict[str, ResourceUsage]
    ):
        # Tuple of {resource_name : (used, total)}
        d = defaultdict(lambda: [0.0, 0.0])
        for resource_name, resource_total in node_state.total_resources.items():
            d[resource_name][1] += resource_total
            # Will be subtracted from available later.
            d[resource_name][0] += resource_total

        for (
            resource_name,
            resource_available,
        ) in node_state.available_resources.items():
            d[resource_name][0] -= resource_available

        for k, (used, total) in d.items():
            usage[k] = ResourceUsage(
                resource_name=k,
                used=used,
                total=total,
            )

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
            node_id = binary_to_hex(node_state.node_id)
            if len(node_state.ray_node_type_name) == 0:
                # We don't have a node type name, but this is needed for showing
                # healthy nodes. This happens when we don't use cluster launcher.
                # but start ray manually. We will use node id as node type name.
                ray_node_type_name = f"node_{node_id}"
            else:
                ray_node_type_name = node_state.ray_node_type_name

            # Parse the resource usage if it's not dead
            node_resource_usage = None
            failure_detail = None
            if node_state.status == NodeStatus.DEAD:
                # TODO(rickyx): Technically we could get a more verbose
                # failure detail from GCS, but existing ray status treats
                # all ray failures as raylet death.
                failure_detail = NODE_DEATH_CAUSE_RAYLET_DIED
            else:
                usage = defaultdict(ResourceUsage)
                cls._parse_node_resource_usage(node_state, usage)
                node_resource_usage = NodeUsage(
                    usage=list(usage.values()),
                    idle_time_ms=node_state.time_since_last_status_change_ms
                    if node_state.status == NodeStatus.IDLE
                    else 0,
                )

            node_info = NodeInfo(
                instance_type_name=node_state.instance_type_name,
                node_status=NodeStatus.Name(node_state.status),
                node_id=binary_to_hex(node_state.node_id),
                ip_address=node_state.node_ip_address,
                ray_node_type_name=ray_node_type_name,
                instance_id=node_state.instance_id,
                resource_usage=node_resource_usage,
                failure_detail=failure_detail,
            )

            if node_state.status == NodeStatus.DEAD:
                dead_nodes.append(node_info)
            else:
                healthy_nodes.append(node_info)

        return healthy_nodes, dead_nodes

    @classmethod
    def _parse_pending(
        cls, state: AutoscalingState
    ) -> Tuple[List[PendingLaunchRequest], List[NodeInfo]]:
        """
        Parse the pending requests/nodes from the autoscaling state.
        Args:
            state: the autoscaling state, empty if there's no autoscaling state
                being reported.
        Returns:
            pending_launches: the list of pending launches
            pending_nodes: the list of pending nodes
        """
        pending_nodes = []
        pending_launches = []
        for pending_request in state.pending_instance_requests:
            launch = PendingLaunchRequest(
                instance_type_name=pending_request.instance_type_name,
                ray_node_type_name=pending_request.ray_node_type_name,
                count=pending_request.count,
            )

            pending_launches.append(launch)

        for pending_node in state.pending_instances:
            pending_nodes.append(
                NodeInfo(
                    instance_type_name=pending_node.instance_type_name,
                    ray_node_type_name=pending_node.ray_node_type_name,
                    details=pending_node.details,
                    instance_id=pending_node.instance_id,
                    ip_address=pending_node.ip_address,
                )
            )

        return pending_launches, pending_nodes
