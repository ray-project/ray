import datetime
from base64 import b64decode
from collections import defaultdict
from typing import Dict, List, Tuple, Callable, Any, Optional

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


def binary_id_to_hex(binary_id: bytes) -> str:
    """A util routine to parse binary repr of id (e.g. node id) to hex repr.)"""
    return binary_to_hex(binary_id)


def count_by(data: Any, keys: List[str])  -> Dict[List[str], int]:
    """
    Count the number of items by the given keys.

    Args:
        data: the data to be counted
        keys: the keys to count by

    Returns:
        counts: the counts
    """
    counts = defaultdict(int)
    for item in data:
        key = tuple(getattr(item, key) for key in keys)
        counts[key] += 1
    return counts


class ClusterStatusFormatter:

    HEADER = """
======== Autoscaler status : {time} ========\n
"""

    # Basic autoscaler info.
    VERBOSE_BASIC = 0

    # Include per node info.
    VERBOSE_MORE = 1

    def __init__(self, time: datetime):
        self._time = time

    def _header(self) -> str:
        return self.HEADER.format(time=self._time)

    def _separator(self) -> str:
        return "-" * len(self.header) + "\n"

    def format(self, data: ClusterStatus, verbose_lvl: int) -> str:
        r = ""
        r += self._header(time=datetime.now())
        r += self._sepratator(len(self.header))

        r += self._format_stats(data, verbose_lvl)

        r += self._format_nodes(data, verbose_lvl)

        r += self._format_usage(data, verbose_lvl)

        r += self._format_demand(data, verbose_lvl)

        if verbose_lvl >= self.VERBOSE_MORE:
            r += self._format_node_usage(data, verbose_lvl)

        return r
    
    def _format_stats(self, data: ClusterStatus, verbose_lvl) -> str:
        r = ""
        stats = data.stats
        if verbose_lvl < self.VERBOSE_MORE:
            return r

        if stats.gcs_request_time_s is not None:
            r += f"GCS request time: {stats.gcs_request_time_s:.3f}s\n"

        if stats.none_terminated_node_request_time_s is not None:
            r += (
                "Node Provider non_terminated_nodes time: "
                f"{stats.none_terminated_node_request_time_s:.3f}s\n"
            )

        if stats.autoscaler_iteration_time_s is not None:
            r += (
                f"Autoscaler iteration time: {stats.autoscaler_iteration_time_s:.3f}s\n"
            )

        return r

    @classmethod
    def _format_nodes(self, data: ClusterStatus, verbose_lvl) -> str:
        r = "Node status\n"
        r += self._separator() 

        r += "Healthy:\n"
        assert len(data.healthy_nodes) > 0
        for node in data.healthy_nodes:
            node_type_count = count_by(node, ["ray_node_type_name"])
            for node_type, count in node_type_count.items():
                r += f" {count} {node_type}\n"

        r += "Pending nodes:\n"
        if len(data.pending_nodes) == 0:
            r += " (no pending nodes)\n"
        else:
            for node in data.pending_nodes:
                r += f"  {node}\n"

        r += "Failed nodes:\n"
        for node in data.failed_nodes:
            r += f"  {node}\n"

        return r

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

        return ClusterStatus(
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

        for request_count in state.pending_resource_requests:
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

        for constraint_request in state.cluster_resource_constraints:
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
        cls, node_state: NodeState, usage: Dict[str, ResourceUsage]
    ):
        for resource_name, resource_total in node_state.total_resources.items():
            usage[resource_name].resource_name = resource_name
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
            node_id = binary_id_to_hex(node_state.node_id)
            if len(node_state.ray_node_type_name) == 0:
                # We don't have a node type name, but this is needed for showing 
                # healthy nodes. This happens when we don't use cluster launcher.
                # but start ray manually. We will use node id as node type name.
                ray_node_type_name = f"node_{node_id}"
            else:
                ray_node_type_name = node_state.ray_node_type_name

            node_info = NodeInfo(
                instance_type_name=node_state.instance_type_name,
                node_status=NodeStatus.Name(node_state.status),
                node_id=binary_id_to_hex(node_state.node_id),
                ip_address=node_state.node_ip_address,
                ray_node_type_name=ray_node_type_name,
                instance_id=node_state.instance_id,
            )

            # Parse the resource usage
            usage = defaultdict(ResourceUsage)
            cls._parse_node_resource_usage(node_state, usage)
            node_resource_usage = NodeUsage(
                usage=list(usage.values()),
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
    def _parse_pending(cls, state: AutoscalingState) -> Tuple[List[PendingLaunchRequest], List[NodeInfo]]:
        """
        Parse the pending requests from the autoscaling state.

        Args:
            state: the autoscaling state
        Returns:
            pending_launches: the list of pending launches
            pending_nodes: the list of pending nodes
        """
        pending_nodes = []
        pending_launches = []
        for pending_request in state.pending_instance_requests:
            pending_node = PendingLaunchRequest(
                instance_type_name=pending_request.instance_type_name,
                node_type_name=pending_request.ray_node_type_name,
                count=pending_request.target_count,
            )

            pending_nodes += [pending_node] * pending_request.target_count

        return pending_nodes
