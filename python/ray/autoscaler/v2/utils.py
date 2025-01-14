from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray._private.utils import binary_to_hex
from ray._raylet import GcsClient
from ray.autoscaler._private.autoscaler import AutoscalerSummary
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilityRecord,
    NodeAvailabilitySummary,
    UnavailableNodeInformation,
)
from ray.autoscaler._private.util import LoadMetricsSummary, format_info_string
from ray.autoscaler.v2.schema import (
    NODE_DEATH_CAUSE_RAYLET_DIED,
    ClusterConstraintDemand,
    ClusterStatus,
    LaunchRequest,
    NodeInfo,
    NodeUsage,
    PlacementGroupResourceDemand,
    RayTaskActorDemand,
    ResourceDemand,
    ResourceDemandSummary,
    ResourceRequestByCount,
    ResourceUsage,
    Stats,
)
from ray.core.generated.autoscaler_pb2 import (
    AffinityConstraint,
    AntiAffinityConstraint,
    AutoscalingState,
    ClusterResourceState,
    GetClusterStatusReply,
    NodeState,
    NodeStatus,
    PlacementConstraint,
    ResourceRequest,
)
from ray.core.generated.autoscaler_pb2 import (
    ResourceRequestByCount as ResourceRequestByCountProto,
)
from ray.experimental.internal_kv import internal_kv_get_gcs_client


def _count_by(data: Any, key: str) -> Dict[str, int]:
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
        key_name = getattr(item, key)
        counts[key_name] += 1
    return counts


class ProtobufUtil:
    """
    A utility class for protobuf objects.
    """

    @staticmethod
    def to_dict(proto):
        """
        Convert a protobuf object to a dict.

        This is a slow conversion, and should only be used for debugging or
        latency insensitve code.

        Args:
            proto: the protobuf object
        Returns:
            dict: the dict
        """
        from ray._private.protobuf_compat import message_to_dict

        return message_to_dict(
            proto,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )

    @staticmethod
    def to_dict_list(protos):
        """
        Convert a list of protobuf objects to a list of dicts.

        Args:
            protos: the list of protobuf objects
        Returns:
            dict_list: the list of dicts
        """
        return [ProtobufUtil.to_dict(proto) for proto in protos]


class ResourceRequestUtil(ProtobufUtil):
    """
    A utility class for resource requests, autoscaler.proto.ResourceRequest
    """

    class PlacementConstraintType(Enum):
        """
        The affinity type for the resource request.
        """

        ANTI_AFFINITY = "ANTI_AFFINITY"
        AFFINITY = "AFFINITY"

    @staticmethod
    def group_by_count(
        requests: List[ResourceRequest],
    ) -> List[ResourceRequestByCountProto]:
        """
        Aggregate resource requests by shape.
        Args:
            requests: the list of resource requests
        Returns:
            resource_requests_by_count: the aggregated resource requests by count
        """
        resource_requests_by_count = defaultdict(int)
        for request in requests:
            serialized_request = request.SerializeToString()
            resource_requests_by_count[serialized_request] += 1

        results = []
        for serialized_request, count in resource_requests_by_count.items():
            request = ResourceRequest()
            request.ParseFromString(serialized_request)
            results.append(ResourceRequestByCountProto(request=request, count=count))

        return results

    @staticmethod
    def ungroup_by_count(
        requests_by_count: List[ResourceRequestByCountProto],
    ) -> List[ResourceRequest]:
        """
        Flatten the resource requests by count to resource requests.
        Args:
            requests_by_count: the resource requests by count
        Returns:
            requests: the flattened resource requests
        """
        reqs = []
        for r in requests_by_count:
            reqs += [r.request] * r.count

        return reqs

    @staticmethod
    def to_resource_map(
        request: ResourceRequest,
    ) -> Dict[str, float]:
        """
        Convert the resource request by count to resource map.
        Args:
            request: the resource request
        Returns:
            resource_map: the resource map
        """
        resource_map = defaultdict(float)
        for k, v in request.resources_bundle.items():
            resource_map[k] += v
        return dict(resource_map)

    @staticmethod
    def to_resource_maps(
        requests: List[ResourceRequest],
    ) -> List[Dict[str, float]]:
        """
        Convert the resource requests by count to resource map.
        Args:
            requests: the resource requests
        Returns:
            resource_maps: list of resource map
        """
        return [ResourceRequestUtil.to_resource_map(r) for r in requests]

    @staticmethod
    def make(
        resources_map: Dict[str, float],
        constraints: Optional[List[Tuple[PlacementConstraintType, str, str]]] = None,
    ) -> ResourceRequest:
        """
        Make a resource request from the given resources map.
        Args:
            resources_map: the resources map
        Returns:
            request: the resource request
        """
        request = ResourceRequest()
        for resource_name, quantity in resources_map.items():
            request.resources_bundle[resource_name] = quantity

        if constraints is None:
            return request

        for constraint_type, label, value in constraints:
            if constraint_type == ResourceRequestUtil.PlacementConstraintType.AFFINITY:
                request.placement_constraints.append(
                    PlacementConstraint(
                        affinity=AffinityConstraint(label_name=label, label_value=value)
                    )
                )
            elif (
                constraint_type
                == ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY
            ):
                request.placement_constraints.append(
                    PlacementConstraint(
                        anti_affinity=AntiAffinityConstraint(
                            label_name=label, label_value=value
                        )
                    )
                )
            else:
                raise ValueError(f"Unknown constraint type: {constraint_type}")

        return request

    @staticmethod
    def combine_requests_with_affinity(
        resource_requests: List[ResourceRequest],
    ) -> List[ResourceRequest]:
        """
        Combine the resource requests with affinity constraints
        into the same request. This is so that requests with affinity
         constraints could be considered and placed together.

        It merges the resource requests with the same affinity constraints
        into one request, and dedup the placement constraints.

        This assumes following:
            1. There's only at most 1 placement constraint, either an affinity
            constraint OR an anti-affinity constraint.

        Args:
            resource_requests: The list of resource requests to be combined.
        Returns:
            A list of combined resource requests.
        """

        # Map of set of serialized affinity constraint to the list of resource requests
        requests_by_affinity: Dict[
            Tuple[str, str], List[ResourceRequest]
        ] = defaultdict(list)
        combined_requests: List[ResourceRequest] = []

        for request in resource_requests:
            assert len(request.placement_constraints) <= 1, (
                "There should be at most 1 placement constraint, "
                "either an affinity constraint OR an anti-affinity constraint."
            )

            if len(request.placement_constraints) == 0:
                # No affinity constraints, just add to the combined requests.
                combined_requests.append(request)
                continue

            constraint = request.placement_constraints[0]

            if constraint.HasField("affinity"):
                affinity = constraint.affinity
                requests_by_affinity[
                    (affinity.label_name, affinity.label_value)
                ].append(request)
            elif constraint.HasField("anti_affinity"):
                # We don't need to combine requests with anti-affinity constraints.
                combined_requests.append(request)

        for (
            affinity_label_name,
            affinity_label_value,
        ), requests in requests_by_affinity.items():
            combined_request = ResourceRequest()

            # Merge the resource bundles with the same affinity constraint.
            for request in requests:
                for k, v in request.resources_bundle.items():
                    combined_request.resources_bundle[k] = (
                        combined_request.resources_bundle.get(k, 0) + v
                    )

            # Add the placement constraint to the combined request.
            affinity_constraint = AffinityConstraint(
                label_name=affinity_label_name, label_value=affinity_label_value
            )
            combined_request.placement_constraints.append(
                PlacementConstraint(affinity=affinity_constraint)
            )

            combined_requests.append(combined_request)

        return combined_requests


class ClusterStatusFormatter:
    """
    A formatter to format the ClusterStatus into a string.

    TODO(rickyx): We right now parse the ClusterStatus to the legacy format
    by using the `format_info_string`.
    In the future, we should refactor the `format_info_string` to directly format
    the ClusterStatus into a string as we migrate eventually away from v1.

    """

    @classmethod
    def format(cls, data: ClusterStatus, verbose: bool = False) -> str:
        lm_summary = cls._parse_lm_summary(data)
        autoscaler_summary = cls._parse_autoscaler_summary(data)

        return format_info_string(
            lm_summary,
            autoscaler_summary,
            time=datetime.fromtimestamp(data.stats.request_ts_s),
            gcs_request_time=data.stats.gcs_request_time_s,
            non_terminated_nodes_time=data.stats.none_terminated_node_request_time_s,
            autoscaler_update_time=data.stats.autoscaler_iteration_time_s,
            verbose=verbose,
        )

    @classmethod
    def _parse_autoscaler_summary(cls, data: ClusterStatus) -> AutoscalerSummary:
        active_nodes = _count_by(data.active_nodes, "ray_node_type_name")
        idle_nodes = _count_by(data.idle_nodes, "ray_node_type_name")
        pending_launches = _count_by(data.pending_launches, "ray_node_type_name")
        pending_nodes = []
        for node in data.pending_nodes:
            # We are using details for the pending node's status.
            # TODO(rickyx): we should probably use instance id rather than ip address
            # here.
            pending_nodes.append(
                (node.ip_address, node.ray_node_type_name, node.details)
            )

        failed_nodes = []
        for node in data.failed_nodes:
            # TODO(rickyx): we should probably use instance id/node id rather
            # than node ip here since node ip is not unique among failed nodes.
            failed_nodes.append((node.ip_address, node.ray_node_type_name))

        # From IP to node type name.
        node_type_mapping = {}
        for node in chain(data.active_nodes, data.idle_nodes):
            node_type_mapping[node.ip_address] = node.ray_node_type_name

        # Transform failed launches to node_availability_summary
        node_availabilities = {}
        for failed_launch in data.failed_launches:
            # TODO(rickyx): we could also add failed timestamp, count info.
            node_availabilities[
                failed_launch.ray_node_type_name
            ] = NodeAvailabilityRecord(
                node_type=failed_launch.ray_node_type_name,
                is_available=False,
                last_checked_timestamp=failed_launch.request_ts_s,
                unavailable_node_information=UnavailableNodeInformation(
                    category="LaunchFailed",
                    description=failed_launch.details,
                ),
            )
        node_availabilities = NodeAvailabilitySummary(
            node_availabilities=node_availabilities
        )

        node_activities = {
            node.node_id: (node.ip_address, node.node_activity)
            for node in data.active_nodes
        }

        return AutoscalerSummary(
            active_nodes=active_nodes,
            idle_nodes=idle_nodes,
            pending_launches=pending_launches,
            pending_nodes=pending_nodes,
            failed_nodes=failed_nodes,
            pending_resources={},  # NOTE: This is not used in ray status.
            node_type_mapping=node_type_mapping,
            node_availability_summary=node_availabilities,
            node_activities=node_activities,
        )

    @classmethod
    def _parse_lm_summary(cls, data: ClusterStatus) -> LoadMetricsSummary:
        usage = {
            u.resource_name: (u.used, u.total) for u in data.cluster_resource_usage
        }
        resource_demands = []
        for demand in data.resource_demands.ray_task_actor_demand:
            for bundle_by_count in demand.bundles_by_count:
                resource_demands.append((bundle_by_count.bundle, bundle_by_count.count))

        pg_demand = []
        pg_demand_strs = []
        pg_demand_str_to_demand = {}
        for pg_demand in data.resource_demands.placement_group_demand:
            s = pg_demand.strategy + "|" + pg_demand.state
            pg_demand_strs.append(s)
            pg_demand_str_to_demand[s] = pg_demand

        pg_freqs = Counter(pg_demand_strs)
        pg_demand = [
            (
                {
                    "strategy": pg_demand_str_to_demand[pg_str].strategy,
                    "bundles": [
                        (bundle_count.bundle, bundle_count.count)
                        for bundle_count in pg_demand_str_to_demand[
                            pg_str
                        ].bundles_by_count
                    ],
                },
                freq,
            )
            for pg_str, freq in pg_freqs.items()
        ]

        request_demand = [
            (bc.bundle, bc.count)
            for constraint_demand in data.resource_demands.cluster_constraint_demand
            for bc in constraint_demand.bundles_by_count
        ]

        usage_by_node = {}
        node_type_mapping = {}
        idle_time_map = {}
        for node in chain(data.active_nodes, data.idle_nodes):
            # TODO(rickyx): we should actually add node type info here.
            # TODO(rickyx): we could also show node idle time.
            usage_by_node[node.node_id] = {
                u.resource_name: (u.used, u.total) for u in node.resource_usage.usage
            }
            node_type_mapping[node.node_id] = node.ray_node_type_name
            idle_time_map[node.node_id] = node.resource_usage.idle_time_ms

        return LoadMetricsSummary(
            usage=usage,
            resource_demand=resource_demands,
            pg_demand=pg_demand,
            request_demand=request_demand,
            node_types=None,  # NOTE: This is not needed in ray status.
            usage_by_node=usage_by_node,
            node_type_mapping=node_type_mapping,
            idle_time_map=idle_time_map,
        )


class ClusterStatusParser:
    @classmethod
    def from_get_cluster_status_reply(
        cls, proto: GetClusterStatusReply, stats: Stats
    ) -> ClusterStatus:
        # parse nodes info
        active_nodes, idle_nodes, failed_nodes = cls._parse_nodes(
            proto.cluster_resource_state
        )

        # parse pending nodes info
        pending_nodes = cls._parse_pending(proto.autoscaling_state)

        # parse launch requests
        pending_launches, failed_launches = cls._parse_launch_requests(
            proto.autoscaling_state
        )

        # parse cluster resource usage
        cluster_resource_usage = cls._parse_cluster_resource_usage(
            proto.cluster_resource_state
        )

        # parse resource demands
        resource_demands = cls._parse_resource_demands(proto.cluster_resource_state)

        # parse stats
        stats = cls._parse_stats(proto, stats)

        return ClusterStatus(
            active_nodes=active_nodes,
            idle_nodes=idle_nodes,
            pending_launches=pending_launches,
            failed_launches=failed_launches,
            pending_nodes=pending_nodes,
            failed_nodes=failed_nodes,
            cluster_resource_usage=cluster_resource_usage,
            resource_demands=resource_demands,
            stats=stats,
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
                bundles_by_count=[
                    ResourceRequestByCount(
                        bundle=dict(r.request.resources_bundle.items()), count=r.count
                    )
                    for r in constraint_request.resource_requests
                ]
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
    ) -> Dict[str, ResourceUsage]:
        """
        Parse the node resource usage from the node state.
        Args:
            node_state: the node state
            usage: the usage dict to be updated. This is a dict of
                {resource_name: ResourceUsage}
        Returns:
            usage: the updated usage dict
        """
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

        # Merge with the passed in usage.
        for k, (used, total) in d.items():
            usage[k].resource_name = k
            usage[k].used += used
            usage[k].total += total

        return usage

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
            if node_state.status != NodeStatus.DEAD:
                cluster_resource_usage = cls._parse_node_resource_usage(
                    node_state, cluster_resource_usage
                )

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
            active_nodes: the list of non-idle nodes
            idle_nodes: the list of idle nodes
            dead_nodes: the list of dead nodes
        """
        active_nodes = []
        dead_nodes = []
        idle_nodes = []
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
                usage = cls._parse_node_resource_usage(node_state, usage)
                node_resource_usage = NodeUsage(
                    usage=list(usage.values()),
                    idle_time_ms=node_state.idle_duration_ms
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
                node_activity=node_state.node_activity,
            )

            if node_state.status == NodeStatus.DEAD:
                dead_nodes.append(node_info)
            elif node_state.status == NodeStatus.IDLE:
                idle_nodes.append(node_info)
            else:
                active_nodes.append(node_info)

        return active_nodes, idle_nodes, dead_nodes

    @classmethod
    def _parse_launch_requests(
        cls, state: AutoscalingState
    ) -> Tuple[List[LaunchRequest], List[LaunchRequest]]:
        """
        Parse the launch requests from the autoscaling state.
        Args:
            state: the autoscaling state, empty if there's no autoscaling state
                being reported.
        Returns:
            pending_launches: the list of pending launches
            failed_launches: the list of failed launches
        """
        pending_launches = []
        for pending_request in state.pending_instance_requests:
            launch = LaunchRequest(
                instance_type_name=pending_request.instance_type_name,
                ray_node_type_name=pending_request.ray_node_type_name,
                count=pending_request.count,
                state=LaunchRequest.Status.PENDING,
                request_ts_s=pending_request.request_ts,
            )

            pending_launches.append(launch)

        failed_launches = []
        for failed_request in state.failed_instance_requests:
            launch = LaunchRequest(
                instance_type_name=failed_request.instance_type_name,
                ray_node_type_name=failed_request.ray_node_type_name,
                count=failed_request.count,
                state=LaunchRequest.Status.FAILED,
                request_ts_s=failed_request.start_ts,
                details=failed_request.reason,
                failed_ts_s=failed_request.failed_ts,
            )

            failed_launches.append(launch)

        return pending_launches, failed_launches

    @classmethod
    def _parse_pending(cls, state: AutoscalingState) -> List[NodeInfo]:
        """
        Parse the pending requests/nodes from the autoscaling state.
        Args:
            state: the autoscaling state, empty if there's no autoscaling state
                being reported.
        Returns:
            pending_nodes: the list of pending nodes
        """
        pending_nodes = []
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

        return pending_nodes


cached_is_autoscaler_v2 = None


def is_autoscaler_v2(
    fetch_from_server: bool = False, gcs_client: Optional[GcsClient] = None
) -> bool:
    """
    Check if the autoscaler is v2 from reading GCS internal KV.

    If the method is called multiple times, the result will be cached in the module.

    Args:
        fetch_from_server: If True, fetch the value from the GCS server, otherwise
            use the cached value.
        gcs_client: The GCS client to use. If not provided, the default GCS
            client will be used.

    Returns:
        is_v2: True if the autoscaler is v2, False otherwise.

    Raises:
        Exception: if GCS address could not be resolved (e.g. ray.init() not called)
    """
    # If env var is set to enable autoscaler v2, we should always return True.
    if ray._config.enable_autoscaler_v2() and not fetch_from_server:
        # TODO(rickyx): Once we migrate completely to v2, we should remove this.
        # While this short-circuit may allow client-server inconsistency
        # (e.g. client running v1, while server running v2), it's currently
        # not possible with existing use-cases.
        return True

    global cached_is_autoscaler_v2
    if cached_is_autoscaler_v2 is not None and not fetch_from_server:
        return cached_is_autoscaler_v2

    if gcs_client is None:
        gcs_client = internal_kv_get_gcs_client()

    assert gcs_client, (
        "GCS client is not available. Please initialize the global GCS client "
        "first by calling ray.init() or explicitly calls to _initialize_internal_kv()."
    )

    # See src/ray/common/constants.h for the definition of this key.
    cached_is_autoscaler_v2 = (
        gcs_client.internal_kv_get(
            ray._raylet.GCS_AUTOSCALER_V2_ENABLED_KEY.encode(),
            namespace=ray._raylet.GCS_AUTOSCALER_STATE_NAMESPACE.encode(),
        )
        == b"1"
    )

    return cached_is_autoscaler_v2


def is_head_node(node_state: NodeState) -> bool:
    """
    Check if the node is a head node from the node state.

    Args:
        node_state: the node state
    Returns:
        is_head: True if the node is a head node, False otherwise.
    """
    # TODO: we should include this bit of information in the future.
    # NOTE: we could use labels in the future to determine if it's a head node.
    return "node:__internal_head__" in dict(node_state.total_resources)
