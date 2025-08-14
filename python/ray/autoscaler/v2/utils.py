from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray._common.utils import binary_to_hex
from ray._raylet import GcsClient
from ray.autoscaler._private import constants
from ray.autoscaler._private.util import (
    format_pg,
    format_resource_demand_summary,
    parse_usage,
)
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
from ray.core.generated.common_pb2 import (
    LabelSelectorConstraint,
    LabelSelector,
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
        label_selectors: Optional[List[List[Tuple[str, int, List[str]]]]] = None,
    ) -> ResourceRequest:
        """
        Make a resource request from the given resources map.
        Args:
            resources_map: Mapping of resource names to quantities.
            constraints: Placement constraints. Each tuple is (constraint_type,
                label_key, label_value), where `constraint_type` is a
                PlacementConstraintType (AFFINITY or ANTI_AFFINITY).
            label_selectors: Optional list of label selectors. Each selector is
                a list of (label_key, operator_enum, label_values) tuples.
        Returns:
            request: the ResourceRequest object
        """
        request = ResourceRequest()
        for resource_name, quantity in resources_map.items():
            request.resources_bundle[resource_name] = quantity

        if constraints is not None:
            for constraint_type, label, value in constraints:
                if (
                    constraint_type
                    == ResourceRequestUtil.PlacementConstraintType.AFFINITY
                ):
                    request.placement_constraints.append(
                        PlacementConstraint(
                            affinity=AffinityConstraint(
                                label_name=label, label_value=value
                            )
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

        if label_selectors is not None:
            for selector in label_selectors:
                selector_proto = LabelSelector()
                for label_key, operator_enum, label_values in selector:
                    selector_proto.label_constraints.append(
                        LabelSelectorConstraint(
                            label_key=label_key,
                            operator=operator_enum,
                            label_values=label_values,
                        )
                    )
                request.label_selectors.append(selector_proto)

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
            Tuple[str, str, Tuple], List[ResourceRequest]
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
                # Combine requests with affinity and label selectors.
                affinity = constraint.affinity
                key = (
                    affinity.label_name,
                    affinity.label_value,
                    ResourceRequestUtil._label_selector_key(request.label_selectors),
                )
                requests_by_affinity[key].append(request)
            elif constraint.HasField("anti_affinity"):
                # We don't need to combine requests with anti-affinity constraints.
                combined_requests.append(request)

        for (
            affinity_label_name,
            affinity_label_value,
            label_selector_key,
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

            combined_request.label_selectors.extend(requests[0].label_selectors)

            combined_requests.append(combined_request)

        return combined_requests

    def _label_selector_key(
        label_selectors: List[LabelSelector],
    ) -> Tuple:
        """
        Convert label selectors into a hashable form for grouping.
        This is used for gang requests with identical label_selectors.
        """
        result = []
        for selector in label_selectors:
            constraints = []
            for constraint in selector.label_constraints:
                constraints.append(
                    (
                        constraint.label_key,
                        constraint.operator,
                        tuple(sorted(constraint.label_values)),
                    )
                )
            result.append(tuple(constraints))
        return tuple(result)


class ClusterStatusFormatter:
    """
    A formatter to format the ClusterStatus into a string.

    """

    @classmethod
    def format(cls, data: ClusterStatus, verbose: bool = False) -> str:
        header, separator_len = cls._header_info(data, verbose)
        separator = "-" * separator_len

        # Parse ClusterStatus information to reportable format
        available_node_report = cls._available_node_report(data)
        idle_node_report = cls._idle_node_report(data)
        pending_report = cls._pending_node_report(data)
        failure_report = cls._failed_node_report(data, verbose)
        cluster_usage_report = cls._cluster_usage_report(data, verbose)
        constraints_report = cls._constraint_report(
            data.resource_demands.cluster_constraint_demand
        )
        demand_report = cls._demand_report(data)
        node_usage_report = (
            ""
            if not verbose
            else cls._node_usage_report(data.active_nodes, data.idle_nodes)
        )

        # Format Cluster Status reports into one output
        formatted_output_lines = [
            header,
            "Node status",
            separator,
            "Active:",
            available_node_report,
            "Idle:",
            idle_node_report,
            "Pending:",
            pending_report,
            failure_report,
            "",
            "Resources",
            separator,
            "Total Usage:",
            cluster_usage_report,
            "Total Constraints:",
            constraints_report,
            "Total Demands:",
            demand_report,
            node_usage_report,
        ]

        formatted_output = "\n".join(formatted_output_lines)

        return formatted_output.strip()

    @staticmethod
    def _node_usage_report(
        active_nodes: List[NodeInfo], idle_nodes: List[NodeInfo]
    ) -> str:
        """[Example]:
        Node: raycluster-autoscaler-small-group-worker-n8hrw (small-group)
         Id: cc22041297e5fc153b5357e41f184c8000869e8de97252cc0291fd17
         Usage:
          1.0/1.0 CPU
          0B/953.67MiB memory
          0B/251.76MiB object_store_memory
         Activity:
          Resource: CPU currently in use.
          Busy workers on node.
        """
        node_id_to_usage: Dict[str, Dict[str, Tuple[float, float]]] = {}
        node_id_to_type: Dict[str, str] = {}
        node_id_to_idle_time: Dict[str, int] = {}
        node_id_to_instance_id: Dict[str, str] = {}
        node_id_to_activities: Dict[str, List[str]] = {}

        # Populate mappings for node types, idle times, instance ids, and activities
        for node in chain(active_nodes, idle_nodes):
            node_id_to_usage[node.node_id] = {
                u.resource_name: (u.used, u.total) for u in node.resource_usage.usage
            }
            node_id_to_type[node.node_id] = node.ray_node_type_name
            node_id_to_idle_time[node.node_id] = node.resource_usage.idle_time_ms
            node_id_to_instance_id[node.node_id] = node.instance_id
            node_id_to_activities[node.node_id] = node.node_activity

        node_usage_report_lines = []
        for node_id, usage in node_id_to_usage.items():
            node_usage_report_lines.append("")  # Add a blank line between nodes

            node_type_line = f"Node: {node_id_to_instance_id[node_id]}"
            if node_id in node_id_to_type:
                node_type = node_id_to_type[node_id]
                node_type_line += f" ({node_type})"
            node_usage_report_lines.append(node_type_line)
            node_usage_report_lines.append(f" Id: {node_id}")

            if node_id_to_idle_time.get(node_id, 0) > 0:
                node_usage_report_lines.append(
                    f" Idle: {node_id_to_idle_time[node_id]} ms"
                )

            node_usage_report_lines.append(" Usage:")
            for line in parse_usage(usage, verbose=True):
                node_usage_report_lines.append(f"  {line}")

            activities = node_id_to_activities.get(node_id, [])
            node_usage_report_lines.append(" Activity:")
            if activities is None or len(activities) == 0:
                node_usage_report_lines.append("  (no activity)")
            else:
                for activity in activities:
                    node_usage_report_lines.append(f"  {activity}")

        # Join the list into a single string with new lines
        return "\n".join(node_usage_report_lines)

    @staticmethod
    def _header_info(data: ClusterStatus, verbose: bool) -> (str, int):
        # Get the request timestamp or default to the current time
        time = (
            datetime.fromtimestamp(data.stats.request_ts_s)
            if data.stats.request_ts_s
            else datetime.now()
        )

        # Gather the time statistics
        gcs_request_time = data.stats.gcs_request_time_s
        non_terminated_nodes_time = data.stats.none_terminated_node_request_time_s
        autoscaler_update_time = data.stats.autoscaler_iteration_time_s

        # Create the header with autoscaler status
        header = "=" * 8 + f" Autoscaler status: {time} " + "=" * 8
        separator_len = len(header)

        # Add verbose details if required
        if verbose:
            details = []
            if gcs_request_time:
                details.append(f"GCS request time: {gcs_request_time:3f}s")
            if non_terminated_nodes_time:
                details.append(
                    f"Node Provider non_terminated_nodes time: {non_terminated_nodes_time:3f}s"
                )
            if autoscaler_update_time:
                details.append(
                    f"Autoscaler iteration time: {autoscaler_update_time:3f}s"
                )

            if details:
                header += "\n" + "\n".join(details) + "\n"

        return header, separator_len

    @staticmethod
    def _available_node_report(data: ClusterStatus) -> str:
        active_nodes = _count_by(data.active_nodes, "ray_node_type_name")

        # Build the available node report
        if not active_nodes:
            return " (no active nodes)"
        return "\n".join(
            f" {count} {node_type}" for node_type, count in active_nodes.items()
        )

    @staticmethod
    def _idle_node_report(data: ClusterStatus) -> str:
        idle_nodes = _count_by(data.idle_nodes, "ray_node_type_name")

        # Build the idle node report
        if not idle_nodes:
            return " (no idle nodes)"
        return "\n".join(
            f" {count} {node_type}" for node_type, count in idle_nodes.items()
        )

    @staticmethod
    def _failed_node_report(data: ClusterStatus, verbose: bool) -> str:
        failure_lines = []

        # Process failed launches
        if data.failed_launches:
            sorted_failed_launches = sorted(
                data.failed_launches,
                key=lambda launch: launch.request_ts_s,
                reverse=True,
            )

            for failed_launch in sorted_failed_launches:
                node_type = failed_launch.ray_node_type_name
                category = "LaunchFailed"
                description = failed_launch.details
                attempted_time = datetime.fromtimestamp(failed_launch.request_ts_s)
                formatted_time = f"{attempted_time.hour:02d}:{attempted_time.minute:02d}:{attempted_time.second:02d}"

                line = f" {node_type}: {category} (latest_attempt: {formatted_time})"
                if verbose:
                    line += f" - {description}"

                failure_lines.append(line)

        # Process failed nodes
        for node in data.failed_nodes:
            failure_lines.append(
                f" {node.ray_node_type_name}: NodeTerminated (instance_id: {node.instance_id})"
            )

        # Limit the number of failures displayed
        failure_lines = failure_lines[: constants.AUTOSCALER_MAX_FAILURES_DISPLAYED]

        # Build the failure report
        failure_report = "Recent failures:\n"
        failure_report += (
            "\n".join(failure_lines) if failure_lines else " (no failures)"
        )

        return failure_report

    @staticmethod
    def _pending_node_report(data: ClusterStatus) -> str:
        # Prepare pending launch lines
        pending_lines = [
            f" {node_type}, {count} launching"
            for node_type, count in _count_by(
                data.pending_launches, "ray_node_type_name"
            ).items()
        ]

        # Prepare pending node lines
        pending_lines.extend(
            f" {ip}: {node_type}, {status.lower()}"
            for ip, node_type, status in (
                (node.instance_id, node.ray_node_type_name, node.details)
                for node in data.pending_nodes
            )
        )

        # Construct the pending report
        if pending_lines:
            return "\n".join(pending_lines)
        return " (no pending nodes)"

    @staticmethod
    def _constraint_report(
        cluster_constraint_demand: List[ClusterConstraintDemand],
    ) -> str:
        """Returns a formatted string describing the resource constraints from request_resources().

        Args:
            data: ClusterStatus object containing resource demand information.

        Returns:
            String containing the formatted constraints report, either listing each constraint
            and count or indicating no constraints exist.

        Example:
            >>> cluster_constraint_demand = [
            ...     ClusterConstraintDemand(bundles_by_count=[
            ...         ResourceRequestByCount(bundle={"CPU": 4}, count=2),
            ...         ResourceRequestByCount(bundle={"GPU": 1}, count=1)
            ...     ])
            ... ]
            >>> ClusterStatusFormatter._constraint_report(cluster_constraint_demand)
            " {'CPU': 4}: 2 from request_resources()\\n {'GPU': 1}: 1 from request_resources()"
        """
        constraint_lines = []
        request_demand = [
            (bc.bundle, bc.count)
            for constraint_demand in cluster_constraint_demand
            for bc in constraint_demand.bundles_by_count
        ]
        for bundle, count in request_demand:
            constraint_lines.append(f" {bundle}: {count} from request_resources()")
        if constraint_lines:
            return "\n".join(constraint_lines)
        return " (no request_resources() constraints)"

    @staticmethod
    def _demand_report(data: ClusterStatus) -> str:
        # Process resource demands
        resource_demands = [
            (bundle.bundle, bundle.count)
            for demand in data.resource_demands.ray_task_actor_demand
            for bundle in demand.bundles_by_count
        ]
        demand_lines = []
        if resource_demands:
            demand_lines.extend(format_resource_demand_summary(resource_demands))

        # Process placement group demands
        pg_demand_strs = [
            f"{pg_demand.strategy}|{pg_demand.state}"
            for pg_demand in data.resource_demands.placement_group_demand
        ]
        pg_demand_str_to_demand = {
            f"{pg_demand.strategy}|{pg_demand.state}": pg_demand
            for pg_demand in data.resource_demands.placement_group_demand
        }
        pg_freqs = Counter(pg_demand_strs)

        pg_demand = [
            (
                {
                    "strategy": pg_demand_str_to_demand[pg_str].strategy,
                    "bundles": [
                        (bundle.bundle, bundle.count)
                        for bundle in pg_demand_str_to_demand[pg_str].bundles_by_count
                    ],
                },
                freq,
            )
            for pg_str, freq in pg_freqs.items()
        ]

        for pg, count in pg_demand:
            pg_str = format_pg(pg)
            demand_lines.append(f" {pg_str}: {count}+ pending placement groups")

        # Generate demand report
        if demand_lines:
            return "\n".join(demand_lines)
        return " (no resource demands)"

    @staticmethod
    def _cluster_usage_report(data: ClusterStatus, verbose: bool) -> str:
        # Build usage dictionary
        usage = {
            u.resource_name: (u.used, u.total) for u in data.cluster_resource_usage
        }

        # Parse usage lines
        usage_lines = parse_usage(usage, verbose)

        # Generate usage report
        usage_report = [f" {line}" for line in usage_lines] + [""]

        return "\n".join(usage_report)


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
                labels=dict(node_state.labels),
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
