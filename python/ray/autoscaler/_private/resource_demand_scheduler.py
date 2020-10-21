"""Implements multi-node-type autoscaling.

This file implements an autoscaling algorithm that is aware of multiple node
types (e.g., example-multi-node-type.yaml). The Ray autoscaler will pass in
a vector of resource shape demands, and the resource demand scheduler will
return a list of node types that can satisfy the demands given constraints
(i.e., reverse bin packing).
"""

import copy
import numpy as np
import logging
import collections
from numbers import Number
from typing import List, Dict

from ray.autoscaler.node_provider import NodeProvider
from ray.gcs_utils import PlacementGroupTableData
from ray.core.generated.common_pb2 import PlacementStrategy
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE, NODE_KIND_UNMANAGED, \
    STATUS_UPDATE_FAILED, STATUS_UP_TO_DATE, TAG_RAY_NODE_STATUS

logger = logging.getLogger(__name__)

# e.g., m4.16xlarge.
NodeType = str

# e.g., {"resources": ..., "max_workers": ...}.
NodeTypeConfigDict = str

# e.g., {"GPU": 1}.
ResourceDict = Dict[str, Number]

# e.g., IP address of the node.
NodeID = str


class ResourceDemandScheduler:
    def __init__(self, provider: NodeProvider,
                 node_types: Dict[NodeType, NodeTypeConfigDict],
                 max_workers: int):
        self.provider = provider
        self.node_types = node_types
        self.max_workers = max_workers

    def get_nodes_to_launch(
            self, nodes: List[NodeID], pending_nodes: Dict[NodeType, int],
            resource_demands: List[ResourceDict],
            usage_by_ip: Dict[str, ResourceDict],
            pending_placement_groups: List[PlacementGroupTableData]
    ) -> Dict[NodeType, int]:
        """Given resource demands, return node types to add to the cluster.

        This method:
            (1) calculates the resources present in the cluster.
            (2) calculates the remaining nodes to add to respect min_workers
                constraint per node type.
            (3) for each strict spread placement group, reserve space on
                available nodes and launch new nodes if necessary.
            (4) calculates the unfulfilled resource bundles.
            (5) calculates which nodes need to be launched to fulfill all
                the bundle requests, subject to max_worker constraints.

        Args:
            nodes: List of existing nodes in the cluster.
            pending_nodes: Summary of node types currently being launched.
            resource_demands: Vector of resource demands from the scheduler.
            usage_by_ip: Mapping from ip to available resources.
        """

        node_resources: List[ResourceDict]
        node_type_counts: Dict[NodeType, int]
        node_resources, node_type_counts = \
            self.calculate_node_resources(nodes, pending_nodes, usage_by_ip)

        logger.info("Cluster resources: {}".format(node_resources))
        logger.info("Node counts: {}".format(node_type_counts))
        # Step 2: add nodes to add to satisfy min_workers for each type
        node_resources, node_type_counts, min_workers_nodes_to_add = \
            _add_min_workers_nodes(
                node_resources, node_type_counts, self.node_types)

        # Step 3: add nodes for strict spread groups
        logger.info(f"Placement group demands: {pending_placement_groups}")
        placement_group_demand_vector, strict_spreads = \
            placement_groups_to_resource_demands(pending_placement_groups)
        resource_demands.extend(placement_group_demand_vector)
        placement_group_nodes_to_add, node_resources, node_type_counts = \
            self.reserve_and_allocate_spread(
                strict_spreads, node_resources, node_type_counts)

        # Step 4/5: add nodes for pending tasks, actors, and non-strict spread
        # groups
        unfulfilled, _ = get_bin_pack_residual(node_resources,
                                               resource_demands)
        logger.info("Resource demands: {}".format(resource_demands))
        logger.info("Unfulfilled demands: {}".format(unfulfilled))
        max_to_add = self.max_workers - sum(node_type_counts.values())
        nodes_to_add_based_on_demand = get_nodes_for(
            self.node_types, node_type_counts, max_to_add, unfulfilled)
        # Merge nodes to add based on demand and nodes to add based on
        # min_workers constraint. We add them because nodes to add based on
        # demand was calculated after the min_workers constraint was respected.
        total_nodes_to_add = {}
        for node_type in self.node_types:
            nodes_to_add = (min_workers_nodes_to_add.get(
                node_type, 0) + placement_group_nodes_to_add.get(node_type, 0)
                            + nodes_to_add_based_on_demand.get(node_type, 0))
            if nodes_to_add > 0:
                total_nodes_to_add[node_type] = nodes_to_add

        # Limit the number of concurrent launches
        total_nodes_to_add = self._get_concurrent_resource_demand_to_launch(
            total_nodes_to_add, nodes, pending_nodes)

        logger.info("Node requests: {}".format(total_nodes_to_add))
        return total_nodes_to_add

    def _get_concurrent_resource_demand_to_launch(
            self, to_launch: Dict[NodeType, int],
            non_terminated_nodes: List[NodeID],
            pending_launches_nodes: Dict[NodeType, int]
    ) -> Dict[NodeType, int]:
        """Updates the max concurrent resources to launch for each node type.

        Given the current nodes that should be launched, the non terminated
        nodes (running and pending) and the pending to be launched nodes. This
        method calculates the maximum number of nodes to launch concurrently
        for each node type as follows:
            1) Calculates the running nodes.
            2) Calculates the pending nodes and gets the launching nodes.
            3) Limits the total number of pending + currently-launching +
               to-be-launched nodes to max(5, frac * running_nodes[node_type]).

        Args:
            to_launch: Number of nodes to launch based on resource demand.
            non_terminated_nodes: Non terminated nodes (pending/running).
            pending_launches_nodes: Nodes that are in the launch queue.
        Returns:
            Dict[NodeType, int]: Maximum number of nodes to launch for each
                node type.
        """
        # TODO(ameer): Consider making frac configurable.
        frac = 1
        updated_nodes_to_launch = {}
        running_nodes, pending_nodes = \
            self._separate_running_and_pending_nodes(
                non_terminated_nodes
            )
        for node_type in to_launch:
            # Enforce here max allowed pending nodes to be frac of total
            # running nodes.
            max_allowed_pending_nodes = max(
                5, int(frac * running_nodes[node_type]))
            total_pending_nodes = pending_launches_nodes.get(
                node_type, 0) + pending_nodes[node_type]

            # Allow more nodes if this is to respect min_workers constraint.
            nodes_to_add = max(
                max_allowed_pending_nodes - total_pending_nodes,
                self.node_types[node_type].get("min_workers", 0) -
                total_pending_nodes - running_nodes[node_type])

            if nodes_to_add > 0:
                updated_nodes_to_launch[node_type] = min(
                    nodes_to_add, to_launch[node_type])

        return updated_nodes_to_launch

    def _separate_running_and_pending_nodes(
            self,
            non_terminated_nodes: List[NodeID],
    ) -> (Dict[NodeType, int], Dict[NodeType, int]):
        """Receives non terminated nodes & splits them to pending & running."""

        running_nodes = collections.defaultdict(int)
        pending_nodes = collections.defaultdict(int)
        for node_id in non_terminated_nodes:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                status = tags.get(TAG_RAY_NODE_STATUS)
                if status == STATUS_UP_TO_DATE:
                    running_nodes[node_type] += 1
                elif status != STATUS_UPDATE_FAILED:
                    pending_nodes[node_type] += 1
        return running_nodes, pending_nodes

    def calculate_node_resources(
            self, nodes: List[NodeID], pending_nodes: Dict[NodeID, int],
            usage_by_ip: Dict[str, ResourceDict]
    ) -> (List[ResourceDict], Dict[NodeType, int]):
        """Returns node resource list and node type counts.

           Counts the running nodes, pending nodes.
           Args:
                nodes: Existing nodes.
                pending_nodes: Pending nodes.
           Returns:
                node_resources: a list of running + pending resources.
                    E.g., [{"CPU": 4}, {"GPU": 2}].
                node_type_counts: running + pending workers per node type.
        """

        node_resources = []
        node_type_counts = collections.defaultdict(int)

        def add_node(node_type, available_resources=None):
            if node_type not in self.node_types:
                logger.warn(
                    f"Missing entry for node_type {node_type} in "
                    f"cluster config: {self.node_types} under entry "
                    f"available_node_types. This node's resources will be "
                    f"ignored. If you are using an unmanaged node, manually "
                    f"set the user_node_type tag to \"{NODE_KIND_UNMANAGED}\""
                    f"in your cloud provider's management console.")
                return None
            # Careful not to include the same dict object multiple times.
            available = copy.deepcopy(self.node_types[node_type]["resources"])
            # If available_resources is None this might be because the node is
            # no longer pending, but the raylet hasn't sent a heartbeat to gcs
            # yet.
            if available_resources is not None:
                available = copy.deepcopy(available_resources)

            node_resources.append(available)
            node_type_counts[node_type] += 1

        for node_id in nodes:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                ip = self.provider.internal_ip(node_id)
                available_resources = usage_by_ip.get(ip)
                add_node(node_type, available_resources)

        for node_type, count in pending_nodes.items():
            for _ in range(count):
                add_node(node_type)

        return node_resources, node_type_counts

    def reserve_and_allocate_spread(self,
                                    strict_spreads: List[List[ResourceDict]],
                                    node_resources: List[ResourceDict],
                                    node_type_counts: Dict[NodeType, int]):
        """For each strict spread, attempt to reserve as much space as possible
        on the node, then allocate new nodes for the unfulfilled portion.

        Args:
            strict_spreads (List[List[ResourceDict]]): A list of placement
                groups which must be spread out.
            node_resources (List[ResourceDict]): Available node resources in
                the cluster.
            node_type_counts (Dict[NodeType, int]): The amount of each type of
                node pending or in the cluster.

        Returns:
            Dict[NodeType, int]: Nodes to add.
            List[ResourceDict]: The updated node_resources after the method.
            Dict[NodeType, int]: The updated node_type_counts.

        """
        to_add = collections.defaultdict(int)
        for bundles in strict_spreads:
            # Try to pack as many bundles of this group as possible on existing
            # nodes. The remaining will be allocated on new nodes.
            unfulfilled, node_resources = get_bin_pack_residual(
                node_resources, bundles, strict_spread=True)
            max_to_add = self.max_workers - sum(node_type_counts.values())
            # Allocate new nodes for the remaining bundles that don't fit.
            to_launch = get_nodes_for(
                self.node_types,
                node_type_counts,
                max_to_add,
                unfulfilled,
                strict_spread=True)
            _inplace_add(node_type_counts, to_launch)
            _inplace_add(to_add, to_launch)
            new_node_resources = _node_type_counts_to_node_resources(
                self.node_types, to_launch)
            # Update node resources to include newly launched nodes and their
            # bundles.
            unfulfilled, including_reserved = get_bin_pack_residual(
                new_node_resources, unfulfilled, strict_spread=True)
            assert not unfulfilled
            node_resources += including_reserved
        return to_add, node_resources, node_type_counts

    def debug_string(self, nodes: List[NodeID],
                     pending_nodes: Dict[NodeID, int],
                     usage_by_ip: Dict[str, ResourceDict]) -> str:
        node_resources, node_type_counts = self.calculate_node_resources(
            nodes, pending_nodes, usage_by_ip)

        out = "Worker node types:"
        for node_type, count in node_type_counts.items():
            out += "\n - {}: {}".format(node_type, count)
            if pending_nodes.get(node_type):
                out += " ({} pending)".format(pending_nodes[node_type])

        return out


def _node_type_counts_to_node_resources(
        node_types: Dict[NodeType, NodeTypeConfigDict],
        node_type_counts: Dict[NodeType, int]) -> List[ResourceDict]:
    """Converts a node_type_counts dict into a list of node_resources."""
    resources = []
    for node_type, count in node_type_counts.items():
        # Be careful, each entry in the list must be deep copied!
        resources += [
            node_types[node_type]["resources"].copy() for _ in range(count)
        ]
    return resources


def _add_min_workers_nodes(
        node_resources: List[ResourceDict],
        node_type_counts: Dict[NodeType, int],
        node_types: Dict[NodeType, NodeTypeConfigDict],
) -> (List[ResourceDict], Dict[NodeType, int], Dict[NodeType, int]):
    """Updates resource demands to respect the min_workers constraint.

    Args:
        node_resources: Resources of exisiting nodes already launched/pending.
        node_type_counts: Counts of existing nodes already launched/pending.
        node_types: Node types config.

    Returns:
        node_resources: The updated node resources after adding min_workers
            constraint per node type.
        node_type_counts: The updated node counts after adding min_workers
            constraint per node type.
        total_nodes_to_add: The nodes to add to respect min_workers constraint.
    """
    total_nodes_to_add_dict = {}
    for node_type, config in node_types.items():
        existing = node_type_counts.get(node_type, 0)
        target = config.get("min_workers", 0)
        if existing < target:
            total_nodes_to_add_dict[node_type] = target - existing
            node_type_counts[node_type] = target
            available = copy.deepcopy(node_types[node_type]["resources"])
            node_resources.extend(
                [available] * total_nodes_to_add_dict[node_type])

    return node_resources, node_type_counts, total_nodes_to_add_dict


def get_nodes_for(node_types: Dict[NodeType, NodeTypeConfigDict],
                  existing_nodes: Dict[NodeType, int],
                  max_to_add: int,
                  resources: List[ResourceDict],
                  strict_spread: bool = False) -> Dict[NodeType, int]:
    """Determine nodes to add given resource demands and constraints.

    Args:
        node_types: node types config.
        existing_nodes: counts of existing nodes already launched.
            This sets constraints on the number of new nodes to add.
        max_to_add: global constraint on nodes to add.
        resources: resource demands to fulfill.
        strict_spread: If true, each element in `resources` must be placed on a
            different node.

    Returns:
        Dict of count to add for each node type.

    """
    nodes_to_add = collections.defaultdict(int)

    while resources and sum(nodes_to_add.values()) < max_to_add:
        utilization_scores = []
        for node_type in node_types:
            if (existing_nodes.get(node_type, 0) + nodes_to_add.get(
                    node_type, 0) >= node_types[node_type]["max_workers"]):
                continue
            node_resources = node_types[node_type]["resources"]
            if strict_spread:
                # If handling strict spread, only one bundle can be placed on
                # the node.
                score = _utilization_score(node_resources, [resources[0]])
            else:
                score = _utilization_score(node_resources, resources)
            if score is not None:
                utilization_scores.append((score, node_type))

        # Give up, no feasible node.
        if not utilization_scores:
            # TODO (Alex): We will hit this case every time a placement group
            # starts up because placement groups are scheduled via custom
            # resources. This will behave properly with the current utilization
            # score heuristic, but it's a little dangerous and misleading.
            logger.info(
                "No feasible node type to add for {}".format(resources))
            break

        utilization_scores = sorted(utilization_scores, reverse=True)
        best_node_type = utilization_scores[0][1]
        nodes_to_add[best_node_type] += 1
        if strict_spread:
            resources = resources[1:]
        else:
            allocated_resource = node_types[best_node_type]["resources"]
            residual, _ = get_bin_pack_residual([allocated_resource],
                                                resources)
            assert len(residual) < len(resources), (resources, residual)
            resources = residual

    return nodes_to_add


def _utilization_score(node_resources: ResourceDict,
                       resources: ResourceDict) -> float:
    remaining = copy.deepcopy(node_resources)

    fittable = []
    for r in resources:
        if _fits(remaining, r):
            fittable.append(r)
            _inplace_subtract(remaining, r)
    if not fittable:
        return None

    util_by_resources = []
    for k, v in node_resources.items():
        util = (v - remaining[k]) / v
        util_by_resources.append(v * (util**3))

    # Prioritize using all resources first, then prioritize overall balance
    # of multiple resources.
    return (min(util_by_resources), np.mean(util_by_resources))


def get_bin_pack_residual(node_resources: List[ResourceDict],
                          resource_demands: List[ResourceDict],
                          strict_spread: bool = False) -> List[ResourceDict]:
    """Return a subset of resource_demands that cannot fit in the cluster.

    TODO(ekl): this currently does not guarantee the resources will be packed
    correctly by the Ray scheduler. This is only possible once the Ray backend
    supports a placement groups API.

    Args:
        node_resources (List[ResourceDict]): List of resources per node.
        resource_demands (List[ResourceDict]): List of resource bundles that
            need to be bin packed onto the nodes.
        strict_spread (bool): If true, each element in resource_demands must be
            placed on a different entry in `node_resources`.

    Returns:
        List[ResourceDict] the residual list resources that do not fit.

    """

    unfulfilled = []

    # A most naive bin packing algorithm.
    nodes = copy.deepcopy(node_resources)
    used = []
    for demand in resource_demands:
        found = False
        node = None
        for i in range(len(nodes)):
            node = nodes[i]
            if _fits(node, demand):
                found = True
                # In the strict_spread case, we can't reuse nodes.
                if strict_spread:
                    used.append(node)
                    del nodes[i]
                break
        if found and node:
            _inplace_subtract(node, demand)
        else:
            unfulfilled.append(demand)

    return unfulfilled, nodes + used


def _fits(node: ResourceDict, resources: ResourceDict) -> bool:
    for k, v in resources.items():
        if v > node.get(k, 0.0):
            return False
    return True


def _inplace_subtract(node: ResourceDict, resources: ResourceDict) -> None:
    for k, v in resources.items():
        assert k in node, (k, node)
        node[k] -= v
        assert node[k] >= 0.0, (node, k, v)


def _inplace_add(a: collections.defaultdict, b: Dict) -> None:
    """Generically adds values in `b` to `a`.
    a[k] should be defined for all k in b.keys()"""
    for k, v in b.items():
        a[k] += v


def placement_groups_to_resource_demands(
        pending_placement_groups: List[PlacementGroupTableData]):
    """Preprocess placement group requests into regular resource demand vectors
    when possible. The policy is:
        * STRICT_PACK - Convert to a single bundle.
        * PACK - Flatten into a resource demand vector.
        * STRICT_SPREAD - Cannot be converted.
        * SPREAD - Flatten into a resource demand vector.

    Args:
        pending_placement_groups (List[PlacementGroupData]): List of
        PlacementGroupLoad's.

    Returns:
        List[ResourceDict]: The placement groups which were converted to a
            resource demand vector.
        List[List[ResourceDict]]: The placement groups which should be strictly
            spread.
    """
    resource_demand_vector = []
    unconverted = []
    for placement_group in pending_placement_groups:
        shapes = [
            dict(bundle.unit_resources) for bundle in placement_group.bundles
        ]
        if (placement_group.strategy == PlacementStrategy.PACK
                or placement_group.strategy == PlacementStrategy.SPREAD):
            resource_demand_vector.extend(shapes)
        elif placement_group.strategy == PlacementStrategy.STRICT_PACK:
            combined = collections.defaultdict(float)
            for shape in shapes:
                for label, quantity in shape.items():
                    combined[label] += quantity
            resource_demand_vector.append(combined)
        elif (placement_group.strategy == PlacementStrategy.STRICT_SPREAD):
            unconverted.append(shapes)
        else:
            logger.error(
                f"Unknown placement group request type: {placement_group}. "
                f"Please file a bug report "
                f"https://github.com/ray-project/ray/issues/new.")
    return resource_demand_vector, unconverted
