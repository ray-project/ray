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
from typing import List, Dict

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE, NODE_KIND_UNMANAGED

logger = logging.getLogger(__name__)

# e.g., m4.16xlarge.
NodeType = str

# e.g., {"resources": ..., "max_workers": ...}.
NodeTypeConfigDict = str

# e.g., {"GPU": 1}.
ResourceDict = str

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
            usage_by_ip: Dict[str, ResourceDict]) -> Dict[NodeType, int]:
        """Given resource demands, return node types to add to the cluster.

        This method:
            (1) calculates the resources present in the cluster.
            (2) calculates the remaining nodes to add to respect min_workers
                    constraint per node type.
            (3) calculates the unfulfilled resource bundles.
            (4) calculates which nodes need to be launched to fulfill all
                the bundle requests, subject to max_worker constraints.

        Args:
            nodes: List of existing nodes in the cluster.
            pending_nodes: Summary of node types currently being launched.
            resource_demands: Vector of resource demands from the scheduler.
            usage_by_ip: Mapping from ip to available resources.
        """

        node_resources, node_type_counts = \
            self.calculate_node_resources(nodes, pending_nodes, usage_by_ip)

        node_resources, node_type_counts, min_workers_nodes_to_add = \
            _add_min_workers_nodes(
                node_resources, node_type_counts, self.node_types)

        nodes_to_add_based_on_demand = {}
        if resource_demands is not None:
            logger.info("Cluster resources: {}".format(node_resources))
            logger.info("Node counts: {}".format(node_type_counts))
            unfulfilled = get_bin_pack_residual(node_resources,
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
            nodes_to_add = nodes_to_add_based_on_demand.get(node_type, 0) + \
                min_workers_nodes_to_add.get(node_type, 0)
            if nodes_to_add > 0:
                total_nodes_to_add[node_type] = nodes_to_add

        logger.info("Node requests: {}".format(total_nodes_to_add))
        return total_nodes_to_add

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
                  existing_nodes: Dict[NodeType, int], max_to_add: int,
                  resources: List[ResourceDict]) -> Dict[NodeType, int]:
    """Determine nodes to add given resource demands and constraints.

    Args:
        node_types: node types config.
        existing_nodes: counts of existing nodes already launched.
            This sets constraints on the number of new nodes to add.
        max_to_add: global constraint on nodes to add.
        resources: resource demands to fulfill.

    Returns:
        Dict of count to add for each node type.
    """
    nodes_to_add = collections.defaultdict(int)
    allocated_resources = []

    while resources and sum(nodes_to_add.values()) < max_to_add:
        utilization_scores = []
        for node_type in node_types:
            if (existing_nodes.get(node_type, 0) + nodes_to_add.get(
                    node_type, 0) >= node_types[node_type]["max_workers"]):
                continue
            node_resources = node_types[node_type]["resources"]
            score = _utilization_score(node_resources, resources)
            if score is not None:
                utilization_scores.append((score, node_type))

        # Give up, no feasible node.
        if not utilization_scores:
            logger.info(
                "No feasible node type to add for {}".format(resources))
            break

        utilization_scores = sorted(utilization_scores, reverse=True)
        best_node_type = utilization_scores[0][1]
        nodes_to_add[best_node_type] += 1
        allocated_resources.append(node_types[best_node_type]["resources"])
        residual = get_bin_pack_residual(allocated_resources[-1:], resources)
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


def get_bin_pack_residual(
        node_resources: List[ResourceDict],
        resource_demands: List[ResourceDict]) -> List[ResourceDict]:
    """Return a subset of resource_demands that cannot fit in the cluster.

    TODO(ekl): this currently does not guarantee the resources will be packed
    correctly by the Ray scheduler. This is only possible once the Ray backend
    supports a placement groups API.

    Args:
        node_resources (List[ResourceDict]): List of resources per node.
        resource_demands (List[ResourceDict]): List of resource bundles that
            need to be bin packed onto the nodes.

    Returns:
        List[ResourceDict] the residual list resources that do not fit.
    """

    unfulfilled = []

    # A most naive bin packing algorithm.
    nodes = copy.deepcopy(node_resources)
    for demand in resource_demands:
        found = False
        for node in nodes:
            if _fits(node, demand):
                _inplace_subtract(node, demand)
                found = True
                break
        if not found:
            unfulfilled.append(demand)

    return unfulfilled


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
