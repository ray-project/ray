import copy
import numpy as np
import logging
import collections
from typing import List, Dict, Tuple

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE

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

    def debug_string(self, nodes: List[NodeID],
                     pending_nodes: Dict[NodeID, int]) -> str:
        node_resources, node_type_counts = self.calculate_node_resources(
            nodes, pending_nodes)

        out = "Worker instance types:"
        for node_type, count in node_type_counts.items():
            out += "\n - {}: {}".format(node_type, count)
            if pending_nodes.get(node_type):
                out += " ({} pending)".format(pending_nodes[node_type])

        return out

    def calculate_node_resources(
            self, nodes: List[NodeID], pending_nodes: Dict[NodeID, int]
    ) -> (List[ResourceDict], Dict[NodeType, int]):
        """Returns node resource list and instance type counts."""

        node_resources = []
        node_type_counts = collections.defaultdict(int)

        def add_instance(node_type):
            if node_type not in self.node_types:
                raise RuntimeError("Missing entry for node_type {} in "
                                   "available_node_types config: {}".format(
                                       node_type, self.node_types))
            # Careful not to include the same dict object multiple times.
            node_resources.append(
                copy.deepcopy(self.node_types[node_type]["resources"]))
            node_type_counts[node_type] += 1

        for node_id in nodes:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                add_instance(node_type)

        for node_type, count in pending_nodes.items():
            for _ in range(count):
                add_instance(node_type)

        return node_resources, node_type_counts

    def get_instances_to_launch(self, nodes: List[NodeID],
                                pending_nodes: Dict[NodeType, int],
                                resource_demands: List[ResourceDict]
                                ) -> List[Tuple[NodeType, int]]:
        """Get a list of instance types that should be added to the cluster.

        This method:
            (1) calculates the resources present in the cluster.
            (2) calculates the unfulfilled resource bundles.
            (3) calculates which instances need to be launched to fulfill all
                the bundle requests, subject to max_worker constraints.
        """

        if resource_demands is None:
            logger.info("No resource demands")
            return []

        node_resources, node_type_counts = self.calculate_node_resources(
            nodes, pending_nodes)
        logger.info("Cluster resources: {}".format(node_resources))
        logger.info("Instance counts: {}".format(node_type_counts))

        unfulfilled = get_bin_pack_residual(node_resources, resource_demands)
        logger.info("Resource demands: {}".format(resource_demands))
        logger.info("Unfulfilled demands: {}".format(unfulfilled))

        instances = get_instances_for(self.node_types, node_type_counts,
                                      self.max_workers - len(nodes),
                                      unfulfilled)
        logger.info("Instance requests: {}".format(instances))
        return instances


def get_instances_for(
        node_types: Dict[NodeType, NodeTypeConfigDict],
        existing_instances: Dict[NodeType, int], max_to_add: int,
        resources: List[ResourceDict]) -> List[Tuple[NodeType, int]]:
    """Determine instances to add given resource demands and constraints.

    Args:
        node_types: instance types config.
        existing_instances: counts of existing instances already launched.
            This sets constraints on the number of new instances to add.
        max_to_add: global constraint on instances to add.
        resources: resource demands to fulfill.

    Returns:
        List of instances types and count to add.
    """
    instances_to_add = collections.defaultdict(int)
    allocated_resources = []

    while resources and sum(instances_to_add.values()) < max_to_add:
        utilization_scores = []
        for node_type in node_types:
            if (existing_instances.get(node_type, 0) + instances_to_add.get(
                    node_type, 0) >= node_types[node_type]["max_workers"]):
                continue
            node_resources = node_types[node_type]["resources"]
            score = _utilization_score(node_resources, resources)
            if score is not None:
                utilization_scores.append((score, node_type))

        # Give up, no feasible node.
        if not utilization_scores:
            logger.info("No feasible instance to add for {}".format(resources))
            break

        utilization_scores = sorted(utilization_scores, reverse=True)
        best_node_type = utilization_scores[0][1]
        instances_to_add[best_node_type] += 1
        allocated_resources.append(node_types[best_node_type]["resources"])
        residual = get_bin_pack_residual(allocated_resources[-1:], resources)
        assert len(residual) < len(resources), (resources, residual)
        resources = residual

    return list(instances_to_add.items())


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
