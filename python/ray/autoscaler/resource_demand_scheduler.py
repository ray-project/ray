import logging
from typing import List

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_INSTANCE_TYPE

logger = logging.getLogger(__name__)


class ResourceDemandScheduler:
    def __init__(self, provider: NodeProvider, instance_types: dict, max_workers: int):
        self.provider = provider
        self.instance_types = instance_types
        self.max_workers = max_workers

    def get_instances_to_launch(
            self, nodes: List[str], resource_demands: List[dict]):
        """Get a list of instance types that should be added to the cluster."""

        if resource_demands is None:
            logger.info("No resource demands")
            return []

        node_resources = []
        for node_id in nodes:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_INSTANCE_TYPE in tags:
                instance_type = tags[TAG_RAY_INSTANCE_TYPE]
                if instance_type not in self.instance_types:
                    raise RuntimeError(
                        "Missing entry for instance_type {} in "
                        "available_instance_types config: {}".format(
                            instance_type, self.instance_types))
                node_resources.append(self.instance_types[instance_type])
        logger.info("Node resources: {}".format(node_resources))

        unfulfilled = get_bin_pack_residual(node_resources, resource_demands)
        logger.info("Unfulfilled resources: {}".format(node_resources))

        instances = self._instances_for(unfulfilled)
        logger.info("Instance requests: {}".format(node_resources))
        return instances

    def _instances_for(self, resources: List[dict]):
        pass


def get_bin_pack_residual(
        node_resources: List[dict], resource_demands: List[dict]):
    """Return a subset of resource_demands that cannot fit in the cluster.

    TODO(ekl): this currently does not guarantee the resources will be packed
    correctly by the Ray scheduler. This is only possible once the Ray backend
    supports a placement groups API.

    Args:
        node_resources (List[dict]): List of resources per node.
        resource_demands (List[dict]): List of resource bundles that need to
            be bin packed onto the nodes.

    Returns:
        List[dict] the residual list resources that do not fit.
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


def _fits(node, resources):
    for k, v in resources.items():
        if v > node.get(k, 0.0):
            return False
    return True


def _inplace_subtract(node, resources):
    for k, v in resources.items():
        assert k in node, (k, node)
        node[k] -= v
        assert node[k] >= 0.0, (node, k, v)
