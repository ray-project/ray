"""Implements multi-node-type autoscaling.

This file implements an autoscaling algorithm that is aware of multiple node
types (e.g., example-multi-node-type.yaml). The Ray autoscaler will pass in
a vector of resource shape demands, and the resource demand scheduler will
return a list of node types that can satisfy the demands given constraints
(i.e., reverse bin packing).
"""

import collections
import copy
import logging
import os
from abc import abstractmethod
from functools import partial
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from ray._private.gcs_utils import PlacementGroupTableData
from ray.autoscaler._private.constants import (
    AUTOSCALER_CONSERVE_GPU_NODES,
    AUTOSCALER_UTILIZATION_SCORER_KEY,
)
from ray.autoscaler._private.loader import load_function_or_class
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilitySummary,
)
from ray.autoscaler._private.util import (
    NodeID,
    NodeIP,
    NodeType,
    NodeTypeConfigDict,
    ResourceDict,
    is_placement_group_resource,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_UNMANAGED,
    NODE_KIND_WORKER,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.core.generated.common_pb2 import PlacementStrategy

logger = logging.getLogger(__name__)

# The minimum number of nodes to launch concurrently.
UPSCALING_INITIAL_NUM_NODES = 5

NodeResources = ResourceDict
ResourceDemands = List[ResourceDict]


class UtilizationScore:
    """This fancy class just defines the `UtilizationScore` protocol to be
    some type that is a "totally ordered set" (i.e. things that can be sorted).

    What we're really trying to express is

    ```
    UtilizationScore = TypeVar("UtilizationScore", bound=Comparable["UtilizationScore"])
    ```

    but Comparable isn't a real type and, and a bound with a type argument
    can't be enforced (f-bounded polymorphism with contravariance). See Guido's
    comment for more details: https://github.com/python/typing/issues/59.

    This isn't just a `float`. In the case of the default scorer, it's a
    `Tuple[float, float]` which is quite difficult to map to a single number.

    """

    @abstractmethod
    def __eq__(self, other: "UtilizationScore") -> bool:
        pass

    @abstractmethod
    def __lt__(self: "UtilizationScore", other: "UtilizationScore") -> bool:
        pass

    def __gt__(self: "UtilizationScore", other: "UtilizationScore") -> bool:
        return (not self < other) and self != other

    def __le__(self: "UtilizationScore", other: "UtilizationScore") -> bool:
        return self < other or self == other

    def __ge__(self: "UtilizationScore", other: "UtilizationScore") -> bool:
        return not self < other


class UtilizationScorer:
    def __call__(
        node_resources: NodeResources,
        resource_demands: ResourceDemands,
        *,
        node_availability_summary: NodeAvailabilitySummary,
    ) -> Optional[UtilizationScore]:
        pass


class ResourceDemandScheduler:
    def __init__(
        self,
        provider: NodeProvider,
        node_types: Dict[NodeType, NodeTypeConfigDict],
        max_workers: int,
        head_node_type: NodeType,
        upscaling_speed: float,
    ) -> None:
        self.provider = provider
        self.node_types = copy.deepcopy(node_types)
        self.node_resource_updated = set()
        self.max_workers = max_workers
        self.head_node_type = head_node_type
        self.upscaling_speed = upscaling_speed

        utilization_scorer_str = os.environ.get(
            AUTOSCALER_UTILIZATION_SCORER_KEY,
            "ray.autoscaler._private.resource_demand_scheduler"
            "._default_utilization_scorer",
        )
        self.utilization_scorer: UtilizationScorer = load_function_or_class(
            utilization_scorer_str
        )

    def _get_head_and_workers(self, nodes: List[NodeID]) -> Tuple[NodeID, List[NodeID]]:
        """Returns the head node's id and the list of all worker node ids,
        given a list `nodes` of all node ids in the cluster.
        """
        head_id, worker_ids = None, []
        for node in nodes:
            tags = self.provider.node_tags(node)
            if tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD:
                head_id = node
            elif tags[TAG_RAY_NODE_KIND] == NODE_KIND_WORKER:
                worker_ids.append(node)
        return head_id, worker_ids

    def reset_config(
        self,
        provider: NodeProvider,
        node_types: Dict[NodeType, NodeTypeConfigDict],
        max_workers: int,
        head_node_type: NodeType,
        upscaling_speed: float = 1,
    ) -> None:
        """Updates the class state variables.

        For legacy yamls, it merges previous state and new state to make sure
        inferered resources are not lost.
        """
        self.provider = provider
        self.node_types = copy.deepcopy(node_types)
        self.node_resource_updated = set()
        self.max_workers = max_workers
        self.head_node_type = head_node_type
        self.upscaling_speed = upscaling_speed

    def is_feasible(self, bundle: ResourceDict) -> bool:
        for node_type, config in self.node_types.items():
            max_of_type = config.get("max_workers", 0)
            node_resources = config["resources"]
            if (node_type == self.head_node_type or max_of_type > 0) and _fits(
                node_resources, bundle
            ):
                return True
        return False

    def get_nodes_to_launch(
        self,
        nodes: List[NodeID],
        launching_nodes: Dict[NodeType, int],
        resource_demands: List[ResourceDict],
        unused_resources_by_ip: Dict[NodeIP, ResourceDict],
        pending_placement_groups: List[PlacementGroupTableData],
        max_resources_by_ip: Dict[NodeIP, ResourceDict],
        ensure_min_cluster_size: List[ResourceDict],
        node_availability_summary: NodeAvailabilitySummary,
    ) -> (Dict[NodeType, int], List[ResourceDict]):
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
            launching_nodes: Summary of node types currently being launched.
            resource_demands: Vector of resource demands from the scheduler.
            unused_resources_by_ip: Mapping from ip to available resources.
            pending_placement_groups: Placement group demands.
            max_resources_by_ip: Mapping from ip to static node resources.
            ensure_min_cluster_size: Try to ensure the cluster can fit at least
                this set of resources. This differs from resources_demands in
                that we don't take into account existing usage.

            node_availability_summary: A snapshot of the current
                NodeAvailabilitySummary.

        Returns:
            Dict of count to add for each node type, and residual of resources
            that still cannot be fulfilled.
        """
        utilization_scorer = partial(
            self.utilization_scorer, node_availability_summary=node_availability_summary
        )
        self._update_node_resources_from_runtime(nodes, max_resources_by_ip)

        node_resources: List[ResourceDict]
        node_type_counts: Dict[NodeType, int]
        node_resources, node_type_counts = self.calculate_node_resources(
            nodes, launching_nodes, unused_resources_by_ip
        )

        logger.debug("Cluster resources: {}".format(node_resources))
        logger.debug("Node counts: {}".format(node_type_counts))
        # Step 2: add nodes to add to satisfy min_workers for each type
        (
            node_resources,
            node_type_counts,
            adjusted_min_workers,
        ) = _add_min_workers_nodes(
            node_resources,
            node_type_counts,
            self.node_types,
            self.max_workers,
            self.head_node_type,
            ensure_min_cluster_size,
            utilization_scorer=utilization_scorer,
        )

        # Step 3: get resource demands of placement groups and return the
        # groups that should be strictly spread.
        logger.debug(f"Placement group demands: {pending_placement_groups}")
        # TODO(Clark): Refactor placement group bundle demands such that their placement
        # group provenance is mantained, since we need to keep an accounting of the
        # cumulative CPU cores allocated as fulfilled during bin packing in order to
        # ensure that a placement group's cumulative allocation is under the placement
        # group's max CPU fraction per node. Without this, and placement group with many
        # bundles might not be schedulable, but will fail to trigger scale-up since the
        # max CPU fraction is properly applied to the cumulative bundle requests for a
        # single node.
        #
        # placement_group_demand_vector: List[Tuple[List[ResourceDict], double]]
        #
        # bin_pack_residual() can keep it's packing priority; we just need to account
        # for (1) the running CPU allocation for the bundle's placement group for that
        # particular node, and (2) the max CPU cores allocatable for a single placement
        # group for that particular node.
        (
            placement_group_demand_vector,
            strict_spreads,
        ) = placement_groups_to_resource_demands(pending_placement_groups)
        # Place placement groups demand vector at the beginning of the resource
        # demands vector to make it consistent (results in the same types of
        # nodes to add) with pg_demands_nodes_max_launch_limit calculated later
        resource_demands = placement_group_demand_vector + resource_demands

        (
            spread_pg_nodes_to_add,
            node_resources,
            node_type_counts,
        ) = self.reserve_and_allocate_spread(
            strict_spreads,
            node_resources,
            node_type_counts,
            utilization_scorer,
        )

        # Calculate the nodes to add for bypassing max launch limit for
        # placement groups and spreads.
        unfulfilled_placement_groups_demands, _ = get_bin_pack_residual(
            node_resources,
            placement_group_demand_vector,
        )
        # Add 1 to account for the head node.
        max_to_add = self.max_workers + 1 - sum(node_type_counts.values())
        pg_demands_nodes_max_launch_limit, _ = get_nodes_for(
            self.node_types,
            node_type_counts,
            self.head_node_type,
            max_to_add,
            unfulfilled_placement_groups_demands,
            utilization_scorer=utilization_scorer,
        )
        placement_groups_nodes_max_limit = {
            node_type: spread_pg_nodes_to_add.get(node_type, 0)
            + pg_demands_nodes_max_launch_limit.get(node_type, 0)
            for node_type in self.node_types
        }

        # Step 4/5: add nodes for pending tasks, actors, and non-strict spread
        # groups
        unfulfilled, _ = get_bin_pack_residual(node_resources, resource_demands)
        logger.debug("Resource demands: {}".format(resource_demands))
        logger.debug("Unfulfilled demands: {}".format(unfulfilled))
        nodes_to_add_based_on_demand, final_unfulfilled = get_nodes_for(
            self.node_types,
            node_type_counts,
            self.head_node_type,
            max_to_add,
            unfulfilled,
            utilization_scorer=utilization_scorer,
        )
        logger.debug("Final unfulfilled: {}".format(final_unfulfilled))
        # Merge nodes to add based on demand and nodes to add based on
        # min_workers constraint. We add them because nodes to add based on
        # demand was calculated after the min_workers constraint was respected.
        total_nodes_to_add = {}

        for node_type in self.node_types:
            nodes_to_add = (
                adjusted_min_workers.get(node_type, 0)
                + spread_pg_nodes_to_add.get(node_type, 0)
                + nodes_to_add_based_on_demand.get(node_type, 0)
            )
            if nodes_to_add > 0:
                total_nodes_to_add[node_type] = nodes_to_add

        # Limit the number of concurrent launches
        total_nodes_to_add = self._get_concurrent_resource_demand_to_launch(
            total_nodes_to_add,
            unused_resources_by_ip.keys(),
            nodes,
            launching_nodes,
            adjusted_min_workers,
            placement_groups_nodes_max_limit,
        )

        logger.debug("Node requests: {}".format(total_nodes_to_add))
        return total_nodes_to_add, final_unfulfilled

    def _update_node_resources_from_runtime(
        self, nodes: List[NodeID], max_resources_by_ip: Dict[NodeIP, ResourceDict]
    ):
        """Update static node type resources with runtime resources

        This will update the cached static node type resources with the runtime
        resources. Because we can not know the exact autofilled memory or
        object_store_memory from config file.
        """
        need_update = len(self.node_types) != len(self.node_resource_updated)

        if not need_update:
            return
        for node_id in nodes:
            tags = self.provider.node_tags(node_id)

            if TAG_RAY_USER_NODE_TYPE not in tags:
                continue

            node_type = tags[TAG_RAY_USER_NODE_TYPE]
            if (
                node_type in self.node_resource_updated
                or node_type not in self.node_types
            ):
                # continue if the node type has been updated or is not an known
                # node type
                continue
            ip = self.provider.internal_ip(node_id)
            runtime_resources = max_resources_by_ip.get(ip)
            if runtime_resources:
                runtime_resources = copy.deepcopy(runtime_resources)
                resources = self.node_types[node_type].get("resources", {})
                for key in ["CPU", "GPU", "memory", "object_store_memory"]:
                    if key in runtime_resources:
                        resources[key] = runtime_resources[key]
                self.node_types[node_type]["resources"] = resources

                node_kind = tags[TAG_RAY_NODE_KIND]
                if node_kind == NODE_KIND_WORKER:
                    # Here, we do not record the resources have been updated
                    # if it is the head node kind. Because it need be updated
                    # by worker kind runtime resource. The most difference
                    # between head and worker is the memory resources. The head
                    # node needs to configure redis memory which is not needed
                    # for worker nodes.
                    self.node_resource_updated.add(node_type)

    def _get_concurrent_resource_demand_to_launch(
        self,
        to_launch: Dict[NodeType, int],
        connected_nodes: List[NodeIP],
        non_terminated_nodes: List[NodeID],
        pending_launches_nodes: Dict[NodeType, int],
        adjusted_min_workers: Dict[NodeType, int],
        placement_group_nodes: Dict[NodeType, int],
    ) -> Dict[NodeType, int]:
        """Updates the max concurrent resources to launch for each node type.

        Given the current nodes that should be launched, the non terminated
        nodes (running and pending) and the pending to be launched nodes. This
        method calculates the maximum number of nodes to launch concurrently
        for each node type as follows:
            1) Calculates the running nodes.
            2) Calculates the pending nodes and gets the launching nodes.
            3) Limits the total number of pending + currently-launching +
               to-be-launched nodes to:
                   max(
                       5,
                       self.upscaling_speed * max(running_nodes[node_type], 1)
                   ).

        Args:
            to_launch: List of number of nodes to launch based on resource
                demand for every node type.
            connected_nodes: Running nodes (from LoadMetrics).
            non_terminated_nodes: Non terminated nodes (pending/running).
            pending_launches_nodes: Nodes that are in the launch queue.
            adjusted_min_workers: Nodes to launch to satisfy
                min_workers and request_resources(). This overrides the launch
                limits since the user is hinting to immediately scale up to
                this size.
            placement_group_nodes: Nodes to launch for placement groups.
                This overrides the launch concurrency limits.
        Returns:
            Dict[NodeType, int]: Maximum number of nodes to launch for each
                node type.
        """
        updated_nodes_to_launch = {}
        running_nodes, pending_nodes = self._separate_running_and_pending_nodes(
            non_terminated_nodes,
            connected_nodes,
        )
        for node_type in to_launch:
            # Enforce here max allowed pending nodes to be frac of total
            # running nodes.
            max_allowed_pending_nodes = max(
                UPSCALING_INITIAL_NUM_NODES,
                int(self.upscaling_speed * max(running_nodes[node_type], 1)),
            )
            total_pending_nodes = (
                pending_launches_nodes.get(node_type, 0) + pending_nodes[node_type]
            )

            upper_bound = max(
                max_allowed_pending_nodes - total_pending_nodes,
                # Allow more nodes if this is to respect min_workers or
                # request_resources() or placement groups.
                adjusted_min_workers.get(node_type, 0)
                + placement_group_nodes.get(node_type, 0),
            )

            if upper_bound > 0:
                updated_nodes_to_launch[node_type] = min(
                    upper_bound, to_launch[node_type]
                )

        return updated_nodes_to_launch

    def _separate_running_and_pending_nodes(
        self,
        non_terminated_nodes: List[NodeID],
        connected_nodes: List[NodeIP],
    ) -> (Dict[NodeType, int], Dict[NodeType, int]):
        """Splits connected and non terminated nodes to pending & running."""

        running_nodes = collections.defaultdict(int)
        pending_nodes = collections.defaultdict(int)
        for node_id in non_terminated_nodes:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                node_ip = self.provider.internal_ip(node_id)
                if node_ip in connected_nodes:
                    running_nodes[node_type] += 1
                else:
                    pending_nodes[node_type] += 1
        return running_nodes, pending_nodes

    def calculate_node_resources(
        self,
        nodes: List[NodeID],
        pending_nodes: Dict[NodeID, int],
        unused_resources_by_ip: Dict[str, ResourceDict],
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
                # We should not get here, but if for some reason we do, log an
                # error and skip the errant node_type.
                logger.error(
                    f"Missing entry for node_type {node_type} in "
                    f"cluster config: {self.node_types} under entry "
                    "available_node_types. This node's resources will be "
                    "ignored. If you are using an unmanaged node, manually "
                    f"set the {TAG_RAY_NODE_KIND} tag to "
                    f'"{NODE_KIND_UNMANAGED}" in your cloud provider\'s '
                    "management console."
                )
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
                available_resources = unused_resources_by_ip.get(ip)
                add_node(node_type, available_resources)

        for node_type, count in pending_nodes.items():
            for _ in range(count):
                add_node(node_type)

        return node_resources, node_type_counts

    def reserve_and_allocate_spread(
        self,
        strict_spreads: List[List[ResourceDict]],
        node_resources: List[ResourceDict],
        node_type_counts: Dict[NodeType, int],
        utilization_scorer: Callable[
            [NodeResources, ResourceDemands], Optional[UtilizationScore]
        ],
    ):
        """For each strict spread, attempt to reserve as much space as possible
        on the node, then allocate new nodes for the unfulfilled portion.

        Args:
            strict_spreads (List[List[ResourceDict]]): A list of placement
                groups which must be spread out.
            node_resources (List[ResourceDict]): Available node resources in
                the cluster.
            node_type_counts (Dict[NodeType, int]): The amount of each type of
                node pending or in the cluster.
            utilization_scorer: A function that, given a node
                type, its resources, and resource demands, returns what its
                utilization would be.

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
                node_resources, bundles, strict_spread=True
            )
            max_to_add = self.max_workers + 1 - sum(node_type_counts.values())
            # Allocate new nodes for the remaining bundles that don't fit.
            to_launch, _ = get_nodes_for(
                self.node_types,
                node_type_counts,
                self.head_node_type,
                max_to_add,
                unfulfilled,
                utilization_scorer=utilization_scorer,
                strict_spread=True,
            )
            _inplace_add(node_type_counts, to_launch)
            _inplace_add(to_add, to_launch)
            new_node_resources = _node_type_counts_to_node_resources(
                self.node_types, to_launch
            )
            # Update node resources to include newly launched nodes and their
            # bundles.
            unfulfilled, including_reserved = get_bin_pack_residual(
                new_node_resources, unfulfilled, strict_spread=True
            )
            assert not unfulfilled
            node_resources += including_reserved
        return to_add, node_resources, node_type_counts

    def debug_string(
        self,
        nodes: List[NodeID],
        pending_nodes: Dict[NodeID, int],
        unused_resources_by_ip: Dict[str, ResourceDict],
    ) -> str:
        node_resources, node_type_counts = self.calculate_node_resources(
            nodes, pending_nodes, unused_resources_by_ip
        )

        out = "Worker node types:"
        for node_type, count in node_type_counts.items():
            out += "\n - {}: {}".format(node_type, count)
            if pending_nodes.get(node_type):
                out += " ({} pending)".format(pending_nodes[node_type])

        return out


def _node_type_counts_to_node_resources(
    node_types: Dict[NodeType, NodeTypeConfigDict],
    node_type_counts: Dict[NodeType, int],
) -> List[ResourceDict]:
    """Converts a node_type_counts dict into a list of node_resources."""
    resources = []
    for node_type, count in node_type_counts.items():
        # Be careful, each entry in the list must be deep copied!
        resources += [node_types[node_type]["resources"].copy() for _ in range(count)]
    return resources


def _add_min_workers_nodes(
    node_resources: List[ResourceDict],
    node_type_counts: Dict[NodeType, int],
    node_types: Dict[NodeType, NodeTypeConfigDict],
    max_workers: int,
    head_node_type: NodeType,
    ensure_min_cluster_size: List[ResourceDict],
    utilization_scorer: Callable[
        [NodeResources, ResourceDemands, str], Optional[UtilizationScore]
    ],
) -> (List[ResourceDict], Dict[NodeType, int], Dict[NodeType, int]):
    """Updates resource demands to respect the min_workers and
    request_resources() constraints.

    Args:
        node_resources: Resources of exisiting nodes already launched/pending.
        node_type_counts: Counts of existing nodes already launched/pending.
        node_types: Node types config.
        max_workers: global max_workers constaint.
        ensure_min_cluster_size: resource demands from request_resources().
        utilization_scorer: A function that, given a node
            type, its resources, and resource demands, returns what its
            utilization would be.

    Returns:
        node_resources: The updated node resources after adding min_workers
            and request_resources() constraints per node type.
        node_type_counts: The updated node counts after adding min_workers
            and request_resources() constraints per node type.
        total_nodes_to_add_dict: The nodes to add to respect min_workers and
            request_resources() constraints.
    """
    total_nodes_to_add_dict = {}
    for node_type, config in node_types.items():
        existing = node_type_counts.get(node_type, 0)
        target = min(config.get("min_workers", 0), config.get("max_workers", 0))
        if node_type == head_node_type:
            # Add 1 to account for head node.
            target = target + 1
        if existing < target:
            total_nodes_to_add_dict[node_type] = target - existing
            node_type_counts[node_type] = target
            node_resources.extend(
                [
                    copy.deepcopy(node_types[node_type]["resources"])
                    for _ in range(total_nodes_to_add_dict[node_type])
                ]
            )

    if ensure_min_cluster_size:
        max_to_add = max_workers + 1 - sum(node_type_counts.values())
        max_node_resources = []
        # Fit request_resources() on all the resources as if they are idle.
        for node_type in node_type_counts:
            max_node_resources.extend(
                [
                    copy.deepcopy(node_types[node_type]["resources"])
                    for _ in range(node_type_counts[node_type])
                ]
            )
        # Get the unfulfilled to ensure min cluster size.
        resource_requests_unfulfilled, _ = get_bin_pack_residual(
            max_node_resources, ensure_min_cluster_size
        )
        # Get the nodes to meet the unfulfilled.
        nodes_to_add_request_resources, _ = get_nodes_for(
            node_types,
            node_type_counts,
            head_node_type,
            max_to_add,
            resource_requests_unfulfilled,
            utilization_scorer=utilization_scorer,
        )
        # Update the resources, counts and total nodes to add.
        for node_type in nodes_to_add_request_resources:
            nodes_to_add = nodes_to_add_request_resources.get(node_type, 0)
            if nodes_to_add > 0:
                node_type_counts[node_type] = nodes_to_add + node_type_counts.get(
                    node_type, 0
                )
                node_resources.extend(
                    [
                        copy.deepcopy(node_types[node_type]["resources"])
                        for _ in range(nodes_to_add)
                    ]
                )
                total_nodes_to_add_dict[
                    node_type
                ] = nodes_to_add + total_nodes_to_add_dict.get(node_type, 0)
    return node_resources, node_type_counts, total_nodes_to_add_dict


def get_nodes_for(
    node_types: Dict[NodeType, NodeTypeConfigDict],
    existing_nodes: Dict[NodeType, int],
    head_node_type: NodeType,
    max_to_add: int,
    resources: List[ResourceDict],
    utilization_scorer: Callable[
        [NodeResources, ResourceDemands, str], Optional[UtilizationScore]
    ],
    strict_spread: bool = False,
) -> (Dict[NodeType, int], List[ResourceDict]):
    """Determine nodes to add given resource demands and constraints.

    Args:
        node_types: node types config.
        existing_nodes: counts of existing nodes already launched.
            This sets constraints on the number of new nodes to add.
        max_to_add: global constraint on nodes to add.
        resources: resource demands to fulfill.
        strict_spread: If true, each element in `resources` must be placed on a
            different node.
        utilization_scorer: A function that, given a node
            type, its resources, and resource demands, returns what its
            utilization would be.

    Returns:
        Dict of count to add for each node type, and residual of resources
        that still cannot be fulfilled.
    """
    nodes_to_add: Dict[NodeType, int] = collections.defaultdict(int)

    while resources and sum(nodes_to_add.values()) < max_to_add:
        utilization_scores = []
        for node_type in node_types:
            max_workers_of_node_type = node_types[node_type].get("max_workers", 0)
            if head_node_type == node_type:
                # Add 1 to account for head node.
                max_workers_of_node_type = max_workers_of_node_type + 1
            if (
                existing_nodes.get(node_type, 0) + nodes_to_add.get(node_type, 0)
                >= max_workers_of_node_type
            ):
                continue
            node_resources = node_types[node_type]["resources"]
            if strict_spread:
                # If handling strict spread, only one bundle can be placed on
                # the node.
                score = utilization_scorer(node_resources, [resources[0]], node_type)
            else:
                score = utilization_scorer(node_resources, resources, node_type)
            if score is not None:
                utilization_scores.append((score, node_type))

        # Give up, no feasible node.
        if not utilization_scores:
            if not any(
                is_placement_group_resource(resource)
                for resources_dict in resources
                for resource in resources_dict
            ):
                logger.warning(
                    f"The autoscaler could not find a node type to satisfy the "
                    f"request: {resources}. Please specify a node type with the "
                    f"necessary resources."
                )
            break

        utilization_scores = sorted(utilization_scores, reverse=True)
        best_node_type = utilization_scores[0][1]
        nodes_to_add[best_node_type] += 1
        if strict_spread:
            resources = resources[1:]
        else:
            allocated_resource = node_types[best_node_type]["resources"]
            residual, _ = get_bin_pack_residual([allocated_resource], resources)
            assert len(residual) < len(resources), (resources, residual)
            resources = residual

    return nodes_to_add, resources


def _resource_based_utilization_scorer(
    node_resources: ResourceDict,
    resources: List[ResourceDict],
    *,
    node_availability_summary: NodeAvailabilitySummary,
) -> Optional[Tuple[int, float, float]]:
    remaining = copy.deepcopy(node_resources)
    is_gpu_node = "GPU" in node_resources and node_resources["GPU"] > 0
    any_gpu_task = any("GPU" in r for r in resources)

    # Prefer not to launch a GPU node if there aren't any GPU requirements in the
    # resource bundle.
    if AUTOSCALER_CONSERVE_GPU_NODES:
        if is_gpu_node and not any_gpu_task:
            # The lowest possible score.
            return (-1, -float("inf"), -float("inf"))

    fittable = []
    resource_types = set()
    for r in resources:
        for k, v in r.items():
            if v > 0:
                resource_types.add(k)
        if _fits(remaining, r):
            fittable.append(r)
            _inplace_subtract(remaining, r)
    if not fittable:
        return None

    util_by_resources = []
    num_matching_resource_types = 0
    for k, v in node_resources.items():
        # Don't divide by zero.
        if v < 1:
            # Could test v == 0 on the nose, but v < 1 feels safer.
            # (Note that node resources are integers.)
            continue
        if k in resource_types:
            num_matching_resource_types += 1
        util = (v - remaining[k]) / v
        util_by_resources.append(v * (util**3))

    # Could happen if node_resources has only zero values.
    if not util_by_resources:
        return None

    # Prioritize matching multiple resource types first, then prioritize
    # using all resources, then prioritize overall balance
    # of multiple resources.
    return (
        num_matching_resource_types,
        min(util_by_resources),
        np.mean(util_by_resources),
    )


def _default_utilization_scorer(
    node_resources: ResourceDict,
    resources: List[ResourceDict],
    node_type: str,
    *,
    node_availability_summary: NodeAvailabilitySummary,
):
    return _resource_based_utilization_scorer(
        node_resources, resources, node_availability_summary=node_availability_summary
    )


def get_bin_pack_residual(
    node_resources: List[ResourceDict],
    resource_demands: List[ResourceDict],
    strict_spread: bool = False,
) -> (List[ResourceDict], List[ResourceDict]):
    """Return a subset of resource_demands that cannot fit in the cluster.

    TODO(ekl): this currently does not guarantee the resources will be packed
    correctly by the Ray scheduler. This is only possible once the Ray backend
    supports a placement groups API.

    Args:
        node_resources (List[ResourceDict]): List of resources per node.
        resource_demands (List[ResourceDict]): List of resource bundles that
            need to be bin packed onto the nodes.
        strict_spread: If true, each element in resource_demands must be
            placed on a different entry in `node_resources`.

    Returns:
        List[ResourceDict]: the residual list resources that do not fit.
        List[ResourceDict]: The updated node_resources after the method.
    """

    unfulfilled = []

    # A most naive bin packing algorithm.
    nodes = copy.deepcopy(node_resources)
    # List of nodes that cannot be used again due to strict spread.
    used = []
    # We order the resource demands in the following way:
    # More complex demands first.
    # Break ties: heavier demands first.
    # Break ties: lexicographically (to ensure stable ordering).
    for demand in sorted(
        resource_demands,
        key=lambda demand: (
            len(demand.values()),
            sum(demand.values()),
            sorted(demand.items()),
        ),
        reverse=True,
    ):
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
    pending_placement_groups: List[PlacementGroupTableData],
):
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
        shapes = [dict(bundle.unit_resources) for bundle in placement_group.bundles]
        if (
            placement_group.strategy == PlacementStrategy.PACK
            or placement_group.strategy == PlacementStrategy.SPREAD
        ):
            resource_demand_vector.extend(shapes)
        elif placement_group.strategy == PlacementStrategy.STRICT_PACK:
            combined = collections.defaultdict(float)
            for shape in shapes:
                for label, quantity in shape.items():
                    combined[label] += quantity
            resource_demand_vector.append(combined)
        elif placement_group.strategy == PlacementStrategy.STRICT_SPREAD:
            unconverted.append(shapes)
        else:
            logger.error(
                f"Unknown placement group request type: {placement_group}. "
                f"Please file a bug report "
                f"https://github.com/ray-project/ray/issues/new."
            )
    return resource_demand_vector, unconverted
