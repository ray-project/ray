import collections
import copy
import logging
import os
import time
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Callable, Dict, List, Optional, Tuple, Union

import yaml

import ray
from ray._private.gcs_utils import PlacementGroupTableData
from ray._raylet import GcsClient
from ray.autoscaler._private.constants import (
    AUTOSCALER_CONSERVE_GPU_NODES,
    AUTOSCALER_UTILIZATION_SCORER_KEY,
)
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.loader import load_function_or_class
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilitySummary,
)
from ray.autoscaler._private.resource_demand_scheduler import (
    ResourceDemandScheduler,
    UtilizationScore,
    UtilizationScorer,
    _add_min_workers_nodes,
    _inplace_add,
    _node_type_counts_to_node_resources,
    placement_groups_to_resource_demands,
    get_bin_pack_residual,
    get_nodes_for,
)
from ray.autoscaler._private.util import (
    NodeID,
    NodeIP,
    NodeType,
    NodeTypeConfigDict,
    ResourceDict,
    is_placement_group_resource,
    validate_config,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_UNMANAGED,
    NODE_KIND_WORKER,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.autoscaler.v2.instance_manager.node_provider import NodeProvider
from ray.autoscaler.v2.instance_manager.subscribers.reconciler import InstanceReconciler
from ray.autoscaler.v2.schema import (
    DEFAULT_UPSCALING_SPEED,
    ClusterStatus,
    IAutoscaler,
    LaunchRequest,
    NodeInfo,
    PlacementGroupResourceDemand,
    Stats,
)
from ray.autoscaler.v2.utils import ClusterStatusParser
from ray.core.generated.autoscaler_pb2 import GetClusterStatusReply
from ray.core.generated.common_pb2 import PlacementStrategy
from ray.core.generated.instance_manager_pb2 import Instance

DEFAULT_RPC_TIMEOUT_S = 10


logger = logging.getLogger(__name__)


def get_gcs_client(gcs_address: Optional[str] = None):
    """Get the GCS client."""
    if gcs_address is None:
        gcs_address = ray.get_runtime_context().gcs_address
    assert gcs_address is not None, (
        "GCS address should have been detected if getting from runtime context"
        "or passed in explicitly."
    )
    return GcsClient(address=gcs_address)


def request_cluster_resources(
    to_request: List[dict], timeout: int = DEFAULT_RPC_TIMEOUT_S
):
    """Request resources from the autoscaler.

    This will add a cluster resource constraint to GCS. GCS will asynchronously
    pass the constraint to the autoscaler, and the autoscaler will try to provision the
    requested minimal bundles in `to_request`.

    If the cluster already has `to_request` resources, this will be an no-op.
    Future requests submitted through this API will overwrite the previous requests.

    NOTE:
        This function has to be invoked in a ray worker/driver, i.e., after `ray.init()`

    Args:
        to_request: A list of resource bundles to request the cluster to have.
            Each bundle is a dict of resource name to resource quantity, e.g:
            [{"CPU": 1}, {"GPU": 1}].
        timeout: Timeout in seconds for the request to be timeout

    """

    # Aggregate bundle by shape.
    resource_requests_by_count = defaultdict(int)
    for request in to_request:
        bundle = frozenset(request.items())
        resource_requests_by_count[bundle] += 1

    bundles = []
    counts = []
    for bundle, count in resource_requests_by_count.items():
        bundles.append(dict(bundle))
        counts.append(count)

    get_gcs_client().request_cluster_resource_constraint(
        bundles, counts, timeout_s=timeout
    )


def get_cluster_status(
    gcs_address: Optional[str] = None, timeout: int = DEFAULT_RPC_TIMEOUT_S
) -> ClusterStatus:
    """
    Get the cluster status from the autoscaler.

    Args:
        gcs_address: The GCS address to query. If not specified, will use the
            GCS address from the runtime context.
        timeout: Timeout in seconds for the request to be timeout

    Returns:
        A ClusterStatus object.
    """
    req_time = time.time()
    str_reply = get_gcs_client(gcs_address).get_cluster_status(timeout_s=timeout)
    reply_time = time.time()
    reply = GetClusterStatusReply()
    reply.ParseFromString(str_reply)

    # TODO(rickyx): To be more accurate, we could add a timestamp field from the reply.
    return ClusterStatusParser.from_get_cluster_status_reply(
        reply,
        stats=Stats(gcs_request_time_s=reply_time - req_time, request_ts_s=req_time),
    )


@dataclass
class AutoscalerError:
    class Code(Enum):
        CONNECTION_ERROR = 1
        NOT_AVAILABLE = 2

    code: Code
    msg: Optional[str] = ""


logger = logging.getLogger(__name__)

# The minimum number of nodes to launch concurrently.
UPSCALING_INITIAL_NUM_NODES = 5

NodeResources = ResourceDict
ResourceDemands = List[ResourceDict]


class ResourceDemandSchedulerV2:
    def __init__(
        self,
        node_types: Dict[NodeType, NodeTypeConfigDict],
        max_workers: int,
        head_node_type: NodeType,
        upscaling_speed: float,
    ) -> None:

        self.node_types = copy.deepcopy(node_types)
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

    def reset_config(
        self,
        node_types: Dict[NodeType, NodeTypeConfigDict],
        max_workers: int,
        head_node_type: NodeType,
        upscaling_speed: float = 1,
    ) -> None:
        """Updates the class state variables.

        For legacy yamls, it merges previous state and new state to make sure
        inferered resources are not lost.
        """
        self.node_types = copy.deepcopy(node_types)
        self.max_workers = max_workers
        self.head_node_type = head_node_type
        self.upscaling_speed = upscaling_speed

    def get_nodes_to_launch(
        self,
        pending_instances: List[Instance],
        cluster_status: ClusterStatus,
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

        node_resources: List[ResourceDict]
        node_type_counts: Dict[NodeType, int]
        node_resources, node_type_counts = self.calculate_node_resources(
            pending_instances, cluster_status
        )

        # Step 2: add nodes to add to satisfy min_workers for each type
        ensure_min_cluster_size = []

        for (
            cluster_constraint_demand
        ) in cluster_status.resource_demands.cluster_constraint_demand:
            ensure_min_cluster_size.extend(
                cluster_constraint_demand.as_resource_dicts()
            )

        logger.info("node_resources = {}".format(node_resources))
        logger.info("node_type_counts = {}".format(node_type_counts))
        logger.info("ensure_min_cluster_size = {}".format(ensure_min_cluster_size))
        logger.info("self.node_types = {}".format(self.node_types))
        logger.info("self.max_workers = {}".format(self.max_workers))
        logger.info("self.head_node_type = {}".format(self.head_node_type))
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

        logger.info("Node counts after min_workers: {}".format(node_type_counts))
        logger.info("Node resources after min_workers: {}".format(node_resources))
        logger.info("Adjusted min_workers: {}".format(adjusted_min_workers))

        # Step 3: get resource demands of placement groups and return the
        # groups that should be strictly spread.
        pending_placement_groups = (
            cluster_status.resource_demands.placement_group_demand
        )
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

        # TODO(rickyx): We should actually look at affinity labels from the resouce requests rather
        # than parsing the placement group metadata (which is mainly for observability now) directly.
        # This is so that affinity/anti-affnitity constraints could be generic enough and
        # to be extended beyond the concept of placement groups.
        def split_placement_group_resource_demands(
            placement_group_demands: List[PlacementGroupResourceDemand],
        ) -> Tuple[List[Dict[str, float]], List[List[Dict[str, float]]]]:
            """Split placement group resource demands into strict spread and non-strict spread.

            Args:
                placement_group_demands: List of placement group resource demands.

            Returns:
                A tuple of strict spread and non-strict spread placement group resource demands.
            """
            strict_spreads: List[List[Dict]] = []
            non_strict_spreads: List[Dict] = []
            for demand in placement_group_demands:
                if demand.strategy in ["PACK", "SPREAD"]:
                    non_strict_spreads.extend(demand.as_resource_dicts())
                elif demand.strategy == "STRICT_SPREAD":
                    strict_spreads.append(demand.as_resource_dicts())
                elif demand.strategy == "STRICT_PACK":
                    combined = collections.defaultdict(float)
                    for shape in demand.as_resource_dicts():
                        for k, v in shape.items():
                            combined[k] += v
                    non_strict_spreads.append(dict(combined))
                else:
                    raise RuntimeError(
                        f"Unknown placement group request type: {demand.strategy}. "
                        f"Please file a bug report "
                        f"https://github.com/ray-project/ray/issues/new."
                    )
            return non_strict_spreads, strict_spreads

        (
            non_strict_pg_demands,
            strict_spreads_pg_demands,
        ) = split_placement_group_resource_demands(
            cluster_status.resource_demands.placement_group_demand
        )
        # Place placement groups demand vector at the beginning of the resource
        # demands vector to make it consistent (results in the same types of
        # nodes to add) with pg_demands_nodes_max_launch_limit calculated later
        resource_demands = (
            non_strict_pg_demands
            + cluster_status.resource_demands.get_task_actor_demand()
        )

        (
            spread_pg_nodes_to_add,
            node_resources,
            node_type_counts,
        ) = self._reserve_and_allocate_spread(
            strict_spreads_pg_demands,
            node_resources,
            node_type_counts,
            utilization_scorer,
        )

        # Calculate the nodes to add for bypassing max launch limit for
        # placement groups and spreads.
        unfulfilled_placement_groups_demands, _ = get_bin_pack_residual(
            node_resources,
            non_strict_pg_demands,
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
        logger.info("Resource demands: {}".format(resource_demands))
        logger.info("Unfulfilled demands: {}".format(unfulfilled))
        nodes_to_add_based_on_demand, final_unfulfilled = get_nodes_for(
            self.node_types,
            node_type_counts,
            self.head_node_type,
            max_to_add,
            unfulfilled,
            utilization_scorer=utilization_scorer,
        )
        logger.info("Final unfulfilled: {}".format(final_unfulfilled))
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
            cluster_status,
            total_nodes_to_add,
            adjusted_min_workers,
            placement_groups_nodes_max_limit,
        )

        logger.info("Node requests: {}".format(total_nodes_to_add))
        return total_nodes_to_add, final_unfulfilled

    def _reserve_and_allocate_spread(
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

    def _get_concurrent_resource_demand_to_launch(
        self,
        cluster_status: ClusterStatus,
        to_launch: Dict[NodeType, int],
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
        active_nodes_count_by_type = defaultdict(int)
        pending_nodes_count_by_type = defaultdict(int)
        pending_launches_by_type = defaultdict(int)

        for active_node in cluster_status.healthy_nodes:
            active_nodes_count_by_type[active_node.ray_node_type_name] += 1

        for pending_node in cluster_status.pending_nodes:
            pending_nodes_count_by_type[pending_node.ray_node_type_name] += 1

        for pending_launch in cluster_status.pending_launches:
            pending_launches_by_type[pending_launch.ray_node_type_name] += 1

        for node_type in to_launch:
            # Enforce here max allowed pending nodes to be frac of total
            # running nodes.
            max_allowed_pending_nodes = max(
                UPSCALING_INITIAL_NUM_NODES,
                int(
                    self.upscaling_speed * max(active_nodes_count_by_type[node_type], 1)
                ),
            )
            total_pending_nodes = pending_nodes_count_by_type.get(
                node_type, 0
            ) + pending_nodes_count_by_type.get(node_type, 0)

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

    def calculate_node_resources(
        self,
        pending_instances: List[Instance],
        cluster_status: ClusterStatus,
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

        active_nodes: List[NodeInfo] = cluster_status.healthy_nodes
        node_resources = []
        node_type_counts = collections.defaultdict(int)

        def add_node_resources(
            node_type: str,
            available_resources: Optional[Dict[str, float]] = None,
        ):
            if node_type not in self.node_types:
                raise RuntimeError(f"Unknown node type {node_type}")

            # Careful not to include the same dict object multiple times.
            available = copy.deepcopy(self.node_types[node_type]["resources"])
            # If available_resources is None this might be because the node is
            # no longer pending, but the raylet hasn't sent a heartbeat to gcs
            # yet.
            if available_resources is not None:
                available = copy.deepcopy(available_resources)

            node_resources.extend([available])
            node_type_counts[node_type] += 1

        for active_node in active_nodes:
            node_type = active_node.ray_node_type_name
            available_resources = active_node.resource_usage.available_resources()
            add_node_resources(
                node_type=node_type, available_resources=available_resources
            )

        for instance in pending_instances:
            node_type = instance.instance_type
            # We don't know the available resources of pending node yet. So
            # we just use the static resources from the config.
            add_node_resources(node_type=node_type)

        return node_resources, node_type_counts


class AutoscalerV2(IAutoscaler):
    def __init__(
        self,
        config_reader: Union[str, Callable[[], dict]],
        node_provider: NodeProvider,
        reconciler: InstanceReconciler,
        session_name: Optional[str] = None,
        gcs_address: Optional[str] = None,
    ) -> None:
        super().__init__()

        self._session_name = session_name
        self._gcs_address = gcs_address
        self._node_provider = node_provider
        self._reconciler = reconciler
        self._resource_demand_scheduler = None
        self._init_config(config_reader)
        self._init_resource_demand_scheduler(node_provider, self._config)

        # TODO:
        # 1. print the config
        # 2. validate the config

    def _init_config(self, config_reader):
        if isinstance(config_reader, str):
            # Auto wrap with file reader.
            def read_fn():
                with open(config_reader) as f:
                    new_config = yaml.safe_load(f.read())
                return new_config

            self._config_reader = read_fn
        else:
            self._config_reader = config_reader

        config = self._config_reader()
        try:
            validate_config(config)
        except Exception as e:
            # TODO, metrics:
            pass

    def _init_resource_demand_scheduler(self, node_provider, config):
        # TODO:
        # deprecated target_utilization_fraction and autoscaling_mode
        self._resource_demand_scheduler = ResourceDemandScheduler(
            self._node_provider,
            self._config["available_node_types"],
            self.config["max_workers"],
            self.config["head_node_type"],
            self._config.get("upscaling_speed", DEFAULT_UPSCALING_SPEED),
        )

    def _update(self) -> Optional[AutoscalerError]:
        # TODO:
        # update metrics
        # Terminate outdated nodes?

        # Get the most recent cluster status
        try:
            cluster_status = get_cluster_status(self._gcs_address)
        except Exception as e:
            logger.error(f"Failed to get cluster status: {e}")
            return AutoscalerError(AutoscalerError.Code.CONNECTION_ERROR, str(e))

        if not self._node_provider.safe_to_scale():
            logger.info(
                "Backing off of autoscaler update."
                f" Will try again in {self.update_interval_s} seconds."
            )
            return AutoscalerError(
                AutoscalerError.Code.NOT_AVAILABLE,
                "Node provider not ready to be scale.",
            )

        # NOTE:
        # No more unhelathy nodes since GCS's response is now a source of truth for Ray nodes.
        to_launch, unfulfilled = self._get_nodes_to_launch(cluster_status)

        # Terminate nodes
        to_terminate = self._get_nodes_to_terminate(cluster_status)

        self._update_clusters(to_launch, to_terminate)

        self._report_autoscaling_state(to_launch, unfulfilled, cluster_status)

    def _get_nodes_to_launch(
        self, cluster_status: ClusterStatus
    ) -> Tuple[Dict[NodeType, int], List[ResourceDict]]:

        return self._resource_demand_scheduler.get_nodes_to_launch(
            pending_instances=self._get_pending_instances(),
            cluster_status=cluster_status,
            node_availability_summary=self.node_provider_availability_tracker.summary(),
        )

    def _get_pending_instances(self) -> List[Instance]:
        instances = self._instance_manager.get_instance_manager_state().instances

        def is_pending(instance: Instance):
            if instance.status in [Instance.QUEUED, Instance.REQUESTED]:
                return True

            if (
                instance.status == Instance.ALLOCATED
                and instance.ray_status == Instance.RAY_INSTALLING
            ):
                return True

            return False

        return [instance for instance in instances if is_pending(instance)]

    def update(self) -> None:
        # TODO:
        # 1. reset the config?

        err = self._update()
        if err is not None:
            # TODO(rickyx): backoff for transient errors?
            logger.error(f"Failed to run autoscaler iteration: {err}")

        #
