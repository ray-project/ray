import collections
import copy
import logging
import os
import time
import uuid
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Callable, Dict, List, Optional, Tuple, Union
from ray.autoscaler.v2.utils import ClusterStatusParser

import yaml

import ray
from ray._private.gcs_utils import PlacementGroupTableData
from typing import List

from ray._raylet import GcsClient
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
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceManager,
    InstanceUtil,
    get_failed_instance_requests,
    get_pending_instance_requests,
    get_pending_ray_nodes,
)
from ray.autoscaler.v2.scheduler import SimpleResourceScheduler
from ray.autoscaler.v2.schema import (
    ClusterStatus,
    IAutoscaler,
    Stats,
)
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    DrainNodeReason,
    NodeStatus,
    NodeState,
    GetClusterStatusReply,
    RayNodeKind,
)
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceManagerState,
    InstanceUpdateEvent,
    InstanceType,
    NodeTypeConfig,
    ResourceScheduleConfig,
    ScheduleResourceBundlesReply,
    ScheduleResourceBundlesRequest,
    UpdateInstanceManagerStateRequest,
    LaunchRequest,
)

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
    gcs_address: str, to_request: List[dict], timeout: int = DEFAULT_RPC_TIMEOUT_S
):
    """Request resources from the autoscaler.

    This will add a cluster resource constraint to GCS. GCS will asynchronously
    pass the constraint to the autoscaler, and the autoscaler will try to provision the
    requested minimal bundles in `to_request`.

    If the cluster already has `to_request` resources, this will be an no-op.
    Future requests submitted through this API will overwrite the previous requests.

    Args:
        gcs_address: The GCS address to query.
        to_request: A list of resource bundles to request the cluster to have.
            Each bundle is a dict of resource name to resource quantity, e.g:
            [{"CPU": 1}, {"GPU": 1}].
        timeout: Timeout in seconds for the request to be timeout

    """
    assert len(gcs_address) > 0, "GCS address is not specified."

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

    GcsClient(gcs_address).request_cluster_resource_constraint(
        bundles, counts, timeout_s=timeout
    )


def get_cluster_resource_state(
    gcs_client: Optional[GcsClient] = None, timeout: int = DEFAULT_RPC_TIMEOUT_S
) -> ClusterResourceState:
    """
    Get the cluster status from the autoscaler.

    Args:
        gcs_address: The GCS address to query. If not specified, will use the
            GCS address from the runtime context.
        timeout: Timeout in seconds for the request to be timeout

    Returns:
        A ClusterStatus object.
    """
    # TODO: use the get_cluster_resource_state endpoint instead.
    if not gcs_client:
        gcs_client = get_gcs_client()
    str_reply = gcs_client.get_cluster_status(timeout_s=timeout)
    reply = GetClusterStatusReply()
    reply.ParseFromString(str_reply)

    return reply.cluster_resource_state


def get_cluster_status(
    gcs_address: str, timeout: int = DEFAULT_RPC_TIMEOUT_S
) -> ClusterStatus:
    """
    Get the cluster status from the autoscaler.

    Args:
        gcs_address: The GCS address to query.
        timeout: Timeout in seconds for the request to be timeout

    Returns:
        A ClusterStatus object.
    """
    assert len(gcs_address) > 0, "GCS address is not specified."
    req_time = time.time()
    str_reply = GcsClient(gcs_address).get_cluster_status(timeout_s=timeout)
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

# TODO: for debugging only now.
_global_autoscaler = None


class AutoscalerV2(IAutoscaler):
    def __init__(
        self,
        config: dict,
        instance_manager: InstanceManager,
        gcs_client: GcsClient,
        # TODO:
        # 1. add the event logger
        # 2. add the metrics reporter
    ) -> None:
        super().__init__()

        # NOTE(rickyx): we need this since NodeProvider doesn't expose the config so far.
        self._instance_manager = instance_manager
        self._resource_demand_scheduler = None
        self._gcs_client = gcs_client
        self._config = copy.deepcopy(config)
        self._scheduler = SimpleResourceScheduler()

        # TODO: is the default correct?
        self._last_seen_version = -1

        # TODO:
        # 1. print the config
        # 2. validate the config
        # 3. maybe unify the config from NodeProviderConfig?

    def get_autoscaling_state(
        self, cluster_resource_state: ClusterResourceState
    ) -> Optional[AutoscalingState]:
        """
        Generate an instance manager state update for new instances.
        1. Idle stop nodes:
            - Send drain node requests to GCS
            - Mark these nodes as RAY_STOPPING
        2. Already stopped nodes:
            - Mark these nodes as RAY_STOPPED
        3. New nodes to launch:
            - Create new nodes with Instance.UNKNOWN status


        TODO:
            1. We currently don't remove any queueing launching requests from the
            instance manager state, but rely on the idle termination eventually.
            However, we could actually actively remove the queueing launching requests
            from the instance manager state if the scheduler determines that the cluster
            is over-provisioned.
        """

        # Get the updated instance manager states.
        im_state = self._instance_manager.get_instance_manager_state().state
        self._set_last_seen_version(im_state.version)

        # Run the scheduling run
        sched_result = self._schedule(cluster_resource_state, im_state)

        updates: List[InstanceUpdateEvent] = []

        updates.extend(self._updates_for_extra_nodes(cluster_resource_state))
        updates.extend(self._updates_for_idle_terminated_nodes(cluster_resource_state))
        updates.extend(
            self._updates_for_ray_stopped_nodes(cluster_resource_state, im_state)
        )
        updates.extend(
            self._updates_for_running_ray_nodes(cluster_resource_state, im_state)
        )

        # TODO: are there dependencies between stopping nodes and the to launch nodes?
        # Maybe not the stopped nodes, since these are already reflected in the cluster
        # resource state.
        # But for stopping nodes (idle terminated nodes). We should not be launching
        # another node with the same node type.
        # self._check_stopping_nodes(stopping_nodes, sched_result.to_launch_nodes)

        # Update the instance manager state
        to_launch = self._get_nodes_to_launch(sched_result)
        if updated_im_state := self._update_instances(
            updates=updates,
            to_launch=to_launch,
        ):
            # Updated the instance manager state
            next_im_state = updated_im_state
        else:
            # No update
            next_im_state = im_state

        # Make the autoscaler state.
        return self._make_autoscaler_state(
            cluster_state=cluster_resource_state,
            sched_result=sched_result,
            im_state=next_im_state,
        )

    def _get_nodes_to_launch(
        self, sched_result: ScheduleResourceBundlesReply
    ) -> List[LaunchRequest]:
        to_launch: List[LaunchRequest] = []
        current_cluster = dict(sched_result.current_cluster_shape)
        target_cluster = dict(sched_result.target_cluster_shape)

        for node_type, count in target_cluster.items():
            if count > current_cluster.get(node_type, 0):
                to_launch.append(
                    LaunchRequest(
                        instance_type=node_type,
                        count=count - current_cluster.get(node_type, 0),
                        id=str(time.time_ns()),
                        request_ts=int(time.time()),
                    )
                )

        return to_launch

    def _schedule(
        self,
        cluster_resource_state: ClusterResourceState,
        instance_manager_state: InstanceManagerState,
    ) -> ScheduleResourceBundlesReply:
        current_instances = [
            i
            for i in instance_manager_state.instances
            if InstanceUtil.is_pending(i) or InstanceUtil.is_allocated(i)
        ]

        req = ScheduleResourceBundlesRequest(
            resource_requests=cluster_resource_state.pending_resource_requests,
            gang_resource_requests=cluster_resource_state.pending_gang_resource_requests,
            cluster_resource_constraints=cluster_resource_state.cluster_resource_constraints,
            # NOTE: this could also just be instances from IM state?
            # The only missing info is the dynamic labels.
            node_states=cluster_resource_state.node_states,
            current_instances=current_instances,
            schedule_config=self.schedule_config,
        )

        reply = self._scheduler.schedule_resource_bundles(req)

        return reply

    @property
    def schedule_config(self) -> ResourceScheduleConfig:
        # TODO: we need to prefill this thing.
        assert (
            self._config.get("available_node_types", None) is not None
        ), "Available node types must be set."

        sched_config = ResourceScheduleConfig()
        for node_type_name, node_config in self._config["available_node_types"].items():
            node_type_config = sched_config.node_type_configs[node_type_name]
            node_type_config.name = node_type_name
            if node_config.get("resources"):
                node_type_config.resources.update(node_config["resources"])
            if node_config.get("max_workers"):
                node_type_config.max_workers = node_config["max_workers"]
            if node_config.get("min_workers"):
                node_type_config.min_workers = node_config["min_workers"]
            if node_config.get("labels"):
                node_type_config.labels = node_config["labels"]

        if self._config.get("max_workers"):
            sched_config.max_num_nodes = self._config["max_workers"] + 1

        return sched_config

    @property
    def idle_terminate_time_s(self) -> int:
        # TODO: what if empty? config validation or default populations.
        return self._config["idle_timeout_minutes"] * 60

    def _updates_for_drain_nodes(
        self, nodes: List[NodeState], reasons: List[Tuple["DrainNodeReason", str]]
    ) -> List[InstanceUpdateEvent]:
        assert len(nodes) == len(reasons)

        updates = []
        for node, reason in zip(nodes, reasons):
            reason_code, reason_msg = reason
            # Maybe batch the drain node requests?
            reply = self._gcs_client.drain_node(
                node_id=node.node_id,
                reason=reason_code,
                reason_msg=reason_msg,
            )
            if reply.is_accepted:
                updates.append(
                    InstanceUpdateEvent(
                        instance_id=node.instance_id,
                        new_ray_status=Instance.RAY_STOPPING,
                    )
                )
            else:
                # TODO: track failed to drain node.
                pass

        return updates

    def _updates_for_running_ray_nodes(
        self, cluster_state: ClusterResourceState, im_state: InstanceManagerState
    ) -> List[InstanceUpdateEvent]:
        updates = []
        # Generate ray status updates for nodes which are running ray
        # but still marked as RAY_STATUS_UNKNOWN in the instance manager state.
        running_ray_nodes = set()

        for node in cluster_state.node_states:
            if node.status in [NodeStatus.RUNNING, NodeStatus.IDLE]:
                running_ray_nodes.add(node.instance_id)

        for instance in im_state.instances:
            if instance.instance_id not in running_ray_nodes:
                continue

            if instance.ray_status != Instance.RAY_STATUS_UNKNOWN:
                continue

            if instance.instance_status != Instance.ALLOCATED:
                logger.error(
                    f"A running ray node {instance.instance_id} is not in "
                    "ALLOCATED status, but is RAY_STATUS_UNKNOWN."
                )
                continue

            updates.append(
                InstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_ray_status=Instance.RAY_RUNNING,
                )
            )

        return updates

    def _updates_for_extra_nodes(
        self, state: ClusterResourceState
    ) -> List[InstanceUpdateEvent]:
        running_nodes_by_type = defaultdict(list)
        idle_nodes_by_type = defaultdict(list)
        alive_nodes_count_by_type = defaultdict(int)
        nodes_to_drain = []
        drain_reasons = []

        # Never try to terminate head node.
        worker_nodes = self._get_worker_nodes(state)

        # Group nodes by node type.
        for node in worker_nodes:
            if node.status not in [NodeStatus.IDLE, NodeStatus.RUNNING]:
                continue

            alive_nodes_count_by_type[node.ray_node_type_name] += 1
            if node.status == NodeStatus.IDLE:
                idle_nodes_by_type[node.ray_node_type_name].append(node)
            elif node.status == NodeStatus.RUNNING:
                running_nodes_by_type[node.ray_node_type_name].append(node)
            else:
                raise RuntimeError(f"Unexpected node status {node.status}")

        # Find out extra nodes with node type.
        for node_type, node_count in alive_nodes_count_by_type.items():
            node_type_config = self._config["available_node_types"][node_type]
            while node_count > node_type_config["max_workers"]:
                node = None
                # TODO: sorting the nodes by some sort of heuristic?
                if len(idle_nodes_by_type[node_type]) > 0:
                    node = idle_nodes_by_type[node_type].pop()
                elif len(running_nodes_by_type[node_type]) > 0:
                    node = running_nodes_by_type[node_type].pop()
                else:
                    raise RuntimeError(
                        f"Extra nodes but no idle or running nodes for node type {node_type}"
                    )
                node_count -= 1
                assert node is not None
                reason = (
                    f"Node is drained because the number of nodes of type {node_type} "
                    f"is {node_count} > max_workers={node_type_config['max_workers']}"
                )
                nodes_to_drain.append(node)
                drain_reasons.append(
                    (DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION, reason)
                )

        logger.debug(f"Alive nodes: {alive_nodes_count_by_type}")
        logger.debug(f"Running nodes by type: {running_nodes_by_type}")
        logger.debug(f"Idle nodes by type: {idle_nodes_by_type}")
        logger.debug(f"Draining {len(nodes_to_drain)} extra nodes.")

        num_max_workers = self._config.get("max_workers", None)
        if num_max_workers is None or num_max_workers >= len(worker_nodes):
            return self._updates_for_drain_nodes(nodes_to_drain, drain_reasons)

        # If there are extra nodes, we should also terminate the extra nodes.
        total_nodes = []

        # Add the idle nodes first
        for _, nodes in idle_nodes_by_type.items():
            total_nodes.extend(nodes)

        # Add the running nodes
        for _, nodes in running_nodes_by_type.items():
            total_nodes.extend(nodes)

        num_extra_nodes = len(total_nodes) - num_max_workers
        nodes_to_drain.extend(total_nodes[:num_extra_nodes])
        drain_reasons.extend(
            [
                (
                    DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION,
                    (
                        f"Node is drained because the total number of nodes is "
                        f"{len(total_nodes)} > max_workers={num_max_workers}"
                    ),
                )
            ]
            * num_extra_nodes
        )

        return self._update_for_drain_nodes(nodes_to_drain, drain_reasons)

    def _get_worker_nodes(self, state: ClusterResourceState) -> List[NodeState]:
        # Never try to idle terminate head node.
        return [
            node
            for node in state.node_states
            if node.ray_node_kind == RayNodeKind.WORKER
        ]

    def _updates_for_idle_terminated_nodes(
        self, state: ClusterResourceState
    ) -> List[InstanceUpdateEvent]:
        drain_nodes = []
        drain_reasons = []
        worker_nodes = self._get_worker_nodes(state)
        for node in worker_nodes:
            if (
                node.status == NodeStatus.IDLE
                and node.idle_duration_ms // 1000 > self.idle_terminate_time_s
            ):
                reason = (
                    DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION,
                    (
                        f"Node is idle for {node.idle_duration_ms // 1000} "
                        f"seconds > timeout={self.idle_terminate_time_s} "
                        "seconds."
                    ),
                )
                drain_nodes.append(node)
                drain_reasons.append(reason)

        return self._updates_for_drain_nodes(drain_nodes, drain_reasons)

    def _updates_for_ray_stopped_nodes(
        self, cluster_state: ClusterResourceState, im_state: InstanceManagerState
    ) -> List[InstanceUpdateEvent]:
        instances = im_state.instances
        dead_nodes_by_instance_id: Dict[str, NodeState] = {
            node.instance_id: node
            for node in cluster_state.node_states
            if node.status == NodeStatus.DEAD and node.instance_id
        }
        updates = []
        for instance in instances:
            if (
                instance.ray_status in (Instance.RAY_RUNNING, Instance.RAY_STOPPING)
                and instance.instance_id in dead_nodes_by_instance_id
            ):
                updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.instance_id,
                        new_ray_status=Instance.RAY_STOPPED,
                    )
                )

        return updates

    def _make_autoscaler_state(
        self,
        cluster_state: ClusterResourceState,
        sched_result: ScheduleResourceBundlesReply,
        im_state: InstanceManagerState,
    ) -> AutoscalingState:
        pending_instance_requests = get_pending_instance_requests(im_state)
        pending_nodes = get_pending_ray_nodes(im_state)
        failed_instance_requests = get_failed_instance_requests(im_state)

        state = AutoscalingState(
            last_seen_cluster_resource_state_version=cluster_state.cluster_resource_state_version,
            # We use the last seen instance storage version for the autoscaler state version.
            autoscaler_state_version=im_state.version,
            pending_instance_requests=pending_instance_requests,
            infeasible_resource_requests=sched_result.infeasible_resource_requests,
            infeasible_gang_resource_requests=sched_result.infeasible_gang_resource_requests,
            infeasible_cluster_resource_constraints=sched_result.infeasible_cluster_resource_constraints,
            pending_instances=pending_nodes,
            failed_instance_requests=failed_instance_requests,
        )

        return state

    def _update_instances(
        self,
        updates: List[InstanceUpdateEvent],
        to_launch: List[LaunchRequest],
    ) -> Optional[InstanceManagerState]:
        if len(updates) == 0 and len(to_launch) == 0:
            return None

        logger.info(f"Updates: {updates}")
        logger.info(f"To launch: {to_launch}")

        update_request = UpdateInstanceManagerStateRequest(
            expected_version=self._get_last_seen_version(),
            launch_requests=to_launch,
            updates=updates,
        )

        reply = self._instance_manager.update_instance_manager_state(
            request=update_request
        )
        if not reply.success:
            # TODO(rickyx): fail to update but not reasons?
            # 1. version mismatches
            # 2. to terminate instance not found?
            # NOTE instances launching should be happening in the bg
            logger.error("Failed to update instance manager state.")
            return None

        self._set_last_seen_version(reply.version)
        return reply.state

    def _set_last_seen_version(self, version: int) -> None:
        self._last_seen_version = version

    def _get_last_seen_version(self) -> int:
        return self._last_seen_version

    def update(self) -> None:
        # TODO:
        # 1. reset the config?

        # Get the most recent cluster status
        self._debug_states()
        try:
            cluster_resource_state = get_cluster_resource_state(self._gcs_client)
        except Exception as e:
            logger.error(f"Failed to get cluster status: {e}")
            raise e
            return AutoscalerError(AutoscalerError.Code.CONNECTION_ERROR, str(e))

        try:
            next_state = self.get_autoscaling_state(cluster_resource_state)
            if next_state:
                self._gcs_client.report_autoscaling_state(
                    state=next_state, timeout_s=DEFAULT_RPC_TIMEOUT_S
                )
            else:
                # TODO: errors
                logger.info("No update to autoscaler state.")
        except Exception as e:
            # TODO(rickyx): backoff for transient errors?
            logger.error(f"Failed to run autoscaler iteration: {e}")
            raise e

    def _debug_states(self) -> None:
        # Dump the instance manager states:

        im_state = self._instance_manager.get_instance_manager_state().state
        print("=============IM State ============")
        print(im_state)
