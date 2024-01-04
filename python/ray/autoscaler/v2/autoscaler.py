from calendar import c
import time

from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple
from attr import dataclass
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceManager,
    InstanceUtil,
)
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig, IConfigReader
from ray.autoscaler.v2.scheduler import (
    IResourceScheduler,
    SchedulingReply,
    SchedulingRequest,
)
from ray._raylet import GcsClient

from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    DrainNodeReason,
    NodeState,
    NodeStatus,
    PendingInstanceRequest,
    PendingInstance,
    FailedInstanceRequest,
)
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance,
    InstanceManagerState,
    InstanceUpdateEvent,
    StatusCode,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
    LaunchRequest,
)

import logging

logger = logging.getLogger(__name__)


class AutoscalerError(Exception):
    """Autoscaler error."""

    def __init__(self, msg: str, status: StatusCode) -> None:
        """
        Args:
            msg: The error message.
            status: The status code.
        """
        self.msg = msg
        self.status = status


class IAutoscaler(ABC):
    """The autoscaler interface."""

    @abstractmethod
    def get_autoscaling_state(
        self, cluster_resource_state: ClusterResourceState
    ) -> Optional[AutoscalingState]:
        """
        Get the autoscaling state.

        Args:
            cluster_resource_state: The cluster resource state that's obtained
                from GCS. This represents the current state of the ray cluster.

        Returns:
            The autoscaling state.
            Or None if the autoscaling state cannot be computed.

        Raises:
            AutoscalerError: If the autoscaling state cannot be computed.
        """
        pass


@dataclass
class AutoscalerInstance:
    """
    AutoscalerInstance represents an instance that's managed by the autoscaler.
    This includes two states:
        1. the instance manager state: information of the underlying cloud node
            and the exact status before/after ray runs.
        2. the ray node state. the ray node like existing resources.

    The two states are linked by the cloud_instance_id.

    It's possible that one state is missing, for example:
    - Instance state could be missing if the instance manager hasn't discovered
    the instance yet. (i.e. the node provider didn't inform the instance manager
    but already started a ray node)
    - Ray node state could be missing if the ray node has been stopped or hasn't
    started yet.
    """

    # The cloud instance id. It could be None if the instance manager hasn't
    # discovered the instance yet.
    cloud_instance_id: Optional[str] = None
    # The instance manager state.
    im_instance: Optional[Instance] = None
    # The ray node state.
    ray_node: Optional[NodeState] = None
    # The cached instance state as updates being populated. We need this
    # to avoid querying the instance manager state again.
    current_instance_status: Optional["Instance.InstanceStatus"] = None


class AutoscalerV2(IAutoscaler):
    def __init__(
        self,
        config_reader: IConfigReader,
        instance_manager: InstanceManager,
        scheduler: IResourceScheduler,
        gcs_client: GcsClient,
    ) -> None:
        """
        Args:
            config_reader: The config reader.
            instance_manager: The instance manager.
            scheduler: The scheduler.
            gcs_client: The GCS client.
        """
        super().__init__()
        self._config_reader = config_reader
        self._instance_manager = instance_manager
        self._scheduler = scheduler
        self._gcs_client = gcs_client

        # The last seen instance manager state version.
        self._last_seen_instance_manager_state_version: Optional[int] = None
        # The last seen cluster resource state version.
        self._last_seen_cluster_resource_state_version: Optional[int] = None

    def get_autoscaling_state(
        self, cluster_resource_state: ClusterResourceState
    ) -> Optional[AutoscalingState]:
        """
        Get the autoscaling state. Overrides IAutoscaler.get_autoscaling_state.

        On a high level this function does the following:
        1. Get the newest autoscaling config.
        2. Update the InstanceManager state with:
            a. The new ray nodes.
            b. The dead ray nodes.
            c. The idle terminated nodes.
            d. The extra nodes.
            e. The new nodes to be started after querying the scheduler.

        Args:
            cluster_resource_state: The cluster resource state that's obtained
                from GCS.

        Returns:
            The autoscaling state.
            Or None if the autoscaling state cannot be computed.

        Raises:
            AutoscalerError: If the autoscaling state cannot be computed, e.g.
                - If any version is stale.
                - If the instance manager state cannot be updated.
        """

        if (
            self._last_seen_cluster_resource_state_version is not None
            and cluster_resource_state.cluster_resource_state_version
            < self._last_seen_cluster_resource_state_version
        ):
            raise AutoscalerError(
                msg=f"Stale cluster resource state version: "
                f"{cluster_resource_state.cluster_resource_state_version} < "
                f"{self._last_seen_cluster_resource_state_version}",
                status=StatusCode.VERSION_MISMATCH,
            )
        self._last_seen_cluster_resource_state_version = (
            cluster_resource_state.cluster_resource_state_version
        )

        # Get the newest autoscaling config.
        autoscaling_config = self._config_reader.get_autoscaling_config()

        # Get the updated instance manager states.
        im_state = self._get_instance_manager_state()

        im_update = UpdateInstanceManagerStateRequest()
        # Get the scheduling decisions.
        sched_result = self._schedule(
            autoscaling_config, cluster_resource_state, im_state
        )
        target_cluster_shape = dict(sched_result.target_cluster_shape)

        # Merge the ray node states from the cluster resource state and the
        # instance manager state.
        instances = self._merge_ray_node_states(cluster_resource_state, im_state)

        self._get_updates_new_ray_nodes(im_update, instances)
        self._get_updates_dead_ray_nodes(im_update, instances)
        self._get_updates_idle_terminated_nodes(
            im_update, instances, autoscaling_config
        )
        self._get_updates_max_worker_nodes_per_node(
            im_update, autoscaling_config, instances
        )
        self._get_updates_max_nodes(im_update, autoscaling_config, instances)
        self._get_updates_nodes_to_launch(im_update, instances, target_cluster_shape)

        self._update_instance_manager_state(im_update)
        updated_im_state = self._get_instance_manager_state()

        return self._make_autoscaling_state(
            updated_im_state,
            sched_result,
        )

    def _make_autoscaling_state(
        self,
        im_state: InstanceManagerState,
        sched_result: SchedulingReply,
    ) -> AutoscalingState:
        """
        Make the autoscaling state.

        Args:
            im_state: The current instance manager state.
            sched_result: The scheduling result.

        Returns:
            The autoscaling state.
        """
        infeasible_resource_requests = []
        for r_count in sched_result.infeasible_resource_requests:
            infeasible_resource_requests.extend([r_count.request] * r_count.count)

        return AutoscalingState(
            # TODO: reject stale updates
            last_seen_cluster_resource_state_version=self._last_seen_cluster_resource_state_version,
            autoscaler_state_version=self._last_seen_instance_manager_state_version,
            pending_instance_requests=self._get_pending_instances_requests(im_state),
            infeasible_resource_requests=infeasible_resource_requests,
            infeasible_gang_resource_requests=sched_result.infeasible_gang_resource_requests,
            infeasible_cluster_resource_constraints=sched_result.infeasible_cluster_resource_constraints,
            pending_instances=self._get_pending_instances(im_state),
            failed_instance_requests=self._get_failed_instance_requests(im_state),
        )

    def _get_pending_instances_requests(
        self, im_state: InstanceManagerState
    ) -> List[PendingInstanceRequest]:
        pending_instance_requests = []
        ns_to_s = 10**9
        for instance in im_state.instances:
            if instance.status in [Instance.REQUESTED, Instance.QUEUED]:
                pending_instance_requests.append(
                    PendingInstanceRequest(
                        ray_node_type_name=instance.instance_type,
                        count=1,
                        request_ts=InstanceUtil.get_status_times_ns(instance.status)
                        // ns_to_s
                        if instance.status == Instance.REQUESTED
                        else 0,
                    )
                )
        return pending_instance_requests

    def _get_pending_instances(
        self, im_state: InstanceManagerState
    ) -> List[PendingInstance]:
        pending_instances = []
        for instance in im_state.instances:
            if instance.status in [Instance.ALLOCATED, Instance.RAY_INSTALLING]:
                pending_instances.append(
                    PendingInstance(
                        ray_node_type_name=instance.instance_type,
                        instance_id=instance.cloud_instance_id,
                        ip_address=instance.external_ip,
                    )
                )

        return pending_instances

    def _get_failed_instance_requests(
        self, im_state: InstanceManagerState
    ) -> List[FailedInstanceRequest]:
        failed_instance_requests = []
        ns_to_s = 10**9
        for instance in im_state.instances:
            if instance.status in [
                Instance.ALLOCATION_FAILED,
                Instance.RAY_INSTALL_FAILED,
            ]:
                start_ts = (
                    InstanceUtil.get_status_times_ns(
                        instance, select_instance_status=Instance.QUEUED
                    )[0]
                    // ns_to_s
                )
                failed_ts = (
                    InstanceUtil.get_status_times_ns(
                        instance, select_instance_status=instance.status
                    )[0]
                    // ns_to_s
                )

                failed_instance_requests.append(
                    FailedInstanceRequest(
                        ray_node_type_name=instance.instance_type,
                        count=1,
                        reason=instance.exit_details,
                        start_ts=start_ts,
                        failed_ts=failed_ts,
                    )
                )
        return failed_instance_requests

    def _get_updates_nodes_to_launch(
        self,
        update_req: UpdateInstanceManagerStateRequest,
        instances: List[AutoscalerInstance],
        target_cluster_shape: Dict[str, int],
    ) -> None:

        instances_by_type = self._get_target_cluster(instances)

        for node_type, target_num_nodes in target_cluster_shape.items():
            num_nodes_to_launch = max(
                0, target_num_nodes - len(instances_by_type.get(node_type, []))
            )

            if num_nodes_to_launch > 0:
                update_req.launch_requests.append(
                    LaunchRequest(
                        instance_type=node_type,
                        count=num_nodes_to_launch,
                        id=str(time.ns()),
                        request_ts_ms=int(time.time() * 1000),
                    )
                )

    def _get_updates_idle_terminated_nodes(
        self,
        update_req: UpdateInstanceManagerStateRequest,
        instances: List[AutoscalerInstance],
        autoscaling_config: AutoscalingConfig,
    ) -> None:

        idle_terminate_threshold_ms = (
            autoscaling_config.get_idle_terminate_threshold_s() * 1000
        )

        for instance in instances:
            if (
                instance.im_instance is not None
                and instance.ray_node is not None
                and instance.ray_node.status == NodeStatus.IDLE
                and instance.ray_node.idle_duration_ms > idle_terminate_threshold_ms
            ):
                # Drain the ray stop protocol.
                if self._gcs_client.drain_node(
                    instance.ray_node.node_id.decode("utf-8"),
                    DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION,
                    f"Idle termination of {idle_terminate_threshold_ms} ms",
                ):
                    # Update the instance status to RAY_STOPPING
                    update_req.updates.append(
                        InstanceUpdateEvent(
                            new_instance_status=Instance.RAY_STOPPING,
                            details=f"Ray node is idle for {instance.ray_node.idle_duration_ms} ms "
                            f"> idle termination of {idle_terminate_threshold_ms} ms",
                        )
                    )
                else:
                    logger.info(
                        f"Failed to drain ray node {instance.ray_node.node_id.decode('utf-8')} for idle termination"
                    )

    def _merge_ray_node_states(
        self,
        cluster_resource_state: ClusterResourceState,
        im_state: InstanceManagerState,
    ) -> List[AutoscalerInstance]:
        """Merge the ray node states from the cluster resource state and the
        instance manager state.

        For each instance manager Instance, we try to find the corresponding
        ray node state from the cluster resource state.

        Args:
            cluster_resource_state (ClusterResourceState): The cluster resource state.
            im_state (InstanceManagerState): The instance manager state.

        Returns:
            List[AutoscalerInstance]: The merged ray node states.
        """
        cloud_ids_to_instances: Dict[str, AutoscalerV2.AutoscalerInstance] = {}
        instances_without_cloud_instance_id = []

        for im_instance in im_state.instances:
            cloud_instance_id = (
                im_instance.cloud_instance_id
                if im_instance.HasField("cloud_instance_id")
                else ""
            )

            # This is an autoscaler instance that doesn't have a cloud instance
            # assigned.
            if not cloud_instance_id:
                instances_without_cloud_instance_id.append(
                    AutoscalerInstance(
                        im_instance=im_instance,
                        current_instance_status=im_instance.status,
                    )
                )
                continue

            cloud_ids_to_instances[cloud_instance_id] = AutoscalerInstance(
                cloud_instance_id=cloud_instance_id,
                im_instance=im_instance,
                current_instance_status=im_instance.status,
            )

        for ray_node in cluster_resource_state.node_states:
            cloud_instance_id = ray_node.instance_id

            if cloud_instance_id not in cloud_ids_to_instances:
                # These are the ray nodes that are not known by the instance
                # manager. They should eventually be discovered by the instance
                # manager after reconciliation or it's an ray node not managed by
                # the instance manager.
                cloud_ids_to_instances[cloud_instance_id] = AutoscalerInstance(
                    cloud_instance_id=cloud_instance_id,
                    ray_node=ray_node,
                )
            else:
                if cloud_ids_to_instances[cloud_instance_id].ray_node is not None:
                    # TODO: handle this error. For now, this should no happen since we
                    # require unique ray node for a cloud node instance as a contract
                    # with node providers.
                    logger.error(
                        f"Duplicate ray node {cloud_instance_id} found: node {ray_node.node_id.decode('utf-8')} "
                        f"and node {cloud_ids_to_instances[cloud_instance_id].ray_node.node_id.decode('utf-8')}."
                        f"Ignoring the new node {ray_node.node_id.decode('utf-8')}"
                    )
                    continue

                cloud_ids_to_instances[cloud_instance_id].ray_node = ray_node

        return (
            list(cloud_ids_to_instances.values()) + instances_without_cloud_instance_id
        )

    def _get_updates_new_ray_nodes(
        self,
        update_req: UpdateInstanceManagerStateRequest,
        instances: List[AutoscalerInstance],
    ) -> None:
        """Get the updates for new Ray nodes.

        For an instance manager instance that's not running ray yet, if we
        discover a ray node for it, we update the instance status to RAY_RUNNING.

        Args:
            update_req: The update request.
            instances: The autoscaler instances.

        """
        for instance in instances:
            if (
                instance.im_instance is not None
                and instance.ray_node is not None
                and InstanceUtil.is_ray_running_reachable(
                    instance.current_instance_status
                )
                and instance.ray_node.status in [NodeStatus.RUNNING, NodeStatus.IDLE]
            ):
                # Update the instance status to RAY_RUNNING.
                update_req.updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.im_instance.instance_id,
                        new_instance_status=Instance.RAY_RUNNING,
                        details="Running ray node",
                    )
                )
                instance.current_instance_status = Instance.RAY_RUNNING

    def _get_updates_dead_ray_nodes(
        self,
        update_req: UpdateInstanceManagerStateRequest,
        instances: List[AutoscalerInstance],
    ) -> None:
        """Get the updates for dead Ray nodes.

        For an instance manager instance that's running ray, if we discover
        the ray node is dead, we update the instance status to RAY_STOPPED.

        """

        for instance in instances:
            if (
                instance.im_instance is not None
                and instance.ray_node is not None
                and InstanceUtil.is_ray_stop_reachable(instance.current_instance_status)
                and instance.ray_node.status == NodeStatus.DEAD
            ):
                # Update the instance status to RAY_STOPPED.
                update_req.updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.im_instance.instance_id,
                        new_instance_status=Instance.RAY_STOPPED,
                        details="Ray node is dead",
                    )
                )
                instance.current_instance_status = Instance.RAY_STOPPED

    def _get_target_cluster(
        self, instances: List[AutoscalerInstance]
    ) -> Dict[str, List[AutoscalerInstance]]:
        """
        Get the target cluster shape with instances grouped by node type.

        These return instances that are:
        1. Actively running ray, OR
        2. Pending (e.g. requested, queued, allocated, installing ray etc.) where
        it's possible to run ray and become part of the ray cluster.

        Args:
            instances: The autoscaler instances.

        Returns:
            Dict[str, List[AutoscalerInstance]]: The target cluster shape
                with instances grouped by node type.

        """
        instances_by_type = defaultdict(list)
        for instance in instances:
            # Skip instances that InstanceManager isn't aware of. These could be
            # ray nodes that are not managed by autoscaler.
            if instance.im_instance is None:
                continue

            # Add all the instances that could contribute to the cluster shape,
            # including instances that are not running ray yet.
            if (
                InstanceUtil.is_ray_running_reachable(instance.current_instance_status)
                or instance.current_instance_status == Instance.RAY_RUNNING
            ):
                instances_by_type[instance.im_instance.instance_type].append(instance)

        return instances_by_type

    def _get_updates_max_nodes(
        self,
        update_req: UpdateInstanceManagerStateRequest,
        autoscaling_config: AutoscalingConfig,
        instances: List[AutoscalerInstance],
    ) -> None:
        # Enforce max number of nodes for all.
        target_instances_by_type = self._get_target_cluster(instances)
        total_num_nodes = sum([len(ll) for ll in target_instances_by_type.values()])
        extra_nodes_all = max(
            0,
            total_num_nodes
            - (autoscaling_config.get_max_num_worker_nodes() + 1),  # For head node
        )

        # Iterate through each worker node type in a round-robin manner and terminate
        # one instance from each type if possible.
        # This is an arbitrary policy for default, if needed, we should make this
        # configurable.
        logger.info(f"Terminating {extra_nodes_all} extra nodes...")
        while extra_nodes_all > 0:
            terminate_any = False
            for node_type, instances_list in target_instances_by_type.items():
                if len(instances_list) == 0:
                    continue

                min_worker_nodes = autoscaling_config.get_node_type_configs()[
                    node_type
                ].min_worker_nodes

                if min_worker_nodes >= len(instances_list):
                    # No extra nodes to terminate for this node type.
                    continue

                instance = instances_list.pop()
                if instance.ray_node is not None:
                    self._gcs_client._drain_node(
                        instance.ray_node.node_id.decode("utf-8"),
                        DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION,
                        "Terminating extra node",
                    )
                update_req.updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.im_instance.instance_id,
                        new_instance_status=Instance.STOPPING,
                        details="Terminating extra node",
                    )
                )
                instance.current_instance_status = Instance.STOPPING
                extra_nodes_all -= 1
                terminate_any = True

                if extra_nodes_all == 0:
                    break

            if not terminate_any and extra_nodes_all > 0:
                # We have extra nodes to terminate but we can't find any node to
                # terminate, this is an error.
                logger.error(
                    f"Failed to terminate extra nodes, extra_nodes remained={extra_nodes_all},",
                    f"target_instances_by_type={target_instances_by_type}",
                )

    def _get_updates_max_worker_nodes_per_node(
        self,
        update_req: UpdateInstanceManagerStateRequest,
        autoscaling_config: AutoscalingConfig,
        instances: List[AutoscalerInstance],
    ) -> None:
        """Get the updates for extra nodes.

        Create instances updates for the extra nodes by terminating them.
        """
        extra_nodes_to_terminate_count = defaultdict(int)
        # Get the node types configs.
        node_type_configs = autoscaling_config.get_node_type_configs()

        # Group current instances by node type.
        target_instances_by_type = self._get_target_cluster(instances)

        # Compute the extra nodes to terminate per type.
        for node_type, instances_list in target_instances_by_type.items():
            if node_type not in node_type_configs:
                # No longer exists in the config, terminate all the instances.
                extra_nodes_to_terminate_count[node_type] = len(instances_list)

            # Compute the number of extra instances to terminate.
            extra_nodes_to_terminate_count[node_type] = max(
                0, len(instances_list) - node_type_configs[node_type].max_worker_nodes
            )

        # Enforce max number of worker nodes instances per node type.
        for node_type, instances_list in target_instances_by_type.items():
            num_node_to_terminate = extra_nodes_to_terminate_count[node_type]
            if num_node_to_terminate == 0:
                continue

            instances_list.sort(key=self._sort_instances_to_terminate)
            assert len(instances_list) >= num_node_to_terminate
            for instance in instances_list[:num_node_to_terminate]:
                if instance.ray_node is not None:
                    self._gcs_client.drain_node(
                        instance.ray_node.node_id.decode("utf-8"),
                        DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION,
                        "Terminating extra node",
                    )
                update_req.updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.im_instance.instance_id,
                        new_instance_status=Instance.STOPPING,
                        details="Terminating extra node",
                    )
                )
                instance.current_instance_status = Instance.STOPPING

    # Select the instances to terminate.
    @staticmethod
    def _sort_instances_to_terminate(
        instance: AutoscalerInstance,
    ) -> Tuple[bool, int, float]:
        # Sort the instances to terminate non-gracefully, in the following order:
        #  1. first if the instances are not running ray.
        #  2. then if instances are idle.
        #  3. then with lowest resources util in avg across all resources.
        # Instances with lower rankings will be terminated first.

        running_ray = instance.ray_node is not None
        idle_time_ms = (
            instance.ray_node.idle_time_ms if instance.ray_node is not None else 0
        )

        utils_per_resources = {}
        if instance.ray_node is None:
            return (running_ray, idle_time_ms, 0)

        for resource, total in instance.ray_node.total_resources.items():
            if total == 0:
                utils_per_resources[resource] = 0
            else:
                utils_per_resources[resource] = (
                    total - instance.ray_node.resources_available.get(resource, 0)
                ) / total

        avg_util = (
            sum(utils_per_resources.values()) / len(utils_per_resources)
            if utils_per_resources
            else 0
        )

        return (running_ray, idle_time_ms, avg_util)

    def _schedule(
        self,
        config: AutoscalingConfig,
        cluster_resource_state: ClusterResourceState,
        im_state: InstanceManagerState,
    ) -> SchedulingReply:
        """Schedule the resources."""
        req = SchedulingRequest(
            node_type_configs=config.get_node_type_configs(),
            max_num_worker_nodes=config.get_max_num_worker_nodes(),
            resource_requests=cluster_resource_state.pending_resource_requests,
            gang_resource_requests=cluster_resource_state.pending_gang_resource_requests,
            cluster_resource_constraints=cluster_resource_state.cluster_resource_constraints,
            current_nodes=cluster_resource_state.node_states,
            current_instances=im_state.instances,
        )
        return self._scheduler.schedule(req)

    def _get_instance_manager_state(self) -> InstanceManagerState:
        """Get the instance manager state

        Returns:
            The instance manager state.

        Raises:
            AutoscalerError: If the instance manager state cannot be obtained.
        """
        reply = self._instance_manager.get_instance_manager_state(
            GetInstanceManagerStateRequest()
        )

        if reply.status.code != StatusCode.OK:
            raise AutoscalerError(
                msg=f"Failed to get instance manager state: {reply.status.message}",
                status=reply.status,
            )

        self._last_seen_instance_manager_state_version = reply.state.version
        return reply.state

    def _update_instance_manager_state(
        self, update_req: UpdateInstanceManagerStateRequest
    ) -> InstanceManagerState:
        """Update the instance manager state.

        Args:
            update_req: The update request.

        Returns:
            The updated instance manager state.

        Raises:
            AutoscalerError: If the instance manager state cannot be updated.
        """
        update_req.expected_version = self._last_seen_instance_manager_state_version
        reply = self._instance_manager.update_instance_manager_state(request=update_req)

        if reply.status.code != StatusCode.OK:
            raise AutoscalerError(
                msg=f"Failed to update instance manager state: {reply.status.message}",
                status=reply.status,
            )
