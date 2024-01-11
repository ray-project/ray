from calendar import c
import time

from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple
from attr import dataclass
from ray.autoscaler.v2.instance_manager import (
    InstanceManager,
)
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig, IConfigReader
from ray.autoscaler.v2.schema import InstanceUtil, AutoscalerInstance, NodeType
from ray.autoscaler.v2.scheduler import (
    IResourceScheduler,
    SchedulingReply,
    SchedulingRequest,
    TerminationReason,
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
    def compute_autoscaling_state(
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


class DefaultAutoscaler(IAutoscaler):
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

    def compute_autoscaling_state(
        self, cluster_resource_state: ClusterResourceState
    ) -> Optional[AutoscalingState]:
        """
        Get the autoscaling state. Overrides IAutoscaler.compute_autoscaling_state.

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

        # Merge the ray node states from the cluster resource state and the
        # instance manager state.
        instances = self._merge_ray_node_states(cluster_resource_state, im_state)

        # Get the scheduling decisions.
        sched_result = self._schedule(
            autoscaling_config, cluster_resource_state, im_state
        )
        to_launch = sched_result.to_launch

        # Compute the updates to existing instances.
        updates: List[InstanceUpdateEvent] = []

        updates.extend(self._make_updates_new_ray_nodes(instances))
        updates.extend(self._make_updates_dead_ray_nodes(instances))
        updates.extend(self._make_updates_stopping_instances(sched_result.to_terminate))

        self._update_instance_manager_state(updates, to_launch)
        updated_im_state = self._get_instance_manager_state()

        return self._make_autoscaling_state(
            updated_im_state,
            sched_result,
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
            cluster_resource_state: The cluster resource state.
            im_state: The instance manager state.

        Returns:
            List[AutoscalerInstance]: The merged ray node states.
        """
        cloud_ids_to_instances: Dict[str, AutoscalerInstance] = {}
        instances_without_cloud_instance_id = []

        for im_instance in im_state.instances:
            if im_instance.status in [Instance.STOPPED]:
                # Skip stopped instances.
                continue

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
                    )
                )
                continue

            cloud_ids_to_instances[cloud_instance_id] = AutoscalerInstance(
                cloud_instance_id=cloud_instance_id,
                im_instance=im_instance,
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
                        f"Duplicate ray node {cloud_instance_id} found: node "
                        f"{ray_node.node_id.decode('utf-8')} "
                        f"and node {cloud_ids_to_instances[cloud_instance_id].ray_node.node_id.decode('utf-8')}."  # noqa E501
                        f"Ignoring the new node {ray_node.node_id.decode('utf-8')}"
                    )
                    continue

                cloud_ids_to_instances[cloud_instance_id].ray_node = ray_node

        return (
            list(cloud_ids_to_instances.values()) + instances_without_cloud_instance_id
        )

    def _make_updates_stopping_instances(
        self,
        to_stop: Dict[AutoscalerInstance, TerminationReason],
    ) -> List[InstanceUpdateEvent]:

        updates = []
        for instance, reason in to_stop.items():
            detail_str = ""
            if reason == TerminationReason.IDLE_TERMINATE:
                # Drain the node for idle termination.
                assert instance.ray_node is not None, "ray node shouldn't be None"
                idle_time_ms = instance.ray_node.idle_duration_ms
                accepted = self._gcs_client.drain_node(
                    instance.ray_node.node_id.decode("utf-8"),
                    DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION,
                    f"Idle termination after {idle_time_ms / 1000} seconds",
                )
                if accepted:
                    # Update the instance status to RAY_STOPPING
                    detail_str = f"Ray node is idle for {idle_time_ms} ms"
                else:
                    logger.info(
                        f"Failed to drain ray node {instance.ray_node.node_id.decode('utf-8')} for idle termination"
                    )
                    continue
            elif reason in [
                TerminationReason.MAX_WORKER_NODES,
                TerminationReason.MAX_WORKER_NODES_PER_NODE_TYPE,
            ]:
                # Drain the node for max worker nodes.
                assert instance.ray_node is not None, "ray node shouldn't be None"
                self._gcs_client.drain_node(
                    instance.ray_node.node_id.decode("utf-8"),
                    DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION,
                    f"Terminating extra node for {reason}",
                )
                detail_str = f"Terminating extra node for {reason}"

            assert detail_str != "", "detail_str shouldn't be empty"
            updates.append(
                InstanceUpdateEvent(
                    instance_id=instance.im_instance.instance_id,
                    new_instance_status=Instance.RAY_STOPPING,
                    details=detail_str,
                )
            )
            InstanceUtil.set_status(instance.im_instance, Instance.RAY_STOPPING)

    def _make_updates_new_ray_nodes(
        self,
        instances: List[AutoscalerInstance],
    ) -> List[InstanceUpdateEvent]:
        """Get the updates for new Ray nodes.

        For an instance manager instance that's not running ray yet, if we
        discover a ray node for it, we update the instance status to RAY_RUNNING.

        Args:
            update_req: The update request.
            instances: The autoscaler instances.
        """
        im_updates = []
        for instance in instances:
            if instance.im_instance is None or instance.ray_node is None:
                # Skip instances that InstanceManager isn't aware of, as well
                # as instances that have no ray yet.
                continue

            # Mark any ray-running pending instances that are now running ray
            # as RAY_RUNNING.
            if (
                InstanceUtil.is_ray_running_reachable(instance.im_instance.status)
                and instance.is_ray_running()
            ):
                # Update the instance manager instance status to RAY_RUNNING.
                im_updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.im_instance.instance_id,
                        new_instance_status=Instance.RAY_RUNNING,
                        details="Running ray node",
                    )
                )
                instance.im_instance.status = Instance.RAY_RUNNING

        logger.debug(f"Instance updates: {len(im_updates)} new ray nodes")
        return im_updates

    def _make_updates_dead_ray_nodes(
        self,
        instances: List[AutoscalerInstance],
    ) -> List[InstanceUpdateEvent]:
        """Get the updates for dead Ray nodes.

        For an instance manager instance that's running ray, if we discover
        the ray node is dead, we update the instance status to RAY_STOPPED.

        """
        im_updates = []
        for instance in instances:
            if instance.im_instance is None or instance.ray_node is None:
                # Skip instances that InstanceManager isn't aware of, as well
                # as instances that have no ray yet.
                continue

            if (
                InstanceUtil.is_ray_stop_reachable(instance.im_instance.status)
                and instance.is_ray_stop()
            ):
                # Update the instance status to RAY_STOPPED.
                im_updates.append(
                    InstanceUpdateEvent(
                        instance_id=instance.im_instance.instance_id,
                        new_instance_status=Instance.RAY_STOPPED,
                        details="Ray node is dead",
                    )
                )
                InstanceUtil.set_status(instance.im_instance, Instance.RAY_STOPPED)

        return im_updates

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
            gang_resource_requests=cluster_resource_state.pending_gang_resource_requests,  # noqa E501
            cluster_resource_constraints=cluster_resource_state.cluster_resource_constraints,  # noqa E501
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
        self,
        updates: List[InstanceUpdateEvent],
        to_launch: Dict[NodeType, int],
    ) -> None:
        """Update the instance manager state.

        Args:
            update_req: The update request.

        Returns:
            The updated instance manager state.

        Raises:
            AutoscalerError: If the instance manager state cannot be updated.
        """
        update_req = UpdateInstanceManagerStateRequest()
        update_req.expected_version = self._last_seen_instance_manager_state_version
        update_req.updates.extend(updates)
        update_req.launches.extend(
            [
                LaunchRequest(
                    instance_type=ray_node_type_name,
                    count=count,
                    id=str(time.ns()),
                    request_ts_ms=int(time.time() * 1000),
                )
                for ray_node_type_name, count in to_launch.items()
            ]
        )
        reply = self._instance_manager.update_instance_manager_state(request=update_req)

        if reply.status.code != StatusCode.OK:
            raise AutoscalerError(
                msg=f"Failed to update instance manager state: {reply.status.message}",
                status=reply.status,
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
