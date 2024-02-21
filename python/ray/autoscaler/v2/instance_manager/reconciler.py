import logging
import math
import uuid
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from ray._private.protobuf_compat import message_to_dict
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstallError
from ray.autoscaler.v2.scheduler import IResourceScheduler
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    NodeState,
    NodeStatus,
)
from ray.core.generated.instance_manager_pb2 import GetInstanceManagerStateRequest
from ray.core.generated.instance_manager_pb2 import Instance as IMInstance
from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent as IMInstanceUpdateEvent,
)
from ray.core.generated.instance_manager_pb2 import (
    StatusCode,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class Reconciler:
    """
    A singleton class that reconciles the instance states of the instance manager
    for autoscaler.

    """

    @staticmethod
    def reconcile(
        instance_manager: InstanceManager,
        scheduler: IResourceScheduler,
        cloud_provider: ICloudInstanceProvider,
        ray_cluster_resource_state: ClusterResourceState,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
        ray_install_errors: List[RayInstallError],
        autoscaling_config: AutoscalingConfig,
    ) -> AutoscalingState:
        """
        The reconcile method computes InstanceUpdateEvents for the instance manager
        by:

        1. Reconciling the instance manager's instances with external states like
        the cloud provider's, the ray cluster's states, the ray installer's results.
        It performs "passive" status transitions for the instances (where the status
        transition should only be reflecting the external states of the cloud provider
        and the ray cluster, and should not be actively changing them)

        2. Stepping the instances to the active states by computing instance status
        transitions that are needed and updating the instance manager's state.
        These transitions should be "active" where the transitions have side effects
        (through InstanceStatusSubscriber) to the cloud provider and the ray cluster.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_cluster_resource_state: The ray cluster's resource state.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
            cloud_provider_errors: The errors from the cloud provider.
            ray_install_errors: The errors from RayInstaller.

        """
        Reconciler._sync_from(
            instance_manager=instance_manager,
            ray_nodes=ray_cluster_resource_state.node_states,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
            cloud_provider_errors=cloud_provider_errors,
            ray_install_errors=ray_install_errors,
        )

        autoscaling_state = AutoscalingState()
        Reconciler._step_next(
            autoscaling_state=autoscaling_state,
            instance_manager=instance_manager,
            scheduler=scheduler,
            cloud_provider=cloud_provider,
            ray_cluster_resource_state=ray_cluster_resource_state,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
            autoscaling_config=autoscaling_config,
        )
        return autoscaling_state

    @staticmethod
    def _sync_from(
        instance_manager: InstanceManager,
        ray_nodes: List[NodeState],
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
        ray_install_errors: List[RayInstallError],
    ):
        """
        Reconcile the instance states of the instance manager from external states like
        the cloud provider's, the ray cluster's states, the ray installer's results,
        etc.

        For each instance, we try to figure out if we need to transition the instance
        status to a new status, and if so, what the new status should be.

        These transitions should be purely "passive", meaning they should only be
        reflecting the external states of the cloud provider and the ray cluster,
        and should not be actively changing the states of the cloud provider or the ray
        cluster.

        More specifically, we will reconcile status transitions for:
            1.  QUEUED/REQUESTED -> ALLOCATED:
                When a instance with launch request id (indicating a previous launch
                request was made) could be assigned to an unassigned cloud instance
                of the same instance type.
            2.  REQUESTED -> ALLOCATION_FAILED:
                When there's an error from the cloud provider for launch failure so
                that the instance becomes ALLOCATION_FAILED.
            3.  * -> RAY_RUNNING:
                When a ray node on a cloud instance joins the ray cluster, we will
                transition the instance to RAY_RUNNING.
            4.  * -> TERMINATED:
                When the cloud instance is already terminated, we will transition the
                instance to TERMINATED.
            5.  TERMINATING -> TERMINATION_FAILED:
                When there's an error from the cloud provider for termination failure.
            6.  * -> RAY_STOPPED:
                When ray was stopped on the cloud instance, we will transition the
                instance to RAY_STOPPED.
            7.  * -> RAY_INSTALL_FAILED:
                When there's an error from RayInstaller.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's states of ray nodes.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
            cloud_provider_errors: The errors from the cloud provider.
            ray_install_errors: The errors from RayInstaller.

        """

        # Handle 1 & 2 for cloud instance allocation.
        Reconciler._handle_cloud_instance_allocation(
            instance_manager,
            non_terminated_cloud_instances,
            cloud_provider_errors,
        )
        Reconciler._handle_cloud_instance_terminated(
            instance_manager, non_terminated_cloud_instances
        )
        Reconciler._handle_ray_status_transition(instance_manager, ray_nodes)

        Reconciler._handle_cloud_instance_termination_errors(
            instance_manager, cloud_provider_errors
        )

        Reconciler._handle_ray_install_failed(instance_manager, ray_install_errors)

    @staticmethod
    def _step_next(
        autoscaling_state: AutoscalingState,
        instance_manager: InstanceManager,
        scheduler: IResourceScheduler,
        cloud_provider: ICloudInstanceProvider,
        ray_cluster_resource_state: ClusterResourceState,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        autoscaling_config: AutoscalingConfig,
    ):
        """
        Step the reconciler to the next state by computing instance status transitions
        that are needed and updating the instance manager's state.

        Specifically, we will:
            1. Shut down leak cloud instances
                Leaked cloud instances that are not managed by the instance manager.
            2. Terminating instances with ray stopped or ray install failure.
            3. Scale up/down the cluster:
              (* -> RAY_STOPPING)
                b. Extra cloud due to max nodes config.
                c. Cloud instances with outdated configs.
            4. Create new instances
              (new QUEUED)
                Create new instances based on the IResourceScheduler's decision for
                scaling up.
            5. Request cloud provider to launch new instances.
              (QUEUED -> REQUESTED)
            6. Install ray
              (ALLOCATED -> RAY_INSTALLING)
                When ray could be installed and launched.
            7. Handle any stuck instances with timeouts.

        Args:
            instance_manager: The instance manager to reconcile.
            scheduler: The resource scheduler to make scaling decisions.
            ray_cluster_resource_state: The ray cluster's resource state.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.

        """

        Reconciler._handle_instances_launch(
            instance_manager=instance_manager, autoscaling_config=autoscaling_config
        )

    #######################################################
    # Utility methods for reconciling instance states.
    #######################################################

    @staticmethod
    def _handle_cloud_instance_allocation(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
    ):
        im_instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        # Compute intermediate states.
        instances_with_launch_requests: List[IMInstance] = [
            instance for instance in im_instances if instance.launch_request_id
        ]
        assigned_cloud_instance_ids: Set[CloudInstanceId] = {
            instance.cloud_instance_id for instance in im_instances
        }
        launch_errors: Dict[str, LaunchNodeError] = {
            error.request_id: error
            for error in cloud_provider_errors
            if isinstance(error, LaunchNodeError)
        }
        unassigned_cloud_instances_by_type: Dict[
            str, List[CloudInstance]
        ] = defaultdict(list)

        for cloud_instance_id, cloud_instance in non_terminated_cloud_instances.items():
            if cloud_instance_id not in assigned_cloud_instance_ids:
                unassigned_cloud_instances_by_type[cloud_instance.node_type].append(
                    cloud_instance
                )

        # Sort the request instance by the increasing request time.
        instances_with_launch_requests.sort(
            key=lambda instance: InstanceUtil.get_status_transition_times_ns(
                instance, IMInstance.REQUESTED
            )
        )

        # For each instance, try to allocate or fail the allocation.
        for instance in instances_with_launch_requests:
            # Try allocate or fail with errors.
            update_event = Reconciler._try_resolve_pending_allocation(
                instance, unassigned_cloud_instances_by_type, launch_errors
            )
            if not update_event:
                continue

            logger.debug(
                "Updating {}({}) with {}".format(
                    instance.instance_id,
                    IMInstance.InstanceStatus.Name(instance.status),
                    message_to_dict(update_event),
                )
            )
            updates[instance.instance_id] = update_event

        # Update the instance manager for the events.
        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _try_resolve_pending_allocation(
        im_instance: IMInstance,
        unassigned_cloud_instances_by_type: Dict[str, List[CloudInstance]],
        launch_errors: Dict[str, LaunchNodeError],
    ) -> Optional[IMInstanceUpdateEvent]:
        """
        Allocate, or fail the cloud instance allocation for the instance.

        Args:
            im_instance: The instance to allocate or fail.
            unassigned_cloud_instances_by_type: The unassigned cloud instances by type.
            launch_errors: The launch errors from the cloud provider.

        Returns:
            Instance update to ALLOCATED: if there's a matching unassigned cloud
                instance with the same type.
            Instance update to ALLOCATION_FAILED: if the instance allocation failed
                with errors.
            None: if there's no update.

        """
        unassigned_cloud_instance = None

        # Try to allocate an unassigned cloud instance.
        # TODO(rickyx): We could also look at the launch request id
        # on the cloud node and the im instance later once all node providers
        # support request id. For now, we only look at the instance type.
        if len(unassigned_cloud_instances_by_type.get(im_instance.instance_type, [])):
            unassigned_cloud_instance = unassigned_cloud_instances_by_type[
                im_instance.instance_type
            ].pop()

        if unassigned_cloud_instance:
            return IMInstanceUpdateEvent(
                instance_id=im_instance.instance_id,
                new_instance_status=IMInstance.ALLOCATED,
                cloud_instance_id=unassigned_cloud_instance.cloud_instance_id,
            )

        # If there's a launch error, transition to ALLOCATION_FAILED.
        launch_error = launch_errors.get(im_instance.launch_request_id)
        if launch_error and launch_error.node_type == im_instance.instance_type:
            return IMInstanceUpdateEvent(
                instance_id=im_instance.instance_id,
                new_instance_status=IMInstance.ALLOCATION_FAILED,
                details=str(launch_error),
            )
        # No update.
        return None

    @staticmethod
    def _handle_ray_install_failed(
        instance_manager: InstanceManager, ray_install_errors: List[RayInstallError]
    ):

        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        # Get all instances with RAY_INSTALLING status.
        instances_with_ray_installing = {
            instance.instance_id: instance
            for instance in instances
            if instance.status == IMInstance.RAY_INSTALLING
        }

        install_errors = {error.im_instance_id: error for error in ray_install_errors}

        # For each instance with RAY_INSTALLING status, check if there's any
        # install error.
        for instance_id, instance in instances_with_ray_installing.items():
            install_error = install_errors.get(instance_id)
            if install_error:
                updates[instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance_id,
                    new_instance_status=IMInstance.RAY_INSTALL_FAILED,
                    details=install_error.details,
                )
                logger.debug(
                    "Updating {}({}) with {}".format(
                        instance_id,
                        IMInstance.InstanceStatus.Name(instance.status),
                        message_to_dict(updates[instance_id]),
                    )
                )

        # Update the instance manager for the events.
        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_cloud_instance_terminated(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
    ):
        """
        For any IM (instance manager) instance with a cloud node id, if the mapped
        cloud instance is no longer running, transition the instance to TERMINATED.

        Args:
            instance_manager: The instance manager to reconcile.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
        """
        updates = {}
        instances, version = Reconciler._get_im_instances(instance_manager)

        instances_with_cloud_instance_assigned = {
            instance.cloud_instance_id: instance
            for instance in instances
            if instance.cloud_instance_id
        }

        for (
            cloud_instance_id,
            instance,
        ) in instances_with_cloud_instance_assigned.items():
            if cloud_instance_id in non_terminated_cloud_instances.keys():
                # The cloud instance is still running.
                continue

            # The cloud instance is terminated.
            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.TERMINATED,
                details=f"Cloud instance {cloud_instance_id} is terminated.",
            )

            logger.debug(
                "Updating {}({}) with {}".format(
                    instance.instance_id,
                    IMInstance.InstanceStatus.Name(instance.status),
                    message_to_dict(updates[instance.instance_id]),
                )
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_cloud_instance_termination_errors(
        instance_manager: InstanceManager,
        cloud_provider_errors: List[CloudInstanceProviderError],
    ):
        """
        If any TERMINATING instances have termination errors, transition the instance to
        TERMINATION_FAILED.

        We will retry the termination for the TERMINATION_FAILED instances in the next
        reconciler step.

        Args:
            instance_manager: The instance manager to reconcile.
            cloud_provider_errors: The errors from the cloud provider.

        """
        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        termination_errors = {
            error.cloud_instance_id: error
            for error in cloud_provider_errors
            if isinstance(error, TerminateNodeError)
        }

        terminating_instances_by_cloud_instance_id = {
            instance.cloud_instance_id: instance
            for instance in instances
            if instance.status == IMInstance.TERMINATING
        }

        for cloud_instance_id, failure in termination_errors.items():
            instance = terminating_instances_by_cloud_instance_id.get(cloud_instance_id)
            if not instance:
                # The instance is no longer in TERMINATING status.
                continue

            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.TERMINATION_FAILED,
                details=str(failure),
            )
            logger.debug(
                "Updating {}({}) with {}".format(
                    instance.instance_id,
                    IMInstance.InstanceStatus.Name(instance.status),
                    message_to_dict(updates[instance.instance_id]),
                )
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _get_im_instances(
        instance_manager: InstanceManager,
    ) -> Tuple[List[IMInstance], int]:
        reply = instance_manager.get_instance_manager_state(
            request=GetInstanceManagerStateRequest()
        )
        assert reply.status.code == StatusCode.OK
        im_state = reply.state
        return im_state.instances, im_state.version

    @staticmethod
    def _update_instance_manager(
        instance_manager: InstanceManager,
        version: int,
        updates: Dict[str, IMInstanceUpdateEvent],
    ) -> None:
        if not updates:
            return

        updates = list(updates.values()) or []

        reply = instance_manager.update_instance_manager_state(
            request=UpdateInstanceManagerStateRequest(
                expected_version=version,
                updates=updates,
            )
        )
        # TODO: While it's possible that a version mismatch
        # happens, or some other failures could happen. But given
        # the current implementation:
        #   1. There's only 1 writer (the reconciler) for updating the instance
        #       manager states, so there shouldn't be version mismatch.
        #   2. Any failures in one reconciler step should be caught at a higher
        #       level and be retried in the next reconciler step. If the IM
        #       fails to be updated, we don't have sufficient info to handle it
        #       here.
        assert (
            reply.status.code == StatusCode.OK
        ), f"Failed to update instance manager: {reply}"

    @staticmethod
    def _handle_ray_status_transition(
        instance_manager: InstanceManager, ray_nodes: List[NodeState]
    ):
        """
        Handle the ray status transition for the instance manager.

        If a new ray node running on the instance, transition it to RAY_RUNNING.
        If a ray node stopped, transition it to RAY_STOPPED.
        If a ray node is draining, transition it to RAY_STOPPING.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's states of ray nodes.
        """
        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        im_instances_by_cloud_instance_id = {
            i.cloud_instance_id: i for i in instances if i.cloud_instance_id
        }
        ray_nodes_by_cloud_instance_id = {}
        for n in ray_nodes:
            if n.instance_id:
                ray_nodes_by_cloud_instance_id[n.instance_id] = n
            else:
                # This should only happen to a ray node that's not managed by us.
                logger.warning(
                    f"Ray node {n.node_id.decode()} has no instance id. "
                    "This only happens to a ray node that's not managed by autoscaler. "
                    "If not, please file a bug at https://github.com/ray-project/ray"
                )

        for cloud_instance_id, ray_node in ray_nodes_by_cloud_instance_id.items():
            if cloud_instance_id not in im_instances_by_cloud_instance_id:
                # This is a ray node that's not managed by the instance manager.
                # or we haven't discovered the instance yet. There's nothing
                # much we could do here.
                logger.info(
                    f"Ray node {ray_node.node_id.decode()} has no matching instance in "
                    f"instance manager with cloud instance id={cloud_instance_id}."
                )
                continue

            im_instance = im_instances_by_cloud_instance_id[cloud_instance_id]
            reconciled_im_status = Reconciler._reconciled_im_status_from_ray_status(
                ray_node.status, im_instance.status
            )

            if reconciled_im_status != im_instance.status:
                updates[im_instance.instance_id] = IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=reconciled_im_status,
                    details="Reconciled from ray node status "
                    f"{NodeStatus.Name(ray_node.status)} "
                    f"for ray node {ray_node.node_id.decode()}",
                    ray_node_id=ray_node.node_id.decode(),
                )
                logger.debug(
                    "Updating {}({}) with {}.".format(
                        im_instance.instance_id,
                        IMInstance.InstanceStatus.Name(im_instance.status),
                        message_to_dict(updates[im_instance.instance_id]),
                    )
                )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _reconciled_im_status_from_ray_status(
        ray_status: NodeStatus, cur_im_status: IMInstance.InstanceStatus
    ) -> "IMInstance.InstanceStatus":
        """
        Reconcile the instance status from the ray node status.
        Args:
            ray_status: the current ray node status.
            cur_im_status: the current IM instance status.
        Returns:
            The reconciled IM instance status

        Raises:
            ValueError: If the ray status is unknown.
        """
        reconciled_im_status = None
        if ray_status in [NodeStatus.RUNNING, NodeStatus.IDLE]:
            reconciled_im_status = IMInstance.RAY_RUNNING
        elif ray_status == NodeStatus.DEAD:
            reconciled_im_status = IMInstance.RAY_STOPPED
        elif ray_status == NodeStatus.DRAINING:
            reconciled_im_status = IMInstance.RAY_STOPPING
        else:
            raise ValueError(f"Unknown ray status: {ray_status}")

        if (
            cur_im_status == reconciled_im_status
            or cur_im_status
            in InstanceUtil.get_reachable_statuses(reconciled_im_status)
        ):
            # No need to reconcile if the instance is already in the reconciled status
            # or has already transitioned beyond it.
            return cur_im_status

        return reconciled_im_status

    @staticmethod
    def _handle_instances_launch(
        instance_manager: InstanceManager, autoscaling_config: AutoscalingConfig
    ):

        instances, version = Reconciler._get_im_instances(instance_manager)

        queued_instances = []
        requested_instances = []
        allocated_instances = []

        for instance in instances:
            if instance.status == IMInstance.QUEUED:
                queued_instances.append(instance)
            elif instance.status == IMInstance.REQUESTED:
                requested_instances.append(instance)
            elif instance.cloud_instance_id:
                allocated_instances.append(instance)

        if not queued_instances:
            # No QUEUED instances
            return

        to_launch = Reconciler._compute_to_launch(
            queued_instances,
            requested_instances,
            allocated_instances,
            autoscaling_config.get_upscaling_speed(),
            autoscaling_config.get_max_concurrent_launches(),
        )

        # Transition the instances to REQUESTED for instance launcher to
        # launch them.
        updates = {}
        for instance_type, instances in to_launch.items():
            for instance in instances:
                # Reuse launch request id for any QUEUED instances that have been
                # requested before due to retry.
                launch_request_id = (
                    str(uuid.uuid4())
                    if len(instance.launch_request_id) == 0
                    else instance.launch_request_id
                )
                updates[instance.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_instance_status=IMInstance.REQUESTED,
                    launch_request_id=launch_request_id,
                    instance_type=instance_type,
                )
                logger.debug(
                    "Updating {}({}) with {}".format(
                        instance.instance_id,
                        IMInstance.InstanceStatus.Name(instance.status),
                        message_to_dict(updates[instance.instance_id]),
                    )
                )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _compute_to_launch(
        queued_instances: List[IMInstance],
        requested_instances: List[IMInstance],
        allocated_instances: List[IMInstance],
        upscaling_speed: float,
        max_concurrent_launches: int,
    ) -> Dict[NodeType, List[IMInstance]]:
        def _group_by_type(instances):
            instances_by_type = defaultdict(list)
            for instance in instances:
                instances_by_type[instance.instance_type].append(instance)
            return instances_by_type

        # Sort the instances by the time they were queued.
        def _sort_by_earliest_queued(instance: IMInstance) -> List[int]:
            queue_times = InstanceUtil.get_status_transition_times_ns(
                instance, IMInstance.QUEUED
            )
            return sorted(queue_times)

        queued_instances_by_type = _group_by_type(queued_instances)
        requested_instances_by_type = _group_by_type(requested_instances)
        allocated_instances_by_type = _group_by_type(allocated_instances)

        total_num_requested_to_launch = len(requested_instances)
        all_to_launch: Dict[NodeType : List[IMInstance]] = defaultdict(list)

        for (
            instance_type,
            queued_instances_for_type,
        ) in queued_instances_by_type.items():
            requested_instances_for_type = requested_instances_by_type.get(
                instance_type, []
            )
            allocated_instances_for_type = allocated_instances_by_type.get(
                instance_type, []
            )

            num_desired_to_upscale = max(
                1,
                math.ceil(
                    upscaling_speed
                    * (
                        len(requested_instances_for_type)
                        + len(allocated_instances_for_type)
                    )
                ),
            )

            # Enforce global limit, at most we can launch `max_concurrent_launches`
            num_to_launch = min(
                max_concurrent_launches - total_num_requested_to_launch,
                num_desired_to_upscale,
            )

            # Cap both ends 0 <= num_to_launch <= num_queued
            num_to_launch = max(0, num_to_launch)
            num_to_launch = min(len(queued_instances_for_type), num_to_launch)

            to_launch = sorted(queued_instances_for_type, key=_sort_by_earliest_queued)[
                :num_to_launch
            ]

            all_to_launch[instance_type].extend(to_launch)
            total_num_requested_to_launch += num_to_launch

        return all_to_launch
