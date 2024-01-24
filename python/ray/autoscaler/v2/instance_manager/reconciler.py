from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
)

from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent as IMInstanceUpdateEvent,
    Instance as IMInstance,
)
from ray.core.generated.autoscaler_pb2 import ClusterResourceState


class IReconciler(ABC):
    @abstractmethod
    def reconcile(
        self, instances: List[IMInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        pass


class CloudProviderReconciler(IReconciler):
    def __init__(self, provider: ICloudInstanceProvider) -> None:
        self._provider = provider
        super().__init__()

    def reconcile(
        self, instances: List[IMInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        Reconcile the instance storage with the node provider.
        This is responsible for transitioning the instance status of:
        1. to ALLOCATED: when a REQUESTED instance could be assigned to an unassigned
            cloud instance.
        2. to STOPPED: when an ALLOCATED instance no longer has the assigned cloud
            instance found in node provider.
        3. to ALLOCATION_FAILED: when a REQUESTED instance failed to be assigned to
            an unassigned cloud instance.
        4. to STOPPING: when an instance being terminated fails to be terminated
            for some reasons, we will retry the termination.
        """
        pass

    def _reconcile_allocated(
        self, non_terminated_cloud_nodes: Dict[CloudInstanceId, CloudInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:

        """
        For any REQUESTED instances, if there's any unassigned non-terminated
        cloud instance that matches the instance type from the same request,
        assign it and transition it to ALLOCATED.

        """

        # Find all requested instances, by launch request id.

        # Find all non_terminated cloud_nodes by launch request id.

        # For the same request, find any unassigned non-terminated cloud instance
        # that matches the instance type. Assign them to REQUESTED instances
        # and transition them to ALLOCATED.
        pass

    def _reconcile_failed_to_allocate(
        self, cloud_provider_errors: List[CloudInstanceProviderError]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        For any REQUESTED instances, if there's errors in allocating the cloud instance,
        transition the instance to ALLOCATION_FAILED.
        """

        # Find all requested instances, by launch request id.

        # Find all launch errors by launch request id.

        # For the same request, transition the instance to ALLOCATION_FAILED.
        # TODO(rickyx): we don't differentiate transient errors (which might be retryable)
        # and permanent errors (which are not retryable).
        pass

    def _reconcile_stopped(
        self, non_terminated_cloud_nodes: Dict[CloudInstanceId, CloudInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        For any IM (instance manager) instance with a cloud node id, if the mapped
        cloud instance is no longer running, transition the instance to STOPPED.
        """

        # Find all instances with cloud instance id, by cloud instance id.

        # Check if any matched cloud instance is still running.
        # If not, transition the instance to STOPPED. (The instance will have the cloud
        # instance id removed, and GCed later.)
        pass

    def _reconcile_failed_to_terminate(
        self, cloud_provider_errors: List[CloudInstanceProviderError]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        For any STOPPING instances, if there's errors in terminating the cloud instance,
        we will retry the termination by setting it to STOPPING again.
        """
        pass


class StuckInstanceReconciler(IReconciler):
    def reconcile(self, *args, **kwargs) -> Dict[str, IMInstanceUpdateEvent]:
        """
        ***********************
        **STUCK STATES
        ***********************
        For instances that's stuck in the following states, we will try to recover
        them:

        - REQUESTED -> QUEUED
            WHEN: happens when the cloud instance provider is not able to allocate
                cloud instances, and the instance is in REQUESTED state for longer than
                the timeout.
            ACTION: We will retry the allocation by setting it back to QUEUED.

        - ALLOCATED -> STOPPING
            WHEN: happens when the cloud instance provider is responsible for
                installing and running ray on the cloud instance, and the cloud instance
                is not able to start ray for some reason.
            ACTION: We will terminate the instances since it's a leak after a timeout
                for installing/running ray, setting it to STOPPING.

        - RAY_INSTALLING -> RAY_INSTALL_FAILED
            WHEN: this happens when the cloud instance provider is responsible for
                installing ray on the cloud instance, and the cloud instance is not
                able to start ray for some reason (e.g. immediate crash after ray start)
            ACTION: We will fail the installation and terminate the instance after a
                timeout, transitioning to RAY_INSTALL_FAILED.

        - STOPPING -> STOPPING
            WHEN: the cloud node provider taking long or failed to terminate the cloud
                instance.
            ACTION: We will retry the termination by setting it to STOPPING, triggering
                another termination request to the cloud instance provider.

        ***********************
        **LONG LASTING STATES
        ***********************
        These states are expected to be long lasting. So we will not take any actions.

        - RAY_RUNNING:
            Normal state - unlimited time.
        - STOPPED:
            Normal state - terminal, and unlimited time.
        - ALLOCATION_FAILED:
            Normal state - terminal, and unlimited time.
        - QUEUED
            WHEN: if there's many instances queued to be launched, and we limit the
                number of concurrent instance launches, the instance might be stuck
                in QUEUED state for a long time.
            ACTION: No actions, print warnings if needed (or adjust the scaling-up
                configs)

        ***********************
        **TRANSIENT STATES
        ***********************
        These states are expected to be transient, when they don't, it usually indicates
        a bug in the instance manager or the cloud instance provider as the process loop
        which is none-blocking is now somehow not making progress.

        - RAY_STOPPING:
            WHEN: this state should be transient, instances in this state are being
                drained through the drain protocol (and the drain request should
                eventually finish).
            ACTION: There's no drain request timeout so we will wait for the drain to
                finish, and print errors if it's taking too long.

        - RAY_INSTALL_FAILED or RAY_STOPPED
            WHEN: these state should be transient, cloud instances should be terminating
                or terminated soon. This could happen if the subscribers for the status
                transition failed to act upon the status update.
            ACTION: Print errors if stuck for too long - this is a bug.
        """
        pass


class RayStateReconciler(IReconciler):
    def __init__(self, ray_cluster_resource_state: ClusterResourceState) -> None:
        self._ray_cluster_resource_state = ray_cluster_resource_state
        super().__init__()

    def reconcile(
        self, instances: List[IMInstance]
    ) -> Dict[str, IMInstanceUpdateEvent]:
        """
        Reconcile the instances states for Ray node state changes.

        1. Newly running ray nodes -> RAY_RUNNING status change.
            When an instance in (ALLOCATED or RAY_INSTALLING) successfully launched the
            ray node, we will discover the newly launched ray node and transition the
            instance to RAY_RUNNING.

        2. Newly dead ray nodes -> RAY_STOPPED status change.
            When an instance not in RAY_STOPPED is no longer running the ray node, we
            will transition the instance to RAY_STOPPED.
        """
        pass
