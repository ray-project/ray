import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional

from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    GetInstanceManagerStateRequest,
    Instance,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class InstanceManager(ABC):
    """
    See `InstanceManagerService` in instance_manager.proto

    This handles the following updates to an instance:
        1. when creating new instances
            An instance is created from an autoscaler's launch request.
            This initializes an instance object with:
            status = Instance.QUEUED
        2. when ray is stopping
            This happens when the autoscaler is terminating the ray
            process on the instance, e.g. idle termination.
            status = Instance.RAY_STOPPING
        3. when ray is already stopped.
            Only the ray cluster has the true status of the ray process
            on an instance, so autoscaler will update an instance's ray
            to be stopped.
            status = Instance.RAY_STOPPED

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4
    """

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        """
        Updates the instance manager state.

        Args:
            request: The request to update the instance manager state.

        Returns:
            The reply to the request.
        """
        pass

    @abstractmethod
    def get_instance_manager_state(
        self, request: GetInstanceManagerStateRequest
    ) -> GetInstanceManagerStateReply:
        """
        Gets the instance manager state.

        Args:
            request: The request to get the instance manager state.

        Returns:
            The reply to the request.
        """
        pass


class InstanceUtil:
    """
    A helper class to group updates and operations on an Instance object defined
    in instance_manager.proto
    """

    @staticmethod
    def new_instance(
        instance_id: str,
        instance_type: str,
        resources: Dict[str, float],
        request_id: str = "",
    ) -> Instance:
        instance = Instance()
        instance.version = 0  # it will be populated by the underlying storage.
        instance.instance_id = instance_id
        instance.instance_type = instance_type
        instance.launch_request_id = request_id
        instance.total_resources.update(resources)
        instance.status = Instance.QUEUED
        InstanceUtil._record_status_transition(
            instance, Instance.QUEUED, "created from InstanceUtil"
        )
        return instance

    @staticmethod
    def is_cloud_instance_allocated(instance: Instance) -> bool:
        """
        Returns True if the instance is allocated by the cloud provider, i.e.
        there's already a cloud node for the instance.
        """
        assert instance.status != Instance.UNKNOWN
        return instance.status in {
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.STOPPING,
            Instance.RAY_INSTALL_FAILED,
        }

    @staticmethod
    def is_ray_pending(instance: Instance) -> bool:
        """
        Returns True if the instance is pending ray installation or ray startup,
        regardless of whether the instance is allocated by the cloud provider.
        """
        assert instance.status != Instance.UNKNOWN
        return instance.status in [
            Instance.UNKNOWN,
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.ALLOCATION_FAILED,
            Instance.RAY_INSTALL_FAILED,
        ]

    @staticmethod
    def set_status(
        instance: Instance,
        new_instance_status: Instance.InstanceStatus,
        details: str = "",
    ):
        """Transitions the instance to the new state.

        This should be called by the reconciler and the instance manager.

        Args:
            instance: The instance to update.
            new_instance_status: The new status to transition to.
            details: The details of the transition.

        Raises:
            ValueError if the transition is not allowed.
        """
        InstanceUtil._check_valid_next_instance_status(instance, new_instance_status)
        instance.status = new_instance_status
        InstanceUtil._record_status_transition(instance, new_instance_status, details)

    @staticmethod
    def _record_status_transition(
        instance: Instance, status: Instance.InstanceStatus, details: str
    ):
        """Records the status transition.

        This should be called by the instance manager.

        Args:
            instance: The instance to update.
            status: The new status to transition to.
        """
        now_ms = time.time_ns() // 1000000
        if (
            len(instance.status_history) > 0
            and instance.status_history[-1].timestamp_ms > now_ms
        ):
            raise ValueError(
                f"Invalid timestamp: new timestamp({now_ms}) < previous timestamp("
                f"({instance.status_history[-1].timestamp_ms})"
            )

        instance.status_history.append(
            Instance.StatusHistory(
                instance_status=status,
                timestamp_ms=now_ms,
                details=details,
            )
        )

    @staticmethod
    def _check_valid_next_instance_status(
        instance: Instance, new_status: Instance.InstanceStatus
    ) -> None:
        """Checks if the transition is allowed.

        Args:
            instance: The instance to update.
            new_status: The new status to transition to.

        Raises:
            ValueError if the transition is not allowed.
        """
        valid_transitions = {
            Instance.QUEUED: {
                # Cloud provider requested to launch a node for the instance.
                Instance.REQUESTED
            },
            Instance.REQUESTED: {
                # Cloud provider allocated a cloud node for the instance.
                Instance.ALLOCATED,
                # Cloud provider failed to allocate one. Either timeout or
                # failed immediately.
                Instance.ALLOCATION_FAILED,
            },
            Instance.ALLOCATED: {
                # Ray needs to be install and launch on the provisioned cloud node.
                Instance.RAY_INSTALLING,
                # Ray is already installed and running on the provisioned cloud node.
                Instance.RAY_RUNNING,
                # Instance is requested to be stopped, e.g. instance leaked.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            Instance.RAY_INSTALLING: {
                # Ray installed and launched successfully.
                Instance.RAY_RUNNING,
                # Ray installation failed.
                Instance.RAY_INSTALL_FAILED,
                # Instance is requested to be stopped, e.g. instance leaked.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            Instance.RAY_RUNNING: {
                # Ray is requested to be stopped, e.g. instance idle.
                Instance.RAY_STOPPING,
                # Ray is stopped (either by autoscaler, by user, or crashed).
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            Instance.RAY_STOPPING: {
                # Ray is stopped.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            Instance.RAY_STOPPED: {
                # Cloud node is requested to be stopped after ray stopped.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            Instance.STOPPING: {Instance.STOPPED},
            Instance.STOPPED: {},  # Terminal state.
            Instance.ALLOCATION_FAILED: {
                # Autoscaler might retry to allocate the instance.
                Instance.QUEUED
            },
            Instance.RAY_INSTALL_FAILED: {
                # Autoscaler requests to shutdown the instance when ray install failed.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
        }

        if new_status not in valid_transitions[instance.status]:
            raise ValueError(
                f"Invalid transition from {instance.status} to {new_status}"
            )

    @staticmethod
    def get_status_time_ms(
        instance: Instance,
        instance_status: Instance.InstanceStatus,
        reverse: bool = False,
    ) -> Optional[int]:
        for status_update in sorted(
            instance.status_history, key=lambda x: x.timestamp_ms, reverse=reverse
        ):
            if status_update.instance_status != instance_status:
                continue

            return status_update.timestamp_ms

        return None
