from collections import defaultdict
import copy
import logging
import time
import uuid
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, override

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.core.generated.instance_manager_pb2 import (
    Status,
    GetInstanceManagerStateReply,
    Instance,
    InstanceManagerState,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
    LaunchRequest,
)

logger = logging.getLogger(__name__)


class InstanceManager(metaclass=ABCMeta):
    """
    See `InstanceManagerService` in instance_manager.proto
    """

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        pass

    @abstractmethod
    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        pass


class SimpleInstanceManager(InstanceManager):
    """Simple instance manager that manages instances in memory.

    This handles the following updates to an instance:
        1. when creating new instances
            An instance is created from an autoscaler's launch request.
            This initializes an instance object with:
            status = (Instance.UNKNOWN, Instance.RAY_STATUS_UNKNOWN)
        2. when ray is stopping
            This happens when the autoscaler is terminating the ray
            process on the instance, e.g. idle termination.
            status = (Instance.ALLOCATED, Instance.RAY_STOPPING)
        3. when ray is already stopped.
            Only the ray cluster has the true status of the ray process
            on an instance, so autoscaler will update an instance's ray
            to be stopped.
            status = (Instance.ALLOCATED, Instance.RAY_STOPPED)

        The instance manager should not update the instance status directly,
        but instead, it should update the instance's ray status. And
        the reconciler/instance launcher will update the instance status
        based on the ray status and the instance status with the underlying
        node provider.

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        available_node_types: Dict[str, Any],
        status_change_subscribers: Optional[List[InstanceUpdatedSubscriber]] = None,
    ) -> None:
        super().__init__()
        self._instance_configs = available_node_types
        self._instance_storage = instance_storage
        instance_storage.add_status_change_subscribers(status_change_subscribers)

    @override
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        # 1. Handle instance status updates.
        ids_to_updates = {update.instance_id: update for update in request.updates}
        if len(ids_to_updates) == 0:
            to_update_instances, version = {}, request.expected_version
        else:
            to_update_instances, version = self._instance_storage.get_instances(
                ids_to_updates.keys()
            )
            if request.expected_version >= 0 and request.expected_version != version:
                reply = UpdateInstanceManagerStateReply()
                reply.status = Status.OK
                reply.version = version
                return reply

        for instance in to_update_instances.values():
            update = ids_to_updates[instance.instance_id]
            InstanceUtil.set_status(instance, new_ray_status=update.new_ray_status)

        # 2. Handle new instances to start
        new_instances = []
        for launch_request in request.launch_requests:
            instances = self._create_instances(launch_request)
            new_instances.extend(instances)

        expected_version = (
            request.expected_version if request.expected_version else None
        )

        # 3. Update the instance storage.
        ok, version = self._instance_storage.batch_upsert_instances(
            new_instances + list(to_update_instances.values()),
            expected_version,
        )

        if ok:
            reply.status = Status.OK
        else:
            if version != expected_version:
                reply.status = Status.VERSION_MISMATCH
            else:
                reply.status = Status.UNKNOWN_ERROR

        reply = UpdateInstanceManagerStateReply()
        reply.state.CopyFrom(self.get_instance_manager_state().state)
        reply.version = reply.state.version
        return reply

    @override
    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        reply = GetInstanceManagerStateReply()
        reply.state.CopyFrom(self._get_instance_manager_state())
        reply.status = Status.OK
        return reply

    def _get_instance_manager_state(self) -> InstanceManagerState:
        """
        Utility function to get the current instance manager state.
        """
        instances, version = self._instance_storage.get_instances()
        state = InstanceManagerState()
        state.version = version
        state.instances.extend(instances.values())
        return state

    def _create_instances(self, request: LaunchRequest) -> List[Instance]:
        instances = []
        instance_type = request.instance_type
        assert instance_type in self._instance_configs, (
            f"instance type {instance_type} not found in "
            f"instance configs {self._instance_configs}"
        )
        for _ in range(request.count):
            instance = self.new_instance(
                instance_id=str(uuid.uuid4()),
                instance_type=instance_type,
                # TODO: do we always have the "resources" field?
                resources=copy.deepcopy(
                    self._instance_configs[instance_type]["resources"]
                ),
                request_id=request.id,
            )

            instances.append(instance)
        return instances


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
        InstanceUtil.set_status(instance, Instance.UNKNOWN, Instance.RAY_STATUS_UNKNOWN)
        return instance

    @staticmethod
    def is_allocated(instance: Instance) -> bool:
        return instance.ray_status == Instance.ALLOCATED

    @staticmethod
    def is_pending(instance: Instance) -> bool:
        return instance.ray_status in [
            Instance.REQUESTED,
            Instance.QUEUED,
            Instance.UNKNOWN,
        ]

    @staticmethod
    def set_status(
        instance: Instance,
        new_instance_status: Optional["Instance.Status"] = None,
        new_ray_status: Optional["Instance.RayStatus"] = None,
    ):
        """Transitions the instance to the new state.

        This should be called by the reconciler and the instance manager.
        """
        if new_instance_status is not None:
            instance.status = new_instance_status

        if new_ray_status is not None:
            instance.ray_status = new_ray_status

        InstanceUtil.check_valid_instance(instance)

        now_ms = time.time_ns() // 1000000
        if len(instance.status_history) > 0:
            assert (
                instance.status_history[-1].timestamp_ms <= now_ms
            ), "Status history is not sorted"

        instance.status_history.append(
            Instance.StatusHistory(
                instance_status=instance.status,
                ray_status=instance.ray_status,
                timestamp_ms=now_ms,
            )
        )

    @staticmethod
    def get_status_time_s(
        instance: Instance,
        instance_status: Optional["Instance.InstanceStatus"] = None,
        ray_status: Optional["Instance.RayStatus"] = None,
        reverse: bool = False,
    ) -> Optional[int]:
        assert instance_status or ray_status, "Must specify at least one status"

        for status_update in sorted(
            instance.status_history, key=lambda x: x.timestamp_ms, reverse=reverse
        ):
            if instance_status and status_update.instance_status != instance_status:
                continue

            if ray_status and status_update.ray_status != ray_status:
                continue

            return status_update.timestamp_ms // 1000

        return None

    @staticmethod
    def check_valid_instance(instance: Instance):
        """Returns true if the instance is valid."""

        # Check on the instance status and ray status combo.
        if instance.status in [
            Instance.UNKNOWN,
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.STOPPING,
            Instance.STOPPED,
            Instance.GARBAGE_COLLECTED,
        ]:
            assert instance.ray_status == Instance.RAY_STATUS_UNKNOWN

        if instance.status == Instance.ALLOCATED:
            # This is technically all ray state. Just for clarity.
            assert instance.ray_status in [
                Instance.RAY_STATUS_UNKNOWN,
                Instance.RAY_INSTALLING,
                Instance.RAY_RUNNING,
                Instance.RAY_STOPPING,
                Instance.RAY_STOPPED,
                Instance.RAY_INSTALL_FAILED,
            ]
