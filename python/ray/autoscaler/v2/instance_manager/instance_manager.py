import copy
import logging
import time
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from ray.autoscaler.v2.instance_manager.config import InstancesConfigReader
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    GetInstanceManagerStateRequest,
    GetInstancesConfigReply,
    GetInstancesConfigRequest,
    Instance,
    InstancesConfig,
    LaunchRequest,
    Status,
    StatusCode,
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

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4
    """

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        pass

    @abstractmethod
    def get_instance_manager_state(
        self, request: GetInstanceManagerStateRequest
    ) -> GetInstanceManagerStateReply:
        pass

    @abstractmethod
    def get_instances_config(
        self, request: GetInstancesConfigRequest
    ) -> GetInstancesConfigReply:
        pass


class InMemoryInstanceManager(InstanceManager):
    """An instance manager implementation that manages instances in memory.

    It uses direct method calls instead of rpc since
    it works with an in-memory local instance storage.

    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        instances_config_reader: InstancesConfigReader,
        status_change_subscribers: Optional[List[InstanceUpdatedSubscriber]] = None,
    ) -> None:
        """
        Args:
            instance_storage: The instance storage to use.
            instances_config_reader: The instance config reader that would yield the
                updated instances config.
            status_change_subscribers: The subscribers to notify when an instance's
                status changes.
        """
        super().__init__()
        self._instances_config_reader = instances_config_reader
        self._instance_storage = instance_storage
        instance_storage.add_status_change_subscribers(status_change_subscribers)

    def get_instances_config(
        self, request: GetInstancesConfigRequest
    ) -> GetInstancesConfigReply:
        """
        Gets the instances config.

        Override:
            Overrides the base class method.

        Returns:
            GetInstancesConfigReply: The instances config reply containing
            the most recent instances config if successful, otherwise the error
            message.
        """
        reply = GetInstancesConfigReply()
        config, error = self._get_instances_config()
        if config is None:
            reply.status = Status.UNAVAILABLE
            reply.error_message = error
        else:
            reply.status = Status.OK
            reply.config.CopyFrom(config)

        return reply

    def _get_instances_config(self) -> Tuple[Optional[InstancesConfig], Optional[str]]:
        """Gets the instances config.

        Returns:
            - A valid instances config if successful, otherwise None.
            - The error message if failed, otherwise None.
        """
        try:
            config = self._instances_config_reader.get_instances_config()
        except Exception as e:
            return None, str(e)
        else:
            return config, None

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        """
        Updates the instance manager state with the updated instances, and new
        instances to launch.

        Overrides:
            Overrides the base class method.
        """

        def _reply(status: Status, version: int) -> UpdateInstanceManagerStateReply:
            reply = UpdateInstanceManagerStateReply()
            reply.status.CopyFrom(status)
            reply.version = version
            return reply

        # 0. Get the instances config.
        instances_config, err = self._get_instances_config()

        if instances_config is None:
            return _reply(
                Status(code=StatusCode.UNAVAILABLE, message=err),
                request.expected_version,
            )

        # 1. Get instances to be updated.
        ids_to_updates = {update.instance_id: update for update in request.updates}
        to_update_instances = {}
        if len(ids_to_updates) > 0:
            to_update_instances, version = self._instance_storage.get_instances(
                ids_to_updates.keys()
            )
            if request.expected_version >= 0 and request.expected_version != version:
                # Version mismatch will fail the entire operations.
                return _reply(
                    Status(
                        code=StatusCode.VERSION_MISMATCH, message="version mismatched"
                    ),
                    version,
                )

            for instance in to_update_instances.values():
                update = ids_to_updates[instance.instance_id]
                InstanceUtil.set_status(
                    instance,
                    new_ray_status=update.new_ray_status,
                    new_instance_status=update.new_instance_status,
                )

        # 2. Get new instances to start.
        new_instances = []
        for launch_request in request.launch_requests:
            # TODO: we will need to log in autoscaler events for any failure in
            # creating instances.
            instances = self._create_instances(launch_request, instances_config)
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
            code = StatusCode.OK
            msg = ""
        elif version != expected_version:
            code = StatusCode.VERSION_MISMATCH
            msg = f"expected({expected_version}) != actual ({version})"
        else:
            code = StatusCode.UNAVAILABLE
            msg = "unable to update instance storage"

        return _reply(Status(code=code, message=msg), version)

    def get_instance_manager_state(
        self, request: GetInstanceManagerStateRequest
    ) -> GetInstanceManagerStateReply:
        """
        Returns:
            GetInstanceManagerStateReply: The current instance manager state.

        Override:
            Overrides the base class method.
        """
        instances, version = self._instance_storage.get_instances()
        reply = GetInstanceManagerStateReply()
        reply.state.version = version
        reply.state.instances.extend(instances.values())
        reply.status.code = StatusCode.OK
        return reply

    def _create_instances(
        self, request: LaunchRequest, config: InstancesConfig
    ) -> List[Instance]:
        """
        Utility function to create instances from a launch request.

        Args:
            request: The launch request.
            config: The instances config.

        Returns:
            List[Instance]: The list of instances created.
        """
        instances = []
        instance_type = request.instance_type

        instance_type_config = config.available_node_type_configs.get(
            instance_type, None
        )
        if instance_type_config is None:
            return []

        resources = dict(instance_type_config.resources)
        for _ in range(request.count):
            # Create an autoscaler Instance object, note the actual
            # cloud instance is not requested yet.
            instance = InstanceUtil.new_instance(
                instance_id=str(uuid.uuid4()),
                instance_type=instance_type,
                resources=copy.deepcopy(resources),
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
