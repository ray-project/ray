import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from ray.autoscaler.v2.instance_manager.common import (
    InstanceUtil,
    InvalidInstanceUpdateError,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    GetInstanceManagerStateRequest,
    Instance,
    InstanceUpdateEvent,
    StatusCode,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class InstanceUpdatedSubscriber(ABC):
    """Subscribers to instance status changes."""

    @abstractmethod
    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        pass


class InstanceManager:
    """
    See `InstanceManagerService` in instance_manager.proto

    This handles updates to an instance, or inserts a new instance if
    it's an insert update. We should only be inserting new instances
    of the below statuses:
        1. ALLOCATED: For unmanaged instance not initialized by InstanceManager,
            e.g. head node
        2. QUEUED: For new instance being queued to launch.
        3. TERMINATING: For leaked cloud instance that needs to be terminated.

    For full status transitions, see:
    https://docs.google.com/document/d/1NzQjA8Mh-oMc-QxXOa529oneWCoA8sDiVoNkBqqDb4U/edit#heading=h.k9a1sp4qpqj4

    Not thread safe, should be used as a singleton.
    """

    def __init__(
        self,
        instance_storage: InstanceStorage,
        instance_status_update_subscribers: Optional[List[InstanceUpdatedSubscriber]],
    ):
        self._instance_storage = instance_storage
        self._status_update_subscribers = instance_status_update_subscribers or []

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        """
        Updates the instance manager state.

        If there's a any failure, no updates would be made and the reply
        would contain the latest version of the instance manager state,
        and the error info.

        Args:
            request: The request to update the instance manager state.

        Returns:
            The reply to the request.
        """

        # Handle updates
        ids_to_updates = {update.instance_id: update for update in request.updates}
        to_update_instances, version = self._instance_storage.get_instances(
            instance_ids=ids_to_updates.keys()
        )

        if request.expected_version >= 0 and request.expected_version != version:
            err_str = (
                f"Version mismatch: expected: {request.expected_version}, "
                f"actual: {version}"
            )
            logger.warn(err_str)
            return self._get_update_im_state_reply(
                StatusCode.VERSION_MISMATCH,
                version,
                err_str,
            )

        # Handle instances states update.
        to_upsert_instances = []
        for instance_id, update in ids_to_updates.items():
            try:
                if instance_id in to_update_instances:
                    instance = self._update_instance(
                        to_update_instances[instance_id], update
                    )
                else:
                    instance = self._create_instance(update)
            except InvalidInstanceUpdateError as e:
                logger.error(e)
                return InstanceManager._get_update_im_state_reply(
                    StatusCode.INVALID_VALUE, version, str(e)
                )

            to_upsert_instances.append(instance)

        # Updates the instance storage.
        result = self._instance_storage.batch_upsert_instances(
            updates=to_upsert_instances,
            expected_storage_version=version,
        )

        if not result.success:
            if result.version != version:
                err_str = (
                    f"Version mismatch: expected: {version}, actual: {result.version}"
                )
                logger.warn(err_str)
                return self._get_update_im_state_reply(
                    StatusCode.VERSION_MISMATCH, result.version, err_str
                )
            else:
                err_str = "Failed to update instance storage."
                logger.error(err_str)
                return self._get_update_im_state_reply(
                    StatusCode.UNKNOWN_ERRORS, result.version, err_str
                )

        # Successful updates.
        for subscriber in self._status_update_subscribers:
            subscriber.notify(request.updates)

        return self._get_update_im_state_reply(StatusCode.OK, result.version)

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
        reply = GetInstanceManagerStateReply()
        instances, version = self._instance_storage.get_instances()
        reply.state.instances.extend(instances.values())
        reply.state.version = version
        reply.status.code = StatusCode.OK

        return reply

    #########################################
    # Private methods
    #########################################

    @staticmethod
    def _get_update_im_state_reply(
        status_code: StatusCode, version: int, error_message: str = ""
    ) -> UpdateInstanceManagerStateReply:
        """
        Returns a UpdateInstanceManagerStateReply with the given status code and
        version.

        Args:
            status_code: The status code.
            version: The version.
            error_message: The error message if any.

        Returns:
            The reply.
        """
        reply = UpdateInstanceManagerStateReply()
        reply.status.code = status_code
        reply.version = version
        if error_message:
            reply.status.message = error_message
        return reply

    @staticmethod
    def _apply_update(instance: Instance, update: InstanceUpdateEvent):
        """
        Apply status specific update to the instance.

        Args:
            instance: The instance to update.
            update: The update to apply.

        Raises:
            InvalidInstanceUpdateError: If the update is invalid.
        """

        if update.new_instance_status == Instance.ALLOCATED:
            if not update.cloud_instance_id:
                raise InvalidInstanceUpdateError(
                    instance_id=update.instance_id,
                    cur_status=instance.status,
                    update=update,
                    details=("ALLOCATED update must have cloud_instance_id"),
                )
            instance.cloud_instance_id = update.cloud_instance_id
        elif update.new_instance_status == Instance.TERMINATED:
            instance.cloud_instance_id = ""
        elif update.new_instance_status == Instance.RAY_RUNNING:
            if not update.ray_node_id:
                raise InvalidInstanceUpdateError(
                    instance_id=update.instance_id,
                    cur_status=instance.status,
                    update=update,
                    details=("RAY_RUNNING update must have ray_node_id"),
                )
            instance.node_id = update.ray_node_id
        elif update.new_instance_status == Instance.REQUESTED:
            if not update.launch_request_id:
                raise InvalidInstanceUpdateError(
                    instance_id=update.instance_id,
                    cur_status=instance.status,
                    update=update,
                    details=("REQUESTED update must have launch_request_id"),
                )
            instance.launch_request_id = update.launch_request_id

    @staticmethod
    def _create_instance(update: InstanceUpdateEvent) -> Instance:
        """
        Create a new instance from the given update.
        """

        if update.new_instance_status not in [
            # For unmanaged instance not initialized by InstanceManager,
            # e.g. head node
            Instance.ALLOCATED,
            # For new instance being queued to launch.
            Instance.QUEUED,
            # For leaked cloud instance that needs to be terminated.
            Instance.TERMINATING,
        ]:
            raise InvalidInstanceUpdateError(
                instance_id=update.instance_id,
                cur_status=Instance.UNKNOWN,
                update=update,
                details=(
                    "Invalid status for new instance, must be one of "
                    "[ALLOCATED, QUEUED, TERMINATING]"
                ),
            )

        # Create a new instance first for common fields.
        instance = InstanceUtil.new_instance(
            instance_id=update.instance_id,
            instance_type=update.instance_type,
            status=update.new_instance_status,
            details=update.details,
        )

        # Apply the status specific updates.
        InstanceManager._apply_update(instance, update)
        return instance

    @staticmethod
    def _update_instance(instance: Instance, update: InstanceUpdateEvent) -> Instance:
        """
        Update the instance with the given update.

        Args:
            instance: The instance to update.
            update: The update to apply.

        Returns:
            The updated instance.

        Raises:
            InvalidInstanceUpdateError: If the update is invalid.
        """
        if not InstanceUtil.set_status(instance, update.new_instance_status):
            raise InvalidInstanceUpdateError(
                instance_id=update.instance_id,
                cur_status=instance.status,
                update=update,
                details="Invalid status transition",
            )
        InstanceManager._apply_update(instance, update)

        return instance
