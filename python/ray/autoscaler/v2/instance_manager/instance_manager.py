import logging
import time
import uuid
from abc import ABCMeta, abstractmethod
from typing import Optional

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
)
from ray.autoscaler.v2.instance_manager.storage import Storage
from ray.core.generated.instance_manager_pb2 import (
    AvailableInstanceTypes,
    GetAvailableInstanceTypesResponse,
    GetInstanceManagerStateReply,
    Instance,
    InstanceManagerState,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)


class InstanceManager(metaclass=ABCMeta):
    @abstractmethod
    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        pass

    @abstractmethod
    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        pass

    @abstractmethod
    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        pass


class SimpleInstanceManager(InstanceManager):
    def __init__(
        self,
        cluster_id: str,
        storage: Storage,
        instance_types: AvailableInstanceTypes,
        stopped_node_gc_timeout_s: int = 1800,
        status_change_subscriber: Optional[InstanceUpdatedSuscriber] = None,
    ) -> None:
        super().__init__()
        self._cluster_id = cluster_id
        self._instance_types = instance_types
        self._stopped_node_gc_timeout_s = stopped_node_gc_timeout_s
        self._instance_storage = InstanceStorage(
            cluster_id, storage, status_change_subscriber
        )

    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        return GetAvailableInstanceTypesResponse(instance_types=self._instance_types)

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        nodes_to_terminate = list(request.nodes_to_terminate)
        to_terminate_instances, version = self._instance_storage.get_instances(
            nodes_to_terminate
        )

        # handle version mismatch
        if request.expected_version >= 0 and request.expected_version != version:
            reply = UpdateInstanceManagerStateReply()
            reply.success = False
            reply.version = version
            return reply

        # handle teriminating instances
        for instance in to_terminate_instances.values():
            if not self._transition_state(instance, Instance.STOPPING):
                reply = UpdateInstanceManagerStateReply()
                reply.success = False
                reply.version = version
                reply.error_message = (
                    "Failed to transition instance "
                    "{instance.instance_id} from {instance.status} to STOPPING"
                )
                return reply

        # handle new instances to start
        new_instances = {}
        for instance_type in request.new_nodes_to_start:
            instance = Instance()
            instance.instance_id = str(uuid.uuid4())
            instance.instance_type = instance_type.type_name
            instance.status = Instance.QEUEUD
            instance.ray_status = Instance.RAY_STATUS_UNKOWN
            new_instances.append(instance)

        expected_version = (
            request.expected_version if request.expected_version else None
        )
        result, version = self._instance_storage.upsert_instances(
            self._table_name,
            new_instances + to_terminate_instances.values(),
            expected_version,
        )

        reply = UpdateInstanceManagerStateReply()
        reply.success = result
        reply.version = version
        return reply

    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        instances, version = self._instance_storage.get_instances()
        state = InstanceManagerState()
        state.version = version
        state.instances.extend(instances.values())
        reply = GetInstanceManagerStateReply()
        reply.state = state
        return reply

    def _transition_state(self, instance: Instance, new_state: int) -> bool:
        instance.status = new_state
        return True

    def gc_stopped_nodes(self) -> bool:
        instances, version = self._instance_storage.get_instances()
        to_gc_instances = []
        to_gc_instance_ids = []
        for instance in instances:
            if (
                instance.status == Instance.STOPPED
                and instance.timestamp_since_last_state_change
                + self._stopped_node_gc_timeout_s
                < time.time()
            ):
                logger.info("GCing stopped node %s", instance.instance_id)
                to_gc_instance_ids.append(instance.instance_id)
                to_gc_instances.append(instance)
        if not to_gc_instances:
            return False

        result = self._instance_storage.delete_instances(to_gc_instances, version)[0]
        return result
