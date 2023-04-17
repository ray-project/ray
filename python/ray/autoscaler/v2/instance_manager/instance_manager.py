import logging
import uuid
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional, Tuple

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
        self, cluster_id: str, storage: Storage, instance_types: AvailableInstanceTypes
    ) -> None:
        super().__init__()
        self._storage = storage
        self._cluster_id = cluster_id
        self._table_name = f"instance_table@{cluster_id}"
        self._instance_types = instance_types

    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        return GetAvailableInstanceTypesResponse(instance_types=self._instance_types)

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        mutations = {}

        nodes_to_terminate = list(request.nodes_to_terminate)
        to_terminate_instances, version = self._get_instances(nodes_to_terminate)

        # handle version mismatch
        if request.expected_version >= 0 and request.expected_version != version:
            reply = UpdateInstanceManagerStateReply()
            reply.success = False
            reply.version = version
            return reply

        # handle teriminating instances
        for instance in to_terminate_instances.values():
            if not self._transition_state(instance, Instance.TERMINATING):
                reply = UpdateInstanceManagerStateReply()
                reply.success = False
                reply.version = version
                reply.error_message = (
                    f"Failed to transition instance "
                    "{instance.instance_id} from {instance.instance_state} to TERMINATING"
                )
                return reply
            mutations[instance.instance_id] = instance.SerializeToString()

        # handle new instances to start
        for instance_type in request.new_nodes_to_start:
            instance = Instance()
            instance.instance_id = str(uuid.uuid4())
            instance.instance_type = instance_type.type_name
            instance.instance_state = Instance.QUEUED
            mutations[instance.instance_id] = instance.SerializeToString()

        expected_version = (
            request.expected_version if request.expected_version else None
        )
        result, version = self._storage.update(
            self._table_name, mutations, {}, expected_version
        )

        reply = UpdateInstanceManagerStateReply()
        reply.success = result
        reply.version = version
        return reply

    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        instances, version = self._get_instances()
        state = InstanceManagerState()
        state.version = version
        state.instances.extend(instances.values())
        reply = GetInstanceManagerStateReply()
        reply.state = state
        return reply

    def _get_instances(
        self, instance_ids: List[str]
    ) -> Tuple[Dict[str, Instance], int]:
        pairs, version = self._storage.get(self._table_name, instance_ids)
        instances = {}
        for instance_id, instance_data in pairs.items():
            instance = Instance()
            instance.ParseFromString(instance_data)
            instances[instance_id] = instance
        return instances, version

    def _transition_state(self, instance: Instance, new_state: int) -> bool:
        instance.instance_state = new_state
        return True
