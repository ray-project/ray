import logging
import abc
import uuid

from abc import abstractmethod
from ray.autoscaler.v2.instance_manager.storage import Storage

from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceState,
    GetAvailableInstanceTypesResponse,
    GetInstanceManagerStateReply,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)

class InstanceManager(metaclass=abc.ABCMeta):
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
    def __init__(self, cluster_id: str, storage: Storage) -> None:
        super().__init__()
        self._storage = storage
        self._cluster_id = cluster_id
        self._table_name = f"instance_table@{cluster_id}"

    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        pass

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        mutations = {}
        for instance_type in request.new_nodes_to_start:
            instance = Instance()
            instance.instance_id = str(uuid.uuid4())
            instance.instance_type = instance_type.type_name
            instance.instance_state = InstanceState.QUEUED
            mutations[instance.instance_id] = instance.SerializeToString()
        deletions = request.nodes_to_terminate
        expected_version = request.expected_version if request.expected_version else None
        result, version = self._storage.update(self._table_name, mutations, deletions, expected_version)
        
        reply = UpdateInstanceManagerStateReply()
        reply.success = result 
        reply.version = version
        return reply

    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        pass