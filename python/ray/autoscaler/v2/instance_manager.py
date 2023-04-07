import logging

from ray.core.generated.instance_manager_pb2 import (
    GetAvailableInstanceTypesResponse,
    GetInstanceManagerStateReply,
    UpdateInstanceManagerStateReply,
    UpdateInstanceManagerStateRequest,
)

provider_exists = False

logger = logging.getLogger(__name__)

class InstanceManager:
    def get_available_instance_types(self) -> GetAvailableInstanceTypesResponse:
        pass

    def update_instance_manager_state(
        self, request: UpdateInstanceManagerStateRequest
    ) -> UpdateInstanceManagerStateReply:
        pass

    def get_instance_manager_state(self) -> GetInstanceManagerStateReply:
        pass
