from typing import Dict, List
from python.ray._private.utils import binary_to_hex
from python.ray.autoscaler._private.util import format_readonly_node_type
from python.ray.core.generated.autoscaler_pb2 import (
    GetClusterResourceStateReply,
    NodeStatus,
)
from ray._raylet import GcsClient
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
    NodeKind,
)


class ReadOnlyProvider(ICloudInstanceProvider):
    def __init__(self, provider_config: dict):
        self._provider_config = provider_config
        self._gcs_address = provider_config["gcs_address"]

        self._gcs_client = GcsClient(address=self._gcs_address)

    def get_non_terminated(self) -> Dict[str, CloudInstance]:
        str_reply = self._gcs_client.get_cluster_resource_state()
        reply = GetClusterResourceStateReply()
        reply.ParseFromString(str_reply)
        cluster_resource_state = reply.cluster_resource_state

        cloud_instances = {}
        for gcs_node_state in cluster_resource_state.node_states:
            if gcs_node_state.status == NodeStatus.DEAD:
                # Skip dead nodes.
                continue

            # Use node's node id if instance id is not available
            cloud_instance_id = (
                gcs_node_state.instance_id
                if gcs_node_state.instance_id
                else binary_to_hex(gcs_node_state.node_id)
            )

            # TODO: we should add a field to the proto to indicate if the node is head
            # or not.
            is_head = "node:__internal_head__" in dict(gcs_node_state.total_resources)

            cloud_instances[cloud_instance_id] = CloudInstance(
                cloud_instance_id=cloud_instance_id,
                node_kind=NodeKind.HEAD if is_head else NodeKind.WORKER,
                node_type=format_readonly_node_type(
                    binary_to_hex(gcs_node_state.node_id)
                ),
                is_running=True,
            )

        return cloud_instances

    def terminate(self, instance_id: CloudInstanceId) -> None:
        raise NotImplementedError("Cannot terminate instances in read-only mode.")

    def launch(
        self, shape: Dict[CloudInstanceId, int], request_id: CloudInstanceId
    ) -> None:
        raise NotImplementedError("Cannot launch instances in read-only mode.")

    def poll_errors(self) -> List[CloudInstanceProviderError]:
        return []
