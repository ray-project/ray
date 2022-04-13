import logging

import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.experimental.state.common import (
    filter_fields,
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
)

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30


# TODO(sang): Add error handling.
class GcsStateAggregator:
    def __init__(self, gcs_channel):
        self._gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            gcs_channel
        )
        self._gcs_pg_info_stub = gcs_service_pb2_grpc.PlacementGroupInfoGcsServiceStub(
            gcs_channel
        )
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            gcs_channel
        )
        self._gcs_worker_info_stub = gcs_service_pb2_grpc.WorkerInfoGcsServiceStub(
            gcs_channel
        )

    async def get_actors(self) -> dict:
        request = gcs_service_pb2.GetAllActorInfoRequest()
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for message in reply.actor_table_data:
            data = self._message_to_dict(message=message, fields_to_decode=["actor_id"])
            data = filter_fields(data, ActorState)
            result[data["actor_id"]] = data
        return result

    async def get_placement_groups(self) -> dict:
        request = gcs_service_pb2.GetAllPlacementGroupRequest()
        reply = await self._gcs_pg_info_stub.GetAllPlacementGroup(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        logger.error(reply)
        for message in reply.placement_group_table_data:
            data = self._message_to_dict(
                message=message,
                fields_to_decode=["placement_group_id"],
            )
            data = filter_fields(data, PlacementGroupState)
            result[data["placement_group_id"]] = data
        return result

    async def get_nodes(self) -> dict:
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for message in reply.node_info_list:
            data = self._message_to_dict(message=message, fields_to_decode=["node_id"])
            data = filter_fields(data, NodeState)
            result[data["node_id"]] = data
        return result

    async def get_workers(self) -> dict:
        request = gcs_service_pb2.GetAllWorkerInfoRequest()
        reply = await self._gcs_worker_info_stub.GetAllWorkerInfo(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for message in reply.worker_table_data:
            data = self._message_to_dict(
                message=message, fields_to_decode=["worker_id"]
            )
            data["worker_id"] = data["worker_address"]["worker_id"]
            data = filter_fields(data, WorkerState)
            result[data["worker_id"]] = data
        return result

    def _message_to_dict(self, *, message, fields_to_decode) -> dict:
        return dashboard_utils.message_to_dict(
            message,
            fields_to_decode,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )
