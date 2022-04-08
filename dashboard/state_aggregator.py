import logging

from dataclasses import fields

import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.experimental.state.common import (
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
)

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30


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
            data = self.message_to_dict_by_schema(
                message=message, schema=ActorState, ids_to_decode=["actor_id"]
            )
            result[data["actor_id"]] = data
        return result

    async def get_placement_groups(self) -> dict:
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for message in reply.placement_group_table_data:
            data = self.message_to_dict_by_schema(
                message=message,
                schema=PlacementGroupState,
                ids_to_decode=["placement_group_id"],
            )
            result[data["placement_group_id"]] = data
        return result

    async def get_nodes(self) -> dict:
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for message in reply.node_info_list:
            data = self.message_to_dict_by_schema(
                message=message, schema=NodeState, ids_to_decode=["node_id"]
            )
            result[data["node_id"]] = data
        return result

    async def get_workers(self) -> dict:
        request = gcs_service_pb2.GetAllWorkerInfoRequest()
        reply = await self._gcs_worker_info_stub.GetAllWorkerInfo(
            request, timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for message in reply.worker_table_data:
            data = self.message_to_dict_by_schema(
                message=message, schema=WorkerState, ids_to_decode=["worker_id"]
            )
            result[data["worker_id"]] = data
        return result

    def message_to_dict_by_schema(
        self, *, message, schema, ids_to_decode  # gRPC messages
    ) -> dict:
        """Generate the data from gRPC message filtered by the given schema.

        Example:
            message gRPCmessage {
                actor_id
                other_field
            }

            grpc_message = gRPCmessage(actor_id=[ABCD in bytes], other_field=XYZ)

            @dataclass
            class Schema:
                actor_id: str

            message_to_dict_by_schema(grpc_message, schema, ids_to_decode=["actor_id"])
            -> {
                actor_id: ABCD
            }

        Args:
            message: gRPC message.
            schema: The dataclass which contains the fields that correspond to
                gRPC message key. The returned dictionary will only contain keys
                from this dataclass.
            ids_to_decode: A list of fields that need to be decoded
                to hex string from bytes. For example, if the schema contains
                "actor_id", the caller can pass ["actor_id"] to decode the
                bytes id to hex string.
        """
        data = dashboard_utils.message_to_dict(
            message,
            ids_to_decode,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )
        filtered_data = {}
        for field in fields(schema):
            filtered_data[field.name] = data[field.name]
        return filtered_data
