import logging

from typing import List

import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.experimental.state.common import (
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
    data_from_schema,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


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
            request, timeout=DEFAULT_TIMEOUT
        )
        result = {}
        for message in reply.actor_table_data:
            data = self._generate_data(message, ActorState, ["actor_id"])
            result[data["actor_id"]] = data
        return result

    async def get_placement_groups(self) -> dict:
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(
            request, timeout=DEFAULT_TIMEOUT
        )
        result = {}
        for message in reply.placement_group_table_data:
            data = self._generate_data(
                message, PlacementGroupState, ["placement_group_id"]
            )
            result[data["placement_group_id"]] = data
        return result

    async def get_nodes(self) -> dict:
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(
            request, timeout=DEFAULT_TIMEOUT
        )
        result = {}
        for message in reply.node_info_list:
            data = self._generate_data(message, NodeState, ["node_id"])
            result[data["node_id"]] = data
        return result

    async def get_workers(self) -> dict:
        request = gcs_service_pb2.GetAllWorkerInfoRequest()
        reply = await self._gcs_worker_info_stub.GetAllWorkerInfo(
            request, timeout=DEFAULT_TIMEOUT
        )
        result = {}
        for message in reply.worker_table_data:
            logger.info(message)
            data = self._generate_data(message, WorkerState, ["worker_id"])
            result[data["worker_id"]] = data
        return result

    def _generate_data(self, message: dict, schema, ids_to_decode: List[str]):
        return data_from_schema(
            schema,
            dashboard_utils.message_to_dict(
                message,
                ids_to_decode,
                including_default_value_fields=True,
                preserving_proto_field_name=True,
            ),
        )
