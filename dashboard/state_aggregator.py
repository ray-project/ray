import asyncio
import logging

from collections import defaultdict
from typing import List

import ray
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.datacenter import DataSource
import ray.dashboard.memory_utils as memory_utils
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import node_manager_pb2
from ray.experimental.state.common import (
    filter_fields,
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
    TaskState,
    ObjectState,
)

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30


# TODO(sang): Add error handling.
class StateAggregator:
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
        self._raylet_stub = {}
        DataSource.nodes.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._raylet_stub.pop(node_id)
        if change.new:
            node_id, node_info = change.new
            address = "{}:{}".format(
                node_info["nodeManagerAddress"], int(node_info["nodeManagerPort"])
            )
            options = (("grpc.enable_http_proxy", 0),)
            channel = ray._private.utils.init_grpc_channel(
                address, options, asynchronous=True
            )
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._raylet_stub[node_id] = stub

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

    async def get_tasks(self) -> dict:
        async def get_task_info(stub):
            reply = await stub.GetTasksInfo(
                node_manager_pb2.GetTasksInfoRequest(),
                timeout=DEFAULT_RPC_TIMEOUT,
            )
            return reply

        replies = await asyncio.gather(
            *[get_task_info(stub) for stub in self._raylet_stub.values()]
        )

        result = defaultdict(dict)
        for reply in replies:
            tasks = reply.task_info_entries
            for task in tasks:
                data = self._message_to_dict(
                    message=task,
                    fields_to_decode=["task_id"],
                )
                logger.info(data)
                data = filter_fields(data, TaskState)
                result[data["task_id"]] = data
        return result

    async def get_objects(self) -> dict:
        async def get_object_info(stub):
            reply = await stub.GetNodeStats(
                node_manager_pb2.GetNodeStatsRequest(include_memory_info=True),
                timeout=DEFAULT_RPC_TIMEOUT,
            )
            return reply

        replies = await asyncio.gather(
            *[get_object_info(stub) for stub in self._raylet_stub.values()]
        )

        worker_stats = []
        for reply in replies:
            for core_worker_stat in reply.core_workers_stats:
                # NOTE: Set preserving_proto_field_name=False here because
                # `construct_memory_table` requires a dictionary that has
                # modified protobuf name
                # (e.g., workerId instead of worker_id) as a key.
                worker_stats.append(
                    self._message_to_dict(
                        message=core_worker_stat,
                        fields_to_decode=["object_id"],
                        preserving_proto_field_name=False,
                    )
                )
        result = {}
        memory_table = memory_utils.construct_memory_table(worker_stats)
        for entry in memory_table.table:
            data = entry.as_dict()
            data = filter_fields(data, ObjectState)
            result[data["object_ref"]] = data
        return result

    def _message_to_dict(
        self,
        *,
        message,
        fields_to_decode: List[str],
        preserving_proto_field_name: bool = True
    ) -> dict:
        return dashboard_utils.message_to_dict(
            message,
            fields_to_decode,
            including_default_value_fields=True,
            preserving_proto_field_name=preserving_proto_field_name,
        )
