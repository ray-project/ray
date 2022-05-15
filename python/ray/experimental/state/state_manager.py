import logging
import inspect

from functools import wraps

import grpc
import ray

from typing import Dict, List
from ray import ray_constants

from ray.core.generated.gcs_service_pb2 import (
    GetAllActorInfoRequest,
    GetAllActorInfoReply,
    GetAllPlacementGroupRequest,
    GetAllPlacementGroupReply,
    GetAllNodeInfoRequest,
    GetAllNodeInfoReply,
    GetAllWorkerInfoRequest,
    GetAllWorkerInfoReply,
)
from ray.core.generated.node_manager_pb2 import (
    GetTasksInfoRequest,
    GetTasksInfoReply,
    GetNodeStatsRequest,
    GetNodeStatsReply,
)
from ray.core.generated.runtime_env_agent_pb2 import (
    GetRuntimeEnvsInfoRequest,
    GetRuntimeEnvsInfoReply,
)
from ray.core.generated.runtime_env_agent_pb2_grpc import RuntimeEnvServiceStub
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.node_manager_pb2_grpc import NodeManagerServiceStub
from ray.dashboard.modules.job.common import JobInfoStorageClient, JobInfo

logger = logging.getLogger(__name__)


class StateSourceNetworkException(Exception):
    """Exceptions raised when there's a network error from data source query."""

    pass


def handle_network_errors(func):
    """Apply the network error handling logic to each APIs,
        such as retry or exception policies.

    It is a helper method for `StateDataSourceClient`.
    The method can only be used for async methods.
    """
    assert inspect.iscoroutinefunction(func)

    @wraps(func)
    async def api_with_network_error_handler(*args, **kwargs):
        # TODO(sang): Add a retry policy.
        try:
            return await func(*args, **kwargs)
        except (
            # https://grpc.github.io/grpc/python/grpc_asyncio.html#grpc-exceptions
            grpc.aio.AioRpcError,
            grpc.aio.InternalError,
            grpc.aio.AbortError,
            grpc.aio.BaseError,
            grpc.aio.UsageError,
        ) as e:
            raise StateSourceNetworkException(
                f"Failed to query the data source, {func}"
            ) from e

    return api_with_network_error_handler


class StateDataSourceClient:
    """The client to query states from various data sources such as Raylet, GCS, Agents.

    Note that it doesn't directly query core workers. They are proxied through raylets.

    The module is not in charge of service discovery. The caller is responsible for
    finding services and register stubs through `register*` APIs.

    Non `register*` APIs
    - throw a ValueError if it cannot find the source.
    - throw `StateSourceNetworkException` if there's any network errors.
    """

    def __init__(self, gcs_channel: grpc.aio.Channel):
        self.register_gcs_client(gcs_channel)
        self._raylet_stubs = {}
        self._agent_stubs = {}
        self._job_client = JobInfoStorageClient()

    def register_gcs_client(self, gcs_channel: grpc.aio.Channel):
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

    def register_raylet_client(self, node_id: str, address: str, port: int):
        full_addr = f"{address}:{port}"
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            full_addr, options, asynchronous=True
        )
        self._raylet_stubs[node_id] = NodeManagerServiceStub(channel)

    def unregister_raylet_client(self, node_id: str):
        self._raylet_stubs.pop(node_id)

    def register_agent_client(self, node_id, address: str, port: int):
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            f"{address}:{port}", options=options, asynchronous=True
        )
        self._agent_stubs[node_id] = RuntimeEnvServiceStub(channel)

    def unregister_agent_client(self, node_id: str):
        self._agent_stubs.pop(node_id)

    def get_all_registered_raylet_ids(self) -> List[str]:
        return self._raylet_stubs.keys()

    def get_all_registered_agent_ids(self) -> List[str]:
        return self._agent_stubs.keys()

    @handle_network_errors
    async def get_all_actor_info(self, timeout: int = None) -> GetAllActorInfoReply:
        request = GetAllActorInfoRequest()
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(
            request, timeout=timeout
        )
        return reply

    @handle_network_errors
    async def get_all_placement_group_info(
        self, timeout: int = None
    ) -> GetAllPlacementGroupReply:
        request = GetAllPlacementGroupRequest()
        reply = await self._gcs_pg_info_stub.GetAllPlacementGroup(
            request, timeout=timeout
        )
        return reply

    @handle_network_errors
    async def get_all_node_info(self, timeout: int = None) -> GetAllNodeInfoReply:
        request = GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(request, timeout=timeout)
        return reply

    @handle_network_errors
    async def get_all_worker_info(self, timeout: int = None) -> GetAllWorkerInfoReply:
        request = GetAllWorkerInfoRequest()
        reply = await self._gcs_worker_info_stub.GetAllWorkerInfo(
            request, timeout=timeout
        )
        return reply

    def get_job_info(self) -> Dict[str, JobInfo]:
        # Cannot use @handle_network_errors because async def is not supported yet.
        # TODO(sang): Support timeout & make it async
        try:
            return self._job_client.get_all_jobs()
        except Exception as e:
            raise StateSourceNetworkException("Failed to query the job info.") from e

    @handle_network_errors
    async def get_task_info(
        self, node_id: str, timeout: int = None
    ) -> GetTasksInfoReply:
        stub = self._raylet_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Raylet for a node id, {node_id} doesn't exist.")

        reply = await stub.GetTasksInfo(GetTasksInfoRequest(), timeout=timeout)
        return reply

    @handle_network_errors
    async def get_object_info(
        self, node_id: str, timeout: int = None
    ) -> GetNodeStatsReply:
        stub = self._raylet_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Raylet for a node id, {node_id} doesn't exist.")

        reply = await stub.GetNodeStats(
            GetNodeStatsRequest(include_memory_info=True),
            timeout=timeout,
        )
        return reply

    @handle_network_errors
    async def get_runtime_envs_info(
        self, node_id: str, timeout: int = None
    ) -> GetRuntimeEnvsInfoReply:
        stub = self._agent_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Agent for a node id, {node_id} doesn't exist.")

        reply = await stub.GetRuntimeEnvsInfo(
            GetRuntimeEnvsInfoRequest(),
            timeout=timeout,
        )
        return reply
