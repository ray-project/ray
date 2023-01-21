import inspect
import logging
from collections import defaultdict
from functools import wraps
from typing import Dict, List, Optional

import grpc
from grpc.aio._call import UnaryStreamCall

import ray
import ray.dashboard.modules.log.log_consts as log_consts
from ray._private import ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_service_pb2 import (
    GetAllActorInfoReply,
    GetAllActorInfoRequest,
    GetAllNodeInfoReply,
    GetAllNodeInfoRequest,
    GetAllPlacementGroupReply,
    GetAllPlacementGroupRequest,
    GetAllWorkerInfoReply,
    GetAllWorkerInfoRequest,
    GetTaskEventsReply,
    GetTaskEventsRequest,
)
from ray.core.generated.node_manager_pb2 import (
    GetObjectsInfoReply,
    GetObjectsInfoRequest,
    GetTasksInfoReply,
    GetTasksInfoRequest,
)
from ray.core.generated.node_manager_pb2_grpc import NodeManagerServiceStub
from ray.core.generated.reporter_pb2 import (
    ListLogsReply,
    ListLogsRequest,
    StreamLogRequest,
)
from ray.core.generated.reporter_pb2_grpc import LogServiceStub
from ray.core.generated.runtime_env_agent_pb2 import (
    GetRuntimeEnvsInfoReply,
    GetRuntimeEnvsInfoRequest,
)
from ray.core.generated.runtime_env_agent_pb2_grpc import RuntimeEnvServiceStub
from ray.dashboard.datacenter import DataSource
from ray.dashboard.modules.job.common import JobInfo, JobInfoStorageClient
from ray.dashboard.utils import Dict as Dictionary
from ray.experimental.state.common import RAY_MAX_LIMIT_FROM_DATA_SOURCE
from ray.experimental.state.exception import DataSourceUnavailable

logger = logging.getLogger(__name__)

_STATE_MANAGER_GRPC_OPTIONS = [
    *ray_constants.GLOBAL_GRPC_OPTIONS,
    ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
]


def handle_grpc_network_errors(func):
    """Decorator to add a network handling logic.

    It is a helper method for `StateDataSourceClient`.
    The method can only be used for async methods.
    """
    assert inspect.iscoroutinefunction(func)

    @wraps(func)
    async def api_with_network_error_handler(*args, **kwargs):
        """Apply the network error handling logic to each APIs,
        such as retry or exception policies.

        Returns:
            If RPC succeeds, it returns what the original function returns.
            If RPC fails, it raises exceptions.
        Exceptions:
            DataSourceUnavailable: if the source is unavailable because it is down
                or there's a slow network issue causing timeout.
            Otherwise, the raw network exceptions (e.g., gRPC) will be raised.
        """
        try:
            return await func(*args, **kwargs)
        except grpc.aio.AioRpcError as e:
            if (
                e.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                or e.code() == grpc.StatusCode.UNAVAILABLE
            ):
                raise DataSourceUnavailable(
                    "Failed to query the data source. "
                    "It is either there's a network issue, or the source is down."
                )
            else:
                logger.exception(e)
                raise e

    return api_with_network_error_handler


class IdToIpMap:
    def __init__(self):
        # Node IP to node ID mapping.
        self._ip_to_node_id = defaultdict(str)
        # Node ID to node IP mapping.
        self._node_id_to_ip = defaultdict(str)

    def put(self, node_id: str, address: str):
        self._ip_to_node_id[address] = node_id
        self._node_id_to_ip[node_id] = address

    def get_ip(self, node_id: str):
        return self._node_id_to_ip.get(node_id)

    def get_node_id(self, address: str):
        return self._ip_to_node_id.get(address)

    def pop(self, node_id: str):
        """Pop the given node id.

        Returns:
            False if the corresponding node id doesn't exist.
            True if it pops correctly.
        """
        ip = self._node_id_to_ip.get(node_id)
        if not ip:
            return None
        assert ip in self._ip_to_node_id
        self._node_id_to_ip.pop(node_id)
        self._ip_to_node_id.pop(ip)
        return True


class StateDataSourceClient:
    """The client to query states from various data sources such as Raylet, GCS, Agents.

    Note that it doesn't directly query core workers. They are proxied through raylets.

    The module is not in charge of service discovery. The caller is responsible for
    finding services and register stubs through `register*` APIs.

    Non `register*` APIs
    - Return the protobuf directly if it succeeds to query the source.
    - Raises an exception if there's any network issue.
    - throw a ValueError if it cannot find the source.
    """

    def __init__(self, gcs_channel: grpc.aio.Channel, gcs_aio_client: GcsAioClient):
        self.register_gcs_client(gcs_channel)
        self._raylet_stubs = {}
        self._runtime_env_agent_stub = {}
        self._log_agent_stub = {}
        self._job_client = JobInfoStorageClient(gcs_aio_client)
        self._id_id_map = IdToIpMap()

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
        self._gcs_task_info_stub = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(
            gcs_channel
        )

    def register_raylet_client(self, node_id: str, address: str, port: int):
        full_addr = f"{address}:{port}"
        options = _STATE_MANAGER_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            full_addr, options, asynchronous=True
        )
        self._raylet_stubs[node_id] = NodeManagerServiceStub(channel)
        self._id_id_map.put(node_id, address)

    def unregister_raylet_client(self, node_id: str):
        self._raylet_stubs.pop(node_id)
        self._id_id_map.pop(node_id)

    def register_agent_client(self, node_id, address: str, port: int):
        options = _STATE_MANAGER_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            f"{address}:{port}", options=options, asynchronous=True
        )
        self._runtime_env_agent_stub[node_id] = RuntimeEnvServiceStub(channel)
        self._log_agent_stub[node_id] = LogServiceStub(channel)
        self._id_id_map.put(node_id, address)

    def unregister_agent_client(self, node_id: str):
        self._runtime_env_agent_stub.pop(node_id)
        self._log_agent_stub.pop(node_id)
        self._id_id_map.pop(node_id)

    def get_all_registered_raylet_ids(self) -> List[str]:
        return self._raylet_stubs.keys()

    def get_all_registered_agent_ids(self) -> List[str]:
        assert len(self._log_agent_stub) == len(self._runtime_env_agent_stub)
        return self._runtime_env_agent_stub.keys()

    def ip_to_node_id(self, ip: Optional[str]) -> Optional[str]:
        """Return the node id that corresponds to the given ip.

        Args:
            ip: The ip address.

        Returns:
            None if the corresponding id doesn't exist.
            Node id otherwise. If None node_ip is given,
            it will also return None.
        """
        if not ip:
            return None
        return self._id_id_map.get_node_id(ip)

    @handle_grpc_network_errors
    async def get_all_actor_info(
        self, timeout: int = None, limit: int = None
    ) -> Optional[GetAllActorInfoReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE

        request = GetAllActorInfoRequest(limit=limit)
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(
            request, timeout=timeout
        )
        return reply

    @handle_grpc_network_errors
    async def get_all_task_info(
        self, timeout: int = None, limit: int = None, exclude_driver: bool = True
    ) -> Optional[GetTaskEventsReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE
        request = GetTaskEventsRequest(limit=limit, exclude_driver=exclude_driver)
        reply = await self._gcs_task_info_stub.GetTaskEvents(request, timeout=timeout)
        return reply

    @handle_grpc_network_errors
    async def get_all_placement_group_info(
        self, timeout: int = None, limit: int = None
    ) -> Optional[GetAllPlacementGroupReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE

        request = GetAllPlacementGroupRequest(limit=limit)
        reply = await self._gcs_pg_info_stub.GetAllPlacementGroup(
            request, timeout=timeout
        )
        return reply

    @handle_grpc_network_errors
    async def get_all_node_info(
        self, timeout: int = None
    ) -> Optional[GetAllNodeInfoReply]:
        request = GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(request, timeout=timeout)
        return reply

    @handle_grpc_network_errors
    async def get_all_worker_info(
        self, timeout: int = None, limit: int = None
    ) -> Optional[GetAllWorkerInfoReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE

        request = GetAllWorkerInfoRequest(limit=limit)
        reply = await self._gcs_worker_info_stub.GetAllWorkerInfo(
            request, timeout=timeout
        )
        return reply

    async def get_job_info(self) -> Optional[Dict[str, JobInfo]]:
        # Cannot use @handle_grpc_network_errors because async def is not supported yet.
        # TODO(sang): Support timeout & make it async
        try:
            return await self._job_client.get_all_jobs()
        except grpc.aio.AioRpcError as e:
            if (
                e.code == grpc.StatusCode.DEADLINE_EXCEEDED
                or e.code == grpc.StatusCode.UNAVAILABLE
            ):
                raise DataSourceUnavailable(
                    "Failed to query the data source. "
                    "It is either there's a network issue, or the source is down."
                )
            else:
                logger.exception(e)
                raise e

    async def get_all_cluster_events(self) -> Dictionary:
        return DataSource.events

    @handle_grpc_network_errors
    async def get_task_info(
        self, node_id: str, timeout: int = None, limit: int = None
    ) -> Optional[GetTasksInfoReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE

        stub = self._raylet_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Raylet for a node id, {node_id} doesn't exist.")

        reply = await stub.GetTasksInfo(
            GetTasksInfoRequest(limit=limit), timeout=timeout
        )
        return reply

    @handle_grpc_network_errors
    async def get_object_info(
        self, node_id: str, timeout: int = None, limit: int = None
    ) -> Optional[GetObjectsInfoReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE

        stub = self._raylet_stubs.get(node_id)
        if not stub:
            raise ValueError(f"Raylet for a node id, {node_id} doesn't exist.")

        reply = await stub.GetObjectsInfo(
            GetObjectsInfoRequest(limit=limit),
            timeout=timeout,
        )
        return reply

    @handle_grpc_network_errors
    async def get_runtime_envs_info(
        self, node_id: str, timeout: int = None, limit: int = None
    ) -> Optional[GetRuntimeEnvsInfoReply]:
        if not limit:
            limit = RAY_MAX_LIMIT_FROM_DATA_SOURCE

        stub = self._runtime_env_agent_stub.get(node_id)
        if not stub:
            raise ValueError(f"Agent for a node id, {node_id} doesn't exist.")

        reply = await stub.GetRuntimeEnvsInfo(
            GetRuntimeEnvsInfoRequest(limit=limit),
            timeout=timeout,
        )
        return reply

    @handle_grpc_network_errors
    async def list_logs(
        self, node_id: str, glob_filter: str, timeout: int = None
    ) -> ListLogsReply:
        stub = self._log_agent_stub.get(node_id)
        if not stub:
            raise ValueError(f"Agent for node id: {node_id} doesn't exist.")
        return await stub.ListLogs(
            ListLogsRequest(glob_filter=glob_filter), timeout=timeout
        )

    @handle_grpc_network_errors
    async def stream_log(
        self,
        node_id: str,
        log_file_name: str,
        keep_alive: bool,
        lines: int,
        interval: Optional[float],
        timeout: int,
    ) -> UnaryStreamCall:
        stub = self._log_agent_stub.get(node_id)
        if not stub:
            raise ValueError(f"Agent for node id: {node_id} doesn't exist.")
        stream = stub.StreamLog(
            StreamLogRequest(
                keep_alive=keep_alive,
                log_file_name=log_file_name,
                lines=lines,
                interval=interval,
            ),
            timeout=timeout,
        )
        await self._validate_stream(stream)
        return stream

    @staticmethod
    async def _validate_stream(stream):
        metadata = await stream.initial_metadata()
        if metadata.get(log_consts.LOG_GRPC_ERROR) == log_consts.FILE_NOT_FOUND:
            raise ValueError('File "{log_file_name}" not found on node {node_id}')
