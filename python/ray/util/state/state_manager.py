import dataclasses
import inspect
import logging
from functools import wraps
from typing import List, Optional, Tuple
import json

import aiohttp
import grpc
from grpc.aio._call import UnaryStreamCall

import ray
import ray.dashboard.modules.log.log_consts as log_consts
import ray.dashboard.consts as dashboard_consts
from ray._private import ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray._private.utils import hex_to_binary
from ray._raylet import ActorID, JobID, TaskID, NodeID
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import ActorTableData, GcsNodeInfo
from ray.core.generated.gcs_service_pb2 import (
    FilterPredicate,
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
from ray.dashboard.modules.job.common import JobInfoStorageClient
from ray.dashboard.modules.job.pydantic_models import JobDetails, JobType
from ray.dashboard.modules.job.utils import get_driver_jobs
from ray.util.state.common import (
    RAY_MAX_LIMIT_FROM_DATA_SOURCE,
    PredicateType,
    SupportedFilterType,
)
from ray.util.state.exception import DataSourceUnavailable

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
        self._job_client = JobInfoStorageClient(gcs_aio_client)
        self._gcs_aio_client = gcs_aio_client
        self._client_session = aiohttp.ClientSession()

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

    def get_raylet_stub(self, ip: str, port: int):
        options = _STATE_MANAGER_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            f"{ip}:{port}", options, asynchronous=True
        )
        return NodeManagerServiceStub(channel)

    async def get_log_service_stub(self, node_id: NodeID) -> LogServiceStub:
        """Returns None if the agent on the node is not registered in Internal KV."""
        agent_addr = await self._gcs_aio_client.internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id.hex()}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )
        if not agent_addr:
            return None
        ip, http_port, grpc_port = json.loads(agent_addr)
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            f"{ip}:{grpc_port}", options=options, asynchronous=True
        )
        return LogServiceStub(channel)

    async def ip_to_node_id(self, ip: Optional[str]) -> Optional[str]:
        """Return the node id in hex that corresponds to the given ip.

        Args:
            ip: The ip address.

        Returns:
            None if the corresponding id doesn't exist.
            Node id otherwise. If None node_ip is given,
            it will also return None.
        """
        if not ip:
            return None
        # Uses the dashboard agent keys to find ip -> id mapping.
        agent_addr = await self._gcs_aio_client.internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_IP_PREFIX}{ip}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )
        if not agent_addr:
            return None
        node_id, http_port, grpc_port = json.loads(agent_addr)
        return node_id

    @handle_grpc_network_errors
    async def get_all_actor_info(
        self,
        timeout: int = None,
        limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE,
        filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    ) -> Optional[GetAllActorInfoReply]:
        if filters is None:
            filters = []

        req_filters = GetAllActorInfoRequest.Filters()
        for filter in filters:
            key, predicate, value = filter
            if predicate != "=":
                # We only support EQUAL predicate for source side filtering.
                continue
            if key == "actor_id":
                req_filters.actor_id = ActorID(hex_to_binary(value)).binary()
            elif key == "state":
                # Convert to uppercase.
                value = value.upper()
                if value not in ActorTableData.ActorState.keys():
                    raise ValueError(f"Invalid actor state for filtering: {value}")
                req_filters.state = ActorTableData.ActorState.Value(value)
            elif key == "job_id":
                req_filters.job_id = JobID(hex_to_binary(value)).binary()

        request = GetAllActorInfoRequest(limit=limit, filters=req_filters)
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(
            request, timeout=timeout
        )
        return reply

    @handle_grpc_network_errors
    async def get_all_task_info(
        self,
        timeout: int = None,
        limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE,
        filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
        exclude_driver: bool = False,
    ) -> Optional[GetTaskEventsReply]:

        if filters is None:
            filters = []

        req_filters = GetTaskEventsRequest.Filters()
        for filter in filters:
            key, predicate, value = filter
            filter_predicate = None
            if predicate == "=":
                filter_predicate = FilterPredicate.EQUAL
            elif predicate == "!=":
                filter_predicate = FilterPredicate.NOT_EQUAL
            else:
                # We only support EQUAL and NOT_EQUAL predicate for source side
                # filtering. If invalid predicates were specified, it should already be
                # raised when the filters arguments are parsed
                assert False, "Invalid predicate: " + predicate

            if key == "actor_id":
                actor_filter = GetTaskEventsRequest.Filters.ActorIdFilter()
                actor_filter.actor_id = ActorID(hex_to_binary(value)).binary()
                actor_filter.predicate = filter_predicate
                req_filters.actor_filters.append(actor_filter)

            elif key == "job_id":
                job_filter = GetTaskEventsRequest.Filters.JobIdFilter()
                job_filter.job_id = JobID(hex_to_binary(value)).binary()
                job_filter.predicate = filter_predicate
                req_filters.job_filters.append(job_filter)

            elif key == "task_id":
                task_filter = GetTaskEventsRequest.Filters.TaskIdFilter()
                task_filter.task_id = TaskID(hex_to_binary(value)).binary()
                task_filter.predicate = filter_predicate
                req_filters.task_filters.append(task_filter)

            elif key == "name":
                task_name_filter = GetTaskEventsRequest.Filters.TaskNameFilter()
                task_name_filter.task_name = value
                task_name_filter.predicate = filter_predicate
                req_filters.task_name_filters.append(task_name_filter)

            elif key == "state":
                state_filter = GetTaskEventsRequest.Filters.StateFilter()
                state_filter.state = value
                state_filter.predicate = filter_predicate
                req_filters.state_filters.append(state_filter)

            else:
                continue

        req_filters.exclude_driver = exclude_driver

        request = GetTaskEventsRequest(limit=limit, filters=req_filters)
        reply = await self._gcs_task_info_stub.GetTaskEvents(request, timeout=timeout)
        return reply

    @handle_grpc_network_errors
    async def get_all_placement_group_info(
        self, timeout: int = None, limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE
    ) -> Optional[GetAllPlacementGroupReply]:

        request = GetAllPlacementGroupRequest(limit=limit)
        reply = await self._gcs_pg_info_stub.GetAllPlacementGroup(
            request, timeout=timeout
        )
        return reply

    @handle_grpc_network_errors
    async def get_all_node_info(
        self,
        timeout: int = None,
        limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE,
        filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    ) -> Optional[GetAllNodeInfoReply]:
        # TODO(ryw): move this to GcsAioClient.get_all_node_info, i.e.
        # InnerGcsClient.async_get_all_node_info

        if filters is None:
            filters = []

        req_filters = GetAllNodeInfoRequest.Filters()
        for filter in filters:
            key, predicate, value = filter
            if predicate != "=":
                # We only support EQUAL predicate for source side filtering.
                continue

            if key == "node_id":
                req_filters.node_id = NodeID(hex_to_binary(value)).binary()
            elif key == "state":
                value = value.upper()
                if value not in GcsNodeInfo.GcsNodeState.keys():
                    raise ValueError(f"Invalid node state for filtering: {value}")
                req_filters.state = GcsNodeInfo.GcsNodeState.Value(value)
            elif key == "node_name":
                req_filters.node_name = value
            else:
                continue

        request = GetAllNodeInfoRequest(limit=limit, filters=req_filters)
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(request, timeout=timeout)
        return reply

    @handle_grpc_network_errors
    async def get_all_worker_info(
        self,
        timeout: int = None,
        limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE,
        filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    ) -> Optional[GetAllWorkerInfoReply]:

        if filters is None:
            filters = []

        req_filters = GetAllWorkerInfoRequest.Filters()
        for filter in filters:
            key, predicate, value = filter
            # Special treatments for the Ray Debugger.
            if (
                key == "num_paused_threads"
                and predicate in ("!=", ">")
                and value == "0"
            ):
                req_filters.exist_paused_threads = True
                continue
            if key == "is_alive" and predicate == "=" and value == "True":
                req_filters.is_alive = True
                continue
            else:
                continue

        request = GetAllWorkerInfoRequest(limit=limit, filters=req_filters)
        reply = await self._gcs_worker_info_stub.GetAllWorkerInfo(
            request, timeout=timeout
        )
        return reply

    # TODO(rickyx):
    # This is currently mirroring dashboard/modules/job/job_head.py::list_jobs
    # We should eventually unify the logic.
    async def get_job_info(self, timeout: int = None) -> List[JobDetails]:
        # Cannot use @handle_grpc_network_errors because async def is not supported yet.

        driver_jobs, submission_job_drivers = await get_driver_jobs(
            self._gcs_aio_client, timeout=timeout
        )
        submission_jobs = await self._job_client.get_all_jobs(timeout=timeout)
        submission_jobs = [
            JobDetails(
                **dataclasses.asdict(job),
                submission_id=submission_id,
                job_id=submission_job_drivers.get(submission_id).id
                if submission_id in submission_job_drivers
                else None,
                driver_info=submission_job_drivers.get(submission_id),
                type=JobType.SUBMISSION,
            )
            for submission_id, job in submission_jobs.items()
        ]

        return list(driver_jobs.values()) + submission_jobs

    @handle_grpc_network_errors
    async def get_object_info(
        self,
        node_manager_ip: str,
        node_manager_port: int,
        timeout: int = None,
        limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE,
    ) -> Optional[GetObjectsInfoReply]:
        stub = self.get_raylet_stub(node_manager_ip, node_manager_port)

        reply = await stub.GetObjectsInfo(
            GetObjectsInfoRequest(limit=limit),
            timeout=timeout,
        )
        return reply

    async def get_runtime_envs_info(
        self,
        node_ip: str,
        runtime_env_agent_port: int,
        timeout: int = None,
        limit: int = RAY_MAX_LIMIT_FROM_DATA_SOURCE,
    ) -> Optional[GetRuntimeEnvsInfoReply]:
        if not node_ip or not runtime_env_agent_port:
            raise ValueError(
                f"Expected non empty node ip and runtime env agent port, got {node_ip} and {runtime_env_agent_port}."
            )
        timeout = aiohttp.ClientTimeout(total=timeout)
        url = f"http://{node_ip}:{runtime_env_agent_port}/get_runtime_envs_info"
        request = GetRuntimeEnvsInfoRequest(limit=limit)
        data = request.SerializeToString()
        async with self._client_session.post(url, data=data, timeout=timeout) as resp:
            if resp.status >= 200 and resp.status < 300:
                response_data = await resp.read()
                reply = GetRuntimeEnvsInfoReply()
                reply.ParseFromString(response_data)
                return reply
            else:
                raise DataSourceUnavailable(
                    "Failed to query the runtime env agent for get_runtime_envs_info. "
                    "Either there's a network issue, or the source is down. "
                    f"Response is {resp.status}, reason {resp.reason}"
                )

    @handle_grpc_network_errors
    async def list_logs(
        self, node_id: str, glob_filter: str, timeout: int = None
    ) -> ListLogsReply:
        stub = await self.get_log_service_stub(NodeID.from_hex(node_id))
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
        start_offset: Optional[int] = None,
        end_offset: Optional[int] = None,
    ) -> UnaryStreamCall:
        stub = await self.get_log_service_stub(NodeID.from_hex(node_id))
        if not stub:
            raise ValueError(f"Agent for node id: {node_id} doesn't exist.")

        stream = stub.StreamLog(
            StreamLogRequest(
                keep_alive=keep_alive,
                log_file_name=log_file_name,
                lines=lines,
                interval=interval,
                start_offset=start_offset,
                end_offset=end_offset,
            ),
            timeout=timeout,
        )
        metadata = await stream.initial_metadata()
        if metadata.get(log_consts.LOG_GRPC_ERROR) is not None:
            raise ValueError(metadata.get(log_consts.LOG_GRPC_ERROR))
        return stream
