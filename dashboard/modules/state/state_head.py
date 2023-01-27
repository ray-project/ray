import asyncio
import logging
from dataclasses import asdict
from typing import Callable, List, Tuple, Optional

import aiohttp.web
from abc import ABC, abstractmethod
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.consts import (
    RAY_STATE_SERVER_MAX_HTTP_REQUEST,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME,
)
from ray.dashboard.datacenter import DataSource
from ray.dashboard.modules.log.log_manager import LogsManager
from ray.dashboard.optional_utils import rest_response
from ray.dashboard.state_aggregator import StateAPIManager
from ray.dashboard.utils import Change
from ray.experimental.state.common import (
    RAY_MAX_LIMIT_FROM_API_SERVER,
    ListApiOptions,
    GetLogOptions,
    PredicateType,
    SupportedFilterType,
    SummaryApiOptions,
    SummaryApiResponse,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_LIMIT,
    DEFAULT_LOG_LIMIT,
)
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient
from ray.experimental.state.util import convert_string_to_type


logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class RateLimitedModule(ABC):
    """Simple rate limiter

    Inheriting from this class and decorate any class methods will
    apply simple rate limit.
    It will limit the maximal number of concurrent invocations of **all** the
    methods decorated.

    The below Example class will only allow 10 concurrent calls to A() and B()

    E.g.:

        class Example(RateLimitedModule):
            def __init__(self):
                super().__init__(max_num_call=10)

            @RateLimitedModule.enforce_max_concurrent_calls
            async def A():
                ...

            @RateLimitedModule.enforce_max_concurrent_calls
            async def B():
                ...

            async def limit_handler_(self):
                raise RuntimeError("rate limited reached!")

    """

    def __init__(self, max_num_call: int, logger: Optional[logging.Logger] = None):
        """
        Args:
            max_num_call: Maximal number of concurrent invocations of all decorated
                functions in the instance.
                Setting to -1 will disable rate limiting.

            logger: Logger
        """
        self.max_num_call_ = max_num_call
        self.num_call_ = 0
        self.logger_ = logger

    @staticmethod
    def enforce_max_concurrent_calls(func):
        """Decorator to enforce max number of invocations of the decorated func

        NOTE: This should be used as the innermost decorator if there are multiple
        ones.

        E.g., when decorating functions already with @routes.get(...), this must be
        added below then the routes decorators:
            ```
            @routes.get('/')
            @RateLimitedModule.enforce_max_concurrent_calls
            async def fn(self):
                ...

            ```
        """

        async def async_wrapper(self, *args, **kwargs):
            if self.max_num_call_ >= 0 and self.num_call_ >= self.max_num_call_:
                if self.logger_:
                    self.logger_.warning(
                        f"Max concurrent requests reached={self.max_num_call_}"
                    )
                return await self.limit_handler_()
            self.num_call_ += 1
            try:
                ret = await func(self, *args, **kwargs)
            finally:
                self.num_call_ -= 1
            return ret

        # Returning closure here to avoid passing 'self' to the
        # 'enforce_max_concurrent_calls' decorator.
        return async_wrapper

    @abstractmethod
    async def limit_handler_(self):
        """Handler that is invoked when max number of concurrent calls reached"""


class StateHead(dashboard_utils.DashboardHeadModule, RateLimitedModule):
    """Module to obtain state information from the Ray cluster.

    It is responsible for state observability APIs such as
    ray.list_actors(), ray.get_actor(), ray.summary_actors().
    """

    def __init__(
        self,
        dashboard_head,
    ):
        """Initialize for handling RESTful requests from State API Client"""
        dashboard_utils.DashboardHeadModule.__init__(self, dashboard_head)
        # We don't allow users to configure too high a rate limit
        RateLimitedModule.__init__(
            self,
            min(
                RAY_STATE_SERVER_MAX_HTTP_REQUEST,
                RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
            ),
        )
        self._state_api_data_source_client = None
        self._state_api = None
        self._log_api = None

        DataSource.nodes.signal.append(self._update_raylet_stubs)
        DataSource.agents.signal.append(self._update_agent_stubs)

    async def limit_handler_(self):
        return self._reply(
            success=False,
            error_message=(
                "Max number of in-progress requests="
                f"{self.max_num_call_} reached. "
                "To set a higher limit, set environment variable: "
                f"export {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME}='xxx'. "
                f"Max allowed = {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED}"
            ),
            result=None,
        )

    def _get_filters_from_req(
        self, req: aiohttp.web.Request
    ) -> List[Tuple[str, PredicateType, SupportedFilterType]]:
        filter_keys = req.query.getall("filter_keys", [])
        filter_predicates = req.query.getall("filter_predicates", [])
        filter_values = req.query.getall("filter_values", [])
        assert len(filter_keys) == len(filter_values)
        filters = []
        for key, predicate, val in zip(filter_keys, filter_predicates, filter_values):
            filters.append((key, predicate, val))
        return filters

    def _options_from_req(self, req: aiohttp.web.Request) -> ListApiOptions:
        """Obtain `ListApiOptions` from the aiohttp request."""
        limit = int(
            req.query.get("limit")
            if req.query.get("limit") is not None
            else DEFAULT_LIMIT
        )

        if limit > RAY_MAX_LIMIT_FROM_API_SERVER:
            raise ValueError(
                f"Given limit {limit} exceeds the supported "
                f"limit {RAY_MAX_LIMIT_FROM_API_SERVER}. Use a lower limit."
            )

        timeout = int(req.query.get("timeout", 30))
        filters = self._get_filters_from_req(req)
        detail = convert_string_to_type(req.query.get("detail", False), bool)

        return ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        )

    def _summary_options_from_req(self, req: aiohttp.web.Request) -> SummaryApiOptions:
        timeout = int(req.query.get("timeout", DEFAULT_RPC_TIMEOUT))
        filters = self._get_filters_from_req(req)
        return SummaryApiOptions(timeout=timeout, filters=filters)

    def _reply(self, success: bool, error_message: str, result: dict, **kwargs):
        """Reply to the client."""
        return rest_response(
            success=success,
            message=error_message,
            result=result,
            convert_google_style=False,
            **kwargs,
        )

    async def _update_raylet_stubs(self, change: Change):
        """Callback that's called when a new raylet is added to Datasource.

        Datasource is a api-server-specific module that's updated whenever
        api server adds/removes a new node.

        Args:
            change: The change object. Whenever a new node is added
                or removed, this callback is invoked.
                When new node is added: information is in `change.new`.
                When a node is removed: information is in `change.old`.
                When a node id is overwritten by a new node with the same node id:
                    `change.old` contains the old node info, and
                    `change.new` contains the new node info.
        """
        if change.old:
            # When a node is deleted from the DataSource or it is overwritten.
            node_id, node_info = change.old
            self._state_api_data_source_client.unregister_raylet_client(node_id)
        if change.new:
            # When a new node information is written to DataSource.
            node_id, node_info = change.new
            self._state_api_data_source_client.register_raylet_client(
                node_id,
                node_info["nodeManagerAddress"],
                int(node_info["nodeManagerPort"]),
            )

    async def _update_agent_stubs(self, change: Change):
        """Callback that's called when a new agent is added to Datasource."""
        if change.old:
            node_id, _ = change.old
            self._state_api_data_source_client.unregister_agent_client(node_id)
        if change.new:
            # When a new node information is written to DataSource.
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]
            self._state_api_data_source_client.register_agent_client(
                node_id,
                ip,
                int(ports[1]),
            )

    async def _handle_list_api(
        self, list_api_fn: Callable[[ListApiOptions], dict], req: aiohttp.web.Request
    ):
        try:
            result = await list_api_fn(option=self._options_from_req(req))
            return self._reply(
                success=True,
                error_message="",
                result=asdict(result),
            )
        except DataSourceUnavailable as e:
            return self._reply(success=False, error_message=str(e), result=None)

    @routes.get("/api/v0/actors")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_actors(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_ACTORS, "1")
        return await self._handle_list_api(self._state_api.list_actors, req)

    @routes.get("/api/v0/jobs")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_jobs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_JOBS, "1")
        try:
            result = await self._state_api.list_jobs(option=self._options_from_req(req))
            return self._reply(
                success=True,
                error_message="",
                result=asdict(result),
            )
        except DataSourceUnavailable as e:
            return self._reply(success=False, error_message=str(e), result=None)

    @routes.get("/api/v0/nodes")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_nodes(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_NODES, "1")
        return await self._handle_list_api(self._state_api.list_nodes, req)

    @routes.get("/api/v0/placement_groups")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_placement_groups(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_PLACEMENT_GROUPS, "1")
        return await self._handle_list_api(self._state_api.list_placement_groups, req)

    @routes.get("/api/v0/workers")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_workers(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_WORKERS, "1")
        return await self._handle_list_api(self._state_api.list_workers, req)

    @routes.get("/api/v0/tasks")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_tasks(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_TASKS, "1")
        return await self._handle_list_api(self._state_api.list_tasks, req)

    @routes.get("/api/v0/objects")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_objects(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_OBJECTS, "1")
        return await self._handle_list_api(self._state_api.list_objects, req)

    @routes.get("/api/v0/runtime_envs")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_runtime_envs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_RUNTIME_ENVS, "1")
        return await self._handle_list_api(self._state_api.list_runtime_envs, req)

    @routes.get("/api/v0/cluster_events")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_cluster_events(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_CLUSTER_EVENTS, "1")
        return await self._handle_list_api(self._state_api.list_cluster_events, req)

    @routes.get("/api/v0/logs")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_logs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return a list of log files on a given node id.

        Unlike other list APIs that display all existing resources in the cluster,
        this API always require to specify node id and node ip.
        """
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_LOGS, "1")
        glob_filter = req.query.get("glob", "*")
        node_id = req.query.get("node_id", None)
        node_ip = req.query.get("node_ip", None)
        timeout = int(req.query.get("timeout", DEFAULT_RPC_TIMEOUT))

        if not node_id and not node_ip:
            return self._reply(
                success=False,
                error_message=(
                    "Both node id and node ip are not provided. "
                    "Please provide at least one of them."
                ),
                result=None,
            )

        node_id = node_id or self._log_api.ip_to_node_id(node_ip)
        if not node_id:
            return self._reply(
                success=False,
                error_message=(
                    f"Cannot find matching node_id for a given node ip {node_ip}"
                ),
                result=None,
            )

        try:
            result = await self._log_api.list_logs(
                node_id, timeout, glob_filter=glob_filter
            )
        except DataSourceUnavailable as e:
            return self._reply(success=False, error_message=str(e), result=None)

        return self._reply(success=True, error_message="", result=result)

    @routes.get("/api/v0/logs/{media_type}")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def get_logs(self, req: aiohttp.web.Request):
        record_extra_usage_tag(TagKey.CORE_STATE_API_GET_LOG, "1")
        options = GetLogOptions(
            timeout=int(req.query.get("timeout", DEFAULT_RPC_TIMEOUT)),
            node_id=req.query.get("node_id", None),
            node_ip=req.query.get("node_ip", None),
            media_type=req.match_info.get("media_type", "file"),
            filename=req.query.get("filename", None),
            actor_id=req.query.get("actor_id", None),
            task_id=req.query.get("task_id", None),
            pid=req.query.get("pid", None),
            lines=req.query.get("lines", DEFAULT_LOG_LIMIT),
            interval=req.query.get("interval", None),
            suffix=req.query.get("suffix", None),
        )

        response = aiohttp.web.StreamResponse()
        response.content_type = "text/plain"
        await response.prepare(req)

        # NOTE: The first byte indicates the success / failure of individual
        # stream. If the first byte is b"1", it means the stream was successful.
        # If it is b"0", it means it is failed.
        try:
            async for logs_in_bytes in self._log_api.stream_logs(options):
                logs_to_stream = bytearray(b"1")
                logs_to_stream.extend(logs_in_bytes)
                await response.write(bytes(logs_to_stream))
            await response.write_eof()
            return response
        except asyncio.CancelledError:
            # This happens when the client side closes the connection.
            # Fofce close the connection and do no-op.
            response.force_close()
            raise
        except Exception as e:
            logger.exception(e)
            error_msg = bytearray(b"0")
            error_msg.extend(
                f"Closing HTTP stream due to internal server error.\n{e}".encode()
            )

            await response.write(bytes(error_msg))
            await response.write_eof()
            return response

    async def _handle_summary_api(
        self,
        summary_fn: Callable[[SummaryApiOptions], SummaryApiResponse],
        req: aiohttp.web.Request,
    ):
        result = await summary_fn(option=self._summary_options_from_req(req))
        return self._reply(
            success=True,
            error_message="",
            result=asdict(result),
        )

    @routes.get("/api/v0/tasks/summarize")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def summarize_tasks(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_TASKS, "1")
        return await self._handle_summary_api(self._state_api.summarize_tasks, req)

    @routes.get("/api/v0/actors/summarize")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def summarize_actors(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_ACTORS, "1")
        return await self._handle_summary_api(self._state_api.summarize_actors, req)

    @routes.get("/api/v0/objects/summarize")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def summarize_objects(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_OBJECTS, "1")
        return await self._handle_summary_api(self._state_api.summarize_objects, req)

    @routes.get("/api/v0/delay/{delay_s}")
    async def delayed_response(self, req: aiohttp.web.Request):
        """Testing only. Response after a specified delay."""
        delay = int(req.match_info.get("delay_s", 10))
        await asyncio.sleep(delay)
        return self._reply(
            success=True,
            error_message="",
            result={},
            partial_failure_warning=None,
        )

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(
            gcs_channel, self._dashboard_head.gcs_aio_client
        )
        self._state_api = StateAPIManager(self._state_api_data_source_client)
        self._log_api = LogsManager(self._state_api_data_source_client)

    @staticmethod
    def is_minimal_module():
        return False
