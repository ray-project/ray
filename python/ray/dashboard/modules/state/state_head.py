import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
from typing import Optional

import aiohttp.web
from aiohttp.web import Response

import ray
from ray import ActorID
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray._private.ray_constants import env_integer
from ray.core.generated.gcs_pb2 import ActorTableData
from ray.dashboard.consts import (
    RAY_STATE_SERVER_MAX_HTTP_REQUEST,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED,
    RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME,
)
from ray.dashboard.modules.log.log_manager import LogsManager
from ray.dashboard.state_aggregator import StateAPIManager
from ray.dashboard.state_api_utils import (
    do_reply,
    handle_list_api,
    handle_summary_api,
    options_from_req,
)
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.dashboard.subprocesses.utils import ResponseType
from ray.dashboard.utils import HTTPStatusCode, RateLimitedModule
from ray.util.state.common import (
    DEFAULT_DOWNLOAD_FILENAME,
    DEFAULT_LOG_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    GetLogOptions,
)
from ray.util.state.exception import DataSourceUnavailable
from ray.util.state.state_manager import StateDataSourceClient

logger = logging.getLogger(__name__)

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_STATE_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_STATE_HEAD_TPE_MAX_WORKERS", 1
)

# For filtering ANSI escape codes; the byte string used in the regex is equivalent to r'\x1b\[[\d;]+m'.
ANSI_ESC_PATTERN = re.compile(b"\x1b\\x5b[(\x30-\x39)\x3b]+\x6d")


class StateHead(SubprocessModule, RateLimitedModule):
    """Module to obtain state information from the Ray cluster.

    It is responsible for state observability APIs such as
    ray.list_actors(), ray.get_actor(), ray.summary_actors().
    """

    def __init__(self, *args, **kwargs):
        """Initialize for handling RESTful requests from State API Client"""
        SubprocessModule.__init__(self, *args, **kwargs)
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

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_STATE_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="state_head_executor",
        )

        # To make sure that the internal KV is initialized by getting the lazy property
        assert self.gcs_client is not None
        assert ray.experimental.internal_kv._internal_kv_initialized()

    async def limit_handler_(self):
        return do_reply(
            status_code=HTTPStatusCode.TOO_MANY_REQUESTS,
            error_message=(
                "Max number of in-progress requests="
                f"{self.max_num_call_} reached. "
                "To set a higher limit, set environment variable: "
                f"export {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ENV_NAME}='xxx'. "
                f"Max allowed = {RAY_STATE_SERVER_MAX_HTTP_REQUEST_ALLOWED}"
            ),
            result=None,
        )

    @routes.get("/api/v0/actors")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_actors(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_ACTORS, "1")
        return await handle_list_api(self._state_api.list_actors, req)

    @routes.get("/api/v0/jobs")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_jobs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_JOBS, "1")
        try:
            result = await self._state_api.list_jobs(option=options_from_req(req))
            return do_reply(
                status_code=HTTPStatusCode.OK,
                error_message="",
                result=asdict(result),
            )
        except DataSourceUnavailable as e:
            return do_reply(
                status_code=HTTPStatusCode.INTERNAL_ERROR,
                error_message=str(e),
                result=None,
            )

    @routes.get("/api/v0/nodes")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_nodes(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_NODES, "1")
        return await handle_list_api(self._state_api.list_nodes, req)

    @routes.get("/api/v0/placement_groups")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_placement_groups(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_PLACEMENT_GROUPS, "1")
        return await handle_list_api(self._state_api.list_placement_groups, req)

    @routes.get("/api/v0/workers")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_workers(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_WORKERS, "1")
        return await handle_list_api(self._state_api.list_workers, req)

    @routes.get("/api/v0/tasks")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_tasks(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_TASKS, "1")
        return await handle_list_api(self._state_api.list_tasks, req)

    @routes.get("/api/v0/objects")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_objects(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_OBJECTS, "1")
        return await handle_list_api(self._state_api.list_objects, req)

    @routes.get("/api/v0/runtime_envs")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def list_runtime_envs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_LIST_RUNTIME_ENVS, "1")
        return await handle_list_api(self._state_api.list_runtime_envs, req)

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
            return do_reply(
                status_code=HTTPStatusCode.BAD_REQUEST,
                error_message=(
                    "Both node id and node ip are not provided. "
                    "Please provide at least one of them."
                ),
                result=None,
            )
        if not node_id:
            node_id = await self._log_api.ip_to_node_id(node_ip)
        if not node_id:
            return do_reply(
                status_code=HTTPStatusCode.NOT_FOUND,
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
            return do_reply(
                status_code=HTTPStatusCode.INTERNAL_ERROR,
                error_message=str(e),
                result=None,
            )

        return do_reply(
            status_code=HTTPStatusCode.OK,
            error_message="",
            result=result,
        )

    @routes.get("/api/v0/logs/{media_type}", resp_type=ResponseType.STREAM)
    @RateLimitedModule.enforce_max_concurrent_calls
    async def get_logs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        Fetches logs from the given criteria.
        """
        record_extra_usage_tag(TagKey.CORE_STATE_API_GET_LOG, "1")
        options = GetLogOptions(
            timeout=int(req.query.get("timeout", DEFAULT_RPC_TIMEOUT)),
            node_id=req.query.get("node_id", None),
            node_ip=req.query.get("node_ip", None),
            media_type=req.match_info.get("media_type", "file"),
            # The filename to match on the server side.
            filename=req.query.get("filename", None),
            # The filename to download the log as on the client side.
            download_filename=req.query.get(
                "download_filename", DEFAULT_DOWNLOAD_FILENAME
            ),
            actor_id=req.query.get("actor_id", None),
            task_id=req.query.get("task_id", None),
            submission_id=req.query.get("submission_id", None),
            pid=req.query.get("pid", None),
            lines=req.query.get("lines", DEFAULT_LOG_LIMIT),
            interval=req.query.get("interval", None),
            suffix=req.query.get("suffix", "out"),
            attempt_number=req.query.get("attempt_number", 0),
        )

        filtering_ansi_code = req.query.get("filter_ansi_code", False)

        if isinstance(filtering_ansi_code, str):
            filtering_ansi_code = filtering_ansi_code.lower() == "true"

        logger.info(f"Streaming logs with options: {options}")
        logger.info(f"Filtering ANSI escape codes: {filtering_ansi_code}")

        async def get_actor_fn(actor_id: ActorID) -> Optional[ActorTableData]:
            actor_info_dict = await self.gcs_client.async_get_all_actor_info(
                actor_id=actor_id
            )
            if len(actor_info_dict) == 0:
                return None
            return actor_info_dict[actor_id]

        response = aiohttp.web.StreamResponse(
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{options.download_filename}"'
                )
            },
        )
        response.content_type = "text/plain"

        logs_gen = self._log_api.stream_logs(options, get_actor_fn)
        # Handle the first chunk separately and returns 500 if an error occurs.
        try:
            first_chunk = await logs_gen.__anext__()
            # Filter ANSI escape codes
            if filtering_ansi_code:
                first_chunk = ANSI_ESC_PATTERN.sub(b"", first_chunk)
            await response.prepare(req)
            await response.write(first_chunk)
        except StopAsyncIteration:
            pass
        except asyncio.CancelledError:
            # This happens when the client side closes the connection.
            # Force close the connection and do no-op.
            response.force_close()
            raise
        except Exception as e:
            logger.exception("Error while streaming logs")
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        try:
            async for logs in logs_gen:
                # Filter ANSI escape codes
                if filtering_ansi_code:
                    logs = ANSI_ESC_PATTERN.sub(b"", logs)
                await response.write(logs)
        except Exception:
            logger.exception("Error while streaming logs")
            response.force_close()
            raise

        await response.write_eof()
        return response

    @routes.get("/api/v0/tasks/summarize")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def summarize_tasks(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_TASKS, "1")
        return await handle_summary_api(self._state_api.summarize_tasks, req)

    @routes.get("/api/v0/actors/summarize")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def summarize_actors(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_ACTORS, "1")
        return await handle_summary_api(self._state_api.summarize_actors, req)

    @routes.get("/api/v0/objects/summarize")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def summarize_objects(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        record_extra_usage_tag(TagKey.CORE_STATE_API_SUMMARIZE_OBJECTS, "1")
        return await handle_summary_api(self._state_api.summarize_objects, req)

    @routes.get("/api/v0/tasks/timeline")
    @RateLimitedModule.enforce_max_concurrent_calls
    async def tasks_timeline(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        job_id = req.query.get("job_id")
        download = req.query.get("download")
        result = await self._state_api.generate_task_timeline(job_id)
        if download == "1":
            # Support download if specified.
            now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            content_disposition = (
                f'attachment; filename="timeline-{job_id}-{now_str}.json"'
            )
            headers = {"Content-Disposition": content_disposition}
        else:
            headers = None
        return Response(text=result, content_type="application/json", headers=headers)

    @routes.get("/api/v0/delay/{delay_s}")
    async def delayed_response(self, req: aiohttp.web.Request):
        """Testing only. Response after a specified delay."""
        delay = int(req.match_info.get("delay_s", 10))
        await asyncio.sleep(delay)
        return do_reply(
            status_code=HTTPStatusCode.OK,
            error_message="",
            result={},
            partial_failure_warning=None,
        )

    async def run(self):
        await SubprocessModule.run(self)
        gcs_channel = self.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(
            gcs_channel, self.gcs_client
        )
        self._state_api = StateAPIManager(
            self._state_api_data_source_client,
            self._executor,
        )
        self._log_api = LogsManager(self._state_api_data_source_client)
