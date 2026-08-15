import asyncio
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp.web

import ray.dashboard.consts as dashboard_consts
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray import NodeID
from ray._common.network_utils import build_address
from ray._common.usage.usage_constants import CLUSTER_METADATA_KEY
from ray._private.grpc_utils import init_grpc_channel
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
    GLOBAL_GRPC_OPTIONS,
    KV_NAMESPACE_CLUSTER,
    KV_NAMESPACE_DASHBOARD,
    MAX_PROFILING_DURATION_S,
    MIN_PROFILING_DURATION_S,
    RAY_DASHBOARD_ENABLE_PROFILING,
    RAY_DASHBOARD_PROFILING_CPU_DURATION_DEFAULT,
    RAY_DASHBOARD_PROFILING_CPU_FORMAT_DEFAULT,
    RAY_DASHBOARD_PROFILING_IDLE_DEFAULT,
    RAY_DASHBOARD_PROFILING_LEAKS_DEFAULT,
    RAY_DASHBOARD_PROFILING_MEMORY_DURATION_DEFAULT,
    RAY_DASHBOARD_PROFILING_MEMORY_FORMAT_DEFAULT,
    RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT,
    RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT,
    RAY_DASHBOARD_PROFILING_TRACE_PYTHON_ALLOCATORS_DEFAULT,
    env_integer,
)
from ray.autoscaler._private.commands import debug_status
from ray.core.generated import reporter_pb2, reporter_pb2_grpc
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
from ray.dashboard.modules.reporter.utils import HealthChecker
from ray.dashboard.state_aggregator import StateAPIManager
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.util.state.common import ListApiOptions
from ray.util.state.state_manager import StateDataSourceClient

logger = logging.getLogger(__name__)

EMOJI_WARNING = "&#x26A0;&#xFE0F;"
WARNING_FOR_MULTI_TASK_IN_A_WORKER = (
    "Warning: This task is running in a worker process that is running multiple tasks. "
    "This can happen if you are profiling a task right as it finishes or if you"
    "are using the Async Actor or Threaded Actors pattern. "
    "The information that follows may come from any of these tasks:"
)
SVG_STYLE = """<style>
    svg {
        width: 100%;
        height: 100%;
    }
</style>\n"""

# Carries WARNING_FOR_MULTI_TASK_IN_A_WORKER on profiling responses whose body
# can't hold an HTML fragment (`raw`, `speedscope`).
MULTI_TASK_WARNING_HEADER = "X-Ray-Profiling-Warning"

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS", 1
)


def _query_flag(req: aiohttp.web.Request, name: str, default: bool) -> bool:
    """Parse a boolean profiling query flag.

    Falls back to ``default`` (an operator-configurable env var default) when the
    param is absent; an explicit ``"1"``/``"0"`` in the query always wins.
    """
    val = req.query.get(name)
    return default if val is None else val == "1"


def _query_duration(req: aiohttp.web.Request, default: int) -> int:
    """Parse and validate the profiling ``duration`` query param (seconds).

    Falls back to ``default`` when the param is absent (the configured defaults
    are already clamped to the accepted range at load time). Raises
    ``aiohttp.web.HTTPBadRequest`` (HTTP 400) if an explicit value is not an
    integer or is outside ``[MIN_PROFILING_DURATION_S, MAX_PROFILING_DURATION_S]``.
    """
    val = req.query.get("duration")
    if val is None:
        return default
    try:
        duration_s = int(val)
    except ValueError:
        raise aiohttp.web.HTTPBadRequest(
            text="duration query parameter must be an integer"
        )
    if not MIN_PROFILING_DURATION_S <= duration_s <= MAX_PROFILING_DURATION_S:
        raise aiohttp.web.HTTPBadRequest(
            text=(
                f"duration must be between {MIN_PROFILING_DURATION_S} and "
                f"{MAX_PROFILING_DURATION_S} seconds, got: {duration_s}"
            )
        )
    return duration_s


def _task_cpu_profiling_response(
    output: str, format: str, task_ids_in_a_worker: List[str]
) -> aiohttp.web.Response:
    """Build the ``/task/cpu_profile`` response for a successful profile.

    Only ``flamegraph`` output is an SVG a browser can render, so it's the only
    format wrapped in HTML: ``SVG_STYLE`` sizes the image, and the multi-task
    notice is an HTML fragment. ``raw`` is a plain stack listing and
    ``speedscope`` is a JSON document, so prepending markup to either corrupts
    it -- speedscope output stops parsing as JSON. Those are returned verbatim
    as ``text/plain``, matching how ``/worker/cpu_profile`` already switches on
    format, with the multi-task notice moved to a header so it isn't lost.
    """
    multi_task = len(task_ids_in_a_worker) > 1
    warning = WARNING_FOR_MULTI_TASK_IN_A_WORKER + str(task_ids_in_a_worker)

    if format == "flamegraph":
        body = SVG_STYLE + output
        if multi_task:
            body = (
                '<p style="color: #E37400;">{} {} </br> </p> </br>'.format(
                    EMOJI_WARNING, warning
                )
                + body
            )
        return aiohttp.web.Response(text=body, content_type="text/html")

    response = aiohttp.web.Response(text=output, content_type="text/plain")
    if multi_task:
        response.headers[MULTI_TASK_WARNING_HEADER] = warning
    return response


class ReportHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ray_config = None
        # TODO(fyrestone): Avoid using ray.state in dashboard, it's not
        # asynchronous and will lead to low performance. ray disconnect()
        # will be hang when the ray.state is connected and the GCS is exit.
        # Please refer to: https://github.com/ray-project/ray/issues/16328
        self.service_discovery = PrometheusServiceDiscoveryWriter(
            self.gcs_address, self.temp_dir, self.session_dir
        )
        self._state_api = None
        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="reporter_head_executor",
        )

        # Fetched from GCS only once on startup in run(). It's static throughout the
        # the cluster's lifetime.
        self.cluster_metadata = None

        self._health_checker = HealthChecker(self.gcs_client)

    def _profiling_disabled_response(self) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            status=403,
            text=(
                "Profiling is disabled by default for security reasons. "
                "To enable profiling, set the environment variable "
                "RAY_DASHBOARD_ENABLE_PROFILING=1 on the Ray head node. "
                "See https://docs.ray.io/en/latest/ray-observability/"
                "user-guides/profiling.html for details."
            ),
        )

    @routes.get("/api/profiling_enabled")
    async def profiling_enabled(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        # `profiling_defaults` lets the dashboard seed its profiling dialogs with
        # the operator-configured defaults. Keys are google-style-cased in the
        # response, e.g. `cpu_duration` -> `data.profilingDefaults.cpuDuration`.
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="",
            profiling_enabled=RAY_DASHBOARD_ENABLE_PROFILING,
            profiling_defaults={
                "native": RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT,
                "subprocesses": RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT,
                "idle": RAY_DASHBOARD_PROFILING_IDLE_DEFAULT,
                "leaks": RAY_DASHBOARD_PROFILING_LEAKS_DEFAULT,
                "trace_python_allocators": (
                    RAY_DASHBOARD_PROFILING_TRACE_PYTHON_ALLOCATORS_DEFAULT
                ),
                "cpu_duration": RAY_DASHBOARD_PROFILING_CPU_DURATION_DEFAULT,
                "memory_duration": RAY_DASHBOARD_PROFILING_MEMORY_DURATION_DEFAULT,
                # Upper bound the endpoint enforces on `duration` (operator-
                # configurable). Exposed so the UI validates against the real cap
                # instead of a hardcoded one. The lower bound is always 1s.
                "max_duration": MAX_PROFILING_DURATION_S,
                "cpu_format": RAY_DASHBOARD_PROFILING_CPU_FORMAT_DEFAULT,
                "memory_format": RAY_DASHBOARD_PROFILING_MEMORY_FORMAT_DEFAULT,
                # py-spy only appends --native on Linux (see profile_manager),
                # so the UI can disable the Native checkbox for the CPU/stack-
                # trace dialogs off-Linux where it would be a silent no-op.
                # memray native is cross-platform, so its checkbox is unaffected.
                "pyspy_native_supported": sys.platform == "linux",
            },
        )

    @routes.get("/api/v0/cluster_metadata")
    async def get_cluster_metadata(self, req):
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="",
            **self.cluster_metadata,
        )

    @routes.get("/api/cluster_status")
    async def get_cluster_status(self, req):
        """Returns status information about the cluster.

        Currently contains two fields:
            autoscaling_status (str)-- a status message from the autoscaler.
            autoscaling_error (str)-- an error message from the autoscaler if
                anything has gone wrong during autoscaling.

        These fields are both read from the GCS, it's expected that the
        autoscaler writes them there.
        """
        # TODO(rickyx): We should be able to get the cluster status from the
        # autoscaler directly with V2. And we should be able to return structured data
        # rather than a string.

        return_formatted_output = req.query.get("format", "0") == "1"

        (legacy_status, formatted_status_string, error) = await asyncio.gather(
            *[
                self.gcs_client.async_internal_kv_get(
                    key.encode(), namespace=None, timeout=GCS_RPC_TIMEOUT_SECONDS
                )
                for key in [
                    DEBUG_AUTOSCALING_STATUS_LEGACY,
                    DEBUG_AUTOSCALING_STATUS,
                    DEBUG_AUTOSCALING_ERROR,
                ]
            ]
        )

        formatted_status = (
            json.loads(formatted_status_string.decode())
            if formatted_status_string
            else {}
        )

        if not return_formatted_output:
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="Got cluster status.",
                autoscaling_status=legacy_status.decode() if legacy_status else None,
                autoscaling_error=error.decode() if error else None,
                cluster_status=formatted_status if formatted_status else None,
            )
        else:
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="Got formatted cluster status.",
                cluster_status=debug_status(
                    formatted_status_string, error, address=self.gcs_address
                ),
            )

    async def get_task_ids_running_in_a_worker(self, worker_id: str) -> List[str]:
        """
        Retrieves the task IDs of running tasks associated with a specific worker.

        Args:
            worker_id: The ID of the worker.

        Returns:
            List[str]: A list containing the task IDs
            of all the running tasks associated with the worker.
        """
        option = ListApiOptions(
            filters=[("worker_id", "=", worker_id), ("state", "=", "RUNNING")],
            detail=True,
            timeout=10,
        )
        # Call the state API to get all tasks in a worker
        tasks_in_a_worker_result = await self._state_api.list_tasks(option=option)
        tasks_in_a_worker = tasks_in_a_worker_result.result

        # Get task_id from each task in a worker
        task_ids_in_a_worker = [
            task.get("task_id")
            for task in tasks_in_a_worker
            if task and "task_id" in task
        ]
        return task_ids_in_a_worker

    async def get_worker_details_for_running_task(
        self, task_id: str, attempt_number: int
    ) -> Tuple[Optional[int], Optional[str]]:
        """Retrieves worker details for a specific task and attempt number.

        Args:
            task_id: The ID of the task.
            attempt_number: The attempt number of the task.

        Returns:
            Tuple[Optional[int], Optional[str]]: A tuple containing the worker's PID
            (process ID), and worker's ID.

        Raises:
            ValueError: If the task attempt is not running or the state API is not initialized.
        """
        if self._state_api is None:
            raise ValueError("The state API is not initialized yet. Please retry.")
        option = ListApiOptions(
            filters=[
                ("task_id", "=", task_id),
                ("attempt_number", "=", attempt_number),
            ],
            detail=True,
            timeout=10,
        )

        result = await self._state_api.list_tasks(option=option)
        tasks = result.result
        if not tasks:
            return None, None

        pid = tasks[0]["worker_pid"]
        worker_id = tasks[0]["worker_id"]

        state = tasks[0]["state"]
        if state != "RUNNING":
            raise ValueError(
                f"The task attempt is not running: the current state is {state}."
            )

        return pid, worker_id

    @routes.get("/task/traceback")
    async def get_task_traceback(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """Retrieves the traceback information for a specific task.
        Note that one worker process works on one task at a time
        or one worker works on multiple async tasks.

        Args:
            req: A request with the following query parameters:
                task_id: The ID of the task.
                attempt_number: The attempt number of the task.
                node_id: The ID of the node.

        Returns:
            aiohttp.web.Response: The HTTP response containing the traceback information.

        Raises:
            ValueError: If the "task_id" parameter is missing in the request query.
            ValueError: If the "attempt_number" parameter is missing in the request query.
            ValueError: If the worker begins working on another task during the traceback retrieval.
            aiohttp.web.HTTPInternalServerError: If there is an internal server error during the traceback retrieval.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()
        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")
        if "node_id" not in req.query:
            raise ValueError("node_id is required")

        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")
        node_id_hex = req.query.get("node_id")

        addrs = await self._get_stub_address_by_node_id(NodeID.from_hex(node_id_hex))
        if not addrs:
            raise aiohttp.web.HTTPInternalServerError(
                text=f"Failed to get agent address for node {node_id_hex}"
            )
        node_id, ip, http_port, grpc_port = addrs
        reporter_stub = self._make_stub(build_address(ip, grpc_port))

        # `native`/`subprocesses` default to the operator-configured env var
        # (both off unless overridden on the head node).
        native = _query_flag(req, "native", RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT)
        subprocesses = _query_flag(
            req, "subprocesses", RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT
        )

        try:
            (pid, _) = await self.get_worker_details_for_running_task(
                task_id, attempt_number
            )
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        logger.info(
            "Sending stack trace request to {}:{} with native={}, subprocesses={}".format(
                ip, pid, native, subprocesses
            )
        )
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(
                pid=pid, native=native, subprocesses=subprocesses
            )
        )

        """
            In order to truly confirm whether there are any other tasks
            running during the profiling, we need to retrieve all tasks
            that are currently running or have finished, and then parse
            the task events (i.e., their start and finish times) to check
            for any potential overlap. However, this process can be
            quite extensive, so here we will make our best efforts
            to check for any overlapping tasks.
            Therefore, we will check if the task is still running
        """
        try:
            (_, worker_id) = await self.get_worker_details_for_running_task(
                task_id, attempt_number
            )

        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))
        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

        logger.info("Returning stack trace, size {}".format(len(reply.output)))

        task_ids_in_a_worker = await self.get_task_ids_running_in_a_worker(worker_id)
        return aiohttp.web.Response(
            text=(
                WARNING_FOR_MULTI_TASK_IN_A_WORKER
                + str(task_ids_in_a_worker)
                + "\n"
                + reply.output
                if len(task_ids_in_a_worker) > 1
                else reply.output
            )
        )

    @routes.get("/task/cpu_profile")
    async def get_task_cpu_profile(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """Retrieves the CPU profile for a specific task.
        Note that one worker process works on one task at a time
        or one worker works on multiple async tasks.

        Args:
            req: A request with the following query parameters:
                task_id: The ID of the task.
                attempt_number: The attempt number of the task.
                node_id: The ID of the node.
                duration: Optional. Duration in seconds for profiling. Defaults to
                    RAY_DASHBOARD_PROFILING_CPU_DURATION_DEFAULT and must be within
                    [MIN_PROFILING_DURATION_S, MAX_PROFILING_DURATION_S].
                format: Optional. Output format, one of
                    VALID_CPU_PROFILING_FORMATS. Defaults to
                    RAY_DASHBOARD_PROFILING_CPU_FORMAT_DEFAULT.
                native: Optional. Whether to use native profiling. Defaults to
                    RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT.

        Returns:
            aiohttp.web.Response: The HTTP response containing the CPU profile
                data. ``flamegraph`` output is served as ``text/html`` (the SVG
                wrapped for display); every other format is served verbatim as
                ``text/plain``.

        Raises:
            ValueError: If the "task_id" parameter is missing in the request query.
            ValueError: If the "attempt_number" parameter is missing in the request query.
            ValueError: If the worker begins working on another task during the profile retrieval.
            aiohttp.web.HTTPBadRequest: If "duration" is not an integer or falls outside the accepted range.
            aiohttp.web.HTTPInternalServerError: If there is an internal server error during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If the CPU Flame Graph information for the task is not found.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()
        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")
        if "node_id" not in req.query:
            raise ValueError("node_id is required")

        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")
        node_id_hex = req.query.get("node_id")

        duration_s = _query_duration(req, RAY_DASHBOARD_PROFILING_CPU_DURATION_DEFAULT)
        format = req.query.get("format", RAY_DASHBOARD_PROFILING_CPU_FORMAT_DEFAULT)

        # native/idle/subprocesses default to the operator-configured env vars
        # (all off unless overridden on the head node).
        native = _query_flag(req, "native", RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT)
        idle = _query_flag(req, "idle", RAY_DASHBOARD_PROFILING_IDLE_DEFAULT)
        subprocesses = _query_flag(
            req, "subprocesses", RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT
        )
        addrs = await self._get_stub_address_by_node_id(NodeID.from_hex(node_id_hex))
        if not addrs:
            raise aiohttp.web.HTTPInternalServerError(
                text=f"Failed to get agent address for node {node_id_hex}"
            )
        node_id, ip, http_port, grpc_port = addrs
        reporter_stub = self._make_stub(build_address(ip, grpc_port))

        try:
            (pid, _) = await self.get_worker_details_for_running_task(
                task_id, attempt_number
            )
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        logger.info(
            f"Sending CPU profiling request to {build_address(ip, grpc_port)}, pid {pid}, for {task_id} with native={native}, idle={idle}, subprocesses={subprocesses}"
        )

        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid,
                duration=duration_s,
                format=format,
                native=native,
                idle=idle,
                subprocesses=subprocesses,
            )
        )

        """
            In order to truly confirm whether there are any other tasks
            running during the profiling, we need to retrieve all tasks
            that are currently running or have finished, and then parse
            the task events (i.e., their start and finish times) to check
            for any potential overlap. However, this process can be quite
            extensive, so here we will make our best efforts to check
            for any overlapping tasks. Therefore, we will check if
            the task is still running
        """
        try:
            (_, worker_id) = await self.get_worker_details_for_running_task(
                task_id, attempt_number
            )
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)
        logger.info("Returning profiling response, size {}".format(len(reply.output)))

        task_ids_in_a_worker = await self.get_task_ids_running_in_a_worker(worker_id)
        return _task_cpu_profiling_response(reply.output, format, task_ids_in_a_worker)

    @routes.get("/worker/traceback")
    async def get_traceback(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Retrieves the traceback information for a specific worker.

        Args:
            req: A request with the following query parameters:
                pid: Required. The PID of the worker.
                ip or node_id: Required. The IP address or hex ID of the node.
                native: Optional. Whether to include native (C/C++) stack frames.
                    Defaults to RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT.
                subprocesses: Optional. Whether to also trace child processes of
                    the worker. Defaults to
                    RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT.

        Returns:
            aiohttp.web.Response: The HTTP response containing the traceback
            information, or an HTTPInternalServerError if the request fails.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()
        pid = req.query.get("pid")
        ip = req.query.get("ip")
        node_id_hex = req.query.get("node_id")
        if not pid:
            raise ValueError("pid is required")
        if not node_id_hex and not ip:
            raise ValueError("ip or node_id is required")

        if node_id_hex:
            addrs = await self._get_stub_address_by_node_id(
                NodeID.from_hex(node_id_hex)
            )
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at node_id {node_id_hex}"
                )
        else:
            addrs = await self._get_stub_address_by_ip(ip)
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at IP {ip}"
                )

        node_id, ip, http_port, grpc_port = addrs
        reporter_stub = self._make_stub(build_address(ip, grpc_port))
        # `native`/`subprocesses` default to the operator-configured env var
        # (both off unless overridden on the head node).
        native = _query_flag(req, "native", RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT)
        subprocesses = _query_flag(
            req, "subprocesses", RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT
        )
        logger.info(
            f"Sending stack trace request to {build_address(ip, grpc_port)}, pid {pid}, with native={native}, subprocesses={subprocesses}"
        )
        try:
            pid = int(pid)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text="pid must be an integer")
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(
                pid=pid, native=native, subprocesses=subprocesses
            )
        )
        if reply.success:
            logger.info("Returning stack trace, size {}".format(len(reply.output)))
            return aiohttp.web.Response(text=reply.output)
        else:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

    @routes.get("/worker/cpu_profile")
    async def cpu_profile(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Retrieves the CPU profile for a specific worker.

        Args:
            req: A request with the following query parameters:
                pid: Required. The PID of the worker.
                ip or node_id: Required. The IP address or hex ID of the node.
                duration: Optional. Duration in seconds for profiling. Defaults to
                    RAY_DASHBOARD_PROFILING_CPU_DURATION_DEFAULT and must be within
                    [MIN_PROFILING_DURATION_S, MAX_PROFILING_DURATION_S].
                format: Optional. Output format, one of
                    VALID_CPU_PROFILING_FORMATS. Defaults to
                    RAY_DASHBOARD_PROFILING_CPU_FORMAT_DEFAULT.
                native: Optional. Whether to use native profiling. Defaults to
                    RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT.
                idle: Optional. Whether to include off-CPU / sleeping threads
                    in the profile. Defaults to
                    RAY_DASHBOARD_PROFILING_IDLE_DEFAULT.
                subprocesses: Optional. Whether to also profile child processes
                    of the worker. Defaults to
                    RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT.

        Returns:
            aiohttp.web.Response: The HTTP response containing the CPU profile data,
            or an HTTPInternalServerError if the request fails.

        Raises:
            ValueError: If pid is not provided.
            ValueError: If ip or node_id is not provided.
            aiohttp.web.HTTPBadRequest: If "pid" or "duration" is not an integer, or "duration" falls outside the accepted range.
            aiohttp.web.HTTPInternalServerError: If there is an internal server error during the profile retrieval.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()
        pid = req.query.get("pid")
        ip = req.query.get("ip")
        node_id_hex = req.query.get("node_id")
        if not pid:
            raise ValueError("pid is required")
        if not node_id_hex and not ip:
            raise ValueError("ip or node_id is required")

        if node_id_hex:
            addrs = await self._get_stub_address_by_node_id(
                NodeID.from_hex(node_id_hex)
            )
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at node_id {node_id_hex}"
                )
        else:
            addrs = await self._get_stub_address_by_ip(ip)
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at IP {ip}"
                )

        node_id, ip, http_port, grpc_port = addrs
        reporter_stub = self._make_stub(build_address(ip, grpc_port))

        try:
            pid = int(pid)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text="pid must be an integer")
        duration_s = _query_duration(req, RAY_DASHBOARD_PROFILING_CPU_DURATION_DEFAULT)
        format = req.query.get("format", RAY_DASHBOARD_PROFILING_CPU_FORMAT_DEFAULT)

        # native/idle/subprocesses default to the operator-configured env vars
        # (all off unless overridden on the head node).
        native = _query_flag(req, "native", RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT)
        idle = _query_flag(req, "idle", RAY_DASHBOARD_PROFILING_IDLE_DEFAULT)
        subprocesses = _query_flag(
            req, "subprocesses", RAY_DASHBOARD_PROFILING_SUBPROCESSES_DEFAULT
        )
        logger.info(
            f"Sending CPU profiling request to {build_address(ip, grpc_port)}, pid {pid}, with native={native}, idle={idle}, subprocesses={subprocesses}"
        )
        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid,
                duration=duration_s,
                format=format,
                native=native,
                idle=idle,
                subprocesses=subprocesses,
            )
        )
        if reply.success:
            logger.info(
                "Returning profiling response, size {}".format(len(reply.output))
            )
            return aiohttp.web.Response(
                body=reply.output,
                headers={
                    "Content-Type": (
                        "image/svg+xml" if format == "flamegraph" else "text/plain"
                    )
                },
            )
        else:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

    @routes.get("/worker/gpu_profile")
    async def gpu_profile(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Retrieves the Torch GPU profile trace for a specific worker.

        This is a Torch-specific API. It is not supported for other frameworks.

        Params:
            req: A request with the following query parameters:
                pid: Required. The PID of the GPU training worker.
                ip or node_id: Required. The IP address or hex ID of the node where the GPU training worker is running.
                num_iterations: Number of training steps for profiling. Defaults to 4
                    This is the number of calls to the torch Optimizer.step().

        Returns:
            A redirect to the log API to download the GPU profiling trace file.

        Raises:
            aiohttp.web.HTTPInternalServerError: if one of the following happens:
                (1) The GPU profiling dependencies are not installed on the target node.
                (2) The target node doesn't have GPUs.
                (3) The GPU profiling fails or times out.
                    The output will contain a description of the error.
                    For example, trying to profile a non-Torch training process will
                    result in an error.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()

        pid = req.query.get("pid")
        ip = req.query.get("ip")
        node_id_hex = req.query.get("node_id")
        if not pid:
            raise ValueError("pid is required")
        if not node_id_hex and not ip:
            raise ValueError("ip or node_id is required")

        if node_id_hex:
            addrs = await self._get_stub_address_by_node_id(
                NodeID.from_hex(node_id_hex)
            )
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at node_id {node_id_hex}, pid {pid}"
                )
        else:
            addrs = await self._get_stub_address_by_ip(ip)
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at IP {ip}, pid {pid}"
                )

        node_id, ip, http_port, grpc_port = addrs
        reporter_stub = self._make_stub(build_address(ip, grpc_port))

        # Profile for num_iterations training steps (calls to optimizer.step())
        try:
            num_iterations = int(req.query.get("num_iterations", 4))
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text="num_iterations must be an integer")

        logger.info(
            f"Sending GPU profiling request to {build_address(ip, grpc_port)}, pid {pid}. "
            f"Profiling for {num_iterations} training steps."
        )

        try:
            pid = int(pid)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text="pid must be an integer")

        reply = await reporter_stub.GpuProfiling(
            reporter_pb2.GpuProfilingRequest(pid=pid, num_iterations=num_iterations)
        )

        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)
        logger.info("Returning profiling response, size {}".format(len(reply.output)))

        filepath = str(reply.output)
        download_filename = Path(filepath).name

        query = urlencode(
            {
                "node_ip": ip,
                "filename": filepath,
                "download_filename": download_filename,
                "lines": "-1",
            }
        )
        redirect_url = f"/api/v0/logs/file?{query}"
        raise aiohttp.web.HTTPFound(redirect_url)

    @routes.get("/worker/jax_profile")
    async def jax_profile(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Retrieves the JAX profile trace for a specific worker.

        Params:
            req: A request with the following query parameters:
                pid: Required. The PID of the worker.
                port: Optional. The port where JAX profiler server is listening.
                      If not provided, it will be automatically discovered from GCS.
                ip or node_id: Required. The IP address or hex ID of the node.
                duration: Optional. Duration in seconds for profiling (default: 5).

        Returns:
            JSON response with the path where trace files are saved.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()

        pid = req.query.get("pid")
        port = req.query.get("port")
        ip = req.query.get("ip")
        node_id_hex = req.query.get("node_id")

        if not pid:
            raise ValueError("pid is required")
        if not node_id_hex and not ip:
            raise ValueError("ip or node_id is required")

        if node_id_hex:
            addrs = await self._get_stub_address_by_node_id(
                NodeID.from_hex(node_id_hex)
            )
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at node_id {node_id_hex}"
                )
        else:
            addrs = await self._get_stub_address_by_ip(ip)
            if not addrs:
                raise aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to get agent address for node at IP {ip}"
                )

        node_id, ip, http_port, grpc_port = addrs
        reporter_stub = self._make_stub(build_address(ip, grpc_port))

        try:
            duration_s = int(req.query.get("duration", 5))
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(
                text="duration query parameter must be an integer"
            )
        try:
            pid = int(pid)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text="pid must be an integer")

        if not port:
            port_bytes = await self.gcs_client.async_internal_kv_get(
                f"jax_profiler_port:{node_id.hex()}:{pid}".encode(),
                namespace=KV_NAMESPACE_DASHBOARD,
                timeout=GCS_RPC_TIMEOUT_SECONDS,
            )
            if port_bytes:
                try:
                    port = int(port_bytes.decode())
                except (UnicodeDecodeError, ValueError):
                    raise aiohttp.web.HTTPInternalServerError(
                        text=(
                            f"Discovered JAX profiler port for worker {pid} on node"
                            f"{node_id.hex()} is corrupt: {port_bytes!r}"
                        )
                    )
            else:
                raise aiohttp.web.HTTPBadRequest(
                    text=(
                        f"port is required because JAX profiler port could not be "
                        f"automatically discovered for worker {pid} on node {node_id.hex()}"
                    )
                )
        else:
            try:
                port = int(port)
            except ValueError:
                raise aiohttp.web.HTTPBadRequest(text="port must be an integer")

        logger.info(
            f"Sending JAX profiling request to {build_address(ip, grpc_port)}, pid {pid}, port {port}"
        )

        reply = await reporter_stub.JaxProfiling(
            reporter_pb2.JaxProfilingRequest(pid=pid, port=port, duration=duration_s)
        )

        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

        logger.info("Returning profiling response, location {}".format(reply.output))

        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="JAX profiling finished.",
            trace_directory=reply.output,
        )

    @routes.get("/memory_profile")
    async def memory_profile(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Retrieves the memory profile for a specific worker or task.
        Note that for tasks, one worker process works on one task at a time
        or one worker works on multiple async tasks.

        Args:
            req: A request with one of the following sets of query parameters.

                Worker:
                    pid: The PID of the worker.
                    ip or node_id: The IP address or hex ID of the node.

                Task:
                    task_id: The ID of the task.
                    attempt_number: The attempt number of the task.
                    node_id: The ID of the node.

                Both accept the following optional parameters:
                    duration: Duration in seconds for profiling. Defaults to
                        RAY_DASHBOARD_PROFILING_MEMORY_DURATION_DEFAULT and must be
                        within [MIN_PROFILING_DURATION_S, MAX_PROFILING_DURATION_S].
                    format: Output format, one of VALID_MEMORY_PROFILING_FORMATS.
                        Defaults to RAY_DASHBOARD_PROFILING_MEMORY_FORMAT_DEFAULT.
                    native: Whether to include native (C/C++) stack frames.
                        Defaults to RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT.
                    leaks: Whether to report memory leaks instead of peak usage.
                        Defaults to RAY_DASHBOARD_PROFILING_LEAKS_DEFAULT.
                    trace_python_allocators: Whether to record pymalloc
                        allocations. Defaults to
                        RAY_DASHBOARD_PROFILING_TRACE_PYTHON_ALLOCATORS_DEFAULT.

        Returns:
            aiohttp.web.Response: The HTTP response containing the memory profile data.

        Raises:
            aiohttp.web.HTTPBadRequest: If "pid" or "duration" is not an
                integer, or "duration" falls outside the accepted range.
            aiohttp.web.HTTPInternalServerError: If no stub
                found from the given IP address or hex ID value
            aiohttp.web.HTTPInternalServerError: If the
                "task_id" parameter exists but either "attempt_number"
                or "node id" is missing in the request query.
            aiohttp.web.HTTPInternalServerError: If requesting task
                profiling for the worker begins working on another task
                during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If there is
                an internal server error during the profile retrieval.
        """
        if not RAY_DASHBOARD_ENABLE_PROFILING:
            return self._profiling_disabled_response()
        is_task = "task_id" in req.query

        # Either is_task or not, we need to get ip and grpc_port.
        if is_task:
            if "attempt_number" not in req.query:
                raise aiohttp.web.HTTPBadRequest(
                    text=(
                        "Failed to execute task profiling: "
                        "task's attempt number is required"
                    )
                )
            if "node_id" not in req.query:
                raise aiohttp.web.HTTPBadRequest(
                    text=(
                        "Failed to execute task profiling: task's node id is required"
                    )
                )

            task_id = req.query.get("task_id")
            attempt_number = req.query.get("attempt_number")
            try:
                (pid, _) = await self.get_worker_details_for_running_task(
                    task_id, attempt_number
                )
            except ValueError as e:
                raise aiohttp.web.HTTPInternalServerError(text=str(e))
            node_id_hex = req.query.get("node_id")
            addrs = await self._get_stub_address_by_node_id(
                NodeID.from_hex(node_id_hex)
            )
            if not addrs:
                return aiohttp.web.HTTPInternalServerError(
                    text=f"Failed to execute: no agent address found for node {node_id_hex}"
                )
            _, ip, _, grpc_port = addrs
        else:
            if "pid" not in req.query:
                raise aiohttp.web.HTTPBadRequest(text="pid is required")
            try:
                pid = int(req.query["pid"])
            except ValueError:
                raise aiohttp.web.HTTPBadRequest(text="pid must be an integer")
            ip = req.query.get("ip")
            node_id_hex = req.query.get("node_id")

            if not node_id_hex and not ip:
                raise ValueError("ip or node_id is required")

            if node_id_hex:
                addrs = await self._get_stub_address_by_node_id(
                    NodeID.from_hex(node_id_hex)
                )
                if not addrs:
                    return aiohttp.web.HTTPInternalServerError(
                        text=f"Failed to execute: no agent address found for node {node_id_hex}"
                    )
                _, ip, _, grpc_port = addrs
            else:
                addrs = await self._get_stub_address_by_ip(ip)
                if not addrs:
                    return aiohttp.web.HTTPInternalServerError(
                        text=f"Failed to execute: no agent address found for node IP {ip}"
                    )
                _, ip, _, grpc_port = addrs

        assert pid is not None
        ip_port = build_address(ip, grpc_port)

        duration_s = _query_duration(
            req, RAY_DASHBOARD_PROFILING_MEMORY_DURATION_DEFAULT
        )

        # format/native/leaks/trace_python_allocators default to the
        # operator-configured env vars (unless overridden on the head node).
        format = req.query.get("format", RAY_DASHBOARD_PROFILING_MEMORY_FORMAT_DEFAULT)
        native = _query_flag(req, "native", RAY_DASHBOARD_PROFILING_NATIVE_DEFAULT)
        leaks = _query_flag(req, "leaks", RAY_DASHBOARD_PROFILING_LEAKS_DEFAULT)
        trace_python_allocators = _query_flag(
            req,
            "trace_python_allocators",
            RAY_DASHBOARD_PROFILING_TRACE_PYTHON_ALLOCATORS_DEFAULT,
        )

        reporter_stub = self._make_stub(ip_port)

        logger.info(
            f"Retrieving memory profiling request to {build_address(ip, grpc_port)}, pid {pid}, with native={native}"
        )

        reply = await reporter_stub.MemoryProfiling(
            reporter_pb2.MemoryProfilingRequest(
                pid=pid,
                format=format,
                leaks=leaks,
                duration=duration_s,
                native=native,
                trace_python_allocators=trace_python_allocators,
            )
        )

        task_ids_in_a_worker = None
        warning = reply.warning if reply.warning else ""
        if is_task:
            """
            In order to truly confirm whether there are any other tasks
            running during the profiling, Ray needs to retrieve all tasks
            that are currently running or have finished, and then parse
            the task events (i.e., their start and finish times) to check
            for any potential overlap. However, this process can be quite
            extensive, so Ray makes our best efforts to check
            for any overlapping tasks. Therefore, Ray checks if
            the task is still running.
            """
            try:
                (_, worker_id) = await self.get_worker_details_for_running_task(
                    task_id, attempt_number
                )
            except ValueError as e:
                raise aiohttp.web.HTTPInternalServerError(text=str(e))

            task_ids_in_a_worker = await self.get_task_ids_running_in_a_worker(
                worker_id
            )
            if len(task_ids_in_a_worker) > 1:
                warning += (
                    "\n"
                    + WARNING_FOR_MULTI_TASK_IN_A_WORKER
                    + str(task_ids_in_a_worker)
                )

        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)
        logger.info("Returning profiling response, size {}".format(len(reply.output)))

        return aiohttp.web.Response(
            body=(
                '<p style="color: #E37400;">{} {} </br> </p> </br>'.format(
                    EMOJI_WARNING, warning
                )
                + (reply.output)
                if warning != ""
                else reply.output
            ),
            headers={"Content-Type": "text/html"},
        )

    @routes.get("/api/gcs_healthz")
    async def health_check(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        try:
            alive = await self._health_checker.check_gcs_liveness()
            if alive is True:
                return aiohttp.web.Response(
                    text="success",
                    content_type="application/text",
                )
        except Exception as e:
            return aiohttp.web.HTTPServiceUnavailable(
                reason=f"Health check failed: {e}"
            )

        return aiohttp.web.HTTPServiceUnavailable(reason="Health check failed")

    @routes.get("/api/prometheus/sd")
    async def prometheus_service_discovery(self, req) -> aiohttp.web.Response:
        """
        Expose Prometheus metrics targets through HTTP Service Discovery.
        """
        content = self.service_discovery.get_latest_service_discovery_content()
        if not isinstance(content, list):
            error_message = "service discovery error: content is not a list"
            logger.warning(error_message)
            return aiohttp.web.json_response(
                {"error": error_message},
                status=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                headers={"Cache-Control": "no-store"},
            )
        return aiohttp.web.Response(
            text=json.dumps(content),
            content_type="application/json",
            charset="utf-8",
            status=dashboard_utils.HTTPStatusCode.OK,
            headers={"Cache-Control": "no-store"},
        )

    async def _get_stub_address_by_node_id(
        self, node_id: NodeID
    ) -> Optional[Tuple[NodeID, str, int, int]]:
        """
        Given a NodeID, get agent port from InternalKV.

        returns a tuple of (ip, http_port, grpc_port).

        If not found, return None.
        """
        agent_addr_json = await self.gcs_client.async_internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id.hex()}".encode(),
            namespace=KV_NAMESPACE_DASHBOARD,
            timeout=GCS_RPC_TIMEOUT_SECONDS,
        )
        if not agent_addr_json:
            return None
        ip, http_port, grpc_port = json.loads(agent_addr_json)
        return node_id, ip, http_port, grpc_port

    async def _get_stub_address_by_ip(
        self, ip: str
    ) -> Optional[Tuple[str, str, int, int]]:
        agent_addr_json = await self.gcs_client.async_internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_IP_PREFIX}{ip}".encode(),
            namespace=KV_NAMESPACE_DASHBOARD,
            timeout=GCS_RPC_TIMEOUT_SECONDS,
        )
        if not agent_addr_json:
            return None
        node_id, http_port, grpc_port = json.loads(agent_addr_json)
        return NodeID.from_hex(node_id), ip, http_port, grpc_port

    def _make_stub(
        self, ip_port: str
    ) -> Optional[reporter_pb2_grpc.ReporterServiceStub]:
        options = GLOBAL_GRPC_OPTIONS
        channel = init_grpc_channel(ip_port, options=options, asynchronous=True)
        return reporter_pb2_grpc.ReporterServiceStub(channel)

    async def run(self):
        await super().run()
        self._state_api_data_source_client = StateDataSourceClient(
            self.aiogrpc_gcs_channel,
            self.gcs_client,
            dashboard_socket_dir=self._config.socket_dir,
            dashboard_session_name=self._config.session_name,
        )
        # Set up the state API in order to fetch task information.
        # This is only used to get task info. If we have Task APIs in GcsClient we can
        # remove this.
        # TODO(ryw): unify the StateAPIManager in reporter_head and state_head.
        self._state_api = StateAPIManager(
            self._state_api_data_source_client,
            self._executor,
        )

        # Need daemon True to avoid dashboard hangs at exit.
        self.service_discovery.daemon = True
        self.service_discovery.start()

        cluster_metadata = await self.gcs_client.async_internal_kv_get(
            CLUSTER_METADATA_KEY,
            namespace=KV_NAMESPACE_CLUSTER,
        )
        self.cluster_metadata = json.loads(cluster_metadata.decode("utf-8"))
