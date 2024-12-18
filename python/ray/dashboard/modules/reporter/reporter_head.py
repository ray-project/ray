import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioResourceUsageSubscriber
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
    GLOBAL_GRPC_OPTIONS,
    KV_NAMESPACE_CLUSTER,
    env_integer,
)
from ray._private.usage.usage_constants import CLUSTER_METADATA_KEY
from ray._private.utils import get_or_create_event_loop, init_grpc_channel
from ray.autoscaler._private.commands import debug_status
from ray.core.generated import reporter_pb2, reporter_pb2_grpc
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
from ray.dashboard.datacenter import DataSource
from ray.dashboard.state_aggregator import StateAPIManager
from ray.util.state.common import ListApiOptions
from ray.util.state.state_manager import StateDataSourceClient

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

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

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS", 1
)


class ReportHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        self._ray_config = None
        DataSource.agents.signal.append(self._update_stubs)
        # TODO(fyrestone): Avoid using ray.state in dashboard, it's not
        # asynchronous and will lead to low performance. ray disconnect()
        # will be hang when the ray.state is connected and the GCS is exit.
        # Please refer to: https://github.com/ray-project/ray/issues/16328
        assert dashboard_head.gcs_address or dashboard_head.redis_address
        self._gcs_address = dashboard_head.gcs_address
        temp_dir = dashboard_head.temp_dir
        self.service_discovery = PrometheusServiceDiscoveryWriter(
            self._gcs_address, temp_dir
        )
        self._gcs_aio_client = dashboard_head.gcs_aio_client
        self._state_api = None

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="reporter_head_executor",
        )

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.nodes[node_id]["nodeManagerAddress"]
            self._stubs.pop(ip, None)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.nodes[node_id]["nodeManagerAddress"]
            options = GLOBAL_GRPC_OPTIONS
            channel = init_grpc_channel(
                f"{ip}:{ports[1]}", options=options, asynchronous=True
            )
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            self._stubs[ip] = stub

    @routes.get("/api/v0/cluster_metadata")
    async def get_cluster_metadata(self, req):
        return dashboard_optional_utils.rest_response(
            success=True, message="", **self.cluster_metadata
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
                self._gcs_aio_client.internal_kv_get(
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
                success=True,
                message="Got cluster status.",
                autoscaling_status=legacy_status.decode() if legacy_status else None,
                autoscaling_error=error.decode() if error else None,
                cluster_status=formatted_status if formatted_status else None,
            )
        else:
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Got formatted cluster status.",
                cluster_status=debug_status(
                    formatted_status_string, error, address=self._gcs_address
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
        """
        Retrieves worker details for a specific task and attempt number.

        Args:
            task_id: The ID of the task.
            attempt_number: The attempt number of the task.

        Returns:
            Tuple[Optional[int], Optional[str]]: A tuple
            containing the worker's PID (process ID),
            and worker's ID.

        Raises:
            ValueError: If the task attempt is not running or
            the state APi is not initialized.
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
    async def get_task_traceback(self, req) -> aiohttp.web.Response:
        """
        Retrieves the traceback information for a specific task.
        Note that one worker process works on one task at a time
        or one worker works on multiple async tasks.

        Args:
            req (aiohttp.web.Request): The HTTP request object.

        Returns:
            aiohttp.web.Response: The HTTP response containing
            the traceback information.

        Raises:
            ValueError: If the "task_id" parameter
            is missing in the request query.
            ValueError: If the "attempt_number" parameter
            is missing in the request query.
            ValueError: If the worker begins working on
            another task during the traceback retrieval.
            aiohttp.web.HTTPInternalServerError: If there is
            an internal server error during the traceback retrieval.
        """

        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")
        if "node_id" not in req.query:
            raise ValueError("node_id is required")

        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")
        node_id = req.query.get("node_id")

        ip = DataSource.nodes[node_id]["nodeManagerAddress"]

        reporter_stub = self._stubs[ip]

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"

        try:
            (pid, _) = await self.get_worker_details_for_running_task(
                task_id, attempt_number
            )
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        logger.info(
            "Sending stack trace request to {}:{} with native={}".format(
                ip, pid, native
            )
        )
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(pid=pid, native=native)
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
            text=WARNING_FOR_MULTI_TASK_IN_A_WORKER
            + str(task_ids_in_a_worker)
            + "\n"
            + reply.output
            if len(task_ids_in_a_worker) > 1
            else reply.output
        )

    @routes.get("/task/cpu_profile")
    async def get_task_cpu_profile(self, req) -> aiohttp.web.Response:
        """
        Retrieves the CPU profile for a specific task.
        Note that one worker process works on one task at a time
        or one worker works on multiple async tasks.

        Args:
            req (aiohttp.web.Request): The HTTP request object.

        Returns:
            aiohttp.web.Response: The HTTP response containing the CPU profile data.

        Raises:
            ValueError: If the "task_id" parameter is
            missing in the request query.
            ValueError: If the "attempt_number" parameter is
            missing in the request query.
            ValueError: If the maximum duration allowed is exceeded.
            ValueError: If the worker begins working on
            another task during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If there is
            an internal server error during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If the CPU Flame
            Graph information for the task is not found.
        """
        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")
        if "node_id" not in req.query:
            raise ValueError("node_id is required")

        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")
        node_id = req.query.get("node_id")

        ip = DataSource.nodes[node_id]["nodeManagerAddress"]

        duration_s = int(req.query.get("duration", 5))
        if duration_s > 60:
            raise ValueError(f"The max duration allowed is 60 seconds: {duration_s}.")
        format = req.query.get("format", "flamegraph")

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        reporter_stub = self._stubs[ip]

        try:
            (pid, _) = await self.get_worker_details_for_running_task(
                task_id, attempt_number
            )
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        logger.info(
            "Sending CPU profiling request to {}:{} for {} with native={}".format(
                ip, pid, task_id, native
            )
        )

        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid, duration=duration_s, format=format, native=native
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
        return aiohttp.web.Response(
            body='<p style="color: #E37400;">{} {} </br> </p> </br>'.format(
                EMOJI_WARNING,
                WARNING_FOR_MULTI_TASK_IN_A_WORKER + str(task_ids_in_a_worker),
            )
            + SVG_STYLE
            + (reply.output)
            if len(task_ids_in_a_worker) > 1
            else SVG_STYLE + reply.output,
            headers={"Content-Type": "text/html"},
        )

    @routes.get("/worker/traceback")
    async def get_traceback(self, req) -> aiohttp.web.Response:
        if "ip" in req.query and req.query["ip"] in self._stubs:
            reporter_stub = self._stubs[req.query["ip"]]
        else:
            reporter_stub = list(self._stubs.values())[0]
        pid = int(req.query["pid"])
        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        logger.info(
            "Sending stack trace request to {}:{} with native={}".format(
                req.query.get("ip"), pid, native
            )
        )
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(pid=pid, native=native)
        )
        if reply.success:
            logger.info("Returning stack trace, size {}".format(len(reply.output)))
            return aiohttp.web.Response(text=reply.output)
        else:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

    @routes.get("/worker/cpu_profile")
    async def cpu_profile(self, req) -> aiohttp.web.Response:
        if "ip" in req.query and req.query["ip"] in self._stubs:
            reporter_stub = self._stubs[req.query["ip"]]
        else:
            reporter_stub = list(self._stubs.values())[0]
        pid = int(req.query["pid"])
        duration_s = int(req.query.get("duration", 5))
        if duration_s > 60:
            raise ValueError(f"The max duration allowed is 60 seconds: {duration_s}.")
        format = req.query.get("format", "flamegraph")

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        logger.info(
            "Sending CPU profiling request to {}:{} with native={}".format(
                req.query.get("ip"), pid, native
            )
        )
        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid, duration=duration_s, format=format, native=native
            )
        )
        if reply.success:
            logger.info(
                "Returning profiling response, size {}".format(len(reply.output))
            )
            return aiohttp.web.Response(
                body=reply.output,
                headers={
                    "Content-Type": "image/svg+xml"
                    if format == "flamegraph"
                    else "text/plain"
                },
            )
        else:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

    @routes.get("/memory_profile")
    async def memory_profile(self, req) -> aiohttp.web.Response:
        """
        Retrieves the memory profile for a specific worker or task.
        Note that for tasks, one worker process works on one task at a time
        or one worker works on multiple async tasks.

        Args:
            req (aiohttp.web.Request): The HTTP request object.

        Returns:
            aiohttp.web.Response: The HTTP response containing the memory profile data.

        Raises:
            aiohttp.web.HTTPInternalServerError: If no stub
                found from the given IP value
            aiohttp.web.HTTPInternalServerError: If the
                "task_id" parameter exists but either "attempt_number"
                or "node id" is missing in the request query.
            aiohttp.web.HTTPInternalServerError: If the maximum
                duration allowed is exceeded.
            aiohttp.web.HTTPInternalServerError If requesting task
                profiling for the worker begins working on another task
                during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If there is
                an internal server error during the profile retrieval.
        """
        is_task = "task_id" in req.query

        if is_task:
            if "attempt_number" not in req.query:
                return aiohttp.web.HTTPInternalServerError(
                    text=(
                        "Failed to execute task profiling: "
                        "task's attempt number is required"
                    )
                )
            if "node_id" not in req.query:
                return aiohttp.web.HTTPInternalServerError(
                    text=(
                        "Failed to execute task profiling: "
                        "task's node id is required"
                    )
                )

            task_id = req.query.get("task_id")
            attempt_number = req.query.get("attempt_number")
            node_id = req.query.get("node_id")
            ip = DataSource.nodes[node_id]["nodeManagerAddress"]
            try:
                (pid, _) = await self.get_worker_details_for_running_task(
                    task_id, attempt_number
                )
            except ValueError as e:
                raise aiohttp.web.HTTPInternalServerError(text=str(e))
        else:
            pid = int(req.query["pid"])
            ip = req.query.get("ip") or None

        duration_s = int(req.query.get("duration", 10))

        # Default not using `--native`, `--leaks` and `--format` for profiling
        format = req.query.get("format", "flamegraph")
        native = req.query.get("native", False) == "1"
        leaks = req.query.get("leaks", False) == "1"
        trace_python_allocators = req.query.get("trace_python_allocators", False) == "1"

        if ip:
            if ip not in self._stubs:
                return aiohttp.web.HTTPInternalServerError(
                    text="Failed to execute: No stub with given ip value"
                )
            reporter_stub = self._stubs[ip]
        else:
            reporter_stub = list(self._stubs.values())[0]

        logger.info(
            "Retrieving memory profiling request to {}:{}{}".format(
                ip, pid, (f" for {task_id}" if is_task else "")
            )
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
            body='<p style="color: #E37400;">{} {} </br> </p> </br>'.format(
                EMOJI_WARNING, warning
            )
            + (reply.output)
            if warning != ""
            else reply.output,
            headers={"Content-Type": "text/html"},
        )

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(
            gcs_channel, self._dashboard_head.gcs_aio_client
        )
        # Set up the state API in order to fetch task information.
        # TODO(ryw): unify the StateAPIManager in reporter_head and state_head.
        self._state_api = StateAPIManager(
            self._state_api_data_source_client,
            self._executor,
        )

        # Need daemon True to avoid dashboard hangs at exit.
        self.service_discovery.daemon = True
        self.service_discovery.start()
        gcs_addr = self._dashboard_head.gcs_address
        subscriber = GcsAioResourceUsageSubscriber(address=gcs_addr)
        await subscriber.subscribe()
        cluster_metadata = await self._dashboard_head.gcs_aio_client.internal_kv_get(
            CLUSTER_METADATA_KEY,
            namespace=KV_NAMESPACE_CLUSTER,
        )
        self.cluster_metadata = json.loads(cluster_metadata.decode("utf-8"))

        loop = get_or_create_event_loop()

        while True:
            try:
                # The key is b'RAY_REPORTER:{node id hex}',
                # e.g. b'RAY_REPORTER:2b4fbd...'
                key, data = await subscriber.poll()
                if key is None:
                    continue

                # NOTE: Every iteration is executed inside the thread-pool executor
                #       (TPE) to avoid blocking the Dashboard's event-loop
                parsed_data = await loop.run_in_executor(
                    self._executor, json.loads, data
                )

                node_id = key.split(":")[-1]
                DataSource.node_physical_stats[node_id] = parsed_data
            except Exception:
                logger.exception(
                    "Error receiving node physical stats from reporter agent."
                )

    @staticmethod
    def is_minimal_module():
        return False
