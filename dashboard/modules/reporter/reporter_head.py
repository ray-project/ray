import io
import json
import logging
import asyncio
import pstats
import aiohttp.web
from typing import Tuple
import time
import re

import ray
import ray._private.services
import ray._private.utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioResourceUsageSubscriber
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
    GLOBAL_GRPC_OPTIONS,
    KV_NAMESPACE_CLUSTER,
)
from ray.core.generated import reporter_pb2, reporter_pb2_grpc
from ray.dashboard.datacenter import DataSource
from ray._private.usage.usage_constants import CLUSTER_METADATA_KEY
from ray.autoscaler._private.commands import debug_status

from ray.util.state.common import (
    ListApiOptions,
)
from ray.dashboard.state_aggregator import StateAPIManager
from ray.util.state.state_manager import (
    StateDataSourceClient,
)
import cProfile

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable

EMOJI_WARNING = "&#x26A0;&#xFE0F;"
WARNING_FOR_MULTI_TASK_IN_A_WORKER: str = "This task is running in a worker that is running multiple tasks. Please notice the info you see here is for the all the tasks in this worker.The task_id running on this task is: </br>"


def get_warning_text(task_ids):
    return '<p style="color: #E37400;">{} {} </p> </br>'.format(
        EMOJI_WARNING, WARNING_FOR_MULTI_TASK_IN_A_WORKER + str(task_ids)
    )


def add_css_to_svg_with_regex(svg_string):
    # Search for the opening <svg> tag and add the CSS styles as an attribute
    modified_svg_string = re.sub(
        r"<svg(.*?)>", r'<svg\1 style="width: 100%; height: 100%;">', svg_string
    )

    return modified_svg_string


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
        gcs_address = dashboard_head.gcs_address
        temp_dir = dashboard_head.temp_dir
        self.service_discovery = PrometheusServiceDiscoveryWriter(gcs_address, temp_dir)
        self._gcs_aio_client = dashboard_head.gcs_aio_client
        self.set_up_state_api()

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]
            options = GLOBAL_GRPC_OPTIONS
            channel = ray._private.utils.init_grpc_channel(
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
                cluster_status=debug_status(formatted_status_string, error),
            )

    def set_up_state_api(self):
        """
        Set up the state API in order to fetch task information.
        """
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(
            gcs_channel, self._dashboard_head.gcs_aio_client
        )
        self._state_api = StateAPIManager(self._state_api_data_source_client)

    async def get_worker_details_for_task(
        self, task_id: str, attempt_number: int
    ) -> Tuple[int, str, str]:
        """
        Retrieves worker details for a specific task and attempt number.

        Args:
            task_id (str): The ID of the task.
            attempt_number (int): The attempt number of the task.

        Returns:
            tuple: A tuple containing the worker's PID (process ID), IP address and state and the task's IDs for this task
        """
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
            return None, None, None, None

        pid = tasks[0]["worker_pid"]

        node_id = tasks[0]["node_id"]
        ip = DataSource.node_id_to_ip[node_id]

        state = tasks[0].get("state")

        worker_id = tasks[0]["worker_id"]
        worker_option = ListApiOptions(
            filters=[("worker_id", "=", worker_id), ("state", "=", "RUNNING")],
            detail=True,
            timeout=10,
        )
        ## Call the state API to get all tasks in a worker
        tasks_in_a_worker_result = await self._state_api.list_tasks(
            option=worker_option
        )
        tasks_in_a_worker = tasks_in_a_worker_result.result

        ## Get task_id from each task in a worker
        task_ids_in_a_worker = []
        if tasks_in_a_worker is not None:
            task_ids_in_a_worker = [
                task.get("task_id")
                for task in tasks_in_a_worker
                if task and "task_id" in task
            ]
        logger.info(
            f"task_ids_in_a_worker {type(task_ids_in_a_worker)}: {task_ids_in_a_worker}"
        )

        return pid, ip, state, task_ids_in_a_worker

    def validate_work_details(self, pid: int, ip: str, state: str):
        """
        Args:
            pid (int):  The process ID.
            ip (str): The IP address.
            state (str): The current state of the task attempt.

        Raises:
            ValueError: If the PID or IP is None, indicating that the information could not be fetched.
            ValueError: If the task attempt is not in the "RUNNING" state.
        """
        if not pid or not ip:
            raise ValueError(
                f"pid or ip is None, could not fetch the info you need: pid = {pid}, ip = {ip}"
            )

        if state != "RUNNING":
            raise ValueError(
                f"The task attempt is not running: the current state is {state}."
            )

    @routes.get("/task/traceback")
    async def get_task_traceback(self, req) -> aiohttp.web.Response:
        """
        Retrieves the traceback information for a specific task.
        To notice, we have one worker to work on one task at a time or one worker to work on multiple async tasks.

        Args:
            req (aiohttp.web.Request): The HTTP request object.

        Returns:
            aiohttp.web.Response: The HTTP response containing the traceback information.

        Raises:
            ValueError: If the "task_id" parameter is missing in the request query.
            ValueError: If the "attempt_number" parameter is missing in the request query.
            ValueError: If the worker begins working on another task during the traceback retrieval.
            aiohttp.web.HTTPInternalServerError: If there is an internal server error during the traceback retrieval.
        """

        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")

        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")
        pid, ip, state, _ = await self.get_worker_details_for_task(
            task_id, attempt_number
        )

        try:
            self.validate_work_details(pid, ip, state)
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"

        logger.info(
            "Sending stack trace request to {}:{} with native={}".format(
                ip, pid, native
            )
        )
        reporter_stub = self._stubs[ip]
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(pid=pid, native=native)
        )
        ## Get the new pid and ip again to check if the task is still running and the worker is still working on the task
        ## Since there is a task scheduling strategy that a worker may run different tasks at different time
        (
            new_pid,
            new_ip,
            new_state,
            task_ids_in_a_worker,
        ) = await self.get_worker_details_for_task(task_id, attempt_number)

        try:
            self.validate_work_details(new_pid, new_ip, new_state)
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))
        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

        logger.info("Returning stack trace, size {}".format(len(reply.output)))

        return aiohttp.web.Response(
            body=get_warning_text(task_ids_in_a_worker) + reply.output
            if len(task_ids_in_a_worker) > 1
            else reply.output,
            headers={"Content-Type": "text/html"},
        )

    @routes.get("/task/cpu_profile")
    async def get_task_cpu_profile(self, req) -> aiohttp.web.Response:
        """
        Retrieves the CPU profile for a specific task.
        To notice, we have one worker to work on one task at a time or one worker to work on multiple async tasks.

        Args:
            req (aiohttp.web.Request): The HTTP request object.

        Returns:
            aiohttp.web.Response: The HTTP response containing the CPU profile data.

        Raises:
            ValueError: If the "task_id" parameter is missing in the request query.
            ValueError: If the "attempt_number" parameter is missing in the request query.
            ValueError: If the maximum duration allowed is exceeded.
            ValueError: If the worker begins working on another task during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If there is an internal server error during the profile retrieval.
            aiohttp.web.HTTPInternalServerError: If the CPU Flame Graph information for the task is not found.
        """
        profiler = cProfile.Profile()
        profiler.enable()

        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")

        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")

        start_time = time.time()
        pid, ip, state, _ = await self.get_worker_details_for_task(
            task_id, attempt_number
        )
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(
            f"get_worker_details_for_task execution_time {type(execution_time)}: {execution_time}"
        )

        try:
            self.validate_work_details(pid, ip, state)
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        duration = int(req.query.get("duration", 5))
        if duration > 60:
            raise ValueError(f"The max duration allowed is 60: {duration}.")
        format = req.query.get("format", "flamegraph")

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        reporter_stub = self._stubs[ip]

        logger.info(
            "Sending CPU profiling request to {}:{} for {} with native={}".format(
                ip, pid, task_id, native
            )
        )
        start_time = time.time()

        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid, duration=duration, format=format, native=native
            )
        )

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(
            f"CpuProfilingRequest execution_time {type(execution_time)}: {execution_time}"
        )

        ## Get the new pid and ip again to check if the task is still running and the worker is still working on the task
        ## Since there is a task scheduling strategy that a worker may run different tasks at different time
        (
            new_pid,
            new_ip,
            new_state,
            task_ids_in_a_worker,
        ) = await self.get_worker_details_for_task(task_id, attempt_number)

        try:
            self.validate_work_details(new_pid, new_ip, new_state)
        except ValueError as e:
            raise aiohttp.web.HTTPInternalServerError(text=str(e))

        if not reply.success:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

        logger.info("Returning profiling response, size {}".format(len(reply.output)))
        profiler.disable()
        logger.info("ssssss")
        stream = io.StringIO()

        # Create a Stats object from the profiler results
        stats = pstats.Stats(profiler, stream=stream)
        stats.strip_dirs()
        stats.sort_stats("cumulative")

        # Print the profiling results to the stream
        stats.print_stats()

        # Get the profiling results from the stream
        profiler_string = stream.getvalue()
        logger.info(f"profiler_string {type(profiler_string)}: {profiler_string}")
        # return aiohttp.web.Response(
        #     body=get_warning_text(task_ids_in_a_worker)
        #     + add_css_to_svg_with_regex(reply.output)
        #     if len(task_ids_in_a_worker) > 1
        #     else add_css_to_svg_with_regex(reply.output),
        #     headers={"Content-Type": "text/html"},
        # )
        return aiohttp.web.Response(
            body="OK",
            headers={"Content-Type": "text/html"},
        )

    @routes.get("/worker/traceback")
    async def get_traceback(self, req) -> aiohttp.web.Response:
        if "ip" in req.query:
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
        if "ip" in req.query:
            reporter_stub = self._stubs[req.query["ip"]]
        else:
            reporter_stub = list(self._stubs.values())[0]
        pid = int(req.query["pid"])
        duration = int(req.query.get("duration", 5))
        if duration > 60:
            raise ValueError(f"The max duration allowed is 60: {duration}.")
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
                pid=pid, duration=duration, format=format, native=native
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

    async def run(self, server):
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

        while True:
            try:
                # The key is b'RAY_REPORTER:{node id hex}',
                # e.g. b'RAY_REPORTER:2b4fbd...'
                key, data = await subscriber.poll()
                if key is None:
                    continue
                data = json.loads(data)
                node_id = key.split(":")[-1]
                DataSource.node_physical_stats[node_id] = data
            except Exception:
                logger.exception(
                    "Error receiving node physical stats from reporter agent."
                )

    @staticmethod
    def is_minimal_module():
        return False
