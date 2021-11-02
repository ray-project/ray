import aiohttp.web
from functools import wraps
import logging
from typing import Callable
import json
import dataclasses

import ray
import ray.dashboard.utils as dashboard_utils
from ray._private.job_manager import JobManager
from ray.dashboard.modules.job.data_types import (
    JobStatus, JobSubmitRequest, JobSubmitResponse, JobStatusRequest,
    JobStatusResponse, JobLogsRequest, JobLogsResponse)
from ray.experimental.internal_kv import (_initialize_internal_kv,
                                          _internal_kv_initialized)
logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

RAY_INTERNAL_JOBS_NAMESPACE = "_ray_internal_jobs_"


def _ensure_ray_initialized(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, **kwargs):
        if not ray.is_initialized():
            ray.init(address="auto", namespace=RAY_INTERNAL_JOBS_NAMESPACE)
        return f(self, *args, **kwargs)

    return check


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

        # Initialize internal KV to be used by the working_dir setup code.
        _initialize_internal_kv(dashboard_head.gcs_client)
        assert _internal_kv_initialized()

        self._job_manager = None

    @routes.post("/submit")
    @_ensure_ray_initialized
    async def submit(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        submit_request = JobSubmitRequest(json.loads(await req.json()))
        # TODO: (jiaodong) Validate if job request is valid without using
        # pydantic
        job_spec = submit_request.job_spec.get("job_spec")
        job_id = self._job_manager.submit_job(
            entrypoint=job_spec.get("entrypoint"),
            runtime_env=job_spec.get("runtime_env", {}),
            metadata=job_spec.get("metadata", {}))
        with open("/tmp/2", "a+") as file:
            file.write(f"job_spec: {job_spec}, type: {type(job_spec)} \n")
            file.write(f"{job_spec.get('entrypoint')} \n")
        resp = JobSubmitResponse(job_id=job_id)

        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get("/status")
    @_ensure_ray_initialized
    async def status(self, req) -> aiohttp.web.Response:
        status_request = JobStatusRequest(json.loads((await req.json())))
        status: JobStatus = self._job_manager.get_job_status(
            status_request.job_id.get("job_id"))
        resp = JobStatusResponse(job_status=status)

        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get("/logs")
    @_ensure_ray_initialized
    async def logs(self, req) -> aiohttp.web.Response:
        logs_request = JobLogsRequest(json.loads(await req.json()))

        stdout: bytes = self._job_manager.get_job_stdout(
            logs_request.job_id.get("job_id"))
        stderr: bytes = self._job_manager.get_job_stderr(
            logs_request.job_id.get("job_id"))

        # TODO(jiaodong): Support log streaming #19415
        resp = JobLogsResponse(
            stdout=stdout.decode("utf-8"), stderr=stderr.decode("utf-8"))

        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager()
