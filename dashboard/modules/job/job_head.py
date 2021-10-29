import aiohttp.web
from functools import wraps
import logging
from typing import Callable

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
    async def submit(self, req) -> aiohttp.web.Response:
        req_data = dict(await req.json())
        submit_request = JobSubmitRequest(**req_data)
        job_id = self._job_manager.submit_job(
            submit_request.job_spec.entrypoint,
            job_id=submit_request.job_id,
            runtime_env=submit_request.job_spec.runtime_env,
            metadata=submit_request.job_spec.metadata)

        resp = JobSubmitResponse(job_id=job_id)
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            data=resp.dict(),
            message=f"Submitted job {job_id}")

    @routes.get("/status")
    @_ensure_ray_initialized
    async def status(self, req) -> aiohttp.web.Response:
        req_data = dict(await req.json())
        status_request = JobStatusRequest(**req_data)

        status: JobStatus = self._job_manager.get_job_status(
            status_request.job_id)
        resp = JobStatusResponse(job_status=status)
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            data=resp.dict(),
            message=f"Queried status for job {status_request.job_id}")

    @routes.get("/logs")
    @_ensure_ray_initialized
    async def logs(self, req) -> aiohttp.web.Response:
        req_data = dict(await req.json())
        logs_request = JobLogsRequest(**req_data)

        stdout: bytes = self._job_manager.get_job_stdout(logs_request.job_id)
        stderr: bytes = self._job_manager.get_job_stderr(logs_request.job_id)

        # TODO(jiaodong): Support log streaming #19415
        resp = JobLogsResponse(
            stdout=stdout.decode("utf-8"), stderr=stderr.decode("utf-8"))

        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            data=resp.dict(),
            message=f"Logs returned for job {logs_request.job_id}")

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager()
