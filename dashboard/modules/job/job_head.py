import aiohttp.web
from functools import wraps
import logging
from typing import Callable

import ray
import ray.dashboard.utils as dashboard_utils
from ray._private.job_manager import JobManager, JobStatus
from ray.dashboard.modules.job.data_types import (
    JobSubmitRequest, JobSubmitResponse, JobSpec)
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
    async def submit(self, req: JobSubmitRequest) -> aiohttp.web.Response:
        req_data = dict(await req.json())
        job_id = req_data.get("job_id")
        self._job_manager.submit_job(job_id, "echo hello")

        return dashboard_utils.rest_response(
            success=True,
            data=job_id,
            message=f"Submitted job {job_id}")

    @routes.get("/status")
    @_ensure_ray_initialized
    async def status(self, req) -> aiohttp.web.Response:
        req_data = dict(await req.json())
        job_id = req_data.get("job_id")

        status: JobStatus = self._job_manager.get_job_status(job_id)

        return dashboard_utils.rest_response(
            success=True,
            data=status,
            message=f"Queried status for job {job_id}")

    @routes.get("/logs")
    @_ensure_ray_initialized
    async def logs(self, req) -> aiohttp.web.Response:
        req_data = dict(await req.json())
        job_id = req_data.get("job_id")

        stdout: bytes = self._job_manager.get_job_stdout(job_id)
        stderr: bytes = self._job_manager.get_job_stderr(job_id)

        return dashboard_utils.rest_response(
            success=True,
            data={
                "stdout": stdout.decode("utf-8"),
                "stderr": stderr.decode("utf-8")
            },
            message=f"Logs returned for job {job_id}")

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager()
