import aiohttp.web
from functools import wraps
import logging
from typing import Callable
import json
import dataclasses

import ray
import ray.dashboard.utils as dashboard_utils
from ray._private.job_manager import JobManager
from ray._private.runtime_env.packaging import (package_exists,
                                                upload_package_to_gcs)
from ray.dashboard.modules.job.data_types import (
    GetPackageResponse, JobStatus, JobSubmitRequest, JobSubmitResponse,
    JobStatusResponse, JobLogsResponse)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

RAY_INTERNAL_JOBS_NAMESPACE = "_ray_internal_jobs_"

JOBS_API_PREFIX = "/api/jobs/"
JOBS_API_ROUTE_LOGS = JOBS_API_PREFIX + "logs"
JOBS_API_ROUTE_SUBMIT = JOBS_API_PREFIX + "submit"
JOBS_API_ROUTE_STATUS = JOBS_API_PREFIX + "status"
JOBS_API_ROUTE_PACKAGE = JOBS_API_PREFIX + "package"


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

        self._job_manager = None

    @routes.get(JOBS_API_ROUTE_PACKAGE)
    @_ensure_ray_initialized
    async def get_package(self,
                          req: aiohttp.web.Request) -> aiohttp.web.Response:
        package_uri = req.query["package_uri"]
        resp = GetPackageResponse(package_exists=package_exists(package_uri))
        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.put(JOBS_API_ROUTE_PACKAGE)
    @_ensure_ray_initialized
    async def upload_package(self, req: aiohttp.web.Request):
        package_uri = req.query["package_uri"]
        logger.info(f"Uploading package {package_uri} to the GCS.")
        upload_package_to_gcs(package_uri, await req.read())

        return aiohttp.web.Response()

    @routes.post(JOBS_API_ROUTE_SUBMIT)
    @_ensure_ray_initialized
    async def submit(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        # TODO: (jiaodong) Validate if job request is valid without using
        # pydantic.
        submit_request = JobSubmitRequest(**(await req.json()))
        job_id = self._job_manager.submit_job(
            entrypoint=submit_request.entrypoint,
            runtime_env=submit_request.runtime_env,
            metadata=submit_request.metadata)

        resp = JobSubmitResponse(job_id=job_id)
        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get(JOBS_API_ROUTE_STATUS)
    @_ensure_ray_initialized
    async def status(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        job_id = req.query["job_id"]
        status: JobStatus = self._job_manager.get_job_status(job_id)

        resp = JobStatusResponse(job_status=status)
        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get(JOBS_API_ROUTE_LOGS)
    @_ensure_ray_initialized
    async def logs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        job_id = req.query["job_id"]
        stdout: bytes = self._job_manager.get_job_stdout(job_id)
        stderr: bytes = self._job_manager.get_job_stderr(job_id)

        # TODO(jiaodong): Support log streaming #19415
        resp = JobLogsResponse(
            stdout=stdout.decode("utf-8"), stderr=stderr.decode("utf-8"))
        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager()
