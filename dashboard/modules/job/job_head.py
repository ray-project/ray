import aiohttp.web
from base64 import b64decode
from functools import wraps
import logging
from typing import Callable

import ray
import ray.dashboard.utils as dashboard_utils
from ray._private.job_manager import JobManager
from ray._private.runtime_env.packaging import (package_exists,
                                                upload_package_to_gcs)
from ray.dashboard.modules.job.data_types import (
    GetPackageRequest, GetPackageResponse, UploadPackageRequest, JobStatus,
    JobSubmitRequest, JobSubmitResponse, JobStatusRequest, JobStatusResponse,
    JobLogsRequest, JobLogsResponse)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

RAY_INTERNAL_JOBS_NAMESPACE = "_ray_internal_jobs_"

JOB_API_ROUTE_LOGS = "/api/jobs/logs"
JOB_API_ROUTE_SUBMIT = "/api/jobs/submit"
JOB_API_ROUTE_STATUS = "/api/jobs/status"
JOB_API_ROUTE_PACKAGE = "/api/jobs/package"


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

    @routes.get("/api/jobs/package")
    @_ensure_ray_initialized
    async def get_package(self,
                          req: aiohttp.web.Request) -> aiohttp.web.Response:
        req_data = await req.json()
        package_uri = GetPackageRequest(**req_data).package_uri
        already_exists = package_exists(package_uri)
        exists_str = "exists" if already_exists else "does not exist"
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            data=GetPackageResponse(package_exists=already_exists).dict(),
            message=f"Package {package_uri} {exists_str}.")

    @routes.put("/api/jobs/package")
    @_ensure_ray_initialized
    async def upload_package(self,
                             req: aiohttp.web.Request) -> aiohttp.web.Response:
        req_data = await req.json()
        upload_req = UploadPackageRequest(**req_data)
        package_uri = upload_req.package_uri
        logger.info(f"Uploading package {package_uri} to the GCS.")
        upload_package_to_gcs(package_uri,
                              b64decode(upload_req.encoded_package_bytes))
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            message=f"Successfully uploaded {package_uri}.")

    @routes.post("/api/jobs/submit")
    @_ensure_ray_initialized
    async def submit(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        req_data = await req.json()
        submit_request = JobSubmitRequest(**req_data)
        job_id = self._job_manager.submit_job(
            submit_request.job_spec.entrypoint,
            runtime_env=submit_request.job_spec.runtime_env,
            metadata=submit_request.job_spec.metadata)

        resp = JobSubmitResponse(job_id=job_id)
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            data=resp.dict(),
            message=f"Submitted job {job_id}.")

    @routes.get("/api/jobs/status")
    @_ensure_ray_initialized
    async def status(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
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

    @routes.get("/api/jobs/logs")
    @_ensure_ray_initialized
    async def logs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
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
