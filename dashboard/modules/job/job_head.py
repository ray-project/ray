import aiohttp.web
from base64 import b64decode
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
    GetPackageRequest, GetPackageResponse, UploadPackageRequest, JobStatus,
    JobSubmitRequest, JobSubmitResponse, JobStatusRequest, JobStatusResponse,
    JobLogsRequest, JobLogsResponse)

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

        self._job_manager = None

    @routes.get("/package")
    @_ensure_ray_initialized
    async def get_package(self,
                          req: aiohttp.web.Request) -> aiohttp.web.Response:
        package_uri = GetPackageRequest(**(await req.json())).package_uri
        already_exists = package_exists(package_uri)
        exists_str = "exists" if already_exists else "does not exist"
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            data=GetPackageResponse(package_exists=already_exists).dict(),
            message=f"Package {package_uri} {exists_str}.")

    @routes.put("/package")
    @_ensure_ray_initialized
    async def upload_package(self,
                             req: aiohttp.web.Request) -> aiohttp.web.Response:
        upload_req = UploadPackageRequest(**(await req.json()))
        package_uri = upload_req.package_uri
        logger.info(f"Uploading package {package_uri} to the GCS.")
        upload_package_to_gcs(package_uri,
                              b64decode(upload_req.encoded_package_bytes))
        return dashboard_utils.rest_response(
            success=True,
            convert_google_style=False,
            message=f"Successfully uploaded {package_uri}.")

    @routes.post("/submit")
    @_ensure_ray_initialized
    async def submit(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        submit_request = JobSubmitRequest(**(await req.json()))
        # TODO: (jiaodong) Validate if job request is valid without using
        # pydantic
        job_id = self._job_manager.submit_job(
            entrypoint=submit_request.job_spec.get("entrypoint"),
            runtime_env=submit_request.job_spec.get("runtime_env", {}),
            metadata=submit_request.job_spec.get("metadata", {}))
        resp = JobSubmitResponse(job_id=job_id)

        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get("/status")
    @_ensure_ray_initialized
    async def status(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        status_request = JobStatusRequest(**await req.json())
        status: JobStatus = self._job_manager.get_job_status(
            status_request.job_id.get("job_id"))
        resp = JobStatusResponse(job_status=status)

        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get("/logs")
    @_ensure_ray_initialized
    async def logs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        logs_request = JobLogsRequest(**await req.json())

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
