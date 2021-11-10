import aiohttp.web
import dataclasses
from functools import wraps
import logging
from typing import Any, Callable
import json
import traceback
from dataclasses import dataclass

import ray
import ray.dashboard.utils as dashboard_utils
from ray._private.job_manager import JobManager
from ray._private.runtime_env.packaging import (package_exists,
                                                upload_package_to_gcs)
from ray.dashboard.modules.job.data_types import (
    GetPackageResponse, JobStatus, JobSubmitRequest, JobSubmitResponse,
    JobStopResponse, JobStatusResponse, JobLogsResponse)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

RAY_INTERNAL_JOBS_NAMESPACE = "_ray_internal_jobs_"

JOBS_API_PREFIX = "/api/jobs/"
JOBS_API_ROUTE_LOGS = JOBS_API_PREFIX + "logs"
JOBS_API_ROUTE_SUBMIT = JOBS_API_PREFIX + "submit"
JOBS_API_ROUTE_STOP = JOBS_API_PREFIX + "stop"
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

    async def _parse_and_validate_request(self, req: aiohttp.web.Request,
                                          request_type: dataclass) -> Any:
        """Parse request and cast to request type. If parsing failed, return a
        Response object with status 400 and stacktrace instead.
        """
        try:
            # TODO: (jiaodong) Validate if job request is valid without using
            # pydantic.
            result = request_type(**(await req.json()))
        except Exception:
            return aiohttp.web.Response(
                reason=traceback.format_exc().encode("utf-8"),
                status=aiohttp.web.HTTPBadRequest.status_code)
        return result

    @routes.get(JOBS_API_ROUTE_PACKAGE)
    @_ensure_ray_initialized
    async def get_package(self,
                          req: aiohttp.web.Request) -> aiohttp.web.Response:
        package_uri = req.query["package_uri"]
        try:
            exists = package_exists(package_uri)
        except Exception:
            return aiohttp.web.Response(
                reason=traceback.format_exc().encode("utf-8"),
                status=aiohttp.web.HTTPInternalServerError.status_code)

        resp = GetPackageResponse(package_exists=exists)
        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.put(JOBS_API_ROUTE_PACKAGE)
    @_ensure_ray_initialized
    async def upload_package(self, req: aiohttp.web.Request):
        package_uri = req.query["package_uri"]
        logger.info(f"Uploading package {package_uri} to the GCS.")
        try:
            upload_package_to_gcs(package_uri, await req.read())
        except Exception:
            return aiohttp.web.Response(
                reason=traceback.format_exc().encode("utf-8"),
                status=aiohttp.web.HTTPInternalServerError.status_code)

        return aiohttp.web.Response(status=aiohttp.web.HTTPOk.status_code, )

    @routes.post(JOBS_API_ROUTE_SUBMIT)
    @_ensure_ray_initialized
    async def submit(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        result = await self._parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, aiohttp.web.Response):
            return result
        else:
            submit_request = result

        try:
            job_id = self._job_manager.submit_job(
                entrypoint=submit_request.entrypoint,
                job_id=submit_request.job_id,
                runtime_env=submit_request.runtime_env,
                metadata=submit_request.metadata)

            resp = JobSubmitResponse(job_id=job_id)
        except Exception:
            return aiohttp.web.Response(
                reason=traceback.format_exc().encode("utf-8"),
                status=aiohttp.web.HTTPInternalServerError.status_code)

        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.post(JOBS_API_ROUTE_STOP)
    @_ensure_ray_initialized
    async def stop(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        job_id = req.query["job_id"]
        try:
            stopped = self._job_manager.stop_job(job_id)
            resp = JobStopResponse(stopped=stopped)
        except Exception:
            return aiohttp.web.Response(
                reason=traceback.format_exc().encode("utf-8"),
                status=aiohttp.web.HTTPInternalServerError.status_code)

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

        logs: str = self._job_manager.get_job_logs(job_id)
        # TODO(jiaodong): Support log streaming #19415
        resp = JobLogsResponse(logs=logs)
        return aiohttp.web.Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager()
