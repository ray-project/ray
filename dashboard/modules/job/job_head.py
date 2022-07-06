import aiohttp.web
from aiohttp.web import Request, Response
import dataclasses
import logging
from typing import Any
import json
import traceback
from dataclasses import dataclass

import ray
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from ray._private.runtime_env.packaging import (
    package_exists,
    upload_package_to_gcs,
    pin_runtime_env_uri,
)
from ray.dashboard.modules.job.common import (
    CURRENT_VERSION,
    http_uri_components_to_uri,
    JobInfo,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
    VersionResponse,
    validate_request_type,
)
from ray.dashboard.modules.job.job_manager import JobManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._job_manager = None

    async def _parse_and_validate_request(
        self, req: Request, request_type: dataclass
    ) -> Any:
        """Parse request and cast to request type. If parsing failed, return a
        Response object with status 400 and stacktrace instead.
        """
        try:
            return validate_request_type(await req.json(), request_type)
        except Exception as e:
            logger.info(f"Got invalid request type: {e}")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

    def job_exists(self, job_id: str) -> bool:
        status = self._job_manager.get_job_status(job_id)
        return status is not None

    @routes.get("/api/version")
    async def get_version(self, req: Request) -> Response:
        # NOTE(edoakes): CURRENT_VERSION should be bumped and checked on the
        # client when we have backwards-incompatible changes.
        resp = VersionResponse(
            version=CURRENT_VERSION,
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.get("/api/packages/{protocol}/{package_name}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def get_package(self, req: Request) -> Response:
        package_uri = http_uri_components_to_uri(
            protocol=req.match_info["protocol"],
            package_name=req.match_info["package_name"],
        )

        logger.debug(f"Adding temporary reference to package {package_uri}.")
        try:
            pin_runtime_env_uri(package_uri)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        if not package_exists(package_uri):
            return Response(
                text=f"Package {package_uri} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        return Response()

    @routes.put("/api/packages/{protocol}/{package_name}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def upload_package(self, req: Request):
        package_uri = http_uri_components_to_uri(
            protocol=req.match_info["protocol"],
            package_name=req.match_info["package_name"],
        )
        logger.info(f"Uploading package {package_uri} to the GCS.")
        try:
            upload_package_to_gcs(package_uri, await req.read())
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(status=aiohttp.web.HTTPOk.status_code)

    @routes.post("/api/jobs/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def submit_job(self, req: Request) -> Response:
        result = await self._parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request = result

        try:
            job_id = self._job_manager.submit_job(
                entrypoint=submit_request.entrypoint,
                job_id=submit_request.job_id,
                runtime_env=submit_request.runtime_env,
                metadata=submit_request.metadata,
            )

            resp = JobSubmitResponse(job_id=job_id)
        except (TypeError, ValueError):
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.post("/api/jobs/{job_id}/stop")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def stop_job(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]
        if not self.job_exists(job_id):
            return Response(
                text=f"Job {job_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        try:
            stopped = self._job_manager.stop_job(job_id)
            resp = JobStopResponse(stopped=stopped)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/jobs/{job_id}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def get_job_info(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]
        if not self.job_exists(job_id):
            return Response(
                text=f"Job {job_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        data: JobInfo = self._job_manager.get_job_info(job_id)
        return Response(
            text=json.dumps(dataclasses.asdict(data)), content_type="application/json"
        )

    @routes.get("/api/jobs/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def list_jobs(self, req: Request) -> Response:
        data: dict[str, JobInfo] = self._job_manager.list_jobs()
        return Response(
            text=json.dumps(
                {
                    job_id: dataclasses.asdict(job_info)
                    for job_id, job_info in data.items()
                }
            ),
            content_type="application/json",
        )

    @routes.get("/api/jobs/{job_id}/logs")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def get_job_logs(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]
        if not self.job_exists(job_id):
            return Response(
                text=f"Job {job_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        resp = JobLogsResponse(logs=self._job_manager.get_job_logs(job_id))
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/jobs/{job_id}/logs/tail")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def tail_job_logs(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]
        if not self.job_exists(job_id):
            return Response(
                text=f"Job {job_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(req)

        async for lines in self._job_manager.tail_job_logs(job_id):
            await ws.send_str(lines)

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager()

    @staticmethod
    def is_minimal_module():
        return False
