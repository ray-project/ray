import aiohttp.web
from aiohttp.web import Request, Response
import dataclasses
from functools import wraps
import logging
from typing import Any, Callable
import json
import traceback
from dataclasses import dataclass

import ray
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._private.gcs_utils import use_gcs_for_bootstrap
from ray._private.runtime_env.packaging import package_exists, upload_package_to_gcs
from ray.dashboard.modules.job.common import (
    CURRENT_VERSION,
    http_uri_components_to_uri,
    JobStatusInfo,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobStatusResponse,
    JobLogsResponse,
    VersionResponse,
    validate_request_type,
)
from ray.dashboard.modules.job.job_manager import JobManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable

RAY_INTERNAL_JOBS_NAMESPACE = "_ray_internal_jobs"


def _init_ray_and_catch_exceptions(f: Callable) -> Callable:
    @wraps(f)
    async def check(self, *args, **kwargs):
        try:
            if not ray.is_initialized():
                try:
                    if use_gcs_for_bootstrap():
                        address = self._dashboard_head.gcs_address
                        redis_pw = None
                        logger.info(f"Connecting to ray with address={address}")
                    else:
                        ip, port = self._dashboard_head.redis_address
                        redis_pw = self._dashboard_head.redis_password
                        address = f"{ip}:{port}"
                        logger.info(
                            f"Connecting to ray with address={address}, "
                            f"redis_pw={redis_pw}"
                        )
                    ray.init(
                        address=address,
                        namespace=RAY_INTERNAL_JOBS_NAMESPACE,
                        _redis_password=redis_pw,
                    )
                except Exception as e:
                    ray.shutdown()
                    raise e from None

            return await f(self, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Unexpected error in handler: {e}")
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

    return check


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
    @_init_ray_and_catch_exceptions
    async def get_package(self, req: Request) -> Response:
        package_uri = http_uri_components_to_uri(
            protocol=req.match_info["protocol"],
            package_name=req.match_info["package_name"],
        )

        if not package_exists(package_uri):
            return Response(
                text=f"Package {package_uri} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        return Response()

    @routes.put("/api/packages/{protocol}/{package_name}")
    @_init_ray_and_catch_exceptions
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
    @_init_ray_and_catch_exceptions
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
    @_init_ray_and_catch_exceptions
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
    @_init_ray_and_catch_exceptions
    async def get_job_status(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]
        if not self.job_exists(job_id):
            return Response(
                text=f"Job {job_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        status: JobStatusInfo = self._job_manager.get_job_status(job_id)
        resp = JobStatusResponse(status=status.status, message=status.message)
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/jobs/{job_id}/logs")
    @_init_ray_and_catch_exceptions
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
    @_init_ray_and_catch_exceptions
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
