import dataclasses
import json
import logging
import traceback
from typing import Iterator

import aiohttp.web
from aiohttp.web import Request, Response
from aiohttp.client import ClientResponse

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.runtime_env.packaging import (
    package_exists,
    pin_runtime_env_uri,
    upload_package_to_gcs,
)
from ray.dashboard.modules.job.common import (
    http_uri_components_to_uri,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
)
from ray.dashboard.modules.job.pydantic_models import (
    JobDetails,
    JobType,
)
from ray.dashboard.modules.job.utils import (
    parse_and_validate_request,
    get_driver_jobs,
    find_job_by_ids,
)
from ray.dashboard.modules.version import (
    CURRENT_VERSION,
    VersionResponse,
)
from ray.dashboard.modules.job.job_manager import JobManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class JobAgentSubmissionClient:
    """A local client for submitting and interacting with jobs on a specific node
    in the remote cluster.
    Submits requests over HTTP to the job agent on the specific node using the REST API.
    """

    def __init__(
        self,
        dashboard_agent_address: str,
    ):
        self._agent_address = dashboard_agent_address
        self._session = aiohttp.ClientSession()

    async def _raise_error(self, resp: ClientResponse):
        status = resp.status
        error_text = await resp.text()
        raise RuntimeError(f"Request failed with status code {status}: {error_text}.")

    async def submit_job_internal(self, req: JobSubmitRequest) -> JobSubmitResponse:

        logger.debug(f"Submitting job with submission_id={req.submission_id}.")

        async with self._session.post(
            f"{self._agent_address}/api/job_agent/jobs/", json=dataclasses.asdict(req)
        ) as resp:

            if resp.status == 200:
                result_json = await resp.json()
                return JobSubmitResponse(**result_json)
            else:
                await self._raise_error(resp)

    async def stop_job_internal(self, job_id: str) -> JobStopResponse:

        logger.debug(f"Stopping job with job_id={job_id}.")

        async with self._session.post(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}/stop"
        ) as resp:

            if resp.status == 200:
                result_json = await resp.json()
                return JobStopResponse(**result_json)
            else:
                self._raise_error(resp)

    async def get_job_logs_internal(self, job_id: str) -> JobLogsResponse:
        async with self._session.get(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}/logs"
        ) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobLogsResponse(**result_json)
            else:
                self._raise_error(resp)

    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        """Get an iterator that follows the logs of a job."""
        ws = await self._session.ws_connect(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}/logs/tail"
        )

        while True:
            msg = await ws.receive()

            if msg.type == aiohttp.WSMsgType.TEXT:
                yield msg.data
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                pass

    async def close(self, ignore_error=True):
        try:
            await self._session.close()
        except Exception:
            if not ignore_error:
                raise


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._dashboard_head = dashboard_head
        self._job_manager = None

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
    @optional_utils.init_ray_and_catch_exceptions()
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
    @optional_utils.init_ray_and_catch_exceptions()
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
    @optional_utils.init_ray_and_catch_exceptions()
    async def submit_job(self, req: Request) -> Response:
        result = await parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request = result

        request_submission_id = submit_request.submission_id or submit_request.job_id

        try:
            ray._private.usage.usage_lib.record_library_usage("job_submission")

            submission_id = await self._job_manager.submit_job(
                entrypoint=submit_request.entrypoint,
                submission_id=request_submission_id,
                runtime_env=submit_request.runtime_env,
                metadata=submit_request.metadata,
            )

            resp = JobSubmitResponse(job_id=submission_id, submission_id=submission_id)
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

    @routes.post("/api/jobs/{job_or_submission_id}/stop")
    @optional_utils.init_ray_and_catch_exceptions()
    async def stop_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client, self._job_manager, job_or_submission_id
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only stop submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        try:
            stopped = self._job_manager.stop_job(job.submission_id)
            resp = JobStopResponse(stopped=stopped)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/jobs/{job_or_submission_id}")
    async def get_job_info(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client, self._job_manager, job_or_submission_id
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        return Response(
            text=json.dumps(job.dict()),
            content_type="application/json",
        )

    @routes.get("/api/jobs/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_jobs(self, req: Request) -> Response:
        driver_jobs, submission_job_drivers = await get_driver_jobs(
            self._dashboard_head.gcs_aio_client
        )

        submission_jobs = await self._job_manager.list_jobs()
        submission_jobs = [
            JobDetails(
                **dataclasses.asdict(job),
                submission_id=submission_id,
                job_id=submission_job_drivers.get(submission_id).id
                if submission_id in submission_job_drivers
                else None,
                driver_info=submission_job_drivers.get(submission_id),
                type=JobType.SUBMISSION,
            )
            for submission_id, job in submission_jobs.items()
        ]
        return Response(
            text=json.dumps(
                [
                    *[submission_job.dict() for submission_job in submission_jobs],
                    *[job_info.dict() for job_info in driver_jobs.values()],
                ]
            ),
            content_type="application/json",
        )

    @routes.get("/api/jobs/{job_or_submission_id}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client, self._job_manager, job_or_submission_id
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only get logs of submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        resp = JobLogsResponse(logs=self._job_manager.get_job_logs(job.submission_id))
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/jobs/{job_or_submission_id}/logs/tail")
    @optional_utils.init_ray_and_catch_exceptions()
    async def tail_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client, self._job_manager, job_or_submission_id
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only get logs of submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(req)

        async for lines in self._job_manager.tail_job_logs(job.submission_id):
            await ws.send_str(lines)

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager(self._dashboard_head.gcs_aio_client)

    @staticmethod
    def is_minimal_module():
        return False
