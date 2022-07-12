import dataclasses
import json
import logging
import traceback
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp.web
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.runtime_env.packaging import (
    package_exists,
    pin_runtime_env_uri,
    upload_package_to_gcs,
)
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.job.common import (
    http_uri_components_to_uri,
    JobInfo,
    JobLogsResponse,
    JobStatus,
    JobStopResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
    validate_request_type,
    JOB_ID_METADATA_KEY,
    DriverInfo,
)
from ray.dashboard.modules.version import (
    CURRENT_VERSION,
    VersionResponse,
)
from ray.dashboard.modules.job.job_manager import JobManager
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._dashboard_head = dashboard_head
        self._job_manager = None
        self._gcs_job_info_stub = None

    async def _parse_and_validate_request(self, req: Request,
                                          request_type: dataclass) -> Any:
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
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

    @routes.get("/api/jobs/{submission_id}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def get_job_info(self, req: Request) -> Response:
        submission_id = req.match_info["submission_id"]
        if not self.job_exists(submission_id):
            return Response(
                text=f"Job {submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        data: JobInfo = self._job_manager.get_job_info(submission_id)
        _, submission_job_drivers = await self._get_driver_jobs()
        driver = submission_job_drivers.get(submission_id)
        job_response = JobResponse(
            **dataclasses.asdict(data),
            id=submission_id,
            submission_id=submission_id,
            job_id=driver.id if driver else None,
            driver=driver,
            type="submission")

        return Response(
            text=json.dumps(dataclasses.asdict(job_response), ),
            content_type="application/json")

    @routes.get("/api/jobs/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=False)
    async def list_jobs(self, req: Request) -> Response:
        driver_jobs, submission_job_drivers = await self._get_driver_jobs()

        # TODO(aguo): convert _job_manager.list_jobs to an async function.
        submission_jobs = self._job_manager.list_jobs()
        submission_jobs = [
            JobResponse(
                **dataclasses.asdict(job),
                id=submission_id,
                submission_id=submission_id,
                job_id=submission_job_drivers.get(submission_id).id
                if submission_id in submission_job_drivers else None,
                driver=submission_job_drivers.get(submission_id),
                type="submission")
            for submission_id, job in submission_jobs.items()
        ]
        return Response(
            text=json.dumps([
                *submission_jobs,
                *[
                    dataclasses.asdict(job_info)
                    for job_info in driver_jobs.values()
                ],
            ]),
            content_type="application/json",
        )

    async def _get_driver_jobs(self):
        """Returns a tuple of dictionaries related to drivers.

        The first dictionary contains all driver jobs and is keyed by the job's id.
        The second dictionary contains drivers that belong to submission jobs.
        It's keyed by the submission job's id. Only the last driver of a submission job is returned.
        """
        request = gcs_service_pb2.GetAllJobInfoRequest()
        reply = await self._gcs_job_info_stub.GetAllJobInfo(request, timeout=5)

        jobs = {}
        submission_job_drivers = {}
        for job_table_entry in reply.job_info_list:
            job_id = job_table_entry.job_id.hex()
            metadata = dict(job_table_entry.config.metadata)
            job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
            if not job_submission_id:
                driver = DriverInfo(
                    id=job_id,
                    ip_address=job_table_entry.driver_ip_address,
                    pid=job_table_entry.driver_pid,
                )
                job = JobResponse(
                    id=job_id,
                    job_id=job_id,
                    type="driver",
                    status=JobStatus.SUCCEEDED
                    if job_table_entry.is_dead else JobStatus.RUNNING,
                    entrypoint="",
                    start_time=job_table_entry.start_time,
                    end_time=job_table_entry.end_time,
                    metadata=metadata,
                    runtime_env=RuntimeEnv.deserialize(
                        job_table_entry.config.runtime_env_info.
                        serialized_runtime_env).to_dict(),
                    driver=driver,
                )
                jobs[job_id] = job
            else:
                driver = DriverInfo(
                    id=job_id,
                    ip_address=job_table_entry.driver_ip_address,
                    pid=job_table_entry.driver_pid,
                )
                submission_job_drivers[job_submission_id] = driver

        return jobs, submission_job_drivers

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
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json")

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

        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel)

    @staticmethod
    def is_minimal_module():
        return False


@dataclass
class JobResponse(JobInfo):
    """Response model of a job
    """
    #: The type of job. Either "submission" or "driver"
    type: str
    #: The primary id of a job. Currently the "submission_id" for backwards-compatibility reasons.
    #  Eventually, the job_id will be the main id.
    id: Optional[str]
    #: The job id. An id that is created for every driver that is launched in ray.
    #  This can be used to fetch data about jobs using ray core apis.
    job_id: Optional[str]
    #: A submission id is an id created for every submission job. It can be used to fetch
    #  data about jobs using the job submission apis.
    submission_id: Optional[str]
    #: The driver related to this job. For submission jobs,
    #  it is the last driver launched by that job submission,
    #  or None if there is no driver.
    driver: Optional[DriverInfo] = None
