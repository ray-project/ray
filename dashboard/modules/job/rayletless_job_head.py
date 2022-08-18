import asyncio
import dataclasses
import json
import logging
import traceback
from typing import Dict, Optional, Tuple

import aiohttp.web
from aiohttp.web import Request, Response

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray.dashboard.datacenter import DataOrganizer
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.job.common import (
    JobStatus,
    JobSubmitRequest,
    JobInfoStorageClient,
    JOB_ID_METADATA_KEY,
)
from ray.dashboard.modules.job.pydantic_models import (
    DriverInfo,
    JobType,
    JobDetails,
)
from ray.dashboard.modules.version import (
    CURRENT_VERSION,
    VersionResponse,
)
from ray.dashboard.modules.job.sdk import JobAgentSubmissionClient
from ray.dashboard.modules.job.utils import parse_and_validate_request
from ray.runtime_env import RuntimeEnv


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class RayletlessJobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._dashboard_head = dashboard_head
        self._gcs_job_info_stub = None
        self._job_agent_client = None
        self._job_info_store_client = None

    async def find_job_by_ids(self, job_or_submission_id: str) -> Optional[JobDetails]:
        """
        Attempts to find the job with a given submission_id or job id.
        """
        # First try to find by job_id
        driver_jobs, submission_job_drivers = await self._get_driver_jobs()
        job = driver_jobs.get(job_or_submission_id)
        if job:
            return job
        # Try to find a driver with the given id
        submission_id = next(
            (
                id
                for id, driver in submission_job_drivers.items()
                if driver.id == job_or_submission_id
            ),
            None,
        )

        if not submission_id:
            # If we didn't find a driver with the given id,
            # then lets try to search for a submission with given id
            submission_id = job_or_submission_id

        job_info = await self._job_info_store_client.get_info(submission_id)
        if job_info:
            driver = submission_job_drivers.get(submission_id)
            job = JobDetails(
                **dataclasses.asdict(job_info),
                submission_id=submission_id,
                job_id=driver.id if driver else None,
                driver_info=driver,
                type=JobType.SUBMISSION,
            )
            return job

        return None

    async def _get_driver_jobs(
        self,
    ) -> Tuple[Dict[str, JobDetails], Dict[str, DriverInfo]]:
        """Returns a tuple of dictionaries related to drivers.

        The first dictionary contains all driver jobs and is keyed by the job's id.
        The second dictionary contains drivers that belong to submission jobs.
        It's keyed by the submission job's submission id.
        Only the last driver of a submission job is returned.
        """
        request = gcs_service_pb2.GetAllJobInfoRequest()
        reply = await self._gcs_job_info_stub.GetAllJobInfo(request, timeout=5)

        jobs = {}
        submission_job_drivers = {}
        for job_table_entry in reply.job_info_list:
            if job_table_entry.config.ray_namespace.startswith(
                ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX
            ):
                # Skip jobs in any _ray_internal_ namespace
                continue
            job_id = job_table_entry.job_id.hex()
            metadata = dict(job_table_entry.config.metadata)
            job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
            if not job_submission_id:
                driver = DriverInfo(
                    id=job_id,
                    node_ip_address=job_table_entry.driver_ip_address,
                    pid=job_table_entry.driver_pid,
                )
                job = JobDetails(
                    job_id=job_id,
                    type=JobType.DRIVER,
                    status=JobStatus.SUCCEEDED
                    if job_table_entry.is_dead
                    else JobStatus.RUNNING,
                    entrypoint="",
                    start_time=job_table_entry.start_time,
                    end_time=job_table_entry.end_time,
                    metadata=metadata,
                    runtime_env=RuntimeEnv.deserialize(
                        job_table_entry.config.runtime_env_info.serialized_runtime_env
                    ).to_dict(),
                    driver_info=driver,
                )
                jobs[job_id] = job
            else:
                driver = DriverInfo(
                    id=job_id,
                    node_ip_address=job_table_entry.driver_ip_address,
                    pid=job_table_entry.driver_pid,
                )
                submission_job_drivers[job_submission_id] = driver

        return jobs, submission_job_drivers

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
    async def get_package(self, req: Request) -> Response:
        raise NotImplementedError

    @routes.put("/api/packages/{protocol}/{package_name}")
    async def upload_package(self, req: Request):
        raise NotImplementedError

    @routes.post("/api/jobs/")
    async def submit_job(self, req: Request) -> Response:
        result = await parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request = result

        request_submission_id = submit_request.submission_id or submit_request.job_id

        try:

            async def choice_agent_address():
                while True:
                    agent_infos = await DataOrganizer.get_all_agent_infos()
                    agent_address = self._job_agent_client.choice_agent_to_request(
                        agent_infos
                    )
                    if agent_address:
                        return agent_address
                    await asyncio.sleep(
                        dashboard_consts.WAIT_RAYLET_START_INTERVAL_SECONDS
                    )

            agent_address = await asyncio.wait_for(
                choice_agent_address(),
                timeout=dashboard_consts.WAIT_RAYLET_START_TIMEOUT_SECONDS,
            )
            resp = self._job_agent_client.submit_job_internal(
                entrypoint=submit_request.entrypoint,
                agent_address=agent_address,
                submission_id=request_submission_id,
                runtime_env=submit_request.runtime_env,
                metadata=submit_request.metadata,
            )
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
    async def stop_job(self, req: Request) -> Response:
        raise NotImplementedError

    @routes.get("/api/jobs/{job_or_submission_id}")
    async def get_job_info(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await self.find_job_by_ids(job_or_submission_id)
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
    async def list_jobs(self, req: Request) -> Response:
        raise NotImplementedError

    @routes.get("/api/jobs/{job_or_submission_id}/logs")
    async def get_job_logs(self, req: Request) -> Response:
        raise NotImplementedError

    @routes.get("/api/jobs/{job_or_submission_id}/logs/tail")
    async def tail_job_logs(self, req: Request) -> Response:
        raise NotImplementedError

    async def run(self, server):
        if self._job_agent_client is None:
            http_host = self._dashboard_head.http_host
            http_port = self._dashboard_head.http_port
            head_address = f"http://{http_host}:{http_port}"
            self._job_agent_client = JobAgentSubmissionClient(head_address)

        if self._job_info_store_client is None:
            self._job_info_store_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
            )

        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )

    @staticmethod
    def is_minimal_module():
        return False
