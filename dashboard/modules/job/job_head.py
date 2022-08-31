import asyncio
import dataclasses
import json
import logging
import traceback
from collections import OrderedDict
from typing import Dict, Optional, Tuple

import aiohttp.web
from aiohttp.web import Request, Response
from aiohttp.client import ClientResponse

import ray
from ray._private import ray_constants
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
from ray._private.runtime_env.packaging import (
    package_exists,
    pin_runtime_env_uri,
    upload_package_to_gcs,
)
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.datacenter import DataOrganizer
from ray.dashboard.modules.job.common import (
    http_uri_components_to_uri,
    JobStatus,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
    JOB_ID_METADATA_KEY,
)
from ray.dashboard.modules.job.pydantic_models import (
    DriverInfo,
    JobDetails,
    JobType,
)
from ray.dashboard.modules.job.utils import parse_and_validate_request
from ray.dashboard.modules.version import (
    CURRENT_VERSION,
    VersionResponse,
)
from ray.dashboard.modules.job.job_manager import JobManager
from ray.runtime_env import RuntimeEnv

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
        self._address = dashboard_agent_address
        self._session = aiohttp.ClientSession()

    async def _raise_error(self, r: ClientResponse):
        status = r.status
        error_text = await r.text()
        raise RuntimeError(f"Request failed with status code {status}: {error_text}.")

    async def submit_job_internal(self, req: JobSubmitRequest) -> JobSubmitResponse:

        logger.debug(f"Submitting job with submission_id={req.submission_id}.")

        async with self._session.post(
            self._address + "/api/job_agent/jobs/", json=dataclasses.asdict(req)
        ) as resp:

            if resp.status == 200:
                result_json = await resp.json()
                return JobSubmitResponse(**result_json)
            else:
                await self._raise_error(resp)

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
        self._gcs_job_info_stub = None

        self._disable_ray_init = dashboard_consts.RAY_RAYLETLESS_HEAD_NODE
        # this is a queue of JobAgentSubmissionClient
        self._agents = None

    async def choose_agent(self) -> Optional[JobAgentSubmissionClient]:
        """
        Try to disperse as much as possible to select one of
        the `CANDIDATE_AGENT_NUMBER` agents to solve requests.
        the agents will not pop from `self._agents` unless
        it's dead.

        Follow the steps below to select the agent client:
            1. delete dead agent from `self._agents`, make sure
               the `JobAgentSubmissionClient` in `self._agents`
               is always available.
            2. Attempt to put new agents into `self._agents` until
               its size is `CANDIDATE_AGENT_NUMBER`
            3. Returns the element at the head of the `self._agents`
               and put it into `self._agents` again.
        """
        # the number of agents which has an available HTTP port.
        while True:
            raw_agent_infos = await DataOrganizer.get_all_agent_infos()
            agent_infos = {
                key: value
                for key, value in raw_agent_infos.items()
                if value.get("httpPort", -1) > 0
            }
            if len(agent_infos) > 0:
                break
            await asyncio.sleep(dashboard_consts.WAIT_RAYLET_START_INTERVAL_SECONDS)
        # delete dead agents.
        for dead_node in set(self._agents) - set(agent_infos):
            client = self._agents.pop(dead_node)
            await client.close()

        for node_id, agent_info in agent_infos.items():
            if len(self._agents) >= dashboard_consts.CANDIDATE_AGENT_NUMBER:
                break
            node_ip = agent_info["ipAddress"]
            http_port = agent_info["httpPort"]

            agent_http_address = f"http://{node_ip}:{http_port}"

            self._agents[node_id] = JobAgentSubmissionClient(agent_http_address)
            # move agent to the front of the queue.
            self._agents.move_to_end(node_id, last=False)

        # FIFO
        node_id, job_agent_client = self._agents.popitem(last=False)
        self._agents[node_id] = job_agent_client

        return job_agent_client

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

        job_info = await self._job_manager.get_job_info(submission_id)
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
            if dashboard_consts.RAY_RAYLETLESS_HEAD_NODE:
                job_agent_client = await asyncio.wait_for(
                    self.choose_agent(),
                    timeout=dashboard_consts.WAIT_RAYLET_START_TIMEOUT_SECONDS,
                )
                resp = await job_agent_client.submit_job_internal(submit_request)
            else:
                submission_id = await self._job_manager.submit_job(
                    entrypoint=submit_request.entrypoint,
                    submission_id=request_submission_id,
                    runtime_env=submit_request.runtime_env,
                    metadata=submit_request.metadata,
                )

                resp = JobSubmitResponse(
                    job_id=submission_id, submission_id=submission_id
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
    @optional_utils.init_ray_and_catch_exceptions()
    async def stop_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await self.find_job_by_ids(job_or_submission_id)
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
    @optional_utils.init_ray_and_catch_exceptions()
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
    @optional_utils.init_ray_and_catch_exceptions()
    async def list_jobs(self, req: Request) -> Response:
        driver_jobs, submission_job_drivers = await self._get_driver_jobs()

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

    @routes.get("/api/jobs/{job_or_submission_id}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await self.find_job_by_ids(job_or_submission_id)
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
        job = await self.find_job_by_ids(job_or_submission_id)
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

        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )

        self._agents = OrderedDict()

    @staticmethod
    def is_minimal_module():
        return False
