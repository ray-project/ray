import asyncio
import dataclasses
import json
import logging
import traceback
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple

import aiohttp.web
from aiohttp.web import Request, Response

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray.dashboard.datacenter import DataOrganizer
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.dashboard.modules.job.common import (
    JobStatus,
    JobSubmitRequest,
    JobSubmitResponse,
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
from ray.dashboard.modules.job.utils import parse_and_validate_request
from ray.runtime_env import RuntimeEnv


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


# TODO(Catch-Bull): It doesn't  exposed to users for now,
# move to `sdk.py` after the interface is finished.
class JobAgentSubmissionClient(SubmissionClient):
    """A local client for submitting and interacting with jobs on a specific node
    in the remote cluster.
    Submits requests over HTTP to the job agent on the specific node using the REST API.
    """

    def __init__(
        self,
        dashboard_agent_address: Optional[str] = None,
        create_cluster_if_needed: bool = False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a JobAgentSubmissionClient and check the connection to the cluster.
        Args:
            address: address: The IP address and port of a a specific agent. Defaults to
                http://localhost:8265.
            create_cluster_if_needed: Indicates whether the cluster at the specified
                address needs to already be running. Ray doesn't start a cluster
                before interacting with jobs, but external job managers may do so.
            cookies: Cookies to use when sending requests to the HTTP job server.
            metadata: Arbitrary metadata to store along with all jobs.  New metadata
                specified per job will be merged with the global metadata provided here
                via a simple dict update.
            headers: Headers to use when sending requests to the job agent, used
                for cases like authentication to a remote cluster.
        """
        super().__init__(
            address=dashboard_agent_address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
        )

    def submit_job_internal(
        self,
        *,
        entrypoint: str,
        job_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
        submission_id: Optional[str] = None,
    ) -> JobSubmitResponse:
        if job_id:
            logger.warning(
                "job_id kwarg is deprecated. Please use submission_id instead."
            )

        runtime_env = runtime_env or {}
        metadata = metadata or {}
        metadata.update(self._default_metadata)

        # Run the RuntimeEnv constructor to parse local pip/conda requirements files.
        runtime_env = RuntimeEnv(**runtime_env).to_dict()

        submission_id = submission_id or job_id

        req = JobSubmitRequest(
            entrypoint=entrypoint,
            submission_id=submission_id,
            runtime_env=runtime_env,
            metadata=metadata,
        )

        logger.debug(f"Submitting job with submission_id={submission_id}.")

        r = self._do_request(
            "POST",
            "/api/job_agent/jobs/",
            json_data=dataclasses.asdict(req),
        )

        if r.status_code == 200:
            return JobSubmitResponse(**r.json())
        else:
            self._raise_error(r)


# TODO(Catch-Bull): because of `routes` will check duplicate route path,
# So `RayletlessJobHead` cannot inherit `JobHead`, Many functions are
# copied directly from `JobHead`. Finally, after RayletlessJobHead is stable,
# we will delete `JobHead`.
class RayletlessJobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._dashboard_head = dashboard_head
        self._gcs_job_info_stub = None
        self._job_info_store_client = None

        self._agents = None

    async def choice_agent(self) -> Optional[str]:
        # the number of agents which has an available HTTP port.
        while True:
            agent_infos = await DataOrganizer.get_all_agent_infos()
            if (
                sum(
                    map(
                        lambda agent_ports: agent_ports["httpPort"] > 0,
                        agent_infos.values(),
                    )
                )
                > 0
            ):
                break
            await asyncio.sleep(dashboard_consts.WAIT_RAYLET_START_INTERVAL_SECONDS)
        # delete dead agents.
        for dead_node in set(self._agents) - set(agent_infos):
            self._agents.pop(dead_node)

        for node_id, agent_info in agent_infos.items():
            if len(self._agents) >= dashboard_consts.CANDIDATE_AGENT_NUMBER:
                break
            node_ip = agent_info["ipAddress"]
            http_port = agent_info["httpPort"]
            # skip agent which already exists or http port unavailable
            if node_id in self._agents or http_port <= 0:
                continue
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

            job_agent_client = await asyncio.wait_for(
                self.choice_agent(),
                timeout=dashboard_consts.WAIT_RAYLET_START_TIMEOUT_SECONDS,
            )
            resp = job_agent_client.submit_job_internal(
                entrypoint=submit_request.entrypoint,
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

        if self._job_info_store_client is None:
            self._job_info_store_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
            )

        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )

        self._agents = OrderedDict()

    @staticmethod
    def is_minimal_module():
        return False
