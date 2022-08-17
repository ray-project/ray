import asyncio
from collections import OrderedDict
import dataclasses
import json
import logging
import traceback
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import aiohttp.web
from aiohttp.web import Request, Response

import ray
from ray._private import ray_constants
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.datacenter import DataSource
from ray._private.runtime_env.packaging import (
    package_exists,
    pin_runtime_env_uri,
    upload_package_to_gcs,
)
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.job.common import (
    http_uri_components_to_uri,
    JobStatus,
    JobSubmitRequest,
    validate_request_type,
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
from ray.dashboard.modules.job.job_manager import JobManager
from ray.dashboard.modules.job.sdk import JobAgentSubmissionClient
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class RayletlessJobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._dashboard_head = dashboard_head
        self._job_manager = None
        self._gcs_job_info_stub = None
        self._head_address = (
            f"http://{self._dashboard_head.http_host}:{self._dashboard_head.http_port}"
        )

        self._agents = OrderedDict()
        self._agent_job_clients_pool = dict()

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

    async def _choice_agent_to_submit_job(self) -> JobAgentSubmissionClient:
        # the number of agents which has an available HTTP port.
        while (
            sum(map(lambda agent_ports: agent_ports[0] > 0, DataSource.agents.values()))
            == 0
        ):
            await asyncio.sleep(dashboard_consts.WAIT_RAYLET_START_INTERVAL_SECONDS)
        # delete dead agents.
        for dead_node in set(self._agents) - set(DataSource.agents):
            self._agents.pop(dead_node)
        for dead_node in set(self._agent_job_clients_pool) - set(DataSource.agents):
            self._agent_job_clients_pool.pop(dead_node)
        for node_id, (http_port, _) in DataSource.agents.items():
            if len(self._agents) >= dashboard_consts.CANDIDATE_AGENT_NUMBER:
                break
            if node_id in self._agents or http_port <= 0:
                continue
            node_ip = DataSource.node_id_to_ip[node_id]
            agent_http_address = f"http://{node_ip}:{http_port}"

            if node_id not in self._agent_job_clients_pool:
                self._agent_job_clients_pool[node_id] = JobAgentSubmissionClient(
                    agent_http_address,
                    head_address=self._head_address,
                )

            self._agents[node_id] = self._agent_job_clients_pool[node_id]
            # move agent to the front of the queue.
            self._agents.move_to_end(node_id, last=False)

        # FIFO
        node_id, job_agent_client = self._agents.popitem(last=False)
        self._agents[node_id] = job_agent_client

        return job_agent_client

    async def _get_agent_client_by_ip_address(
        self, ip_address
    ) -> Optional[JobAgentSubmissionClient]:
        # There can be multiple raylet processes on a node, return any one of them
        for dead_node in set(self._agent_job_clients_pool) - set(DataSource.agents):
            self._agent_job_clients_pool.pop(dead_node)

        for node_info in DataSource.nodes.values():
            node_id = node_info["nodeId"]
            ip = node_info["nodeManagerAddress"]
            http_port = DataSource.agents[node_id][0]
            if http_port <= 0 or ip != ip_address:
                continue
            agent_http_address = f"http://{ip_address}:{http_port}"
            if node_id not in self._agent_job_clients_pool:
                self._agent_job_clients_pool[node_id] = JobAgentSubmissionClient(
                    agent_http_address,
                    head_address=self._head_address,
                )
            return self._agent_job_clients_pool[node_id]

        return None

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

    async def _get_driver_address_by_job(self, job: JobDetails) -> str:
        if job.driver_info is None:
            # There are the following possibilities:
            #    1. Supervisor actor is not ready
            #    2. The driver is not launched yet or the driver
            #       process is not finished init of CoreWorker.
            # We need to get the address of the supervisor-actor/driver
            # from any agent.
            tmp_job_agent_client = await self._choice_agent_to_submit_job()
            driver_ip_address = tmp_job_agent_client.get_driver_location_internal(
                job.submission_id
            ).ip_address
        else:
            driver_ip_address = job.driver_info.node_ip_address
        return driver_ip_address

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
    async def submit_job(self, req: Request) -> Response:
        result = await self._parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request = result

        request_submission_id = submit_request.submission_id or submit_request.job_id

        try:
            job_agent_client = await asyncio.wait_for(
                self._choice_agent_to_submit_job(),
                dashboard_consts.WAIT_RAYLET_START_TIMEOUT_SECONDS,
            )
            logger.info(f"hejialing test: {job_agent_client._address}")
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
            job_agent_client = await asyncio.wait_for(
                self._choice_agent_to_submit_job(),
                dashboard_consts.WAIT_RAYLET_START_TIMEOUT_SECONDS,
            )
            resp = job_agent_client.stop_job_internal(job.submission_id)
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

        try:
            driver_ip_address = await self._get_driver_address_by_job(job)
            job_agent_client = await self._get_agent_client_by_ip_address(
                driver_ip_address
            )
            if job_agent_client is None:
                return Response(
                    text="The node where the driver is located does not have "
                    "an agent process with an available http port",
                    status=aiohttp.web.HTTPInternalServerError.status_code,
                )
            resp = job_agent_client.get_job_logs_internal(job.submission_id)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/jobs/{job_or_submission_id}/logs/tail")
    async def tail_job_logs(self, req: Request) -> Response:
        raise NotImplementedError

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager(self._dashboard_head.gcs_aio_client)

        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )

    @staticmethod
    def is_minimal_module():
        return False
