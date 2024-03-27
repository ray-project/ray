import asyncio
import dataclasses
import json
import logging
import traceback
from random import sample
from typing import Iterator, Optional

import aiohttp.web
from aiohttp.web import Request, Response
from aiohttp.client import ClientResponse

import ray
import os
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.consts as dashboard_consts
from ray.dashboard.datacenter import DataOrganizer, DataSource
import ray.dashboard.utils as dashboard_utils
from ray._private.runtime_env.packaging import (
    package_exists,
    pin_runtime_env_uri,
    upload_package_to_gcs,
)
from ray.dashboard.modules.job.common import (
    JobDeleteResponse,
    http_uri_components_to_uri,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
    JobInfoStorageClient,
    JOB_ID_METADATA_KEY,
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


from ray._private.gcs_pubsub import GcsAioJobSubmissionSubscriber
from ray.dashboard.modules.job.history_server_storage import (
    append_job_event,
    generate_logagent_url,
)
from ray._private.gcs_pubsub import GcsAioPublisher
from ray._private import ray_constants


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.DashboardHeadRouteTable


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
                await self._raise_error(resp)

    async def delete_job_internal(self, job_id: str) -> JobDeleteResponse:
        logger.debug(f"Deleting job with job_id={job_id}.")

        async with self._session.delete(
            f"{self._agent_address}/api/job_agent/jobs/{job_id}"
        ) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobDeleteResponse(**result_json)
            else:
                await self._raise_error(resp)

    async def get_job_logs_internal(
        self, job_id: str, err: bool = False
    ) -> JobLogsResponse:
        if err:
            query_url = f"{self._agent_address}/api/job_agent/jobs/{job_id}/logs?err=1"
        else:
            query_url = f"{self._agent_address}/api/job_agent/jobs/{job_id}/logs"
        async with self._session.get(query_url) as resp:
            if resp.status == 200:
                result_json = await resp.json()
                return JobLogsResponse(**result_json)
            else:
                await self._raise_error(resp)

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
    """Runs on the head node of a Ray cluster and handles Ray Jobs APIs.

    NOTE(architkulkarni): Please keep this class in sync with the OpenAPI spec at
    `doc/source/cluster/running-applications/job-submission/openapi.yml`.
    We currently do not automatically check that the OpenAPI
    spec is in sync with the implementation. If any changes are made to the
    paths in the @route decorators or in the Responses returned by the
    methods (or any nested fields in the Responses), you will need to find the
    corresponding field of the OpenAPI yaml file and update it manually. Also,
    bump the version number in the yaml file and in this class's `get_version`.
    """

    # Time that we sleep while tailing logs while waiting for
    # the supervisor actor to start. We don't know which node
    # to read the logs from until then.
    WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S = 1

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._dashboard_head = dashboard_head
        self._job_info_client = None

        import os

        self._byted_job_submit_ban_head = os.environ.get(
            "BYTED_RAY_JOB_SUBMIT_BAN_HEAD"
        )
        self._byted_job_submit_only_head = (
            os.environ.get("BYTED_RAY_JOB_SUBMIT_ONLY_HEAD") is not None
        )
        self._byted_ray_pod_ip = os.environ.get("BYTED_RAY_POD_IP")
        self._byted_port3 = os.environ.get("PORT3")

        # It contains all `JobAgentSubmissionClient` that
        # `JobHead` has ever used, and will not be deleted
        # from it unless `JobAgentSubmissionClient` is no
        # longer available (the corresponding agent process is dead)
        self._agents = dict()

        self.gcs_publisher = GcsAioPublisher(address=self._dashboard_head.gcs_address)

    async def choose_agent(
        self, choose_head_node: Optional[bool] = False
    ) -> Optional[JobAgentSubmissionClient]:
        """
        Try to disperse as much as possible to select one of
        the `CANDIDATE_AGENT_NUMBER` agents to solve requests.
        the agents will not pop from `self._agents` unless
        it's dead. Saved in `self._agents` is the agent that was
        used before.
        Strategy:
            1. if the number of `self._agents` has reached
               `CANDIDATE_AGENT_NUMBER`, randomly select one agent from
               `self._agents`.
            2. if not, randomly select one agent from all available agents,
               it is possible that the selected one already exists in
               `self._agents`.
        """
        # the number of agents which has an available HTTP port.
        choost_head_failed_count = 0
        while True:
            head_node_id = None
            raw_agent_infos = await DataOrganizer.get_all_agent_infos()
            if choose_head_node:
                for key, value in raw_agent_infos.items():
                    if value.get("httpPort", -1) > 0 and (
                        value.get("ipAddress", "[]")[1:-1] == self._byted_ray_pod_ip
                        and str(value.get("httpPort", "0")) == self._byted_port3
                    ):
                        head_node_id = key
                        break
            agent_infos = {
                key: value
                for key, value in raw_agent_infos.items()
                if value.get("httpPort", -1) > 0
            }

            if len(agent_infos) == 0:
                agent_infos = {
                    key: value
                    for key, value in raw_agent_infos.items()
                    if value.get("httpPort", -1) > 0
                }

            if (
                choose_head_node
                and head_node_id is None
                and choost_head_failed_count <= 10
            ):
                choost_head_failed_count += 1
                logger.info(
                    "failed to choose ray head in only head mode, try again next time"
                )
                await asyncio.sleep(5)
                continue

            if len(agent_infos) > 0:
                break
            await asyncio.sleep(dashboard_consts.TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS)
        # delete dead agents.
        for dead_node in set(self._agents) - set(agent_infos):
            client = self._agents.pop(dead_node)
            await client.close()

        if choose_head_node and head_node_id is not None:
            agent_info = agent_infos[head_node_id]
            if head_node_id not in self._agents:
                node_ip = agent_info["ipAddress"]
                http_port = agent_info["httpPort"]
                agent_http_address = f"http://{node_ip}:{http_port}"
                self._agents[head_node_id] = JobAgentSubmissionClient(
                    agent_http_address
                )
            return self._agents[head_node_id]
        elif len(self._agents) >= dashboard_consts.CANDIDATE_AGENT_NUMBER:
            node_id = sample(set(self._agents), 1)[0]
            return self._agents[node_id]
        else:
            # Randomly select one from among all agents, it is possible that
            # the selected one already exists in `self._agents`
            node_id = sample(sorted(agent_infos), 1)[0]
            agent_info = agent_infos[node_id]

            if node_id not in self._agents:
                node_ip = agent_info["ipAddress"]
                http_port = agent_info["httpPort"]
                agent_http_address = f"http://{node_ip}:{http_port}"
                self._agents[node_id] = JobAgentSubmissionClient(agent_http_address)

            return self._agents[node_id]

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
        result = await parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request: JobSubmitRequest = result

        try:
            job_agent_client = await asyncio.wait_for(
                self.choose_agent(self._byted_job_submit_only_head),
                timeout=dashboard_consts.WAIT_AVAILABLE_AGENT_TIMEOUT,
            )
            resp = await job_agent_client.submit_job_internal(submit_request)
        except asyncio.TimeoutError:
            return Response(
                text="No available agent to submit job, please try again later.",
                status=aiohttp.web.HTTPInternalServerError.status_code,
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
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client,
            self._job_info_client,
            job_or_submission_id,
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
            job_agent_client = await asyncio.wait_for(
                self.choose_agent(),
                timeout=dashboard_consts.WAIT_AVAILABLE_AGENT_TIMEOUT,
            )
            resp = await job_agent_client.stop_job_internal(job.submission_id)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.delete("/api/jobs/{job_or_submission_id}")
    async def delete_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client,
            self._job_info_client,
            job_or_submission_id,
        )
        if not job:
            return Response(
                text=f"Job {job_or_submission_id} does not exist",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        if job.type is not JobType.SUBMISSION:
            return Response(
                text="Can only delete submission type jobs",
                status=aiohttp.web.HTTPBadRequest.status_code,
            )

        try:
            job_agent_client = await asyncio.wait_for(
                self.choose_agent(),
                timeout=dashboard_consts.WAIT_AVAILABLE_AGENT_TIMEOUT,
            )
            resp = await job_agent_client.delete_job_internal(job.submission_id)
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
            self._dashboard_head.gcs_aio_client,
            self._job_info_client,
            job_or_submission_id,
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

    # TODO(rickyx): This endpoint's logic is also mirrored in state API's endpoint.
    # We should eventually unify the backend logic (and keep the logic in sync before
    # that).
    @routes.get("/api/jobs/")
    async def list_jobs(self, req: Request) -> Response:
        driver_jobs, submission_job_drivers = await get_driver_jobs(
            self._dashboard_head.gcs_aio_client
        )

        submission_jobs = await self._job_info_client.get_all_jobs()
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
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client,
            self._job_info_client,
            job_or_submission_id,
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

        is_err_log = req.query.get("err") == "1"
        try:
            driver_agent_http_address = job.driver_agent_http_address
            driver_node_id = job.driver_node_id
            if driver_agent_http_address is None:
                resp = JobLogsResponse("")
            else:
                if driver_node_id not in self._agents:
                    self._agents[driver_node_id] = JobAgentSubmissionClient(
                        driver_agent_http_address
                    )
                job_agent_client = self._agents[driver_node_id]
                resp = await job_agent_client.get_job_logs_internal(
                    job.submission_id, is_err_log
                )

                download = req.query.get("download")
                if download == "1":
                    if is_err_log:
                        filename = f"job-driver-{job.submission_id}.err"
                    else:
                        filename = f"job-driver-{job.submission_id}.log"
                    content_disposition = f'attachment; filename="{filename}"'
                    headers = {"Content-Disposition": content_disposition}
                    return Response(text=resp.logs, headers=headers)
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
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_head.gcs_aio_client,
            self._job_info_client,
            job_or_submission_id,
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

        driver_agent_http_address = None
        while driver_agent_http_address is None:
            job = await find_job_by_ids(
                self._dashboard_head.gcs_aio_client,
                self._job_info_client,
                job_or_submission_id,
            )
            driver_agent_http_address = job.driver_agent_http_address
            status = job.status
            if status.is_terminal() and driver_agent_http_address is None:
                # Job exited before supervisor actor started.
                return
            await asyncio.sleep(self.WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S)

        driver_node_id = job.driver_node_id
        if driver_node_id not in self._agents:
            self._agents[driver_node_id] = JobAgentSubmissionClient(
                driver_agent_http_address
            )
        job_agent_client = self._agents[driver_node_id]

        async for lines in job_agent_client.tail_job_logs(job.submission_id):
            await ws.send_str(lines)

        return ws

    async def run(self, server):
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
            )

        if dashboard_consts.history_server_enabled():
            await asyncio.gather(self._listen_jobs())

    async def _listen_jobs(self):
        # Receive jobs from channel.
        gcs_addr = self._dashboard_head.gcs_address
        logger.info(f"gcs address {gcs_addr}")
        subscriber = GcsAioJobSubmissionSubscriber(address=gcs_addr)
        await subscriber.subscribe()

        while True:
            logger.info("_listen_jobs from pubsub")
            msgs = await subscriber.poll(batch_size=200)
            for msg in msgs:
                # msg type: src.ray.protobuf.pubsub_pb2.PubMessage
                # append_job_event(msg['key_id'], msg['job_change_message'])
                submission_id = msg.key_id.decode("utf-8")
                job_change_message = json.loads(msg.job_change_message.json)
                job_change_message = add_history_server_job_fields(
                    submission_id, job_change_message
                )
                # reply = await self._dashboard_head.gcs_aio_client.get_all_job_info()
                # job_change_message = add_driver_info_fields(
                #     reply, submission_id, job_change_message
                # )
                # logger.info(
                #     f"_listen_jobs type: {type(job_change_message)} job_change_message: {job_change_message}"
                # )
                append_job_event(
                    self._dashboard_head.history_server_storage,
                    submission_id,
                    job_change_message,
                )
            await asyncio.sleep(5)

    @staticmethod
    def is_minimal_module():
        return False


def add_history_server_job_fields(submission_id, job_change_message):
    logger.info("add_history_server_job_fields")
    log_dir = os.environ.get("BYTED_RAY_REDIRECT_LOG", "/tmp/ray/session_latest/logs")
    node_id = job_change_message["driver_node_id"]
    node = DataSource.nodes.get(node_id)
    logger.info(f"node: {node}")
    if node:
        # Add podname/containername/logname/psm for generating logagent links.
        node_name = node["nodeName"]
        job_change_message["nodeName"] = node_name
        container_name = ""
        if "-head-" in node_name:
            container_name = "ray-head"
        else:
            container_name = "ray-worker"

        # logname
        stdout_log_path = f"{log_dir}/job-driver-{submission_id}.log"
        job_change_message["containerName"] = container_name
        job_change_message["logName"] = stdout_log_path
        hostip = node["nodeManagerAddress"]
        psm = dashboard_consts.get_global_psm()
        job_change_message["byted_log_url"] = generate_logagent_url(
            psm, hostip, node_name, container_name, stdout_log_path
        )

        if (
            ray_constants.RAY_ENABLE_DRIVER_ERR_LOG_FILE_ENVIRONMENT_VARIABLE
            in os.environ
        ):
            stderr_log_path = f"{log_dir}/job-driver-{submission_id}.err"
            job_change_message["errLogName"] = stderr_log_path
            job_change_message["byted_err_log_url"] = generate_logagent_url(
                psm, hostip, node_name, container_name, stderr_log_path
            )

    job_change_message["psm"] = dashboard_consts.get_global_psm()

    return job_change_message


# Reference to get_driver_jobs()
# deprecated because this job is not required for jobs
def add_driver_info_fields(reply, submission_id, job_change_message):
    from ray.dashboard.modules.job.pydantic_models import DriverInfo

    job_driver = None
    for job_table_entry in reply.job_info_list:
        if job_table_entry.config.ray_namespace.startswith(
            ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX
        ):
            # Skip jobs in any _ray_internal_ namespace
            continue
        job_id = job_table_entry.job_id.hex()
        metadata = dict(job_table_entry.config.metadata)
        logger.info(job_table_entry)
        job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
        # Ignore the job has no submission id
        # if not job_submission_id:
        #     continue
        if submission_id == job_submission_id:
            job_driver = DriverInfo(
                id=job_id,
                node_ip_address=job_table_entry.driver_address.ip_address,
                pid=job_table_entry.driver_pid,
            )
            break

    if job_driver:
        job_change_message["driver_info"] = job_driver.dict()
    else:
        logger.error(f"Can not find driver info for submission id: {submission_id}")

    return job_change_message
