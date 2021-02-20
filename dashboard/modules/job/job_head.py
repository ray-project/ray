import json
import logging
import asyncio

import aiohttp.web
from aioredis.pubsub import Receiver

import ray
import ray.gcs_utils
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.modules.job import job_consts
from ray.core.generated import common_pb2
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import (
    DataSource,
    GlobalSignals,
)
from ray.utils import binary_to_hex, hex_to_binary

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def job_table_data_to_dict(message):
    decode_keys = {"jobId", "rayletId"}
    # Job info
    job_payload = message.job_payload
    job_info = {}
    if message.job_payload:
        message.ClearField("job_payload")
        try:
            job_info = json.loads(job_payload)
        except Exception:
            logger.exception(
                "Parse job payload failed, job id: %s, job payload: %s",
                binary_to_hex(message.job_id), job_payload)
    try:
        data = dashboard_utils.message_to_dict(
            message, decode_keys, including_default_value_fields=True)
        job_info.update(data)
        return job_info
    finally:
        message.job_payload = job_payload


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # JobInfoGcsServiceStub
        self._gcs_job_info_stub = None

    async def _next_job_id(self):
        counter_str = await self._dashboard_head.aioredis_client.incr(
            job_consts.REDIS_KEY_JOB_COUNTER)
        job_id_int = int(counter_str)
        if job_id_int & (1 << 31):
            raise Exception(
                f"Job id overflow: {ray.JobID.from_int(job_id_int)}")
        job_id_int |= (1 << 31)
        return ray.JobID.from_int(job_id_int)

    @routes.post("/jobs")
    async def new_job(self, req) -> aiohttp.web.Response:
        job_info = dict(await req.json())
        language = common_pb2.Language.Value(job_info["language"])
        job_id = await self._next_job_id()
        request = gcs_service_pb2.SubmitJobRequest(
            job_id=job_id.binary(),
            language=language,
            job_payload=json.dumps(job_info))
        reply = await self._gcs_job_info_stub.SubmitJob(request)
        if reply.status.code == 0:
            logger.info("Succeeded to submit job %s", job_id.hex())
            return dashboard_utils.rest_response(
                success=True, message="Job submitted.", job_id=job_id.hex())
        else:
            logger.info("Failed to submit job %s", job_id.hex())
            return dashboard_utils.rest_response(
                success=False,
                message=f"Failed to submit job: {reply.status.message}",
                job_id=job_id.hex())

    @routes.delete("/jobs/{job_id}")
    async def drop_job(self, req) -> aiohttp.web.Response:
        job_id = req.match_info.get("job_id")
        request = gcs_service_pb2.DropJobRequest(job_id=hex_to_binary(job_id))
        reply = await self._gcs_job_info_stub.DropJob(request)
        if reply.status.code == 0:
            logger.info("Succeeded to drop job %s", job_id)
            return dashboard_utils.rest_response(
                success=True, message="Job dropped.", job_id=job_id)
        else:
            logger.info("Failed to drop job %s", job_id)
            return dashboard_utils.rest_response(
                success=False,
                message=f"Failed to drop job: {reply.status.message}",
                job_id=job_id)

    @routes.get("/jobs")
    @dashboard_utils.aiohttp_cache
    async def get_all_jobs(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            return dashboard_utils.rest_response(
                success=True,
                message="All job summary fetched.",
                summary=list(DataSource.jobs.values()))
        elif view == "state":
            all_job_state = [{
                k: v[k]
                for k in ("jobId", "state", "timestamp")
            } for v in DataSource.jobs.values()]
            return dashboard_utils.rest_response(
                success=True,
                message="All job states fetched.",
                states=all_job_state)
        else:
            return dashboard_utils.rest_response(
                success=False, message=f"Unknown view {view}")

    @routes.get("/jobs/{job_id}")
    @dashboard_utils.aiohttp_cache
    async def get_job(self, req) -> aiohttp.web.Response:
        job_id = req.match_info.get("job_id")
        view = req.query.get("view")
        if view is None:
            job_detail = {
                "jobInfo": DataSource.jobs.get(job_id, {}),
                "jobActors": DataSource.job_actors.get(job_id, {}),
                "jobWorkers": DataSource.job_workers.get(job_id, []),
            }
            await GlobalSignals.job_info_fetched.send(job_detail)
            return dashboard_utils.rest_response(
                success=True, message="Job detail fetched.", detail=job_detail)
        elif view == "state":
            job_info = DataSource.jobs.get(job_id, {})
            job_state = {
                k: job_info[k]
                for k in ("jobId", "state", "timestamp") if k in job_info
            }
            return dashboard_utils.rest_response(
                success=True, message="Job state fetched.", **job_state)
        else:
            return dashboard_utils.rest_response(
                success=False, message=f"Unknown view {view}")

    async def _update_jobs(self):
        # Subscribe job channel.
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        key = f"{job_consts.JOB_CHANNEL}:*"
        pattern = receiver.pattern(key)
        await aioredis_client.psubscribe(pattern)
        logger.info("Subscribed to %s", key)

        # Get all job info.
        while True:
            try:
                logger.info("Getting all job info from GCS.")
                request = gcs_service_pb2.GetAllJobInfoRequest()
                reply = await self._gcs_job_info_stub.GetAllJobInfo(
                    request, timeout=5)
                if reply.status.code == 0:
                    jobs = {}
                    for job_table_data in reply.job_info_list:
                        data = job_table_data_to_dict(job_table_data)
                        jobs[data["jobId"]] = data
                    # Update jobs.
                    DataSource.jobs.reset(jobs)
                    logger.info("Received %d job info from GCS.", len(jobs))
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllJobInfo: {reply.status.message}")
            except Exception:
                logger.exception("Error Getting all job info from GCS.")
                await asyncio.sleep(
                    job_consts.RETRY_GET_ALL_JOB_INFO_INTERVAL_SECONDS)

        # Receive jobs from channel.
        async for sender, msg in receiver.iter():
            try:
                _, data = msg
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(data)
                message = ray.gcs_utils.JobTableData.FromString(
                    pubsub_message.data)
                job_table_data = job_table_data_to_dict(message)
                job_id = job_table_data["jobId"]
                # Update jobs.
                DataSource.jobs[job_id] = job_table_data
            except Exception:
                logger.exception("Error receiving job info.")

    async def run(self, server):
        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel)

        await asyncio.gather(self._update_jobs())
