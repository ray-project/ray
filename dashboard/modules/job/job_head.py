import logging
import asyncio

import aiohttp.web
from aioredis.pubsub import Receiver

import ray
import ray.gcs_utils
import ray.new_dashboard.modules.job.job_consts as job_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import (
    DataSource,
    GlobalSignals,
)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def job_table_data_to_dict(message):
    decode_keys = {"jobId", "rayletId"}
    return dashboard_utils.message_to_dict(
        message, decode_keys, including_default_value_fields=True)


class JobHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # JobInfoGcsServiceStub
        self._gcs_job_info_stub = None

    @routes.get("/jobs")
    @dashboard_utils.aiohttp_cache
    async def get_all_jobs(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            return dashboard_utils.rest_response(
                success=True,
                message="All job summary fetched.",
                summary=list(DataSource.jobs.values()))
        else:
            return dashboard_utils.rest_response(
                success=False, message="Unknown view {}".format(view))

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
        else:
            return dashboard_utils.rest_response(
                success=False, message="Unknown view {}".format(view))

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
