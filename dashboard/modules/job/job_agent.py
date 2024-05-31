import asyncio
import aiohttp
import dataclasses
import json
import logging
import traceback

from aiohttp.web import Request, Response
from typing import AsyncIterator

import ray
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.job.common import (
    JobDeleteResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.job_log_storage_client import JobLogStorageClient
from ray.dashboard.modules.job.job_manager import JobManager
from ray.dashboard.modules.job.pydantic_models import JobType
from ray.dashboard.modules.job.utils import parse_and_validate_request, find_job_by_ids

routes = optional_utils.DashboardAgentRouteTable
logger = logging.getLogger(__name__)


class JobManagerAgent(dashboard_utils.DashboardAgentModule):
    """TODO"""

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

        self._job_manager = None

        self._job_info_client = None
        self._job_log_client = None

    @routes.post("/api/job_agent/jobs/")
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
            submission_id = await self.get_job_manager().submit_job(
                entrypoint=submit_request.entrypoint,
                submission_id=request_submission_id,
                runtime_env=submit_request.runtime_env,
                metadata=submit_request.metadata,
                entrypoint_num_cpus=submit_request.entrypoint_num_cpus,
                entrypoint_num_gpus=submit_request.entrypoint_num_gpus,
                entrypoint_memory=submit_request.entrypoint_memory,
                entrypoint_resources=submit_request.entrypoint_resources,
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

    @routes.post("/api/job_agent/jobs/{job_or_submission_id}/stop")
    @optional_utils.init_ray_and_catch_exceptions()
    async def stop_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_aio_client,
            self._get_job_info_client(),
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
            stopped = self.get_job_manager().stop_job(job.submission_id)
            resp = JobStopResponse(stopped=stopped)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.delete("/api/job_agent/jobs/{job_or_submission_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def delete_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_aio_client,
            self._get_job_info_client(),
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
            deleted = await self.get_job_manager().delete_job(job.submission_id)
            resp = JobDeleteResponse(deleted=deleted)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    def get_job_manager(self):
        if not self._job_manager:
            self._job_manager = JobManager(
                self._dashboard_agent.gcs_aio_client, self._dashboard_agent.log_dir
            )

        return self._job_manager

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False


class JobObservabilityAgent(dashboard_utils.DashboardAgentModule):
    """TODO"""

    # Time that we will sleep while tailing logs if no new log line is
    # available.
    LOG_TAIL_SLEEP_S = 1

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

        self._job_log_client = JobLogStorageClient()
        self._job_info_client = None

    @routes.get("/api/job_agent/jobs/{job_or_submission_id}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_aio_client,
            self._get_job_info_client(),
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

        resp = JobLogsResponse(
            logs=self._job_log_client.get_logs(job.submission_id)
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/job_agent/jobs/{job_or_submission_id}/logs/tail")
    @optional_utils.init_ray_and_catch_exceptions()
    async def tail_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]
        job = await find_job_by_ids(
            self._dashboard_agent.gcs_aio_client,
            self._get_job_info_client(),
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

        async for lines in self._tail_job_logs(job.submission_id):
            await ws.send_str(lines)

        return ws

    async def _tail_job_logs(self, job_id: str) -> AsyncIterator[str]:
        if await self._job_info_client.get_job_status(job_id) is None:
            raise RuntimeError(f"Job '{job_id}' does not exist.")

        for lines in self._job_log_client.tail_logs(job_id):
            if lines is None:
                # Return if the job has exited and there are no new log lines.
                status = await self._job_info_client.get_job_status(job_id)
                if status.is_terminal():
                    return

                await asyncio.sleep(self.LOG_TAIL_SLEEP_S)
            else:
                yield "".join(lines)

    def _get_job_info_client(self):
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(self._dashboard_agent.gcs_aio_client)

        return self._job_info_client

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
