import aiohttp
from aiohttp.web import Request, Response
import dataclasses
import json
import logging
import traceback

import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.job.common import (
    JobSubmitRequest,
    JobSubmitResponse,
)
from ray.dashboard.modules.job.job_manager import JobManager
from ray.dashboard.modules.job.utils import parse_and_validate_request


routes = optional_utils.ClassMethodRouteTable
logger = logging.getLogger(__name__)


class JobAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_manager = None

    @routes.post("/api/job_agent/jobs/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def submit_job(self, req: Request) -> Response:
        result = await parse_and_validate_request(req, JobSubmitRequest)
        # Request parsing failed, returned with Response object.
        if isinstance(result, Response):
            return result
        else:
            submit_request = result

        try:
            submission_id = await self._job_manager.submit_job(
                entrypoint=submit_request.entrypoint,
                submission_id=submit_request.submission_id,
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

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager(self._dashboard_agent.gcs_aio_client)

    @staticmethod
    def is_minimal_module():
        return False
