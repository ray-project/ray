import aiohttp
from aiohttp.web import Request, Response
import dataclasses
import json
import logging
import traceback
from typing import Any, Tuple, Dict, Optional

import ray
from ray._private import ray_constants, gcs_utils
from ray.core.generated import gcs_service_pb2
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.job.common import (
    JobStatus,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobLogsResponse,
    JobDriverLocationResponse,
    validate_request_type,
    JOB_ID_METADATA_KEY,
)
from ray.dashboard.modules.job.pydantic_models import (
    DriverInfo,
    JobDetails,
    JobType,
)
from ray.dashboard.modules.job.job_manager import JobManager
from ray.runtime_env import RuntimeEnv


routes = optional_utils.ClassMethodRouteTable
logger = logging.getLogger(__name__)


class JobAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_manager = None

    async def _parse_and_validate_request(
        self, req: Request, request_type: dataclasses.dataclass
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

    @routes.post("/api/job_agent/jobs/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def submit_job(self, req: Request) -> Response:
        result = await self._parse_and_validate_request(req, JobSubmitRequest)
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

    @routes.get("/api/job_agent/jobs/{job_or_submission_id}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_job_logs(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]

        resp = JobLogsResponse(logs=self._job_manager.get_job_logs(job_or_submission_id))
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.post("/api/job_agent/jobs/{job_or_submission_id}/stop")
    @optional_utils.init_ray_and_catch_exceptions()
    async def stop_job(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]

        try:
            stopped = self._job_manager.stop_job(job_or_submission_id)
            resp = JobStopResponse(stopped=stopped)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )

        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    @routes.get("/api/job_agent/jobs/{job_or_submission_id}/driver_location")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_driver_location(self, req: Request) -> Response:
        job_or_submission_id = req.match_info["job_or_submission_id"]

        try:
            # the address of supervisor actor is same as driver.
            supervisor_actor = self._job_manager._get_actor_for_job(job_or_submission_id)
            actor_info = gcs_utils.ActorTableData.FromString(
                ray._private.state.state.global_state_accessor.get_actor_info(
                    supervisor_actor._actor_id
                )
            )
            resp = JobDriverLocationResponse(ip_address=actor_info.address.ip_address)
        except Exception:
            return Response(
                text=traceback.format_exc(),
                status=aiohttp.web.HTTPInternalServerError.status_code,
            )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)), content_type="application/json"
        )

    async def run(self, server):
        if not self._job_manager:
            self._job_manager = JobManager(self._dashboard_agent.gcs_aio_client)

    @staticmethod
    def is_minimal_module():
        return False
