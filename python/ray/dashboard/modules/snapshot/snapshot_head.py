import concurrent.futures
import enum
import json
import logging
import os
from datetime import datetime
from typing import Optional

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray import ActorID
from ray._private.gcs_aio_client import GcsAioClient
from ray._private.pydantic_compat import BaseModel, Extra, Field, validator
from ray._private.storage import _load_class
from ray.dashboard.consts import RAY_CLUSTER_ACTIVITY_HOOK
from ray.dashboard.modules.job.common import JobInfoStorageClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.DashboardHeadRouteTable

SNAPSHOT_API_TIMEOUT_SECONDS = 30


class RayActivityStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"


class RayActivityResponse(BaseModel, extra=Extra.allow):
    """
    Pydantic model used to inform if a particular Ray component can be considered
    active, and metadata about observation.
    """

    is_active: RayActivityStatus = Field(
        ...,
        description=(
            "Whether the corresponding Ray component is considered active or inactive, "
            "or if there was an error while collecting this observation."
        ),
    )
    reason: Optional[str] = Field(
        None, description="Reason if Ray component is considered active or errored."
    )
    timestamp: float = Field(
        ...,
        description=(
            "Timestamp of when this observation about the Ray component was made. "
            "This is in the format of seconds since unix epoch."
        ),
    )
    last_activity_at: Optional[float] = Field(
        None,
        description=(
            "Timestamp when last actvity of this Ray component finished in format of "
            "seconds since unix epoch. This field does not need to be populated "
            "for Ray components where it is not meaningful."
        ),
    )

    @validator("reason", always=True)
    def reason_required(cls, v, values, **kwargs):
        if "is_active" in values and values["is_active"] != RayActivityStatus.INACTIVE:
            if v is None:
                raise ValueError(
                    'Reason is required if is_active is "active" or "error"'
                )
        return v


class APIHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._gcs_aio_client: GcsAioClient = dashboard_head.gcs_aio_client
        self._job_info_client = None
        # For offloading CPU intensive work.
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="api_head"
        )

    @routes.get("/api/actors/kill")
    async def kill_actor_gcs(self, req) -> aiohttp.web.Response:
        actor_id = req.query.get("actor_id")
        force_kill = req.query.get("force_kill", False) in ("true", "True")
        no_restart = req.query.get("no_restart", False) in ("true", "True")
        if not actor_id:
            return dashboard_optional_utils.rest_response(
                success=False, message="actor_id is required."
            )

        await self._gcs_aio_client.kill_actor(
            ActorID.from_hex(actor_id),
            force_kill,
            no_restart,
            timeout=SNAPSHOT_API_TIMEOUT_SECONDS,
        )

        message = (
            f"Force killed actor with id {actor_id}"
            if force_kill
            else f"Requested actor with id {actor_id} to terminate. "
            + "It will exit once running tasks complete"
        )

        return dashboard_optional_utils.rest_response(success=True, message=message)

    @routes.get("/api/component_activities")
    async def get_component_activities(self, req) -> aiohttp.web.Response:
        timeout = req.query.get("timeout", None)
        if timeout and timeout.isdigit():
            timeout = int(timeout)
        else:
            timeout = SNAPSHOT_API_TIMEOUT_SECONDS

        # Get activity information for driver
        driver_activity_info = await self._get_job_activity_info(timeout=timeout)
        resp = {"driver": dict(driver_activity_info)}

        if RAY_CLUSTER_ACTIVITY_HOOK in os.environ:
            try:
                cluster_activity_callable = _load_class(
                    os.environ[RAY_CLUSTER_ACTIVITY_HOOK]
                )
                external_activity_output = cluster_activity_callable()
                assert isinstance(external_activity_output, dict), (
                    f"Output of hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]} "
                    "should be Dict[str, RayActivityResponse]. Got "
                    f"output: {external_activity_output}"
                )
                for component_type in external_activity_output:
                    try:
                        component_activity_output = external_activity_output[
                            component_type
                        ]
                        # Parse and validate output to type RayActivityResponse
                        component_activity_output = RayActivityResponse(
                            **dict(component_activity_output)
                        )
                        resp[component_type] = dict(component_activity_output)
                    except Exception as e:
                        logger.exception(
                            f"Failed to get activity status of {component_type} "
                            f"from user hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]}."
                        )
                        resp[component_type] = {
                            "is_active": RayActivityStatus.ERROR,
                            "reason": repr(e),
                            "timestamp": datetime.now().timestamp(),
                        }
            except Exception as e:
                logger.exception(
                    "Failed to get activity status from user "
                    f"hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]}."
                )
                resp["external_component"] = {
                    "is_active": RayActivityStatus.ERROR,
                    "reason": repr(e),
                    "timestamp": datetime.now().timestamp(),
                }

        return aiohttp.web.Response(
            text=json.dumps(resp),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    async def _get_job_activity_info(self, timeout: int) -> RayActivityResponse:
        # Returns if there is Ray activity from drivers (job).
        # Drivers in namespaces that start with _ray_internal_ are not
        # considered activity.
        # This includes the _ray_internal_dashboard job that gets automatically
        # created with every cluster
        try:
            reply = await self._gcs_aio_client.get_all_job_info(
                skip_submission_job_info_field=True,
                skip_is_running_tasks_field=True,
                timeout=timeout,
            )

            num_active_drivers = 0
            latest_job_end_time = 0
            for job_table_entry in reply.values():
                is_dead = bool(job_table_entry.is_dead)
                in_internal_namespace = job_table_entry.config.ray_namespace.startswith(
                    "_ray_internal_"
                )
                latest_job_end_time = (
                    max(latest_job_end_time, job_table_entry.end_time)
                    if job_table_entry.end_time
                    else latest_job_end_time
                )
                if not is_dead and not in_internal_namespace:
                    num_active_drivers += 1

            current_timestamp = datetime.now().timestamp()
            # Latest job end time must be before or equal to the current timestamp.
            # Job end times may be provided in epoch milliseconds. Check if this
            # is true, and convert to seconds
            if latest_job_end_time > current_timestamp:
                latest_job_end_time = latest_job_end_time / 1000
                assert current_timestamp >= latest_job_end_time, (
                    f"Most recent job end time {latest_job_end_time} must be "
                    f"before or equal to the current timestamp {current_timestamp}"
                )

            is_active = (
                RayActivityStatus.ACTIVE
                if num_active_drivers > 0
                else RayActivityStatus.INACTIVE
            )
            return RayActivityResponse(
                is_active=is_active,
                reason=f"Number of active drivers: {num_active_drivers}"
                if num_active_drivers
                else None,
                timestamp=current_timestamp,
                # If latest_job_end_time == 0, no jobs have finished yet so don't
                # populate last_activity_at
                last_activity_at=latest_job_end_time if latest_job_end_time else None,
            )
        except Exception as e:
            logger.exception("Failed to get activity status of Ray drivers.")
            return RayActivityResponse(
                is_active=RayActivityStatus.ERROR,
                reason=repr(e),
                timestamp=datetime.now().timestamp(),
            )

    async def run(self, server):
        # Lazily constructed because dashboard_head's gcs_aio_client
        # is lazily constructed
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
            )

    @staticmethod
    def is_minimal_module():
        return False
