import asyncio
import concurrent.futures
from datetime import datetime
import enum
import logging
import hashlib
import json
import os
from typing import Any, Dict, List, Optional

import aiohttp.web
from pydantic import BaseModel, Extra, Field, validator

import ray
from ray.dashboard.consts import RAY_CLUSTER_ACTIVITY_HOOK
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray._private.storage import _load_class
from ray.core.generated import gcs_pb2, gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.job.common import JOB_ID_METADATA_KEY, JobInfoStorageClient

from ray.job_submission import JobInfo
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.ClassMethodRouteTable

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
        self._gcs_job_info_stub = None
        self._gcs_actor_info_stub = None
        self._dashboard_head = dashboard_head
        self._gcs_aio_client = dashboard_head.gcs_aio_client
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

        request = gcs_service_pb2.KillActorViaGcsRequest()
        request.actor_id = bytes.fromhex(actor_id)
        request.force_kill = force_kill
        request.no_restart = no_restart
        await self._gcs_actor_info_stub.KillActorViaGcs(
            request, timeout=SNAPSHOT_API_TIMEOUT_SECONDS
        )

        message = (
            f"Force killed actor with id {actor_id}"
            if force_kill
            else f"Requested actor with id {actor_id} to terminate. "
            + "It will exit once running tasks complete"
        )

        return dashboard_optional_utils.rest_response(success=True, message=message)

    @routes.get("/api/snapshot")
    async def snapshot(self, req):
        timeout = req.query.get("timeout", None)
        if timeout and timeout.isdigit():
            timeout = int(timeout)
        else:
            timeout = SNAPSHOT_API_TIMEOUT_SECONDS

        actor_limit = int(req.query.get("actor_limit", "1000"))
        (job_info, job_submission_data, actor_data, serve_data,) = await asyncio.gather(
            self.get_job_info(timeout),
            self.get_job_submission_info(timeout),
            self.get_actor_info(actor_limit, timeout),
            self.get_serve_info(timeout),
        )
        snapshot = {
            "jobs": job_info,
            "job_submission": job_submission_data,
            "actors": actor_data,
            "deployments": serve_data,
            "session_name": self._dashboard_head.session_name,
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__,
        }
        return dashboard_optional_utils.rest_response(
            success=True, message="hello", snapshot=snapshot
        )

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
            request = gcs_service_pb2.GetAllJobInfoRequest()
            reply = await self._gcs_job_info_stub.GetAllJobInfo(
                request, timeout=timeout
            )

            num_active_drivers = 0
            latest_job_end_time = 0
            for job_table_entry in reply.job_info_list:
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

    async def _get_job_info(self, metadata: Dict[str, str]) -> Optional[JobInfo]:
        # If a job submission ID has been added to a job, the status is
        # guaranteed to be returned.
        job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
        return await self._job_info_client.get_info(job_submission_id)

    async def get_job_info(self, timeout: int = SNAPSHOT_API_TIMEOUT_SECONDS):
        """Return info for each job.  Here a job is a Ray driver."""
        request = gcs_service_pb2.GetAllJobInfoRequest()
        reply = await self._gcs_job_info_stub.GetAllJobInfo(request, timeout=timeout)

        jobs = {}
        for job_table_entry in reply.job_info_list:
            job_id = job_table_entry.job_id.hex()
            metadata = dict(job_table_entry.config.metadata)
            config = {
                "namespace": job_table_entry.config.ray_namespace,
                "metadata": metadata,
                "runtime_env": RuntimeEnv.deserialize(
                    job_table_entry.config.runtime_env_info.serialized_runtime_env
                ),
            }
            info = await self._get_job_info(metadata)
            entry = {
                "status": None if info is None else info.status,
                "status_message": None if info is None else info.message,
                "is_dead": job_table_entry.is_dead,
                "start_time": job_table_entry.start_time,
                "end_time": job_table_entry.end_time,
                "config": config,
            }
            jobs[job_id] = entry

        return jobs

    async def get_job_submission_info(
        self, timeout: int = SNAPSHOT_API_TIMEOUT_SECONDS
    ):
        """Info for Ray job submission.  Here a job can have 0 or many drivers."""

        jobs = {}
        fetched_jobs = await self._job_info_client.get_all_jobs(timeout)
        for (
            job_submission_id,
            job_info,
        ) in fetched_jobs.items():
            if job_info is not None:
                entry = {
                    "job_submission_id": job_submission_id,
                    "status": job_info.status,
                    "message": job_info.message,
                    "error_type": job_info.error_type,
                    "start_time": job_info.start_time,
                    "end_time": job_info.end_time,
                    "metadata": job_info.metadata,
                    "runtime_env": job_info.runtime_env,
                    "entrypoint": job_info.entrypoint,
                }
                jobs[job_submission_id] = entry
        return jobs

    async def get_actor_info(
        self, limit: int = 1000, timeout: int = SNAPSHOT_API_TIMEOUT_SECONDS
    ):
        # TODO (Alex): GCS still needs to return actors from dead jobs.
        request = gcs_service_pb2.GetAllActorInfoRequest()
        request.show_dead_jobs = True
        request.limit = limit
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(
            request, timeout=timeout
        )
        actors = {}
        for actor_table_entry in reply.actor_table_data:
            actor_id = actor_table_entry.actor_id.hex()
            runtime_env = json.loads(actor_table_entry.serialized_runtime_env)
            entry = {
                "job_id": actor_table_entry.job_id.hex(),
                "state": gcs_pb2.ActorTableData.ActorState.Name(
                    actor_table_entry.state
                ),
                "name": actor_table_entry.name,
                "namespace": actor_table_entry.ray_namespace,
                "runtime_env": runtime_env,
                "start_time": actor_table_entry.start_time,
                "end_time": actor_table_entry.end_time,
                "is_detached": actor_table_entry.is_detached,
                "resources": dict(actor_table_entry.required_resources),
                "actor_class": actor_table_entry.class_name,
                "current_worker_id": actor_table_entry.address.worker_id.hex(),
                "current_raylet_id": actor_table_entry.address.raylet_id.hex(),
                "ip_address": actor_table_entry.address.ip_address,
                "port": actor_table_entry.address.port,
                "metadata": dict(),
            }
            actors[actor_id] = entry

            deployments = await self.get_serve_info()
            for _, deployment_info in deployments.items():
                for replica_actor_id, actor_info in deployment_info["actors"].items():
                    if replica_actor_id in actors:
                        serve_metadata = dict()
                        serve_metadata["replica_tag"] = actor_info["replica_tag"]
                        serve_metadata["deployment_name"] = deployment_info["name"]
                        serve_metadata["version"] = actor_info["version"]
                        actors[replica_actor_id]["metadata"]["serve"] = serve_metadata
        return actors

    async def get_serve_info(
        self, timeout: int = SNAPSHOT_API_TIMEOUT_SECONDS
    ) -> Dict[str, Any]:
        # Conditionally import serve to prevent ModuleNotFoundError from serve
        # dependencies when only ray[default] is installed (#17712)
        try:
            from ray.serve._private.constants import SERVE_CONTROLLER_NAME
            from ray.serve.controller import SNAPSHOT_KEY as SERVE_SNAPSHOT_KEY
        except Exception:
            return {}

        # Serve wraps Ray's internal KV store and specially formats the keys.
        # These are the keys we are interested in:
        # SERVE_CONTROLLER_NAME(+ optional random letters):SERVE_SNAPSHOT_KEY
        serve_keys = await self._gcs_aio_client.internal_kv_keys(
            SERVE_CONTROLLER_NAME.encode(),
            namespace=ray_constants.KV_NAMESPACE_SERVE,
            timeout=timeout,
        )

        tasks = [
            self._gcs_aio_client.internal_kv_get(
                key,
                namespace=ray_constants.KV_NAMESPACE_SERVE,
                timeout=timeout,
            )
            for key in serve_keys
            if SERVE_SNAPSHOT_KEY in key.decode()
        ]

        serve_snapshot_vals = await asyncio.gather(*tasks)

        deployments_per_controller: List[Dict[str, Any]] = [
            json.loads(val.decode()) for val in serve_snapshot_vals
        ]

        # Merge the deployments dicts of all controllers.
        deployments: Dict[str, Any] = {
            k: v for d in deployments_per_controller for k, v in d.items()
        }
        # Replace the keys (deployment names) with their hashes to prevent
        # collisions caused by the automatic conversion to camelcase by the
        # dashboard agent.
        return {
            hashlib.sha1(name.encode()).hexdigest(): info
            for name, info in deployments.items()
        }

    async def run(self, server):
        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )
        self._gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )
        # Lazily constructed because dashboard_head's gcs_aio_client
        # is lazily constructed
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
            )

    @staticmethod
    def is_minimal_module():
        return False
