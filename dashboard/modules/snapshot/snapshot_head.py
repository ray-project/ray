import asyncio
import concurrent.futures
from typing import Any, Dict, List, Optional
import hashlib

import ray
from ray import ray_constants
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
    _internal_kv_get,
    _internal_kv_list,
)
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._private.runtime_env.validation import ParsedRuntimeEnv
from ray.dashboard.modules.job.common import (
    JobStatusInfo,
    JobStatusStorageClient,
    JOB_ID_METADATA_KEY,
)

import json
import aiohttp.web

routes = dashboard_optional_utils.ClassMethodRouteTable


class APIHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._gcs_job_info_stub = None
        self._gcs_actor_info_stub = None
        self._dashboard_head = dashboard_head
        assert _internal_kv_initialized()
        self._job_status_client = JobStatusStorageClient()
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
        await self._gcs_actor_info_stub.KillActorViaGcs(request, timeout=5)

        message = (
            f"Force killed actor with id {actor_id}"
            if force_kill
            else f"Requested actor with id {actor_id} to terminate. "
            + "It will exit once running tasks complete"
        )

        return dashboard_optional_utils.rest_response(success=True, message=message)

    @routes.get("/api/snapshot")
    async def snapshot(self, req):
        job_data, actor_data, serve_data, session_name = await asyncio.gather(
            self.get_job_info(),
            self.get_actor_info(),
            self.get_serve_info(),
            self.get_session_name(),
        )
        snapshot = {
            "jobs": job_data,
            "actors": actor_data,
            "deployments": serve_data,
            "session_name": session_name,
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__,
        }
        return dashboard_optional_utils.rest_response(
            success=True, message="hello", snapshot=snapshot
        )

    def _get_job_status(self, metadata: Dict[str, str]) -> Optional[JobStatusInfo]:
        # If a job submission ID has been added to a job, the status is
        # guaranteed to be returned.
        job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
        return self._job_status_client.get_status(job_submission_id)

    async def get_job_info(self):
        request = gcs_service_pb2.GetAllJobInfoRequest()
        reply = await self._gcs_job_info_stub.GetAllJobInfo(request, timeout=5)

        jobs = {}
        for job_table_entry in reply.job_info_list:
            job_id = job_table_entry.job_id.hex()
            metadata = dict(job_table_entry.config.metadata)
            config = {
                "namespace": job_table_entry.config.ray_namespace,
                "metadata": metadata,
                "runtime_env": ParsedRuntimeEnv.deserialize(
                    job_table_entry.config.runtime_env_info.serialized_runtime_env
                ),
            }
            status = self._get_job_status(metadata)
            entry = {
                "status": None if status is None else status.status,
                "status_message": None if status is None else status.message,
                "is_dead": job_table_entry.is_dead,
                "start_time": job_table_entry.start_time,
                "end_time": job_table_entry.end_time,
                "config": config,
            }
            jobs[job_id] = entry

        return jobs

    async def get_actor_info(self):
        # TODO (Alex): GCS still needs to return actors from dead jobs.
        request = gcs_service_pb2.GetAllActorInfoRequest()
        request.show_dead_jobs = True
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(request, timeout=5)
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
                "resources": dict(actor_table_entry.task_spec.required_resources),
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

    async def get_serve_info(self) -> Dict[str, Any]:
        # Conditionally import serve to prevent ModuleNotFoundError from serve
        # dependencies when only ray[default] is installed (#17712)
        try:
            from ray.serve.controller import SNAPSHOT_KEY as SERVE_SNAPSHOT_KEY
            from ray.serve.constants import SERVE_CONTROLLER_NAME
        except Exception:
            return {}

        # Serve wraps Ray's internal KV store and specially formats the keys.
        # These are the keys we are interested in:
        # SERVE_CONTROLLER_NAME(+ optional random letters):SERVE_SNAPSHOT_KEY
        # TODO: Convert to async GRPC, if CPU usage is not a concern.
        def get_deployments():
            serve_keys = _internal_kv_list(
                SERVE_CONTROLLER_NAME, namespace=ray_constants.KV_NAMESPACE_SERVE
            )
            serve_snapshot_keys = filter(
                lambda k: SERVE_SNAPSHOT_KEY in str(k), serve_keys
            )

            deployments_per_controller: List[Dict[str, Any]] = []
            for key in serve_snapshot_keys:
                val_bytes = _internal_kv_get(
                    key, namespace=ray_constants.KV_NAMESPACE_SERVE
                ) or "{}".encode("utf-8")
                deployments_per_controller.append(json.loads(val_bytes.decode("utf-8")))
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

        return await asyncio.get_event_loop().run_in_executor(
            executor=self._thread_pool, func=get_deployments
        )

    async def get_session_name(self):
        # TODO(yic): Convert to async GRPC.
        def get_session():
            return ray.experimental.internal_kv._internal_kv_get(
                "session_name", namespace=ray_constants.KV_NAMESPACE_SESSION
            ).decode()

        return await asyncio.get_event_loop().run_in_executor(
            executor=self._thread_pool, func=get_session
        )

    async def run(self, server):
        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )
        self._gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel
        )

    @staticmethod
    def is_minimal_module():
        return False
