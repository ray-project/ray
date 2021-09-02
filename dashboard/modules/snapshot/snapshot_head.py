from typing import Dict, Any, List
import hashlib

import ray
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.experimental.internal_kv import (_initialize_internal_kv,
                                          _internal_kv_initialized,
                                          _internal_kv_get, _internal_kv_list)

import ray.new_dashboard.utils as dashboard_utils

import json

routes = dashboard_utils.ClassMethodRouteTable


class SnapshotHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._gcs_job_info_stub = None
        self._gcs_actor_info_stub = None
        self._dashboard_head = dashboard_head

        # Initialize internal KV to be used by the working_dir setup code.
        _initialize_internal_kv(dashboard_head.gcs_client)
        assert _internal_kv_initialized()

    @routes.get("/api/snapshot")
    async def snapshot(self, req):
        job_data = await self.get_job_info()
        actor_data = await self.get_actor_info()
        serve_data = await self.get_serve_info()
        session_name = await self.get_session_name()
        snapshot = {
            "jobs": job_data,
            "actors": actor_data,
            "deployments": serve_data,
            "session_name": session_name,
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__
        }
        return dashboard_utils.rest_response(
            success=True, message="hello", snapshot=snapshot)

    async def get_job_info(self):
        request = gcs_service_pb2.GetAllJobInfoRequest()
        reply = await self._gcs_job_info_stub.GetAllJobInfo(request, timeout=5)

        jobs = {}
        for job_table_entry in reply.job_info_list:
            job_id = job_table_entry.job_id.hex()
            config = {
                "env_vars": dict(job_table_entry.config.worker_env),
                "namespace": job_table_entry.config.ray_namespace,
                "metadata": dict(job_table_entry.config.metadata),
                "runtime_env": json.loads(
                    job_table_entry.config.serialized_runtime_env),
            }
            entry = {
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
        reply = await self._gcs_actor_info_stub.GetAllActorInfo(
            request, timeout=5)
        actors = {}
        for actor_table_entry in reply.actor_table_data:
            actor_id = actor_table_entry.actor_id.hex()
            runtime_env = json.loads(actor_table_entry.serialized_runtime_env)
            entry = {
                "job_id": actor_table_entry.job_id.hex(),
                "state": gcs_pb2.ActorTableData.ActorState.Name(
                    actor_table_entry.state),
                "name": actor_table_entry.name,
                "namespace": actor_table_entry.ray_namespace,
                "runtime_env": runtime_env,
                "start_time": actor_table_entry.start_time,
                "end_time": actor_table_entry.end_time,
                "is_detached": actor_table_entry.is_detached,
                "resources": dict(
                    actor_table_entry.task_spec.required_resources),
                "actor_class": actor_table_entry.class_name,
                "current_worker_id": actor_table_entry.address.worker_id.hex(),
                "current_raylet_id": actor_table_entry.address.raylet_id.hex(),
                "ip_address": actor_table_entry.address.ip_address,
                "port": actor_table_entry.address.port,
                "metadata": dict()
            }
            actors[actor_id] = entry

            deployments = await self.get_serve_info()
            for _, deployment_info in deployments.items():
                for replica_actor_id, actor_info in deployment_info[
                        "actors"].items():
                    if replica_actor_id in actors:
                        serve_metadata = dict()
                        serve_metadata["replica_tag"] = actor_info[
                            "replica_tag"]
                        serve_metadata["deployment_name"] = deployment_info[
                            "name"]
                        serve_metadata["version"] = actor_info["version"]
                        actors[replica_actor_id]["metadata"][
                            "serve"] = serve_metadata
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

        serve_keys = _internal_kv_list(SERVE_CONTROLLER_NAME)
        serve_snapshot_keys = filter(lambda k: SERVE_SNAPSHOT_KEY in str(k),
                                     serve_keys)

        deployments_per_controller: List[Dict[str, Any]] = []
        for key in serve_snapshot_keys:
            val_bytes = _internal_kv_get(key) or "{}".encode("utf-8")
            deployments_per_controller.append(
                json.loads(val_bytes.decode("utf-8")))
        # Merge the deployments dicts of all controllers.
        deployments: Dict[str, Any] = {
            k: v
            for d in deployments_per_controller for k, v in d.items()
        }
        # Replace the keys (deployment names) with their hashes to prevent
        # collisions caused by the automatic conversion to camelcase by the
        # dashboard agent.
        deployments = {
            hashlib.sha1(name.encode()).hexdigest(): info
            for name, info in deployments.items()
        }
        return deployments

    async def get_session_name(self):
        encoded_name = await self._dashboard_head.aioredis_client.get(
            "session_name")
        return encoded_name.decode()

    async def run(self, server):
        self._gcs_job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
            self._dashboard_head.aiogrpc_gcs_channel)
        self._gcs_actor_info_stub = \
            gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
                self._dashboard_head.aiogrpc_gcs_channel)
