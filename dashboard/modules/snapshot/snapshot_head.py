from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_pb2
from ray.core.generated import gcs_service_pb2_grpc

import ray.new_dashboard.utils as dashboard_utils

import json

routes = dashboard_utils.ClassMethodRouteTable


class SnapshotHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._gcs_job_info_stub = None
        self._gcs_actor_info_stub = None
        self._dashboard_head = dashboard_head

    @routes.get("/api/snapshot")
    async def snapshot(self, req):
        job_data = await self.get_job_info()
        actor_data = await self.get_actor_info()
        session_name = await self.get_session_name()
        snapshot = {
            "jobs": job_data,
            "actors": actor_data,
            "session_name": session_name,
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
            }
            actors[actor_id] = entry
        return actors

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
