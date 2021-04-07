import logging
import aiohttp.web
import ray._private.utils
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.actor_utils as actor_utils
from ray.new_dashboard.utils import rest_response
from ray.new_dashboard.datacenter import DataOrganizer, DataSource
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc

from grpc.experimental import aio as aiogrpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class LogicalViewHead(dashboard_utils.DashboardHeadModule):
    @routes.get("/logical/actor_groups")
    async def get_actor_groups(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_all_actors()
        actor_creation_tasks = await DataOrganizer.get_actor_creation_tasks()
        # actor_creation_tasks have some common interface with actors,
        # and they get processed and shown in tandem in the logical view
        # hence we merge them together before constructing actor groups.
        actors.update(actor_creation_tasks)
        actor_groups = actor_utils.construct_actor_groups(actors)
        return rest_response(
            success=True,
            message="Fetched actor groups.",
            actor_groups=actor_groups)

    @routes.get("/logical/actors")
    @dashboard_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="All actors fetched.",
            actors=DataSource.actors)

    @routes.get("/logical/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        try:
            actor_id = req.query["actorId"]
            ip_address = req.query["ipAddress"]
            port = req.query["port"]
        except KeyError:
            return rest_response(success=False, message="Bad Request")
        try:
            options = (("grpc.enable_http_proxy", 0), )
            channel = aiogrpc.insecure_channel(
                f"{ip_address}:{port}", options=options)
            stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

            await stub.KillActor(
                core_worker_pb2.KillActorRequest(
                    intended_actor_id=ray._private.utils.hex_to_binary(
                        actor_id)))

        except aiogrpc.AioRpcError:
            # This always throws an exception because the worker
            # is killed and the channel is closed on the worker side
            # before this handler, however it deletes the actor correctly.
            pass

        return rest_response(
            success=True, message=f"Killed actor with id {actor_id}")

    async def run(self, server):
        pass
