import asyncio
from collections import deque
import logging
import os

import aiohttp.web

import ray._private.ray_constants as ray_constants
import ray._private.utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioActorSubscriber
from ray.core.generated import (
    core_worker_pb2,
    core_worker_pb2_grpc,
    gcs_service_pb2,
    gcs_service_pb2_grpc,
    node_manager_pb2_grpc,
)
from ray.dashboard.datacenter import DataOrganizer, DataSource
from ray.dashboard.modules.actor import actor_consts, actor_utils
from ray.dashboard.modules.actor.actor_utils import actor_classname_from_func_descriptor
from ray.dashboard.optional_utils import rest_response

try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable

MAX_ACTORS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_ACTORS_TO_CACHE", 1000))
ACTOR_CLEANUP_FREQUENCY = 10  # seconds


def actor_table_data_to_dict(message):
    orig_message = dashboard_utils.message_to_dict(
        message,
        {
            "actorId",
            "parentId",
            "jobId",
            "workerId",
            "rayletId",
            "actorCreationDummyObjectId",
            "callerId",
            "taskId",
            "parentTaskId",
            "sourceActorId",
            "placementGroupId",
        },
        including_default_value_fields=True,
    )
    # The complete schema for actor table is here:
    #     src/ray/protobuf/gcs.proto
    # It is super big and for dashboard, we don't need that much information.
    # Only preserve the necessary ones here for memory usage.
    fields = {
        "actorId",
        "jobId",
        "pid",
        "address",
        "state",
        "name",
        "numRestarts",
        "functionDescriptor",
        "timestamp",
        "numExecutedTasks",
    }
    light_message = {k: v for (k, v) in orig_message.items() if k in fields}
    if "functionDescriptor" in light_message:
        actor_class = actor_classname_from_func_descriptor(
            light_message["functionDescriptor"]
        )
        light_message["actorClass"] = actor_class
    return light_message


class ActorHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # ActorInfoGcsService
        self._gcs_actor_info_stub = None
        # A queue of dead actors in order of when they died
        self.dead_actors_queue = deque()
        DataSource.nodes.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._stubs.pop(node_id)
        if change.new:
            # TODO(fyrestone): Handle exceptions.
            node_id, node_info = change.new
            address = "{}:{}".format(
                node_info["nodeManagerAddress"], int(node_info["nodeManagerPort"])
            )
            options = ray_constants.GLOBAL_GRPC_OPTIONS
            channel = ray._private.utils.init_grpc_channel(
                address, options, asynchronous=True
            )
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[node_id] = stub

    async def _update_actors(self):
        # Get all actor info.
        while True:
            try:
                logger.info("Getting all actor info from GCS.")
                request = gcs_service_pb2.GetAllActorInfoRequest()
                reply = await self._gcs_actor_info_stub.GetAllActorInfo(
                    request, timeout=5
                )
                if reply.status.code == 0:
                    actors = {}
                    for message in reply.actor_table_data:
                        actor_table_data = actor_table_data_to_dict(message)
                        actors[actor_table_data["actorId"]] = actor_table_data
                    # Update actors.
                    DataSource.actors.reset(actors)
                    # Update node actors and job actors.
                    job_actors = {}
                    node_actors = {}
                    for actor_id, actor_table_data in actors.items():
                        job_id = actor_table_data["jobId"]
                        node_id = actor_table_data["address"]["rayletId"]
                        job_actors.setdefault(job_id, {})[actor_id] = actor_table_data
                        # Update only when node_id is not Nil.
                        if node_id != actor_consts.NIL_NODE_ID:
                            node_actors.setdefault(node_id, {})[
                                actor_id
                            ] = actor_table_data
                    DataSource.job_actors.reset(job_actors)
                    DataSource.node_actors.reset(node_actors)
                    logger.info("Received %d actor info from GCS.", len(actors))
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllActorInfo: {reply.status.message}"
                    )
            except Exception:
                logger.exception("Error Getting all actor info from GCS.")
                await asyncio.sleep(
                    actor_consts.RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS
                )

        state_keys = ("state", "address", "numRestarts", "timestamp", "pid")

        def process_actor_data_from_pubsub(actor_id, actor_table_data):
            actor_table_data = actor_table_data_to_dict(actor_table_data)
            # If actor is not new registered but updated, we only update
            # states related fields.
            if actor_table_data["state"] != "DEPENDENCIES_UNREADY":
                actor_table_data_copy = dict(DataSource.actors[actor_id])
                for k in state_keys:
                    actor_table_data_copy[k] = actor_table_data[k]
                actor_table_data = actor_table_data_copy
            actor_id = actor_table_data["actorId"]
            job_id = actor_table_data["jobId"]
            node_id = actor_table_data["address"]["rayletId"]
            if actor_table_data["state"] == "DEAD":
                self.dead_actors_queue.append(actor_id)
            # Update actors.
            DataSource.actors[actor_id] = actor_table_data
            # Update node actors (only when node_id is not Nil).
            if node_id != actor_consts.NIL_NODE_ID:
                node_actors = dict(DataSource.node_actors.get(node_id, {}))
                node_actors[actor_id] = actor_table_data
                DataSource.node_actors[node_id] = node_actors
            # Update job actors.
            job_actors = dict(DataSource.job_actors.get(job_id, {}))
            job_actors[actor_id] = actor_table_data
            DataSource.job_actors[job_id] = job_actors

        # Receive actors from channel.
        gcs_addr = self._dashboard_head.gcs_address
        subscriber = GcsAioActorSubscriber(address=gcs_addr)
        await subscriber.subscribe()

        while True:
            try:
                actor_id, actor_table_data = await subscriber.poll()
                if actor_id is not None:
                    # Convert to lower case hex ID.
                    actor_id = actor_id.hex()
                    process_actor_data_from_pubsub(actor_id, actor_table_data)
            except Exception:
                logger.exception("Error processing actor info from GCS.")

    async def _cleanup_actors(self):
        while True:
            try:
                if len(DataSource.actors) > MAX_ACTORS_TO_CACHE:
                    logger.debug("Cleaning up dead actors from GCS")
                    while len(DataSource.actors) > MAX_ACTORS_TO_CACHE:
                        if not self.dead_actors_queue:
                            logger.warning(
                                f"More than {MAX_ACTORS_TO_CACHE} "
                                "live actors are cached"
                            )
                            break
                        actor_id = self.dead_actors_queue.popleft()
                        if actor_id in DataSource.actors:
                            actor = DataSource.actors.pop(actor_id)
                            job_id = actor["jobId"]
                            del DataSource.job_actors[job_id][actor_id]
                            node_id = actor["address"].get("rayletId")
                            if node_id:
                                del DataSource.node_actors[node_id][actor_id]
                await asyncio.sleep(ACTOR_CLEANUP_FREQUENCY)
            except Exception:
                logger.exception("Error cleaning up actor info from GCS.")

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
            success=True, message="Fetched actor groups.", actor_groups=actor_groups
        )

    @routes.get("/logical/actors")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        return rest_response(
            success=True, message="All actors fetched.", actors=DataSource.actors
        )

    @routes.get("/logical/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        try:
            actor_id = req.query["actorId"]
            ip_address = req.query["ipAddress"]
            port = req.query["port"]
        except KeyError:
            return rest_response(success=False, message="Bad Request")
        try:
            options = ray_constants.GLOBAL_GRPC_OPTIONS
            channel = ray._private.utils.init_grpc_channel(
                f"{ip_address}:{port}", options=options, asynchronous=True
            )
            stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

            await stub.KillActor(
                core_worker_pb2.KillActorRequest(
                    intended_actor_id=ray._private.utils.hex_to_binary(actor_id)
                )
            )

        except aiogrpc.AioRpcError:
            # This always throws an exception because the worker
            # is killed and the channel is closed on the worker side
            # before this handler, however it deletes the actor correctly.
            pass

        return rest_response(success=True, message=f"Killed actor with id {actor_id}")

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            gcs_channel
        )

        asyncio.get_event_loop().create_task(self._cleanup_actors())
        await asyncio.gather(self._update_actors())

    @staticmethod
    def is_minimal_module():
        return False
