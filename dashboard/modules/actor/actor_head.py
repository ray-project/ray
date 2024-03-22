import asyncio
import logging
import os
import time

from collections import deque

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioActorSubscriber
from ray.core.generated import (
    gcs_service_pb2,
    gcs_service_pb2_grpc,
)
from ray.dashboard.datacenter import DataSource, DataOrganizer
from ray.dashboard.modules.actor import actor_consts
from ray.dashboard.modules.job.history_server_storage import (
    append_actor_events,
    generate_logagent_url,
)
import ray.dashboard.consts as dashboard_consts

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

MAX_ACTORS_TO_CACHE = int(os.environ.get("RAY_DASHBOARD_MAX_ACTORS_TO_CACHE", 50000))
ACTOR_CLEANUP_FREQUENCY = 1  # seconds


def actor_table_data_to_dict(message):
    orig_message = dashboard_utils.message_to_dict(
        message,
        {
            "actorId",
            "parentId",
            "jobId",
            "workerId",
            "rayletId",
            "callerId",
            "taskId",
            "parentTaskId",
            "sourceActorId",
        },
        always_print_fields_with_no_presence=True,
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
        "timestamp",
        "className",
        "startTime",
        "endTime",
        "reprName",
    }
    light_message = {k: v for (k, v) in orig_message.items() if k in fields}
    light_message["actorClass"] = orig_message["className"]
    exit_detail = "-"
    if "deathCause" in orig_message:
        context = orig_message["deathCause"]
        if "actorDiedErrorContext" in context:
            exit_detail = context["actorDiedErrorContext"]["errorMessage"]  # noqa
        elif "runtimeEnvFailedContext" in context:
            exit_detail = context["runtimeEnvFailedContext"]["errorMessage"]  # noqa
        elif "actorUnschedulableContext" in context:
            exit_detail = context["actorUnschedulableContext"]["errorMessage"]  # noqa
        elif "creationTaskFailureContext" in context:
            exit_detail = context["creationTaskFailureContext"][
                "formattedExceptionString"
            ]  # noqa
    light_message["exitDetail"] = exit_detail
    light_message["startTime"] = int(light_message["startTime"])
    light_message["endTime"] = int(light_message["endTime"])
    light_message["requiredResources"] = dict(message.required_resources)

    return light_message


def add_history_server_actor_fields(actor_table_data):
    node_id = actor_table_data["address"]["rayletId"]
    node = DataSource.nodes.get(node_id)
    log_dir = os.environ.get("BYTED_RAY_REDIRECT_LOG", "/tmp/ray/session_latest/logs")
    logger.debug(f"node: {node}")
    if node:
        # Add podname/containername/logname/psm for generating logagent links.
        node_name = node["nodeName"]
        actor_table_data["nodeName"] = node_name
        container_name = ""
        if "-head-" in node_name:
            container_name = "ray-head"
        else:
            container_name = "ray-worker"

        worker_id = actor_table_data["address"]["workerId"]
        job_id = actor_table_data["jobId"]
        pid = actor_table_data["pid"]
        logname = f"{log_dir}/worker-{worker_id}-{job_id}-{pid}"
        actor_table_data["containerName"] = container_name
        actor_table_data["logName"] = logname
        psm = dashboard_consts.get_global_psm()
        hostip = actor_table_data["address"]["ipAddress"]
        # TODO move to history server only logic, not in running cluster
        # which can make ray cluster dont relay on Crypto
        actor_table_data["bytedLogUrl"] = generate_logagent_url(
            psm, hostip, node_name, container_name, f"{logname}.out"
        )
        actor_table_data["bytedErrUrl"] = generate_logagent_url(
            psm, hostip, node_name, container_name, f"{logname}.err"
        )

    actor_table_data["psm"] = dashboard_consts.get_global_psm()

    return actor_table_data


class ActorHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # ActorInfoGcsService
        self._gcs_actor_info_stub = None
        # A queue of dead actors in order of when they died
        self.dead_actors_queue = deque()

        # -- Internal states --
        self.total_published_events = 0
        self.subscriber_queue_size = 0
        self.accumulative_event_processing_s = 0

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
                        if dashboard_consts.history_server_enabled():
                            actor_table_data = add_history_server_actor_fields(
                                actor_table_data
                            )
                        actors[actor_table_data["actorId"]] = actor_table_data
                    # Update actors.
                    DataSource.actors.reset(actors)
                    # Update node actors and job actors.
                    node_actors = {}
                    for actor_id, actor_table_data in actors.items():
                        node_id = actor_table_data["address"]["rayletId"]
                        # Update only when node_id is not Nil.
                        if node_id != actor_consts.NIL_NODE_ID:
                            node_actors.setdefault(node_id, {})[
                                actor_id
                            ] = actor_table_data
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

        state_keys = (
            "state",
            "address",
            "numRestarts",
            "timestamp",
            "pid",
            "exitDetail",
            "startTime",
            "endTime",
            "reprName",
        )

        def process_actor_data_from_pubsub(actor_id, actor_table_data):
            actor_table_data = actor_table_data_to_dict(actor_table_data)
            # If actor is not new registered but updated, we only update
            # states related fields.
            if actor_table_data["state"] != "DEPENDENCIES_UNREADY":
                actors = DataSource.actors[actor_id]
                for k in state_keys:
                    if k in actor_table_data:
                        actors[k] = actor_table_data[k]
                actor_table_data = actors
            actor_id = actor_table_data["actorId"]
            node_id = actor_table_data["address"]["rayletId"]
            if actor_table_data["state"] == "DEAD":
                self.dead_actors_queue.append(actor_id)
            if dashboard_consts.history_server_enabled():
                actor_table_data = add_history_server_actor_fields(actor_table_data)
            # Update actors.
            DataSource.actors[actor_id] = actor_table_data
            # Update node actors (only when node_id is not Nil).
            if node_id != actor_consts.NIL_NODE_ID:
                node_actors = DataSource.node_actors.get(node_id, {})
                node_actors[actor_id] = actor_table_data
                DataSource.node_actors[node_id] = node_actors

            return actor_table_data

        # Receive actors from channel.
        gcs_addr = self._dashboard_head.gcs_address
        subscriber = GcsAioActorSubscriber(address=gcs_addr)
        await subscriber.subscribe()

        while True:
            try:
                published = await subscriber.poll(batch_size=200)
                start = time.monotonic()
                actor_table_data_batch = []
                for actor_id, actor_table_data in published:
                    if actor_id is not None:
                        # Convert to lower case hex ID.
                        actor_id = actor_id.hex()
                        actor_table_data = process_actor_data_from_pubsub(
                            actor_id, actor_table_data
                        )
                        actor_table_data_batch.append(actor_table_data)

                # NOTE(wanxing): uploading actor events to history server storage in batches,
                # which is more efficient than uploading them one by one.
                if dashboard_consts.history_server_enabled():
                    logger.info(
                        f"append_actor_events, events count: {len(actor_table_data_batch)}"
                    )
                    elapsed_start = time.monotonic()
                    append_actor_events(
                        self._dashboard_head.history_server_storage,
                        actor_table_data_batch,
                    )
                    logger.info(
                        f"append_actor_events done, elapsed: {time.monotonic() - elapsed_start}."
                    )

                # Yield so that we can give time for
                # user-facing APIs to reply to the frontend.
                elapsed = time.monotonic() - start
                waited_time_s = min(elapsed, 0.5)
                await asyncio.sleep(waited_time_s)

                # Update the internal states for debugging.
                self.accumulative_event_processing_s += elapsed
                self.total_published_events += len(published)
                self.subscriber_queue_size = subscriber.queue_size
                logger.info(
                    f"Processing takes {elapsed}. Total process: " f"{len(published)}"
                )
                if self.accumulative_event_processing_s > 0:
                    logger.debug(
                        "Processing throughput: "
                        f"{self.total_published_events / self.accumulative_event_processing_s}"  # noqa
                        " / s"
                    )
                logger.debug(f"queue size: {self.subscriber_queue_size}")
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
                            node_id = actor["address"].get("rayletId")
                            if node_id and node_id != actor_consts.NIL_NODE_ID:
                                del DataSource.node_actors[node_id][actor_id]
                await asyncio.sleep(ACTOR_CLEANUP_FREQUENCY)
            except Exception:
                logger.exception("Error cleaning up actor info from GCS.")

    def get_internal_states(self):
        states = {
            "total_published_events": self.total_published_events,
            "total_dead_actors": len(self.dead_actors_queue),
            "total_actors": len(DataSource.actors),
            "queue_size": self.subscriber_queue_size,
        }
        if self.accumulative_event_processing_s > 0:
            states["event_processing_per_s"] = (
                self.total_published_events / self.accumulative_event_processing_s
            )
        return states

    @routes.get("/logical/actors")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_all_actors()
        return dashboard_optional_utils.rest_response(
            success=True,
            message="All actors fetched.",
            actors=actors,
            # False to avoid converting Ray resource name to google style.
            # It's not necessary here because the fields are already
            # google formatted when protobuf was converted into dict.
            convert_google_style=False,
        )

    @routes.get("/logical/actors/{actor_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_actor(self, req) -> aiohttp.web.Response:
        actor_id = req.match_info.get("actor_id")
        actors = await DataOrganizer.get_all_actors()
        return dashboard_optional_utils.rest_response(
            success=True, message="Actor details fetched.", detail=actors[actor_id]
        )

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            gcs_channel
        )
        await asyncio.gather(self._update_actors(), self._cleanup_actors())

    @staticmethod
    def is_minimal_module():
        return False
