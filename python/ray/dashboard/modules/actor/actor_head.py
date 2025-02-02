import asyncio
import logging
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import aiohttp.web

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioActorSubscriber
from ray._private.utils import get_or_create_event_loop
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
from ray.dashboard.datacenter import DataOrganizer, DataSource
from ray.dashboard.modules.actor import actor_consts

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable

MAX_DESTROYED_ACTORS_TO_CACHE = max(
    0, ray._config.maximum_gcs_destroyed_actor_cached_count()
)

ACTOR_CLEANUP_FREQUENCY = 1  # seconds


ACTOR_TABLE_STATE_COLUMNS = (
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
            "placementGroupId",
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
        "placementGroupId",
        "callSite",
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


class ActorHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)

        self._gcs_actor_channel_subscriber = None
        # A queue of dead actors in order of when they died
        self.destroyed_actors_queue = deque()

        # -- Internal state --
        self._loop = get_or_create_event_loop()
        # NOTE: This executor is intentionally constrained to just 1 thread to
        #       limit its concurrency, therefore reducing potential for GIL contention
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="actor_head_executor"
        )

    async def _update_actors(self):
        """
        Processes actor info. First gets all actors from GCS, then subscribes to
        actor updates. For each actor update, updates DataSource.node_actors and
        DataSource.actors.
        """

        # To prevent Time-of-check to time-of-use issue [1], the get-all-actor-info
        # happens after the subscription. That is, an update between get-all-actor-info
        # and the subscription is not missed.
        #
        # [1] https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use
        gcs_addr = self.gcs_address
        actor_channel_subscriber = GcsAioActorSubscriber(address=gcs_addr)
        await actor_channel_subscriber.subscribe()

        # Get all actor info.
        while True:
            try:
                logger.info("Getting all actor info from GCS.")

                actor_dicts = await self._get_all_actors()
                # Update actors
                DataSource.actors.reset(actor_dicts)

                # Update node actors and job actors.
                node_actors = defaultdict(dict)
                for actor_id_bytes, updated_actor_table in actor_dicts.items():
                    node_id = updated_actor_table["address"]["rayletId"]
                    # Update only when node_id is not Nil.
                    if node_id != actor_consts.NIL_NODE_ID:
                        node_actors[node_id][actor_id_bytes] = updated_actor_table

                # Update node's actor info
                DataSource.node_actors.reset(node_actors)

                logger.info("Received %d actor info from GCS.", len(actor_dicts))

                # Break, once all initial actors are successfully fetched
                break
            except Exception as e:
                logger.exception("Error Getting all actor info from GCS", exc_info=e)
                await asyncio.sleep(
                    actor_consts.RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS
                )

        # Pull incremental updates from the GCS channel
        while True:
            try:
                updated_actor_table_entries = await self._poll_updated_actor_table_data(
                    actor_channel_subscriber
                )

                for (
                    actor_id,
                    updated_actor_table,
                ) in updated_actor_table_entries.items():
                    self._process_updated_actor_table(actor_id, updated_actor_table)

                # TODO emit metrics
                logger.debug(
                    f"Total events processed: {len(updated_actor_table_entries)}, "
                    f"queue size: {actor_channel_subscriber.queue_size}"
                )

            except Exception as e:
                logger.exception("Error processing actor info from GCS.", exc_info=e)

    async def _poll_updated_actor_table_data(
        self, actor_channel_subscriber: GcsAioActorSubscriber
    ) -> Dict[str, Dict[str, Any]]:
        # TODO make batch size configurable
        batch = await actor_channel_subscriber.poll(batch_size=200)

        # NOTE: We're offloading conversion to a TPE to make sure we're not
        #       blocking the event-loop for prolonged period of time irrespective
        #       of the batch size
        def _convert_to_dict():
            return {
                actor_id_bytes.hex(): actor_table_data_to_dict(actor_table_data_message)
                for actor_id_bytes, actor_table_data_message in batch
                if actor_id_bytes is not None
            }

        return await self._loop.run_in_executor(self._executor, _convert_to_dict)

    def _process_updated_actor_table(
        self, actor_id: str, actor_table_data: Dict[str, Any]
    ):
        """NOTE: This method has to be executed on the event-loop, provided that it
        accesses DataSource data structures (to follow its thread-safety model)"""

        # If actor is not new registered but updated, we only update
        # states related fields.
        actor = DataSource.actors.get(actor_id)

        if actor and actor_table_data["state"] != "DEPENDENCIES_UNREADY":
            for k in ACTOR_TABLE_STATE_COLUMNS:
                if k in actor_table_data:
                    actor[k] = actor_table_data[k]
            actor_table_data = actor

        actor_id = actor_table_data["actorId"]
        node_id = actor_table_data["address"]["rayletId"]

        if actor_table_data["state"] == "DEAD":
            self.destroyed_actors_queue.append(actor_id)

        # Update actors.
        DataSource.actors[actor_id] = actor_table_data
        # Update node actors (only when node_id is not Nil).
        if node_id != actor_consts.NIL_NODE_ID:
            node_actors = DataSource.node_actors.get(node_id, {})
            node_actors[actor_id] = actor_table_data
            DataSource.node_actors[node_id] = node_actors

    async def _get_all_actors(self) -> Dict[str, dict]:
        actors = await self.gcs_aio_client.get_all_actor_info(
            timeout=GCS_RPC_TIMEOUT_SECONDS
        )

        # NOTE: We're offloading conversion to a TPE to make sure we're not
        #       blocking the event-loop for prolonged period of time for large clusters
        def _convert_to_dict():
            return {
                actor_id.hex(): actor_table_data_to_dict(actor_table_data)
                for actor_id, actor_table_data in actors.items()
            }

        return await self._loop.run_in_executor(self._executor, _convert_to_dict)

    async def _cleanup_actors(self):
        while True:
            try:
                while len(self.destroyed_actors_queue) > MAX_DESTROYED_ACTORS_TO_CACHE:
                    actor_id = self.destroyed_actors_queue.popleft()
                    if actor_id in DataSource.actors:
                        actor = DataSource.actors.pop(actor_id)
                        node_id = actor["address"].get("rayletId")
                        if node_id and node_id != actor_consts.NIL_NODE_ID:
                            del DataSource.node_actors[node_id][actor_id]
                await asyncio.sleep(ACTOR_CLEANUP_FREQUENCY)
            except Exception:
                logger.exception("Error cleaning up actor info from GCS.")

    @routes.get("/logical/actors")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_actor_infos()
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
        actors = await DataOrganizer.get_actor_infos(actor_ids=[actor_id])
        return dashboard_optional_utils.rest_response(
            success=True, message="Actor details fetched.", detail=actors[actor_id]
        )

    async def run(self, server):
        await asyncio.gather(self._update_actors(), self._cleanup_actors())

    @staticmethod
    def is_minimal_module():
        return False
