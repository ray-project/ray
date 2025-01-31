# actor_data_index.py
import asyncio
import logging
from collections import deque
from concurrent.futures import Executor
from typing import Dict, AsyncGenerator, Tuple, Set

import ray
from ray import ActorID, NodeID
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_pb2
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
from ray._private.gcs_pubsub import GcsAioActorSubscriber
from ray.dashboard.modules.actor import actor_consts
from ray._private.gcs_utils import GcsAioClient

logger = logging.getLogger(__name__)

MAX_DESTROYED_ACTORS_TO_CACHE = max(
    0, ray._config.maximum_gcs_destroyed_actor_cached_count()
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


class ActorDataIndex:
    """
    A class that takes update about actors and updates the index.

    Available indexes:
    - `actors`: actor_id -> actor_table_data
    - `node_actor_id`: node_id -> List[actor_id]

    All indexes and data are typed:
    - actor_id: ActorID
    - actor_table_data: ActorTableData
    - node_id: NodeID

    Data sources:
    - GcsAioActorSubscriber

    Method Naming
    - subscribe_and_update_: subscribes and updates the data. Never returns.
    - subscribe_for_: only subscribes and yields data. Never returns.
    - update_: only updates the data. The data comes from elsewhere.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        executor: Executor,
        gcs_aio_client: GcsAioClient,
    ):
        self.loop = loop
        self.executor = executor
        self.gcs_aio_client = gcs_aio_client
        self.gcs_address = gcs_aio_client.gcs_address

        # Indexes
        self.actors: Dict[ActorID, gcs_pb2.ActorTableData] = {}
        self.node_actors: Dict[NodeID, Set[ActorID]] = {}

        # Internal states
        self.destroyed_actors_queue = deque()

    async def run(self):
        await self.subscribe_and_update_actors()

    def update_actor(self, actor_id: ActorID, actor_table_data: gcs_pb2.ActorTableData):
        """
        Updates indexes self.actors and self.node_actors.
        If the actor is dead, add to self.destroyed_actors_queue and if that is full,
        pop.
        """
        # DO NOT SUBMIT
        # TODO(ryw): the subscriber only updates state related fields, so we need to
        # merge with existing actor dict if any.
        logger.error(f"Updating actor {actor_id} {actor_table_data}")
        node_id = NodeID(actor_table_data.address.raylet_id)

        if actor_id in self.actors:
            # If node id changed, update self.node_actors.
            old_node_id_bytes = self.actors[actor_id].address.raylet_id
            if (
                old_node_id_bytes
                and old_node_id_bytes != actor_table_data.address.raylet_id
            ):
                self.remove_actor_id_from_node_actors(
                    NodeID(old_node_id_bytes), actor_id
                )

            self.actors[actor_id].MergeFrom(actor_table_data)
        else:
            # New actor ID
            self.actors[actor_id] = actor_table_data

        self.add_actor_id_to_node_actors(node_id, actor_id)

        if actor_table_data.state == gcs_pb2.ActorTableData.ActorState.DEAD:
            self.destroyed_actors_queue.append(actor_id)
            self.remove_destroyed_actors_if_needed()

    def remove_actor_id_from_node_actors(self, node_id: NodeID, actor_id: ActorID):
        self.node_actors[node_id].discard(actor_id)
        if len(self.node_actors[node_id]) == 0:
            self.node_actors.pop(node_id)

    def add_actor_id_to_node_actors(self, node_id: NodeID, actor_id: ActorID):
        if node_id not in self.node_actors:
            self.node_actors[node_id] = set()
        self.node_actors[node_id].add(actor_id)

    def remove_destroyed_actors_if_needed(self):
        while len(self.destroyed_actors_queue) > MAX_DESTROYED_ACTORS_TO_CACHE:
            actor_id = self.destroyed_actors_queue.popleft()
            if actor_id in self.actors:
                actor_table_data = self.actors.pop(actor_id)
                node_id = NodeID(actor_table_data.address.raylet_id)
                self.remove_actor_id_from_node_actors(node_id, actor_id)

    async def subscribe_and_update_actors(self):
        """
        Subscribes to actor updates and updates self.actors and self.node_actors.
        """
        async_gen = self.subscribe_for_actors()
        async for actor_id, actor_table_data in async_gen:
            self.update_actor(actor_id, actor_table_data)

    async def subscribe_for_actors(
        self,
    ) -> AsyncGenerator[Tuple[ActorID, gcs_pb2.ActorTableData], None]:
        """
        Processes actor info. First gets all actors from GCS, then subscribes to
        actor updates. For each actor update, updates self.node_actors and
        self.actors.
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

                all_actors = await self.gcs_aio_client.get_all_actor_info(
                    timeout=GCS_RPC_TIMEOUT_SECONDS
                )
                for actor_id, actor_table_data in all_actors.items():
                    yield actor_id, actor_table_data

                logger.info("Received %d actor info from GCS.", len(all_actors))

                # Break, once all initial actors are successfully fetched
                break
            except Exception as e:
                logger.exception(
                    f"Error Getting all actor info from GCS, will retry in {actor_consts.RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS} seconds",
                    exc_info=e,
                )
                await asyncio.sleep(
                    actor_consts.RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS
                )

        # Pull incremental updates from the GCS channel
        while True:
            try:
                batch = await actor_channel_subscriber.poll(batch_size=200)
                logger.error(f"batch: {batch}")
                for actor_id_bytes, actor_table_data in batch:
                    yield ActorID(actor_id_bytes), actor_table_data

                # TODO emit metrics
                logger.debug(
                    f"Total events processed: {len(batch)}, "
                    f"queue size: {actor_channel_subscriber.queue_size}"
                )
                # TODO(ryw): config this
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.exception("Error processing actor info from GCS.", exc_info=e)
