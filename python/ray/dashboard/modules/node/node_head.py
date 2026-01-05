import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Set

import aiohttp.web
import grpc

import ray._private.utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._common.utils import get_or_create_event_loop
from ray._private import ray_constants
from ray._private.collections_utils import split
from ray._private.gcs_pubsub import (
    GcsAioActorSubscriber,
    GcsAioNodeInfoSubscriber,
    GcsAioResourceUsageSubscriber,
)
from ray._private.grpc_utils import init_grpc_channel
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    env_integer,
)
from ray.autoscaler._private.util import (
    LoadMetricsSummary,
    get_per_node_breakdown_as_dict,
    parse_usage,
)
from ray.core.generated import gcs_pb2, node_manager_pb2, node_manager_pb2_grpc
from ray.dashboard.consts import (
    DASHBOARD_AGENT_ADDR_IP_PREFIX,
    DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
    GCS_RPC_TIMEOUT_SECONDS,
)
from ray.dashboard.modules.node import actor_consts, node_consts
from ray.dashboard.modules.node.datacenter import DataOrganizer, DataSource
from ray.dashboard.modules.reporter.reporter_models import StatsPayload
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)


# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS", 1
)

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


def _gcs_node_info_to_dict(message: gcs_pb2.GcsNodeInfo) -> dict:
    return dashboard_utils.message_to_dict(
        message, {"nodeId"}, always_print_fields_with_no_presence=True
    )


def _actor_table_data_to_dict(message):
    orig_message = dashboard_utils.message_to_dict(
        message,
        {
            "actorId",
            "parentId",
            "jobId",
            "workerId",
            "nodeId",
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
        "labelSelector",
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
    light_message["labelSelector"] = dict(message.label_selector)
    return light_message


class NodeHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._stubs = {}
        self._collect_memory_info = False

        # The time where the module is started.
        self._module_start_time = time.time()
        # The time it takes until the head node is registered. None means
        # head node hasn't been registered.
        self._head_node_registration_time_s = None
        # The node ID of the current head node
        self._registered_head_node_id = None
        # Queue of dead nodes to be removed, up to MAX_DEAD_NODES_TO_CACHE
        self._dead_node_queue = deque()

        self._node_executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="node_head_node_executor",
        )

        self._gcs_actor_channel_subscriber = None
        # A queue of dead actors in order of when they died
        self._destroyed_actors_queue = deque()

        # -- Internal state --
        self._loop = get_or_create_event_loop()
        # NOTE: This executor is intentionally constrained to just 1 thread to
        #       limit its concurrency, therefore reducing potential for GIL contention
        self._actor_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="node_head_actor_executor"
        )

        self._background_tasks: Set[asyncio.Task] = set()

    def get_internal_states(self):
        return {
            "head_node_registration_time_s": self._head_node_registration_time_s,
            "registered_nodes": len(DataSource.nodes),
            "module_lifetime_s": time.time() - self._module_start_time,
        }

    async def _subscribe_for_node_updates(self) -> AsyncGenerator[dict, None]:
        """
        Yields the initial state of all nodes, then yields the updated state of nodes.

        It makes GetAllNodeInfo call only once after the subscription is done, to get
        the initial state of the nodes.
        """
        subscriber = GcsAioNodeInfoSubscriber(address=self.gcs_address)
        await subscriber.subscribe()

        # Get all node info from GCS. To prevent Time-of-check to time-of-use issue [1],
        # it happens after the subscription. That is, an update between
        # get-all-node-info and the subscription is not missed.
        # [1] https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use
        all_node_info = await self.gcs_client.async_get_all_node_info(timeout=None)

        def _convert_to_dict(messages: Iterable[gcs_pb2.GcsNodeInfo]) -> List[dict]:
            return [_gcs_node_info_to_dict(m) for m in messages]

        all_node_infos = await self._loop.run_in_executor(
            self._node_executor,
            _convert_to_dict,
            all_node_info.values(),
        )

        for node in all_node_infos:
            yield node

        while True:
            try:
                node_id_updated_info_tuples = await subscriber.poll(
                    batch_size=node_consts.RAY_DASHBOARD_NODE_SUBSCRIBER_POLL_SIZE
                )

                if node_id_updated_info_tuples:
                    _, updated_infos_proto = zip(*node_id_updated_info_tuples)
                else:
                    updated_infos_proto = []

                updated_infos = await self._loop.run_in_executor(
                    self._node_executor,
                    _convert_to_dict,
                    updated_infos_proto,
                )

                for node in updated_infos:
                    yield node
            except Exception:
                logger.exception("Failed handling updated nodes.")

    async def _update_node(self, node: dict):
        node_id = node["nodeId"]  # hex
        if (
            node["isHeadNode"]
            and node["state"] == "ALIVE"
            and self._registered_head_node_id != node_id
        ):
            if self._registered_head_node_id is not None:
                logger.warning(
                    "A new head node has become ALIVE. New head node ID: %s, old head node ID: %s, internal states: %s",
                    node_id,
                    self._registered_head_node_id,
                    self.get_internal_states(),
                )
            self._registered_head_node_id = node_id
            self._head_node_registration_time_s = time.time() - self._module_start_time
            # Put head node ID in the internal KV to be read by JobAgent.
            # TODO(architkulkarni): Remove once State API exposes which
            # node is the head node.
            await self.gcs_client.async_internal_kv_put(
                ray_constants.KV_HEAD_NODE_ID_KEY,
                node_id.encode(),
                overwrite=True,
                namespace=ray_constants.KV_NAMESPACE_JOB,
                timeout=GCS_RPC_TIMEOUT_SECONDS,
            )
        assert node["state"] in ["ALIVE", "DEAD"]
        is_alive = node["state"] == "ALIVE"
        if not is_alive:
            # Remove the agent address from the internal KV.
            keys = [
                f"{DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}",
                f"{DASHBOARD_AGENT_ADDR_IP_PREFIX}{node['nodeManagerAddress']}",
            ]
            tasks = [
                self.gcs_client.async_internal_kv_del(
                    key,
                    del_by_prefix=False,
                    namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
                    timeout=GCS_RPC_TIMEOUT_SECONDS,
                )
                for key in keys
            ]
            await asyncio.gather(*tasks)

            self._dead_node_queue.append(node_id)
            if len(self._dead_node_queue) > node_consts.MAX_DEAD_NODES_TO_CACHE:
                node_id = self._dead_node_queue.popleft()
                DataSource.nodes.pop(node_id, None)
                self._stubs.pop(node_id, None)
        DataSource.nodes[node_id] = node
        # TODO(fyrestone): Handle exceptions.
        address = "{}:{}".format(
            node["nodeManagerAddress"], int(node["nodeManagerPort"])
        )
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        channel = init_grpc_channel(address, options, asynchronous=True)
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        self._stubs[node_id] = stub

    async def _update_nodes(self):
        """
        Subscribe to node updates and update the internal states. If the head node is
        not registered after RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT, it logs a
        warning only once.
        """
        warning_shown = False
        async for node in self._subscribe_for_node_updates():
            await self._update_node(node)
            if not self._head_node_registration_time_s:
                # head node is not registered yet
                if (
                    not warning_shown
                    and (time.time() - self._module_start_time)
                    > node_consts.RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT
                ):
                    logger.warning(
                        "Head node is not registered even after "
                        f"{node_consts.RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT} seconds. "
                        "The API server might not work correctly. Please "
                        "report a Github issue. Internal states :"
                        f"{self.get_internal_states()}"
                    )
                    warning_shown = True

    async def get_nodes_logical_resources(self) -> dict:

        from ray.autoscaler.v2.utils import is_autoscaler_v2

        if is_autoscaler_v2():
            from ray.autoscaler.v2.schema import Stats
            from ray.autoscaler.v2.sdk import ClusterStatusParser

            try:
                # here we have a sync request
                req_time = time.time()
                cluster_status = await self.gcs_client.async_get_cluster_status()
                reply_time = time.time()
                cluster_status = ClusterStatusParser.from_get_cluster_status_reply(
                    cluster_status,
                    stats=Stats(
                        gcs_request_time_s=reply_time - req_time, request_ts_s=req_time
                    ),
                )
            except Exception:
                logger.exception("Error getting cluster status")
                return {}

            per_node_resources = {}
            # TODO(rickyx): we should just return structure data rather than strings.
            for node in chain(cluster_status.active_nodes, cluster_status.idle_nodes):
                if not node.resource_usage:
                    continue

                usage_dict = {
                    r.resource_name: (r.used, r.total)
                    for r in node.resource_usage.usage
                }
                per_node_resources[node.node_id] = "\n".join(
                    parse_usage(usage_dict, verbose=True)
                )

            return per_node_resources

        # Legacy autoscaler status code.
        (status_string, error) = await asyncio.gather(
            *[
                self.gcs_client.async_internal_kv_get(
                    key.encode(), namespace=None, timeout=GCS_RPC_TIMEOUT_SECONDS
                )
                for key in [
                    DEBUG_AUTOSCALING_STATUS,
                    DEBUG_AUTOSCALING_ERROR,
                ]
            ]
        )
        if not status_string:
            return {}
        status_dict = json.loads(status_string)

        lm_summary_dict = status_dict.get("load_metrics_report")
        if lm_summary_dict:
            lm_summary = LoadMetricsSummary(**lm_summary_dict)

        node_logical_resources = get_per_node_breakdown_as_dict(lm_summary)
        return node_logical_resources if error is None else {}

    @routes.get("/nodes")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_nodes(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            all_node_summary_task = DataOrganizer.get_all_node_summary()
            nodes_logical_resource_task = self.get_nodes_logical_resources()

            all_node_summary, nodes_logical_resources = await asyncio.gather(
                all_node_summary_task, nodes_logical_resource_task
            )

            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="Node summary fetched.",
                summary=all_node_summary,
                node_logical_resources=nodes_logical_resources,
            )
        elif view is not None and view.lower() == "hostNameList".lower():
            alive_hostnames = set()
            for node in DataSource.nodes.values():
                if node["state"] == "ALIVE":
                    alive_hostnames.add(node["nodeManagerHostname"])
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="Node hostname list fetched.",
                host_name_list=list(alive_hostnames),
            )
        else:
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                message=f"Unknown view {view}",
            )

    @routes.get("/nodes/{node_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        node_id = req.match_info.get("node_id")
        node_info = await DataOrganizer.get_node_info(node_id)
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Node details fetched.",
            detail=node_info,
        )

    @async_loop_forever(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def _update_node_stats(self):
        timeout = max(2, node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS - 1)

        # NOTE: We copy stubs to make sure
        #       it doesn't change during the iteration (since its being updated
        #       from another async task)
        current_stub_node_id_tuples = list(self._stubs.items())

        node_ids = []
        get_node_stats_tasks = []

        for _, (node_id, stub) in enumerate(current_stub_node_id_tuples):
            node_info = DataSource.nodes.get(node_id)
            if node_info["state"] != "ALIVE":
                continue

            node_ids.append(node_id)
            get_node_stats_tasks.append(
                stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(
                        include_memory_info=self._collect_memory_info
                    ),
                    timeout=timeout,
                )
            )

        responses = []

        # NOTE: We're chunking up fetching of the stats to run in batches of no more
        #       than 100 nodes at a time to avoid flooding the event-loop's queue
        #       with potentially a large, uninterrupted sequence of tasks updating
        #       the node stats for very large clusters.
        for get_node_stats_tasks_chunk in split(get_node_stats_tasks, 100):
            current_chunk_responses = await asyncio.gather(
                *get_node_stats_tasks_chunk,
                return_exceptions=True,
            )

            responses.extend(current_chunk_responses)

            # We're doing short (25ms) yield after every chunk to make sure
            #   - We're not overloading the event-loop with excessive # of tasks
            #   - Allowing 10k nodes stats fetches be sent out performed in 2.5s
            await asyncio.sleep(0.025)

        def postprocess(node_id_response_tuples):
            """Pure function reorganizing the data into {node_id: stats}."""
            new_node_stats = {}

            for node_id, response in node_id_response_tuples:
                if isinstance(response, asyncio.CancelledError):
                    pass
                elif isinstance(response, grpc.RpcError):
                    if response.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        message = (
                            f"Cannot reach the node, {node_id}, after timeout "
                            f" {timeout}. This node may have been overloaded, "
                            "terminated, or the network is slow."
                        )
                    elif response.code() == grpc.StatusCode.UNAVAILABLE:
                        message = (
                            f"Cannot reach the node, {node_id}. "
                            "The node may have been terminated."
                        )
                    else:
                        message = f"Error updating node stats of {node_id}."

                    logger.error(message, exc_info=response)
                elif isinstance(response, Exception):
                    logger.error(
                        f"Error updating node stats of {node_id}.", exc_info=response
                    )
                else:
                    new_node_stats[node_id] = dashboard_utils.node_stats_to_dict(
                        response
                    )

            return new_node_stats

        # NOTE: Zip will silently truncate to shorter argument that potentially
        #       could lead to subtle hard to catch issues, hence the assertion
        assert len(node_ids) == len(
            responses
        ), f"node_ids({len(node_ids)}): {node_ids}, responses({len(responses)}): {responses}"

        new_node_stats = await self._loop.run_in_executor(
            self._node_executor, postprocess, zip(node_ids, responses)
        )

        for node_id, new_stat in new_node_stats.items():
            DataSource.node_stats[node_id] = new_stat

    async def _update_node_physical_stats(self):
        """
        Update DataSource.node_physical_stats by subscribing to the GCS resource usage.
        """
        subscriber = GcsAioResourceUsageSubscriber(address=self.gcs_address)
        await subscriber.subscribe()

        while True:
            try:
                # The key is b'RAY_REPORTER:{node id hex}',
                # e.g. b'RAY_REPORTER:2b4fbd...'
                key, data = await subscriber.poll()
                if key is None:
                    continue

                # NOTE: Every iteration is executed inside the thread-pool executor
                #       (TPE) to avoid blocking the Dashboard's event-loop
                parsed_data = await self._loop.run_in_executor(
                    self._node_executor, _parse_node_stats, data
                )

                node_id = key.split(":")[-1]
                DataSource.node_physical_stats[node_id] = parsed_data
            except Exception:
                logger.exception(
                    "Error receiving node physical stats from _update_node_physical_stats."
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
                DataSource.actors = actor_dicts

                # Update node actors and job actors.
                node_actors = defaultdict(dict)
                for actor_id_bytes, updated_actor_table in actor_dicts.items():
                    node_id = updated_actor_table["address"]["nodeId"]
                    # Update only when node_id is not Nil.
                    if node_id != actor_consts.NIL_NODE_ID:
                        node_actors[node_id][actor_id_bytes] = updated_actor_table

                # Update node's actor info
                DataSource.node_actors = node_actors

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
                actor_id_bytes.hex(): _actor_table_data_to_dict(
                    actor_table_data_message
                )
                for actor_id_bytes, actor_table_data_message in batch
                if actor_id_bytes is not None
            }

        return await self._loop.run_in_executor(self._actor_executor, _convert_to_dict)

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
        node_id = actor_table_data["address"]["nodeId"]

        if actor_table_data["state"] == "DEAD":
            self._destroyed_actors_queue.append(actor_id)

        # Update actors.
        DataSource.actors[actor_id] = actor_table_data
        # Update node actors (only when node_id is not Nil).
        if node_id != actor_consts.NIL_NODE_ID:
            node_actors = DataSource.node_actors.get(node_id, {})
            node_actors[actor_id] = actor_table_data
            DataSource.node_actors[node_id] = node_actors

    async def _get_all_actors(self) -> Dict[str, dict]:
        actors = await self.gcs_client.async_get_all_actor_info(
            timeout=GCS_RPC_TIMEOUT_SECONDS
        )

        # NOTE: We're offloading conversion to a TPE to make sure we're not
        #       blocking the event-loop for prolonged period of time for large clusters
        def _convert_to_dict():
            return {
                actor_id.hex(): _actor_table_data_to_dict(actor_table_data)
                for actor_id, actor_table_data in actors.items()
            }

        return await self._loop.run_in_executor(self._actor_executor, _convert_to_dict)

    async def _cleanup_actors(self):
        while True:
            try:
                while len(self._destroyed_actors_queue) > MAX_DESTROYED_ACTORS_TO_CACHE:
                    actor_id = self._destroyed_actors_queue.popleft()
                    if actor_id in DataSource.actors:
                        actor = DataSource.actors.pop(actor_id)
                        node_id = actor["address"].get("nodeId")
                        if node_id and node_id != actor_consts.NIL_NODE_ID:
                            del DataSource.node_actors[node_id][actor_id]
                await asyncio.sleep(ACTOR_CLEANUP_FREQUENCY)
            except Exception:
                logger.exception("Error cleaning up actor info from GCS.")

    @routes.get("/logical/actors")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        actor_ids: Optional[List[str]] = None
        if "ids" in req.query:
            actor_ids = req.query["ids"].split(",")
        actors = await DataOrganizer.get_actor_infos(actor_ids=actor_ids)
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
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
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Actor details fetched.",
            detail=actors[actor_id],
        )

    @routes.get("/test/dump")
    async def dump(self, req) -> aiohttp.web.Response:
        """
        Dump all data from datacenter. This is used for testing purpose only.
        """
        key = req.query.get("key")
        if key is None:
            all_data = {
                k: dict(v)
                for k, v in DataSource.__dict__.items()
                if not k.startswith("_")
            }
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="Fetch all data from datacenter success.",
                **all_data,
            )
        else:
            data = dict(DataSource.__dict__.get(key))
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message=f"Fetch {key} from datacenter success.",
                **{key: data},
            )

    async def run(self):
        await super().run()
        coros = [
            self._update_nodes(),
            self._update_node_stats(),
            self._update_node_physical_stats(),
            self._update_actors(),
            self._cleanup_actors(),
            DataOrganizer.purge(),
            DataOrganizer.organize(self._node_executor),
        ]
        for coro in coros:
            task = self._loop.create_task(coro)
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)


def _parse_node_stats(node_stats_str: str) -> dict:
    stats_dict = json.loads(node_stats_str)
    if StatsPayload is not None:
        # Validate the response by parsing the stats_dict.
        StatsPayload.parse_obj(stats_dict)
        return stats_dict
    else:
        return stats_dict
