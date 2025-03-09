import asyncio
import json
import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from typing import AsyncGenerator, Iterable, List

import aiohttp.web
import grpc

import ray._private.utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray._private.collections_utils import split
from ray._private.gcs_pubsub import GcsAioNodeInfoSubscriber
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    env_integer,
)
from ray._private.gcs_pubsub import GcsAioResourceUsageSubscriber
from ray._private.utils import get_or_create_event_loop
from ray.autoscaler._private.util import (
    LoadMetricsSummary,
    get_per_node_breakdown_as_dict,
    parse_usage,
)
from ray.core.generated import gcs_pb2, node_manager_pb2, node_manager_pb2_grpc
from ray.dashboard.consts import (
    GCS_RPC_TIMEOUT_SECONDS,
    DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
    DASHBOARD_AGENT_ADDR_IP_PREFIX,
)
from ray.dashboard.datacenter import DataOrganizer, DataSource
from ray.dashboard.modules.node import node_consts
from ray.dashboard.modules.node.node_consts import (
    RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT,
)
from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS", 1
)


def _gcs_node_info_to_dict(message: gcs_pb2.GcsNodeInfo) -> dict:
    return dashboard_utils.message_to_dict(
        message, {"nodeId"}, always_print_fields_with_no_presence=True
    )


class NodeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)

        self._stubs = {}
        self._collect_memory_info = False

        DataSource.nodes.signal.append(self._update_stubs)
        # The time where the module is started.
        self._module_start_time = time.time()
        # The time it takes until the head node is registered. None means
        # head node hasn't been registered.
        self._head_node_registration_time_s = None
        # Queue of dead nodes to be removed, up to MAX_DEAD_NODES_TO_CACHE
        self._dead_node_queue = deque()

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="node_head_executor",
        )

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._stubs.pop(node_id, None)
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
        all_node_info = await self.gcs_aio_client.get_all_node_info(timeout=None)

        def _convert_to_dict(messages: Iterable[gcs_pb2.GcsNodeInfo]) -> List[dict]:
            return [_gcs_node_info_to_dict(m) for m in messages]

        all_node_infos = await get_or_create_event_loop().run_in_executor(
            self._executor,
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

                updated_infos = await get_or_create_event_loop().run_in_executor(
                    self._executor,
                    _convert_to_dict,
                    updated_infos_proto,
                )

                for node in updated_infos:
                    yield node
            except Exception:
                logger.exception("Failed handling updated nodes.")

    async def _update_node(self, node: dict):
        node_id = node["nodeId"]  # hex
        if node["isHeadNode"] and not self._head_node_registration_time_s:
            self._head_node_registration_time_s = time.time() - self._module_start_time
            # Put head node ID in the internal KV to be read by JobAgent.
            # TODO(architkulkarni): Remove once State API exposes which
            # node is the head node.
            await self.gcs_aio_client.internal_kv_put(
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
                self.gcs_aio_client.internal_kv_del(
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
                DataSource.nodes.pop(self._dead_node_queue.popleft(), None)
        DataSource.nodes[node_id] = node

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
                    > RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT
                ):
                    logger.warning(
                        "Head node is not registered even after "
                        f"{RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT} seconds. "
                        "The API server might not work correctly. Please "
                        "report a Github issue. Internal states :"
                        f"{self.get_internal_states()}"
                    )
                    warning_shown = True

    async def get_nodes_logical_resources(self) -> dict:

        from ray.autoscaler.v2.utils import is_autoscaler_v2

        if is_autoscaler_v2():
            from ray.autoscaler.v2.sdk import ClusterStatusParser
            from ray.autoscaler.v2.schema import Stats

            try:
                # here we have a sync request
                req_time = time.time()
                cluster_status = await self.gcs_aio_client.get_cluster_status()
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
                self.gcs_aio_client.internal_kv_get(
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
                success=True,
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
                success=True,
                message="Node hostname list fetched.",
                host_name_list=list(alive_hostnames),
            )
        else:
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Unknown view {view}"
            )

    @routes.get("/nodes/{node_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        node_id = req.match_info.get("node_id")
        node_info = await DataOrganizer.get_node_info(node_id)
        return dashboard_optional_utils.rest_response(
            success=True, message="Node details fetched.", detail=node_info
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
        assert len(node_ids) == len(responses)

        new_node_stats = await get_or_create_event_loop().run_in_executor(
            self._executor, postprocess, zip(node_ids, responses)
        )

        for node_id, new_stat in new_node_stats.items():
            DataSource.node_stats[node_id] = new_stat

    async def _update_node_physical_stats(self):
        """
        Update DataSource.node_physical_stats by subscribing to the GCS resource usage.
        """
        subscriber = GcsAioResourceUsageSubscriber(address=self.gcs_address)
        await subscriber.subscribe()

        loop = get_or_create_event_loop()

        while True:
            try:
                # The key is b'RAY_REPORTER:{node id hex}',
                # e.g. b'RAY_REPORTER:2b4fbd...'
                key, data = await subscriber.poll()
                if key is None:
                    continue

                # NOTE: Every iteration is executed inside the thread-pool executor
                #       (TPE) to avoid blocking the Dashboard's event-loop
                parsed_data = await loop.run_in_executor(
                    self._executor, json.loads, data
                )

                node_id = key.split(":")[-1]
                DataSource.node_physical_stats[node_id] = parsed_data
            except Exception:
                logger.exception(
                    "Error receiving node physical stats from _update_node_physical_stats."
                )

    async def run(self, server):
        await asyncio.gather(
            self._update_nodes(),
            self._update_node_stats(),
            self._update_node_physical_stats(),
        )

    @staticmethod
    def is_minimal_module():
        return False
