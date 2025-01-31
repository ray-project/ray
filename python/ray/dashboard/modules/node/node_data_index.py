from typing import Dict, Any, AsyncGenerator
import time
from dataclasses import dataclass
import asyncio
import concurrent.futures
import json
from collections import deque
import grpc
import logging

import ray
from ray._private.collections_utils import split
from ray import NodeID, WorkerID
from ray.core.generated import (
    gcs_pb2,
    node_manager_pb2,
    node_manager_pb2_grpc,
    common_pb2,
)
from ray.dashboard.modules.reporter import reporter_consts
from ray.dashboard.modules.node import node_consts
from ray._private import ray_constants
from ray.dashboard.consts import (
    GCS_RPC_TIMEOUT_SECONDS,
    DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
    DASHBOARD_AGENT_ADDR_IP_PREFIX,
)
from ray._private.gcs_pubsub import (
    GcsAioNodeInfoSubscriber,
    GcsAioResourceUsageSubscriber,
)

logger = logging.getLogger(__name__)


@dataclass
class WorkerInfo:
    # From node_manager_pb2_grpc.NodeManagerServiceStub.GetNodeStats
    core_worker_stats: common_pb2.CoreWorkerStats
    # From GcsAioResourceUsageSubscriber
    node_physical_stats_for_worker: Dict[str, Any]


class NodeDataIndex:
    """
    A class that takes update about nodes and workers and updates the index.

    Available indexes:
    - `nodes`: node_id -> GcsNodeInfo
    - `node_pid_worker_id`: node_id -> pid -> worker_id
    - `node_physical_stats`: node_id -> dict (JSON)
    - `workers`: worker_id -> {core_worker_stats: node_manager_pb2.CoreWorkerStats}

    All indexes and data are typed:
    - node_id: NodeID
    - pid: int
    - worker_id: WorkerID
    - node_info: GcsNodeInfo
    - core_worker_stats: node_manager_pb2.CoreWorkerStats

    Data sources:
    - GcsAioResourceUsageSubscriber
    - GcsAioNodeInfoSubscriber
    - NodeManagerServiceStub

    Method Naming
    - subscribe_and_update_: subscribes and updates the data. Never returns.
    - subscribe_for_: only subscribes and yields data. Never returns.
    - poll_and_update_: periodically polls the data. Never returns.
    - poll_once_and_update_: polls once and updates the data.
    - poll_once_: polls once and returns data.
    - update_: only updates the data. The data comes from elsewhere.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        executor: concurrent.futures.Executor,
        gcs_address: str,
        module_start_time: float,
    ):
        self.loop = loop
        self.executor = executor
        self.gcs_address = gcs_address
        self.collect_memory_info = False

        # Data
        # From GcsAioNodeInfoSubscriber
        self.nodes: Dict[NodeID, gcs_pb2.GcsNodeInfo] = {}
        # From GcsAioResourceUsageSubscriber
        self.node_physical_stats: Dict[NodeID, Dict] = {}
        # From node_manager_pb2_grpc.NodeManagerServiceStub.GetNodeStats
        self.node_pid_worker_id: Dict[NodeID, Dict[int, WorkerID]] = {}
        # See `self.WorkerInfo`
        self.workers: Dict[WorkerID, WorkerInfo] = {}
        # From node_manager_pb2_grpc.NodeManagerServiceStub.GetNodeStats
        # core_workers_stats is STRIPPED out as they are put into `self.workers`.
        self.node_stats_without_core_worker_stats: Dict[
            NodeID, node_manager_pb2.GetNodeStatsReply
        ] = {}

        # Internal states
        # Queue of dead nodes, up to `node_consts.MAX_DEAD_NODES_TO_CACHE`.
        # Once the queue is full, the oldest node is removed from the queue, and the
        # data in `self.nodes` and other fields is removed.
        self.dead_node_queue: deque[NodeID] = deque()

        # Connections
        self.node_manager_stubs: Dict[
            NodeID, node_manager_pb2_grpc.NodeManagerServiceStub
        ] = {}

        # Stats
        # Timestamp of the module start.
        self.module_start_time = module_start_time
        # Timestamp of the head node registration.
        self.head_node_registration_time: float = None

    @property
    def module_lifetime_s(self):
        return time.time() - self.module_start_time

    @property
    def head_node_registration_time_s(self):
        if self.head_node_registration_time is None:
            return None
        return self.head_node_registration_time - self.module_start_time

    async def run(self):
        await asyncio.gather(
            self.subscribe_and_update_nodes(),
            self.subscribe_and_update_node_physical_stats(),
            self.poll_and_update_core_worker_stats(),
        )

    async def subscribe_and_update_node_physical_stats(self):
        """
        Subscribe to the GCS resource usage.

        Reads:
        - `self.node_pid_worker_id`

        Updates:
        - `self.node_physical_stats`
        - `self.workers[worker_id].node_physical_stats_for_worker`

        Subscriber: GcsAioResourceUsageSubscriber
        Key: b'RAY_REPORTER:{node id hex}',
        Value: JSON bytes. Created in ReporterAgent._collect_stats().
        Notable fields:
        - `workers`: List[dict], where worker["pid"] is used to identify the worker.
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
                parsed_data = await self.loop.run_in_executor(
                    self.executor, json.loads, data
                )

                node_id_hex = key[len(reporter_consts.REPORTER_PREFIX) :]
                node_id = NodeID(node_id_hex)
                self.node_physical_stats[node_id] = parsed_data
                self.update_workers_node_physical_stats_for_node(node_id, parsed_data)
            except Exception:
                logger.exception("Error receiving node physical stats.")

    def remove_node_id(self, node_id: NodeID):
        """Remove the node ID and its workers from all indexes. This does not remove the
        node from self.node_manager_stubs since that should happen immediately in
        `update_node`.
        """
        self.nodes.pop(node_id)
        self.node_physical_stats.pop(node_id)
        self.node_stats_without_core_worker_stats.pop(node_id)
        pid_worker_id = self.node_pid_worker_id.pop(node_id, {})
        for pid in pid_worker_id:
            worker_id = pid_worker_id[pid]
            self.workers.pop(worker_id)

    def create_node_manager_stub(self, node_info: gcs_pb2.GcsNodeInfo):
        """
        Creates a stub for the node manager.
        """
        address = f"{node_info.node_manager_address}:{node_info.node_manager_port}"
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        channel = ray._private.utils.init_grpc_channel(
            address, options, asynchronous=True
        )
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        return stub

    async def update_node(self, node: gcs_pb2.GcsNodeInfo):
        """
        Unconditionally updates self.nodes[node_id].

        If the node is alive:
        - If not already, adds the node ID to self.node_manager_stubs.
        - If not already, and if the node is the head node, puts the node ID in the internal KV.

        If the node is dead:
        - Removes ASHBOARD_AGENT_ADDR_* from internal KV. These keys were added by agent.py.
        - Adds the node ID to self.dead_node_queue. If the queue is full, the oldest node is removed from the queue with its all data.
        - Removes the node from self.node_manager_stubs.
        """
        node_id = NodeID(node.node_id)
        self.nodes[node_id] = node

        is_alive = node.state == gcs_pb2.GcsNodeInfo.GcsNodeState.ALIVE
        if is_alive:
            # Create the stub.
            if node_id not in self.node_manager_stubs:
                self.node_manager_stubs[node_id] = self.create_node_manager_stub(node)
            if node.is_head_node and not self.head_node_registration_time:
                self.head_node_registration_time = time.time()
                # Put head node ID in the internal KV to be read by JobAgent.
                # TODO(architkulkarni): Remove once State API exposes which
                # node is the head node.
                await self.gcs_aio_client.internal_kv_put(
                    ray_constants.KV_HEAD_NODE_ID_KEY,
                    node_id.hex(),
                    overwrite=True,
                    namespace=ray_constants.KV_NAMESPACE_JOB,
                    timeout=GCS_RPC_TIMEOUT_SECONDS,
                )
        else:  # i.e. if not is_alive:
            # Remove the agent address from the internal KV.
            keys = [
                f"{DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id.hex()}",
                f"{DASHBOARD_AGENT_ADDR_IP_PREFIX}{node.node_manager_address}",
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

            self.dead_node_queue.append(node_id)
            if len(self.dead_node_queue) > node_consts.MAX_DEAD_NODES_TO_CACHE:
                self.remove_node_id(self.dead_node_queue.popleft())

            self.node_manager_stubs.pop(node_id)

    async def subscribe_and_update_nodes(self):
        """
        Subscribe to node updates and update the internal states. If the head node is
        not registered after RAY_DASHBOARD_HEAD_NODE_REGISTRATION_TIMEOUT, it logs a
        warning only once.
        """
        warning_shown = False
        async for node in self.subscribe_for_nodes():
            await self.update_node(node)
            if not self.head_node_registration_time:
                # head node is not registered yet
                if (
                    not warning_shown
                    and (time.time() - self.module_start_time)
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

    async def subscribe_for_nodes(self) -> AsyncGenerator[dict, None]:
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

        for node_info in all_node_info.values():
            yield node_info

        while True:
            try:
                node_info_updates = await subscriber.poll(
                    batch_size=node_consts.RAY_DASHBOARD_NODE_SUBSCRIBER_POLL_SIZE
                )
                for _, node_info in node_info_updates:
                    yield node_info
            except Exception:
                logger.exception("Failed handling updated nodes.")

    def update_workers_node_physical_stats_for_node(
        self, node_id: NodeID, node_physical_stats: Dict
    ):
        """
        For workers whose (pid -> worker_id) is known, updates self.workers[worker_id].node_physical_stats_for_worker.
        """
        pid_worker_id = self.node_pid_worker_id.get(node_id, {})
        if not pid_worker_id:
            return
        for worker in node_physical_stats["workers"]:
            pid = worker["pid"]
            worker_id = pid_worker_id.get(pid, None)
            if worker_id is None:
                continue
            if worker_id not in self.workers:
                self.workers[worker_id] = WorkerInfo(
                    core_worker_stats=None,
                    node_physical_stats_for_worker=worker,
                )
            else:
                self.workers[worker_id].node_physical_stats_for_worker = worker

    def update_core_worker_stats(
        self, node_id: NodeID, core_worker_stats: common_pb2.CoreWorkerStats
    ):
        """
        Updates self.workers[worker_id] for the given node.
        """
        worker_id = WorkerID(core_worker_stats.worker_id)
        pid = core_worker_stats.pid
        if worker_id in self.workers:
            self.workers[worker_id].core_worker_stats = core_worker_stats
        else:
            self.workers[worker_id] = WorkerInfo(
                core_worker_stats=core_worker_stats,
                node_physical_stats_for_worker=None,
            )
        self.node_pid_worker_id[node_id][pid] = worker_id
        if node_id in self.node_physical_stats:
            self.update_workers_node_physical_stats_for_node(
                node_id, self.node_physical_stats[node_id]
            )

    async def poll_once_and_update_core_worker_stats_for_node(
        self, node_id: NodeID, stub: node_manager_pb2_grpc.NodeManagerServiceStub
    ):
        """
        Polls once for node stats. Logs errors if any.
        """
        timeout = max(2, node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS - 1)
        if self.nodes[node_id].state != gcs_pb2.GcsNodeInfo.GcsNodeState.ALIVE:
            return

        try:
            reply = await stub.GetNodeStats(
                node_manager_pb2.GetNodeStatsRequest(
                    include_memory_info=self.collect_memory_info
                ),
                timeout=timeout,
            )
            if node_id not in self.node_pid_worker_id:
                self.node_pid_worker_id[node_id] = {}
            for core_worker_stats in reply.core_workers_stats:
                self.update_core_worker_stats(node_id, core_worker_stats)
            reply.core_workers_stats = []
            self.node_stats_without_core_worker_stats[node_id] = reply
        except asyncio.CancelledError:
            return
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                message = (
                    f"Cannot reach the node, {node_id}, after timeout "
                    f" {timeout}. This node may have been overloaded, "
                    "terminated, or the network is slow."
                )
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                message = (
                    f"Cannot reach the node, {node_id}. "
                    "The node may have been terminated."
                )
            else:
                message = f"Error updating node stats of {node_id}."
            logger.error(message, exc_info=e)
        except Exception as e:
            logger.error(f"Error updating node stats of {node_id}.", exc_info=e)

    # @async_loop_forever(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def poll_once_and_update_core_worker_stats(self):
        """
        Polls every NodeManagerServiceStub once for core worker stats. Since we may have
        a large number of stubs, we poll in chunks of 100 stubs at a time.
        """

        # Copy the list to prevent the list from being modified during the iteration.
        node_id_stubs = list(self.node_manager_stubs.items())
        all_results = []
        # NOTE: We're chunking up fetching of the stats to run in batches of no more
        #       than 100 nodes at a time to avoid flooding the event-loop's queue
        #       with potentially a large, uninterrupted sequence of tasks updating
        #       the node stats for very large clusters.
        for chunk in split(node_id_stubs, 100):
            tasks = [
                self.poll_once_and_update_core_worker_stats_for_node(node_id, stub)
                for node_id, stub in chunk
            ]
            all_results.extend(await asyncio.gather(*tasks, return_exceptions=True))
            # We're doing short (25ms) yield after every chunk to make sure
            #   - We're not overloading the event-loop with excessive # of tasks
            #   - Allowing 10k nodes stats fetches be sent out performed in 2.5s
            await asyncio.sleep(0.025)

    async def poll_and_update_core_worker_stats(self):
        """
        Polls every NodeManagerServiceStub for core worker stats. After all stubs are
        polled, and then after NODE_STATS_UPDATE_INTERVAL_SECONDS, we poll again.
        """
        while True:
            await self.poll_once_and_update_core_worker_stats()
            await asyncio.sleep(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
