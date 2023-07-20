# Utils in unit tests that involves grpc.
# NOT used in minimal.

import asyncio
import logging
import time
import pytest
import numpy as np
import grpc
from grpc._channel import _InactiveRpcError

import ray
import ray._private.ray_constants as ray_constants
from ray.core.generated import (
    node_manager_pb2,
    node_manager_pb2_grpc,
    gcs_service_pb2,
    gcs_service_pb2_grpc,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.cluster_utils import AutoscalingCluster


# Send a RPC to the raylet to have it self-destruct its process.
def kill_raylet(raylet, graceful=False):
    raylet_address = f'{raylet["NodeManagerAddress"]}:{raylet["NodeManagerPort"]}'
    channel = grpc.insecure_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    try:
        stub.ShutdownRaylet(node_manager_pb2.ShutdownRayletRequest(graceful=graceful))
    except _InactiveRpcError:
        assert not graceful


# Gets resource usage assuming gcs is local.
def get_resource_usage(gcs_address, timeout=10):
    if not gcs_address:
        gcs_address = ray.worker._global_node.gcs_address

    gcs_channel = ray._private.utils.init_grpc_channel(
        gcs_address, ray_constants.GLOBAL_GRPC_OPTIONS, asynchronous=False
    )

    gcs_node_resources_stub = gcs_service_pb2_grpc.NodeResourceInfoGcsServiceStub(
        gcs_channel
    )

    request = gcs_service_pb2.GetAllResourceUsageRequest()
    response = gcs_node_resources_stub.GetAllResourceUsage(request, timeout=timeout)
    resources_batch_data = response.resource_usage_data

    return resources_batch_data


# Get node stats from node manager.
def get_node_stats(raylet, num_retry=5, timeout=2):
    raylet_address = f'{raylet["NodeManagerAddress"]}:{raylet["NodeManagerPort"]}'
    channel = ray._private.utils.init_grpc_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    for _ in range(num_retry):
        try:
            reply = stub.GetNodeStats(
                node_manager_pb2.GetNodeStatsRequest(), timeout=timeout
            )
            break
        except grpc.RpcError:
            continue
    assert reply is not None
    return reply


def get_and_run_node_killer(
    node_kill_interval_s,
    namespace=None,
    lifetime=None,
    no_start=False,
    max_nodes_to_kill=2,
):
    assert ray.is_initialized(), "The API is only available when Ray is initialized."

    @ray.remote(num_cpus=0)
    class NodeKillerActor:
        def __init__(
            self,
            head_node_id,
            node_kill_interval_s: float = 60,
            max_nodes_to_kill: int = 2,
        ):
            self.node_kill_interval_s = node_kill_interval_s
            self.is_running = False
            self.head_node_id = head_node_id
            self.killed_nodes = set()
            self.done = ray._private.utils.get_or_create_event_loop().create_future()
            self.max_nodes_to_kill = max_nodes_to_kill
            # -- logger. --
            logging.basicConfig(level=logging.INFO)

        def ready(self):
            pass

        async def run(self):
            self.is_running = True
            while self.is_running:
                node_to_kill_ip = None
                node_to_kill_port = None
                while node_to_kill_port is None and self.is_running:
                    nodes = ray.nodes()
                    alive_nodes = self._get_alive_nodes(nodes)
                    for node in nodes:
                        node_id = node["NodeID"]
                        # make sure at least 1 worker node is alive.
                        if (
                            node["Alive"]
                            and node_id != self.head_node_id
                            and node_id not in self.killed_nodes
                            and alive_nodes > 2
                        ):
                            node_to_kill_ip = node["NodeManagerAddress"]
                            node_to_kill_port = node["NodeManagerPort"]
                            break
                    # Give the cluster some time to start.
                    await asyncio.sleep(0.1)

                if not self.is_running:
                    break

                sleep_interval = np.random.rand() * self.node_kill_interval_s
                time.sleep(sleep_interval)

                if node_to_kill_port is not None:
                    try:
                        self._kill_raylet(
                            node_to_kill_ip, node_to_kill_port, graceful=False
                        )
                    except Exception:
                        pass
                    logging.info(
                        f"Killed node {node_id} at address: "
                        f"{node_to_kill_ip}, port: {node_to_kill_port}"
                    )
                    self.killed_nodes.add(node_id)
                if len(self.killed_nodes) >= self.max_nodes_to_kill:
                    break
                await asyncio.sleep(self.node_kill_interval_s - sleep_interval)

            self.done.set_result(True)

        async def stop_run(self):
            was_running = self.is_running
            self.is_running = False
            return was_running

        async def get_total_killed_nodes(self):
            """Get the total number of killed nodes"""
            await self.done
            return self.killed_nodes

        def _kill_raylet(self, ip, port, graceful=False):
            raylet_address = f"{ip}:{port}"
            channel = grpc.insecure_channel(raylet_address)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            try:
                stub.ShutdownRaylet(
                    node_manager_pb2.ShutdownRayletRequest(graceful=graceful)
                )
            except _InactiveRpcError:
                assert not graceful

        def _get_alive_nodes(self, nodes):
            alive_nodes = 0
            for node in nodes:
                if node["Alive"]:
                    alive_nodes += 1
            return alive_nodes

    head_node_id = ray.get_runtime_context().get_node_id()
    # Schedule the actor on the current node.
    node_killer = NodeKillerActor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False
        ),
        namespace=namespace,
        name="node_killer",
        lifetime=lifetime,
    ).remote(
        head_node_id,
        node_kill_interval_s=node_kill_interval_s,
        max_nodes_to_kill=max_nodes_to_kill,
    )
    print("Waiting for node killer actor to be ready...")
    ray.get(node_killer.ready.remote())
    print("Node killer actor is ready now.")
    if not no_start:
        node_killer.run.remote()
    return node_killer


def _ray_start_chaos_cluster(request):
    param = getattr(request, "param", {})
    kill_interval = param.pop("kill_interval", None)
    config = param.pop("_system_config", {})
    config.update(
        {
            "task_retry_delay_ms": 100,
        }
    )
    # Config of workers that are re-started.
    head_resources = param.pop("head_resources")
    worker_node_types = param.pop("worker_node_types")
    cluster = AutoscalingCluster(
        head_resources,
        worker_node_types,
        idle_timeout_minutes=10,  # Don't take down nodes.
        **param,
    )
    cluster.start(_system_config=config)
    ray.init("auto")
    nodes = ray.nodes()
    assert len(nodes) == 1

    if kill_interval is not None:
        node_killer = get_and_run_node_killer(kill_interval)

    yield cluster

    if kill_interval is not None:
        ray.get(node_killer.stop_run.remote())
        killed = ray.get(node_killer.get_total_killed_nodes.remote())
        assert len(killed) > 0
        died = {node["NodeID"] for node in ray.nodes() if not node["Alive"]}
        assert died.issubset(
            killed
        ), f"Raylets {died - killed} that we did not kill crashed"

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_chaos_cluster(request):
    """Returns the cluster and chaos thread."""
    for x in _ray_start_chaos_cluster(request):
        yield x
