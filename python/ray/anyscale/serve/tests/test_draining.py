import asyncio

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray.serve._private.common import ReplicaState
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve.context import get_global_client
from ray.serve.schema import ServeInstanceDetails
from ray.tests.conftest import *  # noqa


def test_draining_with_traffic(monkeypatch, ray_start_cluster):
    """Test that serve draining won't fail requests."""
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "1")
    cluster = ray_start_cluster
    # head node
    cluster.add_node(num_cpus=0, resources={"head": 1})
    ray.init(address=cluster.address)
    # worker nodes
    cluster.add_node(num_cpus=1, resources={"worker1": 1})
    cluster.add_node(num_cpus=1, resources={"worker2": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    worker_node_ids = {
        ray.get(get_node_id.options(resources={"worker1": 1}).remote()),
        ray.get(get_node_id.options(resources={"worker2": 1}).remote()),
    }

    signal_actor = SignalActor.options(resources={"head": 0.1}).remote()

    @serve.deployment
    class Deployment:
        def __init__(self, signal_actor):
            self._signal_actor = signal_actor

        async def __call__(self):
            await self._signal_actor.wait.remote()
            return ray.get_runtime_context().get_node_id()

    # We should have 1 replicas on 1 worker node
    serve.run(
        Deployment.options(name="deployment", num_replicas=1).bind(signal_actor),
        name="app",
    )

    client = get_global_client()

    def get_replicas_of_state(client, replica_state):
        serve_details = ServeInstanceDetails(
            **ray.get(client._controller.get_serve_instance_details.remote())
        )
        return {
            replica.replica_id: replica.node_id
            for replica in serve_details.applications["app"]
            .deployments["deployment"]
            .replicas
            if replica.state == replica_state
        }

    running_replicas = get_replicas_of_state(client, ReplicaState.RUNNING)
    assert len(running_replicas) == 1

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    draining_node_id = list(running_replicas.values())[0]

    @ray.remote(num_cpus=0, resources={"head": 0.1})
    class RequestSenderActor:
        def __init__(self):
            self._stop_event = asyncio.Event()

        async def run(self):
            while not self._stop_event.is_set():
                res = requests.get("http://localhost:8000/deployment/")
                res.raise_for_status()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=0.1)
                except asyncio.exceptions.TimeoutError:
                    pass

        async def stop(self):
            self._stop_event.set()

    request_sender_actor = RequestSenderActor.remote()
    run_obj_ref = request_sender_actor.run.remote()

    # Wait until request reaches the replica.
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) > 0)

    # Simulate spot instance preemption
    is_accepted = gcs_client.drain_node(
        draining_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "spot instance preemption",
    )
    assert is_accepted

    # Wait until the replica is being drained.
    wait_for_condition(
        lambda: len(get_replicas_of_state(client, ReplicaState.STOPPING)) == 1
    )

    # Resume the request handling on the replica so the node can be drained.
    ray.get(signal_actor.send.remote())

    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)

    def check_node_is_drained(node_id):
        cluster_node_info_cache.update()
        return node_id not in cluster_node_info_cache.get_alive_node_ids()

    wait_for_condition(lambda: check_node_is_drained(draining_node_id), timeout=20)

    # The drained replica should be recreated on the third worker.
    expected_replica_node_ids = worker_node_ids - {draining_node_id}

    wait_for_condition(
        lambda: set(get_replicas_of_state(client, ReplicaState.RUNNING).values())
        == expected_replica_node_ids
    )

    # Make sure new requests don't go to the drained nodes.
    for _ in range(5):
        res = requests.get("http://localhost:8000/deployment/")
        res.raise_for_status()
        assert res.text in expected_replica_node_ids

    # Make sure no requests fail due to node draining.
    request_sender_actor.stop.remote()
    ray.get(run_obj_ref)

    serve.shutdown()


def test_draining_without_traffic(monkeypatch, ray_start_cluster):
    """Test that serve can be drained without traffic."""
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "1")
    cluster = ray_start_cluster
    # head node
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    # worker nodes
    cluster.add_node(num_cpus=1, resources={"worker1": 1})
    cluster.add_node(num_cpus=1, resources={"worker2": 1})
    cluster.add_node(num_cpus=1, resources={"worker3": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    worker_node_ids = {
        ray.get(get_node_id.options(resources={"worker1": 1}).remote()),
        ray.get(get_node_id.options(resources={"worker2": 1}).remote()),
        ray.get(get_node_id.options(resources={"worker3": 1}).remote()),
    }

    @serve.deployment
    class Deployment:
        def __call__(self):
            return "hello"

    # We should have 2 replicas on 2 worker nodes
    serve.run(Deployment.options(name="deployment", num_replicas=2).bind(), name="app")

    client = get_global_client()
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    replica_node_ids = {
        replica.node_id
        for replica in serve_details.applications["app"]
        .deployments["deployment"]
        .replicas
        if replica.state == ReplicaState.RUNNING
    }
    assert len(replica_node_ids) == 2
    replica_node_ids = list(replica_node_ids)

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    draining_node_id = replica_node_ids[0]

    # Simulate spot instance preemption
    is_accepted = gcs_client.drain_node(
        draining_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "spot instance preemption",
    )
    assert is_accepted

    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)

    def check_node_is_drained(node_id):
        cluster_node_info_cache.update()
        return node_id not in cluster_node_info_cache.get_alive_node_ids()

    wait_for_condition(lambda: check_node_is_drained(draining_node_id))

    # The drained replica should be recreated on the third worker.
    expected_replica_node_ids = worker_node_ids - {draining_node_id}

    def check_replica_node_ids(expected_replica_node_ids):
        serve_details = ServeInstanceDetails(
            **ray.get(client._controller.get_serve_instance_details.remote())
        )
        replica_node_ids = {
            replica.node_id
            for replica in serve_details.applications["app"]
            .deployments["deployment"]
            .replicas
            if replica.state == ReplicaState.RUNNING
        }
        return replica_node_ids == expected_replica_node_ids

    wait_for_condition(lambda: check_replica_node_ids(expected_replica_node_ids))

    serve.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
