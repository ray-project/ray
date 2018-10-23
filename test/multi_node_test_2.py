from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import pytest

import ray
import ray.services as services
from ray.test.cluster_utils import Cluster

logger = logging.getLogger(__name__)


@pytest.fixture
def start_connected_cluster():
    # Start the Ray processes.
    g = Cluster(initialize_head=True, connect=True)
    yield g
    # The code after the yield will run as teardown code.
    ray.shutdown()
    g.shutdown()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only works with xray.")
def test_cluster():
    """Basic test for adding and removing nodes in cluster."""
    g = Cluster(initialize_head=False)
    node = g.add_node()
    node2 = g.add_node()
    assert node.all_processes_alive()
    assert node2.all_processes_alive()
    g.remove_node(node2)
    g.remove_node(node)
    assert not any(node.any_processes_alive() for node in g.list_all_nodes())


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only works with xray.")
def test_wait_for_nodes(start_connected_cluster):
    """Unit test for `Cluster.wait_for_nodes`.

    Adds 4 workers, waits, then removes 4 workers, waits,
    then adds 1 worker, waits, and removes 1 worker, waits.
    """
    cluster = start_connected_cluster
    workers = [cluster.add_node() for i in range(4)]
    cluster.wait_for_nodes()
    [cluster.remove_node(w) for w in workers]
    cluster.wait_for_nodes()
    worker2 = cluster.add_node()
    cluster.wait_for_nodes()
    cluster.remove_node(worker2)
    cluster.wait_for_nodes()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only works with xray.")
def test_worker_plasma_store_failure(start_connected_cluster):
    cluster = start_connected_cluster
    worker = cluster.add_node()
    cluster.wait_for_nodes()
    # Log monitor doesn't die for some reason
    worker.kill_log_monitor()
    worker.kill_plasma_store()
    worker.process_dict[services.PROCESS_TYPE_RAYLET][0].wait()
    assert not worker.any_processes_alive(), worker.live_processes()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only works with xray.")
def test_actor_reconstruction(start_connected_cluster):
    """Test actor reconstruction when node dies unexpectedly."""
    cluster = start_connected_cluster
    # Add a few nodes to the cluster.
    # Use custom resource to make sure the actor is only created on worker
    # nodes, not on the head node.
    nodes = [cluster.add_node(resources={'a': 1}) for _ in range(4)]

    # This actor will be reconstructed at most once.
    @ray.remote(max_reconstructions=1, resources={'a': 1})
    class MyActor(object):
        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

        def get_object_store_socket(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    # This actor will be created on the only node in the cluster.
    actor = MyActor.remote()

    def kill_node():
        # Kill the node that the actor reside on.
        # Return node's object store socket name.
        object_store_socket = ray.get(actor.get_object_store_socket.remote())
        node_to_remove = None
        for node in cluster.worker_nodes:
            object_store_sockets = [
                address.name
                for address in node.address_info['object_store_addresses']
            ]
            if object_store_socket in object_store_sockets:
                node_to_remove = node
        cluster.remove_node(node_to_remove)
        return object_store_socket

    # Call increase 3 times
    for _ in range(3):
        ray.get(actor.increase.remote())

    # Kill actor's node and the actor should be reconstructed on another node
    object_store_socket1 = kill_node()

    # Call increase again.
    # Check that actor is reconstructed and value is 4.
    assert ray.get(actor.increase.remote()) == 4

    # Kill the node again.
    object_store_socket2 = kill_node()
    # Check the actor was created on a different node.
    assert object_store_socket1 != object_store_socket2

    # The actor has exceeded max reconstructions, and this task should fail.
    with pytest.raises(ray.worker.RayGetError):
        ray.get(actor.increase.remote())
