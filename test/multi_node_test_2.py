from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import pytest

import ray
import ray.services as services
from ray.test.cluster_utils import Cluster

logger = logging.getLogger(__name__)


@pytest.fixture
def start_connected_cluster():
    # Start the Ray processes.
    g = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "resources": dict(CPU=1),
            "_internal_config": json.dumps({
                "num_heartbeats_timeout": 10
            })
        })
    yield g
    # The code after the yield will run as teardown code.
    ray.shutdown()
    g.shutdown()


@pytest.fixture
def start_connected_longer_cluster():
    """Creates a cluster with a longer timeout."""
    g = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "resources": dict(CPU=1),
            "_internal_config": json.dumps({
                "num_heartbeats_timeout": 20
            })
        })
    yield g
    # The code after the yield will run as teardown code.
    ray.shutdown()
    g.shutdown()


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


def test_internal_config(start_connected_longer_cluster):
    """Checks that the internal configuration setting works.

    We set the cluster to timeout nodes after 2 seconds of no timeouts. We
    then remove a node, wait for 1 second to check that the cluster is out
    of sync, then wait another 2 seconds (giving 1 second of leeway) to check
    that the client has timed out.
    """
    cluster = start_connected_longer_cluster
    worker = cluster.add_node()
    cluster.wait_for_nodes()

    cluster.remove_node(worker)
    cluster.wait_for_nodes(retries=10)
    assert ray.global_state.cluster_resources()["CPU"] == 2

    cluster.wait_for_nodes(retries=20)
    assert ray.global_state.cluster_resources()["CPU"] == 1


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

    assert ray.global_state.cluster_resources()["CPU"] == 1
    worker2 = cluster.add_node()
    cluster.wait_for_nodes()
    cluster.remove_node(worker2)
    cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1


def test_worker_plasma_store_failure(start_connected_cluster):
    cluster = start_connected_cluster
    worker = cluster.add_node()
    cluster.wait_for_nodes()
    # Log monitor doesn't die for some reason
    worker.kill_log_monitor()
    worker.kill_plasma_store()
    worker.process_dict[services.PROCESS_TYPE_RAYLET][0].wait()
    assert not worker.any_processes_alive(), worker.live_processes()
