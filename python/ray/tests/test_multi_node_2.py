from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pytest
import time

import ray
import ray.ray_constants as ray_constants
from ray.tests.cluster_utils import Cluster
from ray.tests.conftest import generate_internal_config_map

logger = logging.getLogger(__name__)


def test_cluster():
    """Basic test for adding and removing nodes in cluster."""
    g = Cluster(initialize_head=False)
    node = g.add_node()
    node2 = g.add_node()
    assert node.remaining_processes_alive()
    assert node2.remaining_processes_alive()
    g.remove_node(node2)
    g.remove_node(node)
    assert not any(n.any_processes_alive() for n in [node, node2])


def test_shutdown():
    g = Cluster(initialize_head=False)
    node = g.add_node()
    node2 = g.add_node()
    g.shutdown()
    assert not any(n.any_processes_alive() for n in [node, node2])


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [generate_internal_config_map(num_heartbeats_timeout=20)],
    indirect=True)
def test_internal_config(ray_start_cluster_head):
    """Checks that the internal configuration setting works.

    We set the cluster to timeout nodes after 2 seconds of no timeouts. We
    then remove a node, wait for 1 second to check that the cluster is out
    of sync, then wait another 2 seconds (giving 1 second of leeway) to check
    that the client has timed out.
    """
    cluster = ray_start_cluster_head
    worker = cluster.add_node()
    cluster.wait_for_nodes()

    cluster.remove_node(worker)
    time.sleep(1)
    assert ray.global_state.cluster_resources()["CPU"] == 2

    time.sleep(2)
    assert ray.global_state.cluster_resources()["CPU"] == 1


def test_wait_for_nodes(ray_start_cluster_head):
    """Unit test for `Cluster.wait_for_nodes`.

    Adds 4 workers, waits, then removes 4 workers, waits,
    then adds 1 worker, waits, and removes 1 worker, waits.
    """
    cluster = ray_start_cluster_head
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


def test_worker_plasma_store_failure(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    worker = cluster.add_node()
    cluster.wait_for_nodes()
    # Log monitor doesn't die for some reason
    worker.kill_log_monitor()
    worker.kill_plasma_store()
    worker.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.wait()
    assert not worker.any_processes_alive(), worker.live_processes()
