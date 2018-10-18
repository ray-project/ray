from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
import ray.services as services
from ray.test.cluster_utils import Cluster

logger = logging.getLogger(__name__)


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


def test_wait_for_nodes():
    """Unit test for `Cluster.wait_for_nodes`.

    Adds 4 workers, waits, then removes 4 workers, waits,
    then adds 1 worker, waits, and removes 1 worker, waits.
    """
    g = Cluster(initialize_head=True, connect=True)
    workers = [g.add_node() for i in range(4)]
    g.wait_for_nodes()
    [g.remove_node(w) for w in workers]
    g.wait_for_nodes()
    worker2 = g.add_node()
    g.wait_for_nodes()
    g.remove_node(worker2)
    g.wait_for_nodes()
    ray.shutdown()
    g.shutdown()


def test_worker_plasma_store_failure():
    g = Cluster(initialize_head=True, connect=True)
    worker = g.add_node()
    g.wait_for_nodes()
    # Log monitor doesn't die for some reason
    worker.kill_log_monitor()
    worker.kill_plasma_store()
    worker.process_dict[services.PROCESS_TYPE_RAYLET][0].wait()
    assert not worker.any_processes_alive(), worker.live_processes()
    print("Success")
    g.shutdown()
