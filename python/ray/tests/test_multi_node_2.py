from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pytest
import time

import ray
import ray.ray_constants as ray_constants
from ray.monitor import Monitor
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
    assert ray.cluster_resources()["CPU"] == 2

    time.sleep(2)
    assert ray.cluster_resources()["CPU"] == 1


def setup_monitor(redis_address):
    monitor = Monitor(redis_address, None)
    monitor.subscribe(ray.gcs_utils.XRAY_HEARTBEAT_BATCH_CHANNEL)
    monitor.subscribe(ray.gcs_utils.XRAY_JOB_CHANNEL)  # TODO: Remove?
    monitor.update_raylet_map(_append_port=True)
    monitor._maybe_flush_gcs()
    return monitor


def verify_load_metrics(monitor, expected_resource_usage=None, timeout=10):
    while True:
        monitor.process_messages()
        resource_usage = monitor.load_metrics.get_resource_usage()

        if "memory" in resource_usage[1]:
            del resource_usage[1]["memory"]
        if "object_store_memory" in resource_usage[2]:
            del resource_usage[1]["object_store_memory"]
        if "memory" in resource_usage[2]:
            del resource_usage[2]["memory"]
        if "object_store_memory" in resource_usage[2]:
            del resource_usage[2]["object_store_memory"]

        if expected_resource_usage is None:
            if all(x for x in resource_usage[1:]):
                break
        elif all(x == y
                 for x, y in zip(resource_usage, expected_resource_usage)):
            break
        else:
            timeout -= 1
            time.sleep(1)

        if timeout <= 0:
            raise ValueError("Timeout. {} != {}".format(
                resource_usage, expected_resource_usage))

    return resource_usage


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 1,
    }, {
        "num_cpus": 2,
    }],
    indirect=True)
def test_heartbeats_single(ray_start_cluster_head):
    """Unit test for `Cluster.wait_for_nodes`.

    Test proper metrics.
    """
    cluster = ray_start_cluster_head
    timeout = 5
    monitor = setup_monitor(cluster.redis_address)
    total_cpus = ray.state.cluster_resources()["CPU"]
    verify_load_metrics(monitor, (0.0, {"CPU": 0.0}, {"CPU": total_cpus}))

    @ray.remote
    def work(timeout):
        time.sleep(timeout)
        return True

    work_handle = work.remote(timeout * 2)
    verify_load_metrics(monitor, (1.0 / total_cpus, {
        "CPU": 1.0
    }, {
        "CPU": total_cpus
    }))
    ray.get(work_handle)

    @ray.remote
    class Actor(object):
        def work(self, timeout):
            time.sleep(timeout)
            return True

    test_actor = Actor.remote()
    work_handle = test_actor.work.remote(timeout * 2)

    verify_load_metrics(monitor, (1.0 / total_cpus, {
        "CPU": 1.0
    }, {
        "CPU": total_cpus
    }))

    ray.get(work_handle)


def test_heartbeats_cluster(ray_start_cluster_head):
    """Unit test for `Cluster.wait_for_nodes`.

    Test proper metrics.
    """
    cluster = ray_start_cluster_head
    timeout = 5
    num_workers_nodes = 4
    num_nodes_total = int(num_workers_nodes + 1)
    [cluster.add_node() for i in range(num_workers_nodes)]
    cluster.wait_for_nodes()
    monitor = setup_monitor(cluster.redis_address)

    verify_load_metrics(monitor, (0.0, {"CPU": 0.0}, {"CPU": num_nodes_total}))

    @ray.remote
    class Actor(object):
        def work(self, timeout):
            time.sleep(timeout)
            return True

    test_actors = [Actor.remote() for i in range(num_nodes_total)]

    work_handles = [actor.work.remote(timeout * 2) for actor in test_actors]

    verify_load_metrics(monitor, (num_nodes_total, {
        "CPU": num_nodes_total
    }, {
        "CPU": num_nodes_total
    }))

    ray.get(work_handles)
    verify_load_metrics(monitor, (0.0, {"CPU": 0.0}, {"CPU": num_nodes_total}))
    ray.shutdown()


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

    assert ray.cluster_resources()["CPU"] == 1
    worker2 = cluster.add_node()
    cluster.wait_for_nodes()
    cluster.remove_node(worker2)
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1


def test_worker_plasma_store_failure(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    worker = cluster.add_node()
    cluster.wait_for_nodes()
    # Log monitor doesn't die for some reason
    worker.kill_log_monitor()
    worker.kill_reporter()
    worker.kill_plasma_store()
    worker.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.wait()
    assert not worker.any_processes_alive(), worker.live_processes()
