import logging
import pytest
import time

import ray
import ray.ray_constants as ray_constants
from ray.monitor import Monitor
from ray.cluster_utils import Cluster
from ray.test_utils import generate_internal_config_map, SignalActor

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
    "ray_start_cluster_head", [
        generate_internal_config_map(
            num_heartbeats_timeout=20,
            initial_reconstruction_timeout_milliseconds=12345)
    ],
    indirect=True)
def test_internal_config(ray_start_cluster_head):
    """Checks that the internal configuration setting works.

    We set the cluster to timeout nodes after 2 seconds of no timeouts. We
    then remove a node, wait for 1 second to check that the cluster is out
    of sync, then wait another 2 seconds (giving 1 second of leeway) to check
    that the client has timed out. We also check to see if the config is set.
    """
    cluster = ray_start_cluster_head
    worker = cluster.add_node()
    cluster.wait_for_nodes()

    @ray.remote
    def f():
        assert ray._config.initial_reconstruction_timeout_milliseconds(
        ) == 12345
        assert ray._config.num_heartbeats_timeout() == 20

    ray.get([f.remote() for _ in range(5)])

    cluster.remove_node(worker, allow_graceful=False)
    time.sleep(1)
    assert ray.cluster_resources()["CPU"] == 2

    time.sleep(2)
    assert ray.cluster_resources()["CPU"] == 1


def setup_monitor(address):
    monitor = Monitor(
        address, None, redis_password=ray_constants.REDIS_DEFAULT_PASSWORD)
    monitor.psubscribe(ray.gcs_utils.XRAY_HEARTBEAT_BATCH_PATTERN)
    monitor.psubscribe(ray.gcs_utils.XRAY_JOB_PATTERN)  # TODO: Remove?
    monitor.update_raylet_map(_append_port=True)
    return monitor


def verify_load_metrics(monitor, expected_resource_usage=None, timeout=30):
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
        for key in list(resource_usage[1].keys()):
            if key.startswith("node:"):
                del resource_usage[1][key]
        for key in list(resource_usage[2].keys()):
            if key.startswith("node:"):
                del resource_usage[2][key]

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
    monitor = setup_monitor(cluster.address)
    total_cpus = ray.state.cluster_resources()["CPU"]
    verify_load_metrics(monitor, (0.0, {"CPU": 0.0}, {"CPU": total_cpus}))

    @ray.remote
    def work(signal):
        wait_signal = signal.wait.remote()
        while True:
            ready, not_ready = ray.wait([wait_signal], timeout=0)
            if len(ready) == 1:
                break
            time.sleep(1)

    signal = SignalActor.remote()

    work_handle = work.remote(signal)
    verify_load_metrics(monitor, (1.0 / total_cpus, {
        "CPU": 1.0
    }, {
        "CPU": total_cpus
    }))

    ray.get(signal.send.remote())
    ray.get(work_handle)

    @ray.remote
    class Actor:
        def work(self, signal):
            wait_signal = signal.wait.remote()
            while True:
                ready, not_ready = ray.wait([wait_signal], timeout=0)
                if len(ready) == 1:
                    break
                time.sleep(1)

    signal = SignalActor.remote()

    test_actor = Actor.remote()
    work_handle = test_actor.work.remote(signal)

    verify_load_metrics(monitor, (1.0 / total_cpus, {
        "CPU": 1.0
    }, {
        "CPU": total_cpus
    }))

    ray.get(signal.send.remote())
    ray.get(work_handle)


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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
