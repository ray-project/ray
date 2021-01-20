import logging
import pytest
import time

import ray
import ray.ray_constants as ray_constants
from ray.autoscaler.sdk import request_resources
from ray.monitor import Monitor
from ray.cluster_utils import Cluster
from ray.test_utils import generate_system_config_map, SignalActor

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
        generate_system_config_map(
            num_heartbeats_timeout=20, object_timeout_milliseconds=12345)
    ],
    indirect=True)
def test_system_config(ray_start_cluster_head):
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
        assert ray._config.object_timeout_milliseconds() == 12345
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
    monitor.update_raylet_map(_append_port=True)
    return monitor


def verify_load_metrics(monitor, expected_resource_usage=None, timeout=30):
    request_resources(num_cpus=42)

    # Disable event clearing for test.
    monitor.event_summarizer.clear = lambda *a: None

    while True:
        monitor.update_load_metrics()
        monitor.update_resource_requests()
        monitor.update_event_summary()
        resource_usage = monitor.load_metrics._get_resource_usage()

        # Check resource request propagation.
        req = monitor.load_metrics.resource_requests
        assert req == [{"CPU": 1}] * 42, req

        if "memory" in resource_usage[0]:
            del resource_usage[0]["memory"]
        if "object_store_memory" in resource_usage[1]:
            del resource_usage[0]["object_store_memory"]
        if "memory" in resource_usage[1]:
            del resource_usage[1]["memory"]
        if "object_store_memory" in resource_usage[1]:
            del resource_usage[1]["object_store_memory"]
        for key in list(resource_usage[0].keys()):
            if key.startswith("node:"):
                del resource_usage[0][key]
        for key in list(resource_usage[1].keys()):
            if key.startswith("node:"):
                del resource_usage[1][key]

        if expected_resource_usage is None:
            if all(x for x in resource_usage[0:]):
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

    # Sanity check we emitted a resize event.
    assert any("Resized to" in x for x in monitor.event_summarizer.summary())

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
    verify_load_metrics(monitor, ({"CPU": 0.0}, {"CPU": total_cpus}))

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
    verify_load_metrics(monitor, ({"CPU": 1.0}, {"CPU": total_cpus}))

    ray.get(signal.send.remote())
    ray.get(work_handle)

    @ray.remote(num_cpus=1)
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
    time.sleep(1)  # Time for actor to get placed and the method to start.

    verify_load_metrics(monitor, ({"CPU": 1.0}, {"CPU": total_cpus}))

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


@pytest.mark.parametrize(
    "call_ray_start", [
        "ray start --head --ray-client-server-port 20000 " +
        "--min-worker-port=0 --max-worker-port=0 --port 0"
    ],
    indirect=True)
def test_ray_client(call_ray_start):
    from ray.util.client import ray
    ray.connect("localhost:20000")

    @ray.remote
    def f():
        return "hello client"

    assert ray.get(f.remote()) == "hello client"


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
