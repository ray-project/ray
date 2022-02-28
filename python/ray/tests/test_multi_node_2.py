import logging
import pytest
import time

import ray
import ray.ray_constants as ray_constants
from ray.util.placement_group import placement_group, remove_placement_group
from ray.autoscaler.sdk import request_resources
from ray.autoscaler._private.monitor import Monitor
from ray.cluster_utils import Cluster
from ray._private.gcs_utils import use_gcs_for_bootstrap
from ray._private.test_utils import (
    generate_system_config_map,
    wait_for_condition,
    SignalActor,
)

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
    g.shutdown()


def test_shutdown():
    g = Cluster(initialize_head=False)
    node = g.add_node()
    node2 = g.add_node()
    g.shutdown()
    assert not any(n.any_processes_alive() for n in [node, node2])


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        generate_system_config_map(
            num_heartbeats_timeout=3, object_timeout_milliseconds=12345
        )
    ],
    indirect=True,
)
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
        assert ray._config.num_heartbeats_timeout() == 3

    ray.get([f.remote() for _ in range(5)])

    cluster.remove_node(worker, allow_graceful=False)
    time.sleep(1)
    assert ray.cluster_resources()["CPU"] == 2

    def _node_removed():
        return ray.cluster_resources()["CPU"] == 1

    wait_for_condition(_node_removed, timeout=3)


def setup_monitor(address):
    monitor = Monitor(
        address, None, redis_password=ray_constants.REDIS_DEFAULT_PASSWORD
    )
    return monitor


def assert_correct_pg(pg_response_data, pg_demands, strategy):
    assert len(pg_response_data) == 1
    pg_response_data = pg_response_data[0]
    strategy_mapping_dict_protobuf = {
        "PACK": 0,
        "SPREAD": 1,
        "STRICT_PACK": 2,
        "STRICT_SPREAD": 3,
    }
    assert pg_response_data.strategy == strategy_mapping_dict_protobuf[strategy]
    assert pg_response_data.creator_job_id
    assert pg_response_data.creator_actor_id
    assert pg_response_data.creator_actor_dead
    assert pg_response_data.placement_group_id

    for i, bundle in enumerate(pg_demands):
        assert pg_response_data.bundles[i].unit_resources == bundle
        assert pg_response_data.bundles[i].bundle_id.placement_group_id


# DO NOT CHANGE THIS VERIFICATION WITHOUT NOTIFYING (Eric/Ameer/Alex).
def verify_load_metrics(monitor, expected_resource_usage=None, timeout=30):
    request_resources(num_cpus=42)

    # add placement groups.
    pg_demands = [{"GPU": 2}, {"extra_resource": 2}]
    strategy = "STRICT_PACK"
    pg = placement_group(pg_demands, strategy=strategy)
    pg.ready()
    time.sleep(2)  # wait for placement groups to propagate.

    # Disable event clearing for test.
    monitor.event_summarizer.clear = lambda *a: None

    visited_atleast_once = [set(), set()]
    while True:
        monitor.update_load_metrics()
        monitor.update_resource_requests()
        monitor.update_event_summary()
        resource_usage = monitor.load_metrics._get_resource_usage()

        # Check resource request propagation.
        req = monitor.load_metrics.resource_requests
        assert req == [{"CPU": 1}] * 42, req

        pg_response_data = monitor.load_metrics.pending_placement_groups
        assert_correct_pg(pg_response_data, pg_demands, strategy)

        if "memory" in resource_usage[0]:
            del resource_usage[0]["memory"]
            visited_atleast_once[0].add("memory")
        if "object_store_memory" in resource_usage[0]:
            del resource_usage[0]["object_store_memory"]
            visited_atleast_once[0].add("object_store_memory")
        if "memory" in resource_usage[1]:
            del resource_usage[1]["memory"]
            visited_atleast_once[1].add("memory")
        if "object_store_memory" in resource_usage[1]:
            del resource_usage[1]["object_store_memory"]
            visited_atleast_once[1].add("object_store_memory")
        for key in list(resource_usage[0].keys()):
            if key.startswith("node:"):
                del resource_usage[0][key]
                visited_atleast_once[0].add("node:")
        for key in list(resource_usage[1].keys()):
            if key.startswith("node:"):
                del resource_usage[1][key]
                visited_atleast_once[1].add("node:")
        if expected_resource_usage is None:
            if all(x for x in resource_usage[0:]):
                break
        elif all(x == y for x, y in zip(resource_usage, expected_resource_usage)):
            break
        else:
            timeout -= 1
            time.sleep(1)

        if timeout <= 0:
            raise ValueError(
                "Timeout. {} != {}".format(resource_usage, expected_resource_usage)
            )

    # Sanity check we emitted a resize event.
    assert any("Resized to" in x for x in monitor.event_summarizer.summary())

    assert visited_atleast_once[0] == {"memory", "object_store_memory", "node:"}
    assert visited_atleast_once[0] == visited_atleast_once[1]

    remove_placement_group(pg)

    return resource_usage


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 1,
        },
        {
            "num_cpus": 2,
        },
    ],
    indirect=True,
)
def test_heartbeats_single(ray_start_cluster_head):
    """Unit test for `Cluster.wait_for_nodes`.

    Test proper metrics.
    """
    cluster = ray_start_cluster_head
    if use_gcs_for_bootstrap():
        monitor = setup_monitor(cluster.gcs_address)
    else:
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
    del monitor


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
    "call_ray_start",
    [
        "ray start --head --ray-client-server-port 20000 "
        + "--min-worker-port=0 --max-worker-port=0 --port 0"
    ],
    indirect=True,
)
def test_ray_client(call_ray_start):
    from ray.util.client import ray as ray_client

    ray.client("localhost:20000").connect()

    @ray.remote
    def f():
        return "hello client"

    assert ray_client.get(f.remote()) == "hello client"


def test_detached_actor_autoscaling(ray_start_cluster_head):
    """Make sure that a detached actor, which belongs to a dead job, can start
    workers on nodes that were added after the job ended.
    """
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes(2)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            self.handles = []

        def start_actors(self, n):
            self.handles.extend([Actor.remote() for _ in range(n)])

        def get_children(self):
            return self.handles

        def ping(self):
            pass

    main_actor = Actor.options(lifetime="detached", name="main").remote()
    ray.get(main_actor.ping.remote())

    ray.shutdown()
    ray.init(address=cluster.address, namespace="default_test_namespace")

    main_actor = ray.get_actor("main")
    num_to_start = int(ray.available_resources().get("CPU", 0) + 1)
    print(f"Starting {num_to_start} actors")
    ray.get(main_actor.start_actors.remote(num_to_start))

    actor_handles = ray.get(main_actor.get_children.remote())

    up, down = ray.wait(
        [actor.ping.remote() for actor in actor_handles],
        timeout=5,
        num_returns=len(actor_handles),
    )
    assert len(up) == len(actor_handles) - 1
    assert len(down) == 1

    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes(3)
    up, down = ray.wait(
        [actor.ping.remote() for actor in actor_handles],
        timeout=5,
        num_returns=len(actor_handles),
    )
    assert len(up) == len(actor_handles)
    assert len(down) == 0


def test_multi_node_pgs(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes(2)
    ray.init(address=cluster.address)

    pgs = [ray.util.placement_group([{"CPU": 1}]) for _ in range(4)]

    ready, not_ready = ray.wait([pg.ready() for pg in pgs], timeout=5, num_returns=4)
    assert len(ready) == 2
    assert len(not_ready) == 2

    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes(3)
    ready, not_ready = ray.wait([pg.ready() for pg in pgs], timeout=5, num_returns=4)
    assert len(ready) == 4
    assert len(not_ready) == 0

    for i in range(4, 10):
        cluster.add_node(num_cpus=2)
        cluster.wait_for_nodes(i)
        print(".")
        more_pgs = [ray.util.placement_group([{"CPU": 1}]) for _ in range(2)]
        ready, not_ready = ray.wait(
            [pg.ready() for pg in more_pgs], timeout=5, num_returns=2
        )
        assert len(ready) == 2


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
