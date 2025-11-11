import sys
import time

import pytest

import ray
import ray.cluster_utils
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import get_other_nodes
from ray.util import placement_group_table
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

MB = 1024 * 1024


@ray.remote(num_cpus=1)
class Actor(object):
    def __init__(self):
        self.n = 0

    def value(self):
        return self.n


def test_placement_group_recover_prepare_failure(monkeypatch, ray_start_cluster):
    # Test to make sure that gcs can handle the prepare pg failure
    # by retrying on other nodes.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    monkeypatch.setenv(
        "RAY_testing_asio_delay_us",
        "NodeManagerService.grpc_server.PrepareBundleResources=500000000:500000000",
    )
    worker1 = cluster.add_node(num_cpus=1)
    pg = ray.util.placement_group(
        strategy="STRICT_SPREAD", bundles=[{"CPU": 1}, {"CPU": 1}]
    )
    # actor will wait for the pg to be created
    actor = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()

    # wait for the prepare rpc to be sent
    time.sleep(1)

    # prepare will fail
    cluster.remove_node(worker1)

    monkeypatch.delenv("RAY_testing_asio_delay_us")
    # prepare will retry on this node
    cluster.add_node(num_cpus=1)

    # pg can be created successfully
    ray.get(actor.value.remote())


# Test whether the bundles spread on two nodes can be rescheduled successfully
# when both nodes die at the same time.
def test_placement_group_failover_when_two_nodes_die(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_asio_delay_us",
            "NodeManagerService.grpc_client.PrepareBundleResources=2000000:2000000",
        )
        cluster = ray_start_cluster
        num_nodes = 4
        nodes = []
        for _ in range(num_nodes):
            nodes.append(cluster.add_node(num_cpus=1))
        ray.init(address=cluster.address)

        bundles = [{"CPU": 1, "memory": 100 * MB} for _ in range(num_nodes)]
        placement_group = ray.util.placement_group(
            name="name", strategy="STRICT_SPREAD", bundles=bundles
        )
        assert placement_group.wait(3000)

        # add more nodes for pg bundle rescedule
        other_nodes = get_other_nodes(cluster, exclude_head=True)
        other_nodes_num = len(other_nodes)
        for i in range(other_nodes_num):
            cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()

        for node in other_nodes:
            cluster.remove_node(node)

        # Create actors with echo bundle to make sure all bundle are ready.
        for i in range(num_nodes):
            actor = Actor.options(
                placement_group=placement_group, placement_group_bundle_index=i
            ).remote()
            object_ref = actor.value.remote()
            ray.get(object_ref, timeout=5)


def test_gcs_restart_when_placement_group_failover(
    ray_start_cluster_head_with_external_redis,
):
    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster_head_with_external_redis
    num_nodes = 3
    nodes = []
    for _ in range(num_nodes - 1):
        nodes.append(cluster.add_node(num_cpus=1))

    # Make sure the placement group is ready.
    bundles = [{"CPU": 1, "memory": 100 * MB} for _ in range(num_nodes)]
    placement_group = ray.util.placement_group(
        name="name", strategy="STRICT_SPREAD", bundles=bundles
    )
    assert placement_group.wait(5000)
    actors = []
    for i in range(num_nodes):
        actor = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=i,
            max_restarts=-1,
        ).remote()
        object_ref = actor.value.remote()
        ray.get(object_ref, timeout=5)
        actors.append(actor)

    # Simulate a node dead.
    other_nodes = get_other_nodes(cluster, exclude_head=True)
    cluster.remove_node(other_nodes[0])

    # Make sure placement group state change to rescheduling.
    def _check_pg_whether_be_reschedule():
        table = ray.util.placement_group_table(placement_group)
        return table["state"] == "RESCHEDULING"

    wait_for_condition(
        _check_pg_whether_be_reschedule, timeout=5, retry_interval_ms=1000
    )

    # Simulate gcs restart.
    cluster.head_node.kill_gcs_server()
    cluster.head_node.start_gcs_server()

    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    # Check placement gorup reschedule success after gcs server restart.
    def _check_actor_with_pg_is_ready():
        try:
            for actor in actors:
                object_ref = actor.value.remote()
                ray.get(object_ref, timeout=5)
            return True
        except Exception:
            return False

    wait_for_condition(
        _check_actor_with_pg_is_ready, timeout=10, retry_interval_ms=1000
    )


@pytest.mark.parametrize("kill_bad_node", ["before_gcs_restart", "after_gcs_restart"])
def test_gcs_restart_when_pg_committing(
    monkeypatch, ray_start_cluster_head_with_external_redis, kill_bad_node
):
    """
    Tests GCS restart preserves already-committed bundles for a PREPARED pg.
    Timeline:
    1. Create a placement group with 2 bundles, no nodes yet.
        - [Test] PENDING
    2. Create 2 actors in the pg, one for each bundle.
    3. Create 1 good node, and 1 slow committing node
        - [Test] PREPARED
        - [Test] There should be 1 alive actor.
    4. Kill GCS.
        - [Test] There should be 1 alive actor.
    5. switch `kill_bad_node`
        1. `kill_bad_node` == "before_gcs_restart":
            i. kill the slow committing node.
            ii. restart GCS.
        2. `kill_bad_node` == "after_gcs_restart":
            i. restart GCS.
                - [Test] PREPARED
                - [Test] There should be 1 alive actor.
            ii. kill the slow committing node.
    - [Test] PREPARED -> RESCHEDULING
    - [Test] There should be 1 alive actor.
    6. Add a new, normal node.
        - [Test] RESCHEDULING -> CREATED
        - [Test] There should be 2 alive actors.
    """
    MY_RESOURCE_ONE = {"MyResource": 1}

    @ray.remote(resources=MY_RESOURCE_ONE, num_cpus=0)
    class Actor:
        def ready(self):
            return True

    def alive_actors(actors):
        """Returns a list of actors that are alive."""
        ping_map = {actor.ready.remote(): actor for actor in actors}
        pings = list(ping_map.keys())
        ready, _ = ray.wait(pings, timeout=1)
        assert all(ray.get(ready)), f"{ready=}"
        return [ping_map[ping] for ping in ready]

    cluster = ray_start_cluster_head_with_external_redis

    # 1. Create a placement group with 2 bundles, no nodes yet.
    bundles = [MY_RESOURCE_ONE, MY_RESOURCE_ONE]
    pg = ray.util.placement_group(
        name="pg_2_nodes", strategy="STRICT_SPREAD", bundles=bundles
    )
    assert placement_group_table(pg)["state"] == "PENDING"

    # 2. Create 2 actors in the pg, one for each bundle.
    actor0 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        )
    ).remote()
    actor1 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=1
        )
    ).remote()

    actors = [actor0, actor1]
    print(f"Created 2 actors: {actors}")

    # 3. Create 1 good node, and 1 slow committing node
    cluster.add_node(num_cpus=1, resources=MY_RESOURCE_ONE)
    with monkeypatch.context() as monkeypatch:
        monkeypatch.setenv(
            "RAY_testing_asio_delay_us",
            "NodeManagerService.grpc_server.CommitBundleResources=500000000:500000000",
        )
        bad_node = cluster.add_node(num_cpus=1, resources=MY_RESOURCE_ONE)

    assert not pg.wait(timeout_seconds=1)
    assert placement_group_table(pg)["state"] == "PREPARED"
    # Wait for the actor to be ready. One of them are ready.
    assert len(alive_actors(actors)) == 1

    # 4. Kill GCS.
    cluster.head_node.kill_gcs_server()
    assert len(alive_actors(actors)) == 1

    if kill_bad_node == "before_gcs_restart":
        # 5.1. Kill the slow committing node.
        cluster.remove_node(bad_node)
        # 5.2. Restart GCS.
        cluster.head_node.start_gcs_server()
    else:
        assert kill_bad_node == "after_gcs_restart"
        # 5.1. Restart GCS.
        cluster.head_node.start_gcs_server()
        assert placement_group_table(pg)["state"] == "PREPARED"
        assert len(alive_actors(actors)) == 1
        # 5.2. Kill the slow committing node.
        cluster.remove_node(bad_node)

    time.sleep(1)
    assert placement_group_table(pg)["state"] == "RESCHEDULING"
    assert len(alive_actors(actors)) == 1

    # 6. Add a new, normal node.
    cluster.add_node(num_cpus=1, resources=MY_RESOURCE_ONE)
    assert pg.wait()
    assert placement_group_table(pg)["state"] == "CREATED"
    ray.get([actor.ready.remote() for actor in actors])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
