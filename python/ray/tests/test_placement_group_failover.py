import pytest
import sys
import ray
import ray.cluster_utils
from ray._private.test_utils import get_other_nodes, wait_for_condition

MB = 1024 * 1024


@ray.remote(num_cpus=1)
class Actor(object):
    def __init__(self):
        self.n = 0

    def value(self):
        return self.n


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

    wait_for_condition(_check_actor_with_pg_is_ready, timeout=5, retry_interval_ms=1000)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
