import pytest
import sys
import ray
import ray.cluster_utils
from ray._private.test_utils import (
    get_other_nodes,
)

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
