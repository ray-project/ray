import pytest
import sys

import ray
import ray.cluster_utils
from ray._private.test_utils import (
    get_other_nodes,
    placement_group_assert_no_leak,
)
from ray.util.client.ray_client_helpers import connect_to_client_or_not
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

MOCK_WORKER_STARTUP_SLOWLY_PLUGIN_CLASS_PATH = (
    "ray.tests.test_placement_group_4.MockWorkerStartupSlowlyPlugin"  # noqa
)


class MockWorkerStartupSlowlyPlugin(RuntimeEnvPlugin):
    def validate(runtime_env_dict: dict) -> str:
        return "success"

    @staticmethod
    def create(uri: str, runtime_env_dict: dict, ctx: RuntimeEnvContext) -> float:
        import time

        time.sleep(15)
        return 0


@pytest.mark.parametrize("connect_to_client", [False, True])
def test_placement_group_reschedule_when_node_dead(
    ray_start_cluster_enabled, connect_to_client
):
    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, namespace="default_test_namespace")

    # Make sure both head and worker node are alive.
    nodes = ray.nodes()
    assert len(nodes) == 3
    assert nodes[0]["alive"] and nodes[1]["alive"] and nodes[2]["alive"]

    with connect_to_client_or_not(connect_to_client):
        placement_group = ray.util.placement_group(
            name="name", strategy="SPREAD", bundles=[{"CPU": 2}, {"CPU": 2}, {"CPU": 2}]
        )
        actor_1 = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=0,
            lifetime="detached",
        ).remote()
        actor_2 = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=1,
            lifetime="detached",
        ).remote()
        actor_3 = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=2,
            lifetime="detached",
        ).remote()
        ray.get(actor_1.value.remote())
        ray.get(actor_2.value.remote())
        ray.get(actor_3.value.remote())

        cluster.remove_node(get_other_nodes(cluster, exclude_head=True)[-1])
        cluster.wait_for_nodes()

        actor_4 = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=0,
            lifetime="detached",
        ).remote()
        actor_5 = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=1,
            lifetime="detached",
        ).remote()
        actor_6 = Actor.options(
            placement_group=placement_group,
            placement_group_bundle_index=2,
            lifetime="detached",
        ).remote()
        ray.get(actor_4.value.remote())
        ray.get(actor_5.value.remote())
        ray.get(actor_6.value.remote())
        placement_group_assert_no_leak([placement_group])
        ray.shutdown()


def test_infeasible_pg(ray_start_cluster_enabled):
    """Test infeasible pgs are scheduled after new nodes are added."""
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=2)
    ray.init("auto")

    bundle = {"CPU": 4, "GPU": 1}
    pg = ray.util.placement_group([bundle], name="worker_1", strategy="STRICT_PACK")

    # Placement group is infeasible.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=3)

    state = ray.util.placement_group_table()[pg.id.hex()]["stats"]["scheduling_state"]
    assert state == "INFEASIBLE"

    # Add a new node. PG can now be scheduled.
    cluster.add_node(num_cpus=4, num_gpus=1)
    assert ray.get(pg.ready(), timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
