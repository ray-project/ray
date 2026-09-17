"""The load-bearing claim behind EVICT, checked against a real cluster.

``EVICT`` is supposed to hand off to Ray Train's existing worker-group sizing
path and simply bring the group back up without the bad node. That only works
if Ray's own scheduler honors a ``!in(...)`` node-id selector on placement
group bundles. It does -- this test is what proves it, and what would catch a
regression in Core that silently puts workers back on an evicted host.

No Ray Core change (the REP's ``drain_node`` milestone) is required for this.
"""
import pytest

import ray
from ray.cluster_utils import Cluster
from ray.train.v2._internal.callbacks.health_callback import (
    build_node_exclusion_selector,
)
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.fixture
def two_node_cluster():
    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 2})
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@ray.remote(num_cpus=1)
def _which_node():
    return ray.get_runtime_context().get_node_id()


def test_evicted_node_is_not_scheduled_onto(two_node_cluster):
    alive = [n["NodeID"] for n in ray.nodes() if n["Alive"]]
    assert len(alive) == 2
    evicted = alive[0]

    selector = build_node_exclusion_selector([evicted])
    pg = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="PACK",
        bundle_label_selector=[selector] * 2,
    )
    ray.get(pg.ready(), timeout=60)

    placed = ray.get(
        [
            _which_node.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=i
                )
            ).remote()
            for i in range(2)
        ]
    )
    assert evicted not in placed


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
