import os
import sys

import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak
from ray.util.placement_group import placement_group, placement_group_table
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_bundle_label_selector_with_repeated_labels(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, labels={"ray.io/accelerator-type": "A100"})
    node = cluster.add_node(num_cpus=4, labels={"ray.io/accelerator-type": "TPU"})
    ray.init(address=cluster.address)

    bundles = [{"CPU": 1}, {"CPU": 1}]
    label_selector = [{"ray.io/accelerator-type": "TPU"}] * 2

    pg = placement_group(
        name="repeated_labels_pg",
        bundles=bundles,
        bundle_label_selector=label_selector,
    )
    ray.get(pg.ready())

    bundles_to_node_id = placement_group_table()[pg.id.hex()]["bundles_to_node_id"]
    assert bundles_to_node_id[0] == node.node_id
    assert bundles_to_node_id[1] == node.node_id

    placement_group_assert_no_leak([pg])


def test_unschedulable_bundle_label_selector(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, labels={"ray.io/accelerator-type": "A100"})
    cluster.add_node(num_cpus=1, labels={"ray.io/accelerator-type": "TPU"})
    ray.init(address=cluster.address)

    # request 2 CPUs total, but only 1 CPU available with label ray.io/accelerator-type=A100
    bundles = [{"CPU": 1}, {"CPU": 1}]
    label_selector = [{"ray.io/accelerator-type": "A100"}] * 2

    pg = placement_group(
        name="unschedulable_labels_pg",
        bundles=bundles,
        bundle_label_selector=label_selector,
    )

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=3)

    state = placement_group_table()[pg.id.hex()]["stats"]["scheduling_state"]
    assert state == "NO_RESOURCES"


def test_bundle_label_selectors_match_bundle_resources(ray_start_cluster):
    cluster = ray_start_cluster

    # Add nodes with unique labels and custom resources
    cluster.add_node(
        num_cpus=1, resources={"resource-0": 1}, labels={"region": "us-west4"}
    )
    cluster.add_node(
        num_cpus=1, resources={"resource-1": 1}, labels={"region": "us-east5"}
    )
    cluster.add_node(
        num_cpus=1, resources={"resource-2": 1}, labels={"region": "us-central2"}
    )
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)

    # Bundle label selectors to match the node labels above
    bundle_label_selectors = [
        {"region": "us-west4"},
        {"region": "us-east5"},
        {"region": "us-central2"},
    ]

    # Each bundle requests CPU and a unique custom resource
    bundles = [
        {"CPU": 1, "resource-0": 1},
        {"CPU": 1, "resource-1": 1},
        {"CPU": 1, "resource-2": 1},
    ]

    pg = placement_group(
        name="label_selectors_match_resources",
        bundles=bundles,
        bundle_label_selector=bundle_label_selectors,
    )
    ray.get(pg.ready())

    @ray.remote
    def get_assigned_resources():
        return (
            ray.get_runtime_context().get_node_id(),
            ray.get_runtime_context().get_assigned_resources(),
        )

    node_id_to_label = {
        node["NodeID"]: node["Labels"]["region"] for node in ray.nodes()
    }

    # Launch one task per bundle to check resource mapping
    for i in range(len(bundles)):
        result = ray.get(
            get_assigned_resources.options(
                num_cpus=1,
                resources={f"resource-{i}": 1},
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=i
                ),
            ).remote()
        )
        node_id, assigned = result

        # Check node label matches expected
        assert node_id_to_label[node_id] == bundle_label_selectors[i]["region"]

        # Check resource assignment includes the expected custom resource
        assert f"resource-{i}" in assigned
        assert assigned[f"resource-{i}"] == 1.0

        # Check CPU was assigned
        assert "CPU" in assigned and assigned["CPU"] == 1.0


def test_strict_pack_bundle_label_selector(ray_start_cluster):
    """
    Verifies that placement groups with STRICT_PACK strategy respect bundle_label_selector.

    If the PG is ready, it should schedule all bundles to the same node which satisfies the
    label constraints. If the `bundle_label_selector` is unsatisfiable on a single node,
    the PG should remain pending.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, labels={"region": "us-east"})
    cluster.add_node(num_cpus=4, labels={"region": "us-west"})
    ray.init(address=cluster.address)

    # Success case - both label selectors can be satisfied on a single node.
    success_pg = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="STRICT_PACK",
        bundle_label_selector=[{"region": "us-east"}, {"region": "us-east"}],
    )
    ray.get(success_pg.ready(), timeout=5)

    table = placement_group_table(success_pg)
    assert table["state"] == "CREATED"

    # Failure case - conflicting label selectors match two distinct nodes.
    fail_pg = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="STRICT_PACK",
        bundle_label_selector=[{"region": "us-east"}, {"region": "us-west"}],
    )

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(fail_pg.ready(), timeout=5)

    pg_info = placement_group_table(fail_pg)
    assert pg_info["state"] == "PENDING"

    ray.util.remove_placement_group(success_pg)
    ray.util.remove_placement_group(fail_pg)


def test_strict_spread_bundle_label_selector(ray_start_cluster):
    """
    Verifies that placement groups with STRICT_SPREAD strategy respect bundle_label_selector.

    If the PG is ready, it should schedule all bundles to different which each satisfy their
    respective label constraints. If the `bundle_label_selector` is unsatisfiable on len(bundles)
    unique nodes, the PG should remain pending.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, labels={"type": "A"})
    cluster.add_node(num_cpus=4, labels={"type": "A"})
    cluster.add_node(num_cpus=4, labels={"type": "B"})
    ray.init(address=cluster.address)

    # Success case - label selectors can be satisfied on different nodes.
    success_pg = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="STRICT_SPREAD",
        bundle_label_selector=[{"type": "A"}, {"type": "A"}],
    )
    ray.get(success_pg.ready(), timeout=5)
    assert placement_group_table(success_pg)["state"] == "CREATED"

    # Failure case - conflicting label selectors only satisfied by one node.
    fail_pg = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="STRICT_SPREAD",
        bundle_label_selector=[{"type": "B"}, {"type": "B"}],
    )

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(fail_pg.ready(), timeout=5)

    pg_info = placement_group_table(fail_pg)
    assert pg_info["state"] == "PENDING"

    ray.util.remove_placement_group(success_pg)
    ray.util.remove_placement_group(fail_pg)


def test_pack_strategy_bundle_label_selector(ray_start_cluster):
    """
    Verifies that PACK strategy respects bundle_label_selector.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, labels={"type": "A"})
    cluster.add_node(num_cpus=4, labels={"type": "B"})
    ray.init(address=cluster.address)

    # Success case - label selectors satisfied on one node.
    success_pg_1 = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="PACK",
        bundle_label_selector=[{"type": "A"}, {"type": "A"}],
    )
    ray.get(success_pg_1.ready(), timeout=5)
    assert placement_group_table(success_pg_1)["state"] == "CREATED"

    # Success case (best effort) - label selectors satisfied on different nodes.
    success_pg_2 = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="PACK",
        bundle_label_selector=[{"type": "A"}, {"type": "B"}],
    )
    ray.get(success_pg_2.ready(), timeout=5)
    assert placement_group_table(success_pg_2)["state"] == "CREATED"

    # Failure case - label selectors unsatisfiable by any node.
    fail_pg = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="PACK",
        bundle_label_selector=[{"type": "A"}, {"type": "C"}],
    )

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(fail_pg.ready(), timeout=3)

    pg_info = placement_group_table(fail_pg)
    assert pg_info["state"] == "PENDING"

    ray.util.remove_placement_group(success_pg_1)
    ray.util.remove_placement_group(success_pg_2)
    ray.util.remove_placement_group(fail_pg)


def test_spread_strategy_bundle_label_selector(ray_start_cluster):
    """
    Verifies that SPREAD strategy respects bundle_label_selector.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, labels={"type": "A"})
    cluster.add_node(num_cpus=4, labels={"type": "B"})
    ray.init(address=cluster.address)

    # Success case - label selectors satisfied and SPREAD across nodes.
    success_pg_spread = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="SPREAD",
        bundle_label_selector=[{"type": "A"}, {"type": "B"}],
    )
    ray.get(success_pg_spread.ready(), timeout=5)
    assert placement_group_table(success_pg_spread)["state"] == "CREATED"

    # Success case - label selectors satisfied but forced to use same node.
    success_pg_packed = placement_group(
        bundles=[{"CPU": 1}, {"CPU": 1}],
        strategy="SPREAD",
        bundle_label_selector=[{"type": "A"}, {"type": "A"}],
    )
    ray.get(success_pg_packed.ready(), timeout=5)
    assert placement_group_table(success_pg_packed)["state"] == "CREATED"

    # Failure case - label selectors unsatisfiable by any node.
    fail_pg = placement_group(
        bundles=[{"CPU": 1}], strategy="SPREAD", bundle_label_selector=[{"type": "C"}]
    )

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(fail_pg.ready(), timeout=3)

    pg_info = placement_group_table(fail_pg)
    assert pg_info["state"] == "PENDING"

    ray.util.remove_placement_group(success_pg_spread)
    ray.util.remove_placement_group(success_pg_packed)
    ray.util.remove_placement_group(fail_pg)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
