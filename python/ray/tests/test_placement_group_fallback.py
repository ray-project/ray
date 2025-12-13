import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)


def test_placement_group_fallback_resources(ray_start_cluster):
    """Test fallback based on resource bundles."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Feasible fallback strategy with bundles requesting <= available CPU.
    fallback_strategy = [{"bundles": [{"CPU": 4}]}]

    pg = placement_group(
        name="resource_fallback_pg",
        bundles=[{"CPU": 8}],  # Infeasible initial bundle request.
        strategy="PACK",
        fallback_strategy=fallback_strategy,
    )
    # Placement group is scheduled using fallback.
    ray.get(pg.ready(), timeout=10)

    # Example task to try to schedule to used node.
    @ray.remote(num_cpus=1)
    def check_capacity():
        return "ok"

    # Example task times out because all CPU used by placement group.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(check_capacity.remote(), timeout=2)

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


def test_placement_group_fallback_strategy_labels(ray_start_cluster):
    """
    Test that fallback strategy is used when primary bundles are not feasible
    due to label constraints.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, labels={})  # Unlabelled node
    cluster.add_node(num_cpus=2, labels={"region": "us-west1"})
    ray.init(address=cluster.address)

    fallback_strategy = [
        {"bundles": [{"CPU": 2}], "bundle_label_selector": [{"region": "us-west1"}]}
    ]

    pg = placement_group(
        name="fallback_pg",
        bundles=[{"CPU": 2}],
        bundle_label_selector=[{"region": "us-east1"}],  # Infeasible label
        strategy="PACK",
        fallback_strategy=fallback_strategy,  # Feasible fallback
    )

    # Succeeds due to fallback option
    ray.get(pg.ready(), timeout=10)

    # Verify it was scheduled on the correct node
    table = placement_group_table(pg)
    bundle_node_id = table["bundles_to_node_id"][0]

    found = False
    for node in ray.nodes():
        if node["NodeID"] == bundle_node_id:
            assert node["Labels"]["region"] == "us-west1"
            found = True
            break
    assert found, "Scheduled node not found in cluster state"

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


def test_placement_group_fallback_priority(ray_start_cluster):
    """Test that the first feasible fallback option is chosen from multiple feasible fallbacks."""
    cluster = ray_start_cluster
    # Node has 10 CPUs
    cluster.add_node(num_cpus=10)
    ray.init(address=cluster.address)

    fallback_strategy = [
        {"bundles": [{"CPU": 10}]},  # Infeasible
        {"bundles": [{"CPU": 5}]},  # Feasible
        {"bundles": [{"CPU": 1}]},  # Feasible
    ]

    pg = placement_group(
        name="priority_pg",
        bundles=[{"CPU": 20}],  # Infeasible main bundles.
        strategy="PACK",
        fallback_strategy=fallback_strategy,
    )

    ray.get(pg.ready(), timeout=10)

    # Verify we consumed 5 CPUs, not 1.
    @ray.remote(num_cpus=6)
    def heavy_task():
        return "ok"

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(heavy_task.remote(), timeout=2)

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


def test_placement_group_fallback_bundle_shapes(ray_start_cluster):
    """Test fallback works even when changing the number of bundles."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    # Feasible fallback specifies 2 bundles with 1 CPU each (rather than 1 bundle
    # with 2 CPU).
    fallback_strategy = [{"bundles": [{"CPU": 1}, {"CPU": 1}]}]

    pg = placement_group(
        name="reshape_pg",
        bundles=[{"CPU": 2}],  # Infeasible 2 CPU bundle on any node.
        strategy="SPREAD",
        fallback_strategy=fallback_strategy,
    )

    ray.get(pg.ready(), timeout=10)

    table = placement_group_table(pg)
    assert len(table["bundles"]) == 2

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
