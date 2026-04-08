import sys

import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_placement_group_fallback_state_exposure(ray_start_regular):
    """Test that fallback scheduling options are exposed in placement group table."""
    # Create a PG with fallback
    pg = placement_group([{"CPU": 1}], fallback_strategy=[{"bundles": [{"CPU": 0.5}]}])

    # Wait for it to be ready to ensure it's in GCS
    pg.wait(timeout_seconds=5)

    table = ray.util.placement_group_table(pg)
    assert "scheduling_options" in table
    assert "active_scheduling_option_index" in table

    opts = table["scheduling_options"]
    assert len(opts) == 2
    assert opts[0]["bundles"][0] == {"CPU": 1.0}
    assert opts[1]["bundles"][0] == {"CPU": 0.5}


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
            assert node["Alive"]
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
        {"bundles": [{"CPU": 11}]},  # Infeasible
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


def test_multiple_placement_groups_and_fallbacks(ray_start_cluster):
    """
    Test that multiple placement groups with fallback strategies correctly subtract
    from available resources in the cluster.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=10)
    ray.init(address=cluster.address)

    # Define a fallback strategy that uses 3 CPUs.
    fallback_strategy = [{"bundles": [{"CPU": 3}]}]

    pgs = []
    for i in range(3):
        pg = placement_group(
            name=f"pg_{i}",
            bundles=[{"CPU": 100}],  # Infeasible
            strategy="PACK",
            fallback_strategy=fallback_strategy,
        )
        pgs.append(pg)

    # Create 3 PGs that should all use the fallback strategy.
    for pg in pgs:
        ray.get(pg.ready(), timeout=10)

    # Verify we can still schedule a task utilizing the last CPU (10 total - 9 used by PGs).
    @ray.remote(num_cpus=1)
    def small_task():
        return "ok"

    assert ray.get(small_task.remote(), timeout=5) == "ok"

    # Validate PGs with fallback correctly subtract from the available cluster resources to where
    # a task requesting more CPU than is available times out.
    @ray.remote(num_cpus=2)
    def large_task():
        return "fail"

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(large_task.remote(), timeout=2)

    for pg in pgs:
        remove_placement_group(pg)
    placement_group_assert_no_leak(pgs)


def test_placement_group_fallback_validation(ray_start_cluster):
    """
    Verifies that PG validates resource shape with both primary and fallback bundles.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4, num_gpus=0)
    ray.init(address=cluster.address)

    pg = placement_group(
        name="validation_pg",
        bundles=[{"GPU": 1}],
        strategy="PACK",
        fallback_strategy=[{"bundles": [{"CPU": 1}]}],
    )

    # Task requires CPU, primary option has only GPU.
    # The client-side validation logic should check the fallback strategy
    # and allow this task to proceed.
    @ray.remote(num_cpus=1)
    def run_on_cpu():
        return "success"

    try:
        # If client-side validation fails, this raises ValueError immediately.
        ref = run_on_cpu.options(placement_group=pg).remote()
        assert ray.get(ref) == "success"
    except ValueError as e:
        pytest.fail(f"Validation failed for fallback-compatible task: {e}")

    # Verify bundle_specs returns what was specified for the active
    # fallback strategy.
    ray.get(pg.ready())
    assert pg.bundle_specs[0].get("CPU") == 1
    assert pg.bundle_specs[0].get("GPU") is None

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


def test_pending_placement_group_index_validation(ray_start_cluster):
    """
    Test that when a PG is pending, index validation allows up to the
    maximum number of bundles across all fallback strategies.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, num_gpus=0)  # keep PG pending
    ray.init(address=cluster.address)

    pg = placement_group(
        name="pending_index_pg",
        bundles=[{"CPU": 2}],
        strategy="PACK",
        fallback_strategy=[{"bundles": [{"CPU": 1}, {"CPU": 1}, {"CPU": 1}]}],
    )

    @ray.remote(num_cpus=1)
    def dummy_task():
        return "ok"

    # Requesting index 2 shouldn't raise an error because max possible is 3.
    try:
        dummy_task.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=2
            )
        ).remote()
    except ValueError as e:
        pytest.fail(f"Pending PG incorrectly rejected valid fallback index: {e}")

    # Requesting index 3 should fail immediately since no strategy has more than 3 bundles.
    with pytest.raises(ValueError, match="Valid placement group indexes: 0 to 2"):
        dummy_task.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=3
            )
        ).remote()

    remove_placement_group(pg)


def test_scheduled_placement_group_fallback_index_validation(ray_start_cluster):
    """
    Test that bundle_index validation strictly respects the active
    strategy's bundle count once the PG is scheduled.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    # Infeasible primary strategy with 3 bundles, feasible fallback strategy with 1 bundle.
    pg = placement_group(
        name="index_validation_pg",
        bundles=[{"CPU": 1}, {"CPU": 1}, {"CPU": 1}],
        strategy="STRICT_PACK",
        fallback_strategy=[{"bundles": [{"CPU": 1}]}],
    )

    # Wait for the PG to schedule using the fallback.
    ray.get(pg.ready(), timeout=10)

    # Wait for state to propagate to GCS table
    import time

    success = False
    for _ in range(50):
        table = ray.util.placement_group_table(pg)
        if table.get("active_scheduling_option_index") in (1, -1):
            success = True
            break
        time.sleep(0.1)
    assert (
        success
    ), f"Timed out waiting for active_scheduling_option_index to become 1. Current table: {table}"

    @ray.remote(num_cpus=1)
    def dummy_task():
        return "ok"

    # Requesting bundle_index=0 should work.
    ref = dummy_task.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        )
    ).remote()
    assert ray.get(ref) == "ok"

    # bundle_index=1 passes permissive client validation (within max fallback bounds)
    # but hangs in scheduling because it doesn't match the active strategy.
    ref_invalid = dummy_task.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=1
        )
    ).remote()

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(ref_invalid, timeout=3)

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


def test_pending_placement_group_shape_validation(ray_start_cluster):
    """
    Test that when a PG is pending, resource shape validation allows tasks that fit
    into any of the possible scheduling strategies (primary or fallback).
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, num_gpus=0)  # keep PG pending
    ray.init(address=cluster.address)

    pg = placement_group(
        name="pending_shape_pg",
        bundles=[{"CPU": 2}],
        strategy="PACK",
        fallback_strategy=[{"bundles": [{"GPU": 1}]}],
    )

    @ray.remote(num_cpus=1)
    def needs_cpu():
        return "ok"

    @ray.remote(num_cpus=0, num_gpus=1)
    def needs_gpu():
        return "ok"

    @ray.remote(num_cpus=3)
    def needs_too_much_cpu():
        return "ok"

    # First task fits in primary option and should pass validation
    try:
        needs_cpu.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    except ValueError as e:
        pytest.fail(f"Pending PG incorrectly rejected valid primary shape: {e}")

    # Second task fits in fallback option and should pass validation
    try:
        needs_gpu.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    except ValueError as e:
        pytest.fail(f"Pending PG incorrectly rejected valid fallback shape: {e}")

    # Third task fits doesn't fit in any scheduling option
    with pytest.raises(
        ValueError, match="cannot fit into any bundles across all scheduling strategies"
    ):
        needs_too_much_cpu.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()

    remove_placement_group(pg)


def test_scheduled_placement_group_strict_shape_validation(ray_start_cluster):
    """
    Test that once a PG is scheduled, tasks are validated against the active
    strategy, preventing tasks from hanging by requesting a shape from a fallback.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    # Feasible primary strategy uses CPU, infeasible
    # fallback strategy uses GPU.
    pg = placement_group(
        name="strict_shape_pg",
        bundles=[{"CPU": 1}],
        strategy="PACK",
        fallback_strategy=[{"bundles": [{"GPU": 1}]}],
    )

    # Wait for the PG to schedule using the primary strategy.
    ray.get(pg.ready(), timeout=10)

    @ray.remote(num_cpus=0, num_gpus=1)
    def needs_gpu():
        return "ok"

    # The task requests a GPU. This passes the permissive client check because
    # the fallback strategy has a GPU, but because the active strategy is CPU only,
    # it will hang on the backend. This is intended because fallback strategies can
    # reschedule.
    ref_invalid = needs_gpu.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(ref_invalid, timeout=3)

    remove_placement_group(pg)
    placement_group_assert_no_leak([pg])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
