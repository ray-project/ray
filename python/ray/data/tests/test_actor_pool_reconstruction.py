"""End-to-end tests for pool-bound lineage reconstruction with Ray Data.

These tests validate that when a node dies mid-pipeline, Ray Data pipelines
using actor pool-based map_batches can recover through pool-bound reconstruction:
the pool selects a healthy actor and re-executes the lost lineage.

NOTE: These tests are gated behind CORE_ACTOR_POOL_ENABLED because the
Ray Data <-> Core ActorPool integration is not yet wired end-to-end.
Once ActorPoolMapOperator is updated to use the Core ActorPool primitive,
remove the skipif marker.
"""

import os

import numpy as np
import pytest

import ray

CORE_ACTOR_POOL_ENABLED = os.environ.get("RAY_CORE_ACTOR_POOL_ENABLED", "0") == "1"
skip_reason = (
    "Core ActorPool <-> Ray Data integration not yet complete. "
    "Set RAY_CORE_ACTOR_POOL_ENABLED=1 to run."
)


@pytest.fixture
def cluster(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    head = cluster.add_node(num_cpus=1)  # noqa: F841
    worker1 = cluster.add_node(num_cpus=2)  # noqa: F841
    worker2 = cluster.add_node(num_cpus=2)  # noqa: F841
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    yield cluster
    ray.shutdown()


@pytest.mark.skipif(not CORE_ACTOR_POOL_ENABLED, reason=skip_reason)
def test_pool_reconstruction_no_data_loss(cluster):
    """map_batches(pool) -> collect: kill a worker node, verify all rows present."""
    n_rows = 200

    ds = ray.data.range(n_rows)
    ds = ds.map_batches(
        lambda batch: batch,
        batch_size=10,
        compute=ray.data.ActorPoolStrategy(size=2),
    )
    # Kill a worker node after the pipeline has started but before completion.
    # In a real scenario this would trigger lineage reconstruction for any
    # in-flight tasks whose results were stored on the dead node.
    cluster.remove_node(cluster.worker_nodes[-1])

    result = ds.take_all()
    assert len(result) == n_rows, f"Expected {n_rows} rows, got {len(result)}"


@pytest.mark.skipif(not CORE_ACTOR_POOL_ENABLED, reason=skip_reason)
def test_cascading_pool_reconstruction_with_sort(cluster):
    """map_batches(pool1) -> sort -> map_batches(pool2): kill node, verify results.

    This exercises the cascading reconstruction path:
    1. Pool1 outputs are lost when a node dies.
    2. Pool-bound reconstruction re-executes Pool1 tasks on surviving actors.
    3. The sort (AllToAll) operator consumes the reconstructed outputs.
    4. Pool2 processes the sorted data.
    """
    n_rows = 500

    ds = ray.data.range(n_rows)

    # Stage 1: actor pool map
    ds = ds.map_batches(
        lambda batch: {"val": batch["id"] * 2},
        batch_size=50,
        compute=ray.data.ActorPoolStrategy(size=2),
    )

    # Stage 2: AllToAll (sort)
    ds = ds.sort("val")

    # Stage 3: second actor pool map
    ds = ds.map_batches(
        lambda batch: {"result": batch["val"] + 1},
        batch_size=50,
        compute=ray.data.ActorPoolStrategy(size=2),
    )

    # Kill a worker node to trigger reconstruction in both pools.
    cluster.remove_node(cluster.worker_nodes[-1])

    result = ds.take_all()
    assert len(result) == n_rows, f"Expected {n_rows} rows, got {len(result)}"

    # Verify correctness: result = (id * 2) + 1 for each original id.
    expected = sorted([i * 2 + 1 for i in range(n_rows)])
    actual = sorted([r["result"] for r in result])
    np.testing.assert_array_equal(actual, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
