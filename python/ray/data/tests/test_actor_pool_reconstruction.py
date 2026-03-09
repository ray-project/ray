"""End-to-end tests for pool-bound lineage reconstruction with Ray Data.

These tests validate that when a node dies mid-pipeline, Ray Data pipelines
using actor pool-based map_batches can recover through pool-bound reconstruction:
the pool selects a healthy actor and re-executes the lost lineage.
"""

import numpy as np
import pytest

import ray
from ray.data.context import DataContext


@pytest.fixture
def restore_data_context():
    ctx = DataContext.get_current()
    old = ctx.use_core_actor_pool
    ctx.use_core_actor_pool = True
    yield ctx
    ctx.use_core_actor_pool = old


def test_pool_reconstruction_no_data_loss(ray_start_cluster, restore_data_context):
    """map_batches(pool) -> collect: kill a worker node, verify all rows present."""
    ray.shutdown()

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=3)
    victim = cluster.add_node(num_cpus=2)
    ray.init()

    n_rows = 200

    class Identity:
        def __call__(self, batch):
            return batch

    ds = ray.data.range(n_rows)
    ds = ds.map_batches(
        Identity,
        batch_size=10,
        concurrency=2,
    )
    cluster.remove_node(victim)

    result = ds.take_all()
    assert len(result) == n_rows, f"Expected {n_rows} rows, got {len(result)}"


def test_cascading_pool_reconstruction_with_sort(
    ray_start_cluster, restore_data_context
):
    """map_batches(pool1) -> sort -> map_batches(pool2): kill node, verify results.

    This exercises the cascading reconstruction path:
    1. Pool1 outputs are lost when a node dies.
    2. Pool-bound reconstruction re-executes Pool1 tasks on surviving actors.
    3. The sort (AllToAll) operator consumes the reconstructed outputs.
    4. Pool2 processes the sorted data.
    """
    ray.shutdown()

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=4)
    victim = cluster.add_node(num_cpus=2)
    ray.init()

    n_rows = 500

    class DoubleId:
        def __call__(self, batch):
            return {"val": batch["id"] * 2}

    class IncrementVal:
        def __call__(self, batch):
            return {"result": batch["val"] + 1}

    ds = ray.data.range(n_rows)

    ds = ds.map_batches(
        DoubleId,
        batch_size=50,
        concurrency=2,
    )

    ds = ds.sort("val")

    ds = ds.map_batches(
        IncrementVal,
        batch_size=50,
        concurrency=2,
    )

    cluster.remove_node(victim)

    result = ds.take_all()
    assert len(result) == n_rows, f"Expected {n_rows} rows, got {len(result)}"

    expected = sorted([i * 2 + 1 for i in range(n_rows)])
    actual = sorted([r["result"] for r in result])
    np.testing.assert_array_equal(actual, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
