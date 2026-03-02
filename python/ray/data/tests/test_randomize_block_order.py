import pytest

import ray
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators import RandomizeBlocks
from ray.data._internal.planner import create_planner
from ray.data.context import DataContext
from ray.data.tests.test_util import get_parquet_read_logical_op


def test_randomize_blocks_operator(ray_start_regular_shared):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = RandomizeBlocks(
        read_op,
        seed=0,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "RandomizeBlockOrder"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


@pytest.mark.parametrize("seed", [0, 42])
def test_randomize_blocks_different_order_per_epoch_with_seed(
    ray_start_regular_shared, seed
):
    ds = ray.data.range(100, override_num_blocks=20).randomize_block_order(seed=seed)
    it = ds.iterator()
    epoch1 = [r["id"] for r in it.iter_rows()]
    epoch2 = [r["id"] for r in it.iter_rows()]

    # Different orderings across epochs.
    assert epoch1 != epoch2
    # Same set of values.
    assert sorted(epoch1) == sorted(epoch2)


def test_randomize_blocks_deterministic_with_seed(ray_start_regular_shared):
    ds1 = ray.data.range(100, override_num_blocks=20).randomize_block_order(seed=42)
    ds2 = ray.data.range(100, override_num_blocks=20).randomize_block_order(seed=42)

    result1 = [r["id"] for r in ds1.iter_rows()]
    result2 = [r["id"] for r in ds2.iter_rows()]

    # Same seed and same execution_idx should produce the same ordering.
    assert result1 == result2


def test_randomize_blocks_no_seed_varies(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=20).randomize_block_order()
    it = ds.iterator()
    epoch1 = [r["id"] for r in it.iter_rows()]
    epoch2 = [r["id"] for r in it.iter_rows()]

    # Without a seed, different epochs should (almost certainly) differ.
    assert epoch1 != epoch2
    assert sorted(epoch1) == sorted(epoch2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
