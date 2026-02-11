import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.expressions import monotonically_increasing_id
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "block_type",
    ["arrow", "pandas"],
)
def test_monotonically_increasing_id(ray_start_regular_shared, block_type):
    """Test monotonically_increasing_id() expression produces monotonically increasing IDs."""

    if block_type == "arrow":
        blocks = [pa.table({"a": [1, 2]}), pa.table({"a": [3, 4]})]
    else:
        blocks = [pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [3, 4]})]

    # Create dataset with 2 blocks of 2 rows each
    ds = ray.data.from_blocks(blocks)
    ds = ds.with_column("uid", monotonically_increasing_id())
    expected = {0, 1, (1 << 33) + 0, (1 << 33) + 1}

    all_ids = []
    for batch in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
        block_ids = batch["uid"].to_pylist()
        all_ids.extend(block_ids)
        assert block_ids == sorted(block_ids), "block IDs are not monotonic"

    assert set(all_ids) == expected


def test_monotonically_increasing_id_multiple_expressions(ray_start_regular_shared):
    """
    Test that two monotonically_increasing_id() expressions are isolated
    if executed by the same task.
    """
    ds = ray.data.range(10, override_num_blocks=5)

    # Two monotonically_increasing_id() expressions should have isolated row counts
    ds = ds.with_column("uid1", monotonically_increasing_id())
    ds = ds.with_column("uid2", monotonically_increasing_id())

    result = ds.to_pandas()

    assert list(result["uid1"]) == list(result["uid2"])


def test_monotonically_increasing_id_multi_block_per_task(ray_start_regular_shared):
    """Test that IDs are unique when a single task processes multiple blocks."""
    ctx = ray.data.DataContext.get_current()
    original_max_block_size = ctx.target_max_block_size
    try:
        # Set max block size to 32 bytes ~ 4 int64 rows per block.
        # With 5 read tasks of 20 rows each every task should see 5 blocks.
        ctx.target_max_block_size = 32

        ds = ray.data.range(100, override_num_blocks=5)
        ds = ds.with_column("uid", monotonically_increasing_id())
        result = ds.take_all()

        uids = [row["uid"] for row in result]
        assert len(uids) == 100, f"expected 100 rows, got {len(uids)}"
        assert len(uids) == len(set(uids)), "IDs are not unique across blocks"
    finally:
        ctx.target_max_block_size = original_max_block_size


def test_monotonically_increasing_id_structurally_equals_always_false():
    """Test that structurally_equals() is False for monotonically_increasing_id() expressions."""
    expr1 = monotonically_increasing_id()
    expr2 = monotonically_increasing_id()

    # Should always be false (even to itself) due to non-determinism
    assert not expr1.structurally_equals(expr2)
    assert not expr1.structurally_equals(expr1)


def test_monotonically_increasing_id_shuffle_and_sort(ray_start_regular_shared):
    """Test monotonically_increasing_id() in shuffle and sort."""
    ds = ray.data.range(20, override_num_blocks=5)
    ds = ds.with_column("uid", monotonically_increasing_id())
    ds = ds.random_shuffle()
    ds = ds.sort("uid")

    result = ds.take_all()
    uids = [row["uid"] for row in result]

    assert len(uids) == len(set(uids)), "ids are not unique"
    assert uids == sorted(uids), "ids are not sorted"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
