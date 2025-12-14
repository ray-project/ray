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


def test_monotonically_increasing_id_structurally_equals_always_false():
    """Test that structurally_equals() is False for monotonically_increasing_id() expressions."""
    expr1 = monotonically_increasing_id()
    expr2 = monotonically_increasing_id()

    # Should always be false (even to itself) due to non-determinism
    assert not expr1.structurally_equals(expr2)
    assert not expr1.structurally_equals(expr1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
