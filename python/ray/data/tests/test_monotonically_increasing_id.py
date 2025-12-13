import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.expressions import monotonically_increasing_id
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_monotonically_increasing_id(ray_start_regular_shared):
    """Test monotonically_increasing_id() expression produces monotonically increasing IDs."""

    def check_ids(ds, expected_ids, batch_format="default"):
        all_ids = []
        for block in ds.iter_batches(batch_size=None, batch_format=batch_format):
            if batch_format == "pandas":
                assert isinstance(block, pd.DataFrame)
                block_ids = block["uid"].tolist()
            elif batch_format == "pyarrow":
                assert isinstance(block, pa.Table)
                block_ids = block["uid"].to_pylist()
            elif batch_format == "numpy":
                assert isinstance(block, dict)
                assert isinstance(block["uid"], np.ndarray)
                block_ids = block["uid"].tolist()
            else:
                block_ids = list(block["uid"])

            print(f"block_ids: {block_ids}")
            all_ids.extend(block_ids)
            assert block_ids == sorted(block_ids), "block IDs are not monotonic"
            if len(block_ids) > 1:
                diffs = [
                    block_ids[i + 1] - block_ids[i] for i in range(len(block_ids) - 1)
                ]
                assert all(d == 1 for d in diffs), "block IDs are not consecutive"

        assert set(all_ids) == expected_ids

    # Create dataset with 2 blocks of 2 rows each
    expected = {0, 1, (1 << 33) + 0, (1 << 33) + 1}

    ds = ray.data.from_items(
        [{"a": 1}, {"a": 2}, {"a": 1}, {"a": 2}], override_num_blocks=2
    )
    ds = ds.with_column("uid", monotonically_increasing_id())
    check_ids(ds, expected)

    # pandas
    ds_pandas = ray.data.from_pandas([pd.DataFrame({"a": [1, 2]}) for _ in range(2)])
    ds_pandas = ds_pandas.with_column("uid", monotonically_increasing_id())
    check_ids(ds_pandas, expected, batch_format="pandas")

    # pyarrow
    ds_arrow = ray.data.range(4, override_num_blocks=2)
    ds_arrow = ds_arrow.with_column("uid", monotonically_increasing_id())
    check_ids(ds_arrow, expected, batch_format="pyarrow")

    # numpy batch
    ds_numpy = ray.data.from_numpy([np.ones((2, 1)) for _ in range(2)])
    ds_numpy = ds_numpy.with_column("uid", monotonically_increasing_id())
    check_ids(ds_numpy, expected, batch_format="numpy")


def test_monotonically_increasing_id_multiple_expressions(ray_start_regular_shared):
    """Test that two monotonically_increasing_id() expressions are isolated."""
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
