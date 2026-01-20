"""Integration tests for array namespace expressions.

These tests require Ray and test end-to-end array namespace expression evaluation.
"""

import pandas as pd
import pyarrow as pa
import pytest
from packaging import version

import ray
from ray.data._internal.util import rows_same
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    version.parse(pa.__version__) < version.parse("19.0.0"),
    reason="Namespace expressions tests require PyArrow >= 19.0",
)


def _make_fixed_size_list_table() -> pa.Table:
    values = pa.array([1, 2, 3, 4, 5, 6], type=pa.int64())
    fixed = pa.FixedSizeListArray.from_arrays(values, list_size=2)
    return pa.Table.from_arrays([fixed], names=["features"])


def test_arr_to_list_fixed_size(ray_start_regular_shared):
    table = _make_fixed_size_list_table()
    ds = ray.data.from_arrow(table)

    result = (
        ds.with_column("features", col("features").arr.to_list())
        .select_columns(["features"])
        .to_pandas()
    )
    expected = pd.DataFrame(
        [
            {"features": [1, 2]},
            {"features": [3, 4]},
            {"features": [5, 6]},
        ]
    )

    assert rows_same(result, expected)


def test_arr_to_list_invalid_dtype_raises(ray_start_regular_shared):
    ds = ray.data.from_items([{"value": 1}, {"value": 2}])

    with pytest.raises(
        (ray.exceptions.RayTaskError, ray.exceptions.UserCodeException)
    ) as exc_info:
        ds.with_column("value_list", col("value").arr.to_list()).to_pandas()

    assert "to_list() can only be called on list-like columns" in str(exc_info.value)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
