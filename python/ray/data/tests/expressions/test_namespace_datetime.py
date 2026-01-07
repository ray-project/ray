"""Integration tests for datetime namespace expressions.

These tests require Ray and test end-to-end datetime namespace expression evaluation.
"""

import datetime

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


class TestDatetimeNamespace:
    """Tests for datetime namespace operations."""

    def test_datetime_namespace_all_operations(self, ray_start_regular_shared):
        """Test all datetime namespace operations on a datetime column."""
        ts = datetime.datetime(2024, 1, 2, 10, 30, 0)

        ds = ray.data.from_items([{"ts": ts}])

        result_ds = ds.select(
            [
                col("ts").dt.year().alias("year"),
                col("ts").dt.month().alias("month"),
                col("ts").dt.day().alias("day"),
                col("ts").dt.hour().alias("hour"),
                col("ts").dt.minute().alias("minute"),
                col("ts").dt.second().alias("second"),
                col("ts").dt.strftime("%Y-%m-%d").alias("date_str"),
                col("ts").dt.floor("day").alias("ts_floor"),
                col("ts").dt.ceil("day").alias("ts_ceil"),
                col("ts").dt.round("day").alias("ts_round"),
            ]
        )

        actual = result_ds.to_pandas()

        expected = pd.DataFrame(
            [
                {
                    "year": 2024,
                    "month": 1,
                    "day": 2,
                    "hour": 10,
                    "minute": 30,
                    "second": 0,
                    "date_str": "2024-01-02",
                    "ts_floor": datetime.datetime(2024, 1, 2, 0, 0, 0),
                    "ts_ceil": datetime.datetime(2024, 1, 3, 0, 0, 0),
                    "ts_round": datetime.datetime(2024, 1, 3, 0, 0, 0),
                }
            ]
        )

        assert rows_same(actual, expected)

    def test_dt_namespace_invalid_dtype_raises(self, ray_start_regular_shared):
        """Test that dt namespace on non-datetime column raises an error."""
        ds = ray.data.from_items([{"value": 1}])

        with pytest.raises(Exception):
            ds.select(col("value").dt.year()).to_pandas()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
