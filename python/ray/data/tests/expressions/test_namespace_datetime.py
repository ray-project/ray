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

        result_ds = (
            ds.with_column("year", col("ts").dt.year())
            .with_column("month", col("ts").dt.month())
            .with_column("day", col("ts").dt.day())
            .with_column("hour", col("ts").dt.hour())
            .with_column("minute", col("ts").dt.minute())
            .with_column("second", col("ts").dt.second())
            .with_column("date_str", col("ts").dt.strftime("%Y-%m-%d"))
            .with_column("ts_floor", col("ts").dt.floor("day"))
            .with_column("ts_ceil", col("ts").dt.ceil("day"))
            .with_column("ts_round", col("ts").dt.round("day"))
            .drop_columns(["ts"])
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
                    "ts_floor": pd.Timestamp("2024-01-02"),
                    "ts_ceil": pd.Timestamp("2024-01-03"),
                    # round("day") rounds to nearest day; 10:30 < 12:00 so rounds down
                    "ts_round": pd.Timestamp("2024-01-02"),
                }
            ]
        )

        assert rows_same(actual, expected)

    def test_dt_namespace_invalid_dtype_raises(self, ray_start_regular_shared):
        """Test that dt namespace on non-datetime column raises an error."""
        ds = ray.data.from_items([{"value": 1}])

        with pytest.raises(Exception):
            ds.with_column("year", col("value").dt.year()).to_pandas()

    def test_dt_assume_timezone(self, ray_start_regular_shared):
        """Test assume_timezone expression."""
        import pyarrow as pa

        # Naive timestamps
        table = pa.table(
            {
                "ts": pa.array(
                    [1704067200000000],
                    type=pa.timestamp("us"),  # 2024-01-01 00:00:00
                )
            }
        )
        ds = ray.data.from_arrow(table)
        result = ds.with_column(
            "ts_utc", col("ts").dt.assume_timezone("UTC")
        ).to_pandas()
        assert result["ts_utc"].dt.tz is not None
        assert str(result["ts_utc"].dt.tz) == "UTC"

    def test_dt_tz_convert(self, ray_start_regular_shared):
        """Test tz_convert expression."""
        import pyarrow as pa

        # UTC timestamps
        table = pa.table(
            {
                "ts": pa.array(
                    [1704067200000000],
                    type=pa.timestamp("us", tz="UTC"),  # 2024-01-01 00:00:00 UTC
                )
            }
        )
        ds = ray.data.from_arrow(table)
        result = ds.with_column(
            "ts_ny", col("ts").dt.tz_convert("America/New_York")
        ).to_pandas()
        # UTC midnight = NY 19:00 previous day
        assert result["ts_ny"].dt.tz is not None
        assert str(result["ts_ny"].dt.tz) == "America/New_York"
        assert result["ts_ny"].iloc[0].hour == 19


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
