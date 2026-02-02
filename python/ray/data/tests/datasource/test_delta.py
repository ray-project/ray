import os
from typing import Any, Dict, List

import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Skip all tests in this module if deltalake is not installed
deltalake = pytest.importorskip("deltalake")


@pytest.fixture
def temp_delta_path(tmp_path):
    """Fixture to provide a clean temporary path for Delta tables."""
    return os.path.join(tmp_path, "delta_table")


def _write_to_delta(data: List[Dict[str, Any]], path: str, **kwargs: Any) -> None:
    """Write data to a Delta table."""
    ds = ray.data.from_items(data)
    ds.write_delta(path, **kwargs)


def _read_from_delta(path: str, **kwargs: Any) -> List[Dict[str, Any]]:
    """Read data from a Delta table."""
    ds = ray.data.read_delta(path, **kwargs)
    return ds.take_all()


# =============================================================================
# Basic Write/Read Tests
# =============================================================================


def test_write_delta_basic(ray_start_regular_shared, tmp_path):
    """Test basic write and read."""
    path = os.path.join(tmp_path, "delta_table")
    ds = ray.data.range(100)
    ds.write_delta(path)
    ds_read = ray.data.read_delta(path)
    assert ds_read.count() == 100


def test_write_delta_append_mode(ray_start_regular_shared, temp_delta_path):
    """Test append mode."""
    ds1 = ray.data.range(50)
    ds1.write_delta(temp_delta_path, mode="append")

    ds2 = ray.data.range(30)
    ds2.write_delta(temp_delta_path, mode="append")

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 80


def test_write_delta_overwrite_mode(ray_start_regular_shared, temp_delta_path):
    """Test overwrite mode."""
    ds1 = ray.data.range(50)
    ds1.write_delta(temp_delta_path, mode="append")

    ds2 = ray.data.range(30)
    ds2.write_delta(temp_delta_path, mode="overwrite")

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 30


def test_write_delta_error_mode(ray_start_regular_shared, temp_delta_path):
    """Test error mode."""
    ds1 = ray.data.range(50)
    ds1.write_delta(temp_delta_path, mode="append")

    ds2 = ray.data.range(30)
    with pytest.raises(ValueError, match="already exists"):
        ds2.write_delta(temp_delta_path, mode="error")


def test_write_delta_ignore_mode(ray_start_regular_shared, temp_delta_path):
    """Test ignore mode."""
    ds1 = ray.data.range(50)
    ds1.write_delta(temp_delta_path, mode="append")

    ds2 = ray.data.range(30)
    ds2.write_delta(temp_delta_path, mode="ignore")

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 50


def test_write_delta_empty(ray_start_regular_shared, temp_delta_path):
    """Test writing empty dataset."""
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    ds = ray.data.from_arrow(pa.table({"id": [], "value": []}, schema=schema))
    ds.write_delta(temp_delta_path, schema=schema)
    assert os.path.exists(os.path.join(temp_delta_path, "_delta_log"))


# =============================================================================
# Partitioning Tests
# =============================================================================


def test_write_delta_partitioning(ray_start_regular_shared, temp_delta_path):
    """Test partitioning with single and multiple columns."""
    data = [
        {"year": 2024, "month": 1, "value": 100},
        {"year": 2024, "month": 2, "value": 200},
        {"year": 2023, "month": 12, "value": 300},
    ]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path, partition_cols=["year", "month"])

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 3
    assert os.path.exists(os.path.join(temp_delta_path, "year=2024"))
    assert os.path.exists(os.path.join(temp_delta_path, "year=2023"))


def test_write_delta_partition_error(ray_start_regular_shared, temp_delta_path):
    """Test error when partition column doesn't exist."""
    ds = ray.data.range(10)
    with pytest.raises(ValueError, match="Missing partition columns"):
        ds.write_delta(temp_delta_path, partition_cols=["nonexistent"])


def test_write_delta_partition_mismatch_existing(
    ray_start_regular_shared, temp_delta_path
):
    """Partition columns must match existing table metadata."""
    data = [{"year": 2024, "value": 1}]
    ray.data.from_items(data).write_delta(temp_delta_path, partition_cols=["year"])

    # Mismatched partition spec should fail.
    with pytest.raises(ValueError, match="Partition columns mismatch"):
        ray.data.from_items(data).write_delta(
            temp_delta_path, partition_cols=["year", "month"]
        )


def test_read_delta_with_partition_filters(ray_start_regular_shared, temp_delta_path):
    """Test reading with partition filters."""
    data = [
        {"year": 2023, "month": 1, "value": 100},
        {"year": 2023, "month": 2, "value": 200},
        {"year": 2024, "month": 1, "value": 300},
    ]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path, partition_cols=["year"])

    # Read with partition filter
    ds_filtered = ray.data.read_delta(
        temp_delta_path, partition_filters=[("year", "=", 2024)]
    )
    rows = ds_filtered.take_all()
    assert len(rows) == 1
    assert rows[0]["value"] == 300


# =============================================================================
# Data Type Tests
# =============================================================================


def test_write_delta_nulls(ray_start_regular_shared, temp_delta_path):
    """Test writing data with NULL values."""
    data = [{"id": 1, "value": "a"}, {"id": 2, "value": None}]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path)
    ds_read = ray.data.read_delta(temp_delta_path)
    rows = ds_read.take_all()
    assert any(row["value"] is None for row in rows)


def test_write_delta_various_types(ray_start_regular_shared, temp_delta_path):
    """Test writing various data types."""
    data = [
        {"int_col": 1, "float_col": 1.5, "str_col": "a", "bool_col": True},
        {"int_col": 2, "float_col": 2.5, "str_col": "b", "bool_col": False},
    ]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 2

    rows = ds_read.take_all()
    assert any(row["int_col"] == 1 and row["float_col"] == 1.5 for row in rows)


def test_write_delta_timestamp_data(ray_start_regular_shared, temp_delta_path):
    """Test writing timestamp data."""
    import datetime

    data = [
        {"id": 1, "ts": datetime.datetime(2024, 1, 15, 10, 30, 0)},
        {"id": 2, "ts": datetime.datetime(2024, 2, 20, 14, 45, 0)},
    ]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 2


def test_write_delta_decimal_data(ray_start_regular_shared, temp_delta_path):
    """Test writing decimal data."""
    from decimal import Decimal

    schema = pa.schema([("id", pa.int64()), ("amount", pa.decimal128(10, 2))])
    data = pa.table(
        {"id": [1, 2], "amount": [Decimal("123.45"), Decimal("678.90")]},
        schema=schema,
    )
    ds = ray.data.from_arrow(data)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 2


def test_write_delta_string_data(ray_start_regular_shared, temp_delta_path):
    """Test writing string data with special characters."""
    data = [
        {"id": 1, "text": "hello world"},
        {"id": 2, "text": "special chars: @#$%^&*()"},
        {"id": 3, "text": "unicode: café"},
    ]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 3

    rows = ds_read.take_all()
    texts = {row["text"] for row in rows}
    assert "hello world" in texts
    assert "special chars: @#$%^&*()" in texts


# =============================================================================
# Read Tests
# =============================================================================


def test_read_delta_column_projection(ray_start_regular_shared, temp_delta_path):
    """Read with column projection returns only requested columns."""
    data = [{"id": i, "value": i * 2} for i in range(5)]
    ray.data.from_items(data).write_delta(temp_delta_path)

    ds = ray.data.read_delta(temp_delta_path, columns=["id"])
    rows = ds.take_all()
    assert all(set(row.keys()) == {"id"} for row in rows)
    assert sorted(row["id"] for row in rows) == list(range(5))


def test_read_delta_time_travel_version(ray_start_regular_shared, temp_delta_path):
    """Test reading specific version of Delta table (time travel)."""
    # Write version 0
    ds1 = ray.data.from_items([{"id": 1, "value": "v0"}])
    ds1.write_delta(temp_delta_path, mode="append")

    # Write version 1
    ds2 = ray.data.from_items([{"id": 2, "value": "v1"}])
    ds2.write_delta(temp_delta_path, mode="append")

    # Read latest version (should have both rows)
    ds_latest = ray.data.read_delta(temp_delta_path)
    assert ds_latest.count() == 2

    # Read version 0 (should only have first row)
    ds_v0 = ray.data.read_delta(temp_delta_path, version=0)
    assert ds_v0.count() == 1
    rows_v0 = ds_v0.take_all()
    assert rows_v0[0]["value"] == "v0"


# =============================================================================
# Write Mode Validation Tests
# =============================================================================


def test_write_delta_invalid_mode(ray_start_regular_shared, temp_delta_path):
    """Test that invalid mode raises ValueError."""
    ds = ray.data.range(10)
    with pytest.raises(ValueError, match="Invalid mode"):
        ds.write_delta(temp_delta_path, mode="invalid_mode")


# =============================================================================
# Upsert Mode Tests
# =============================================================================


def test_write_delta_upsert_basic(ray_start_regular_shared, temp_delta_path):
    """Test basic upsert functionality."""
    from ray.data import SaveMode

    # Create initial table
    initial_data = [{"id": 1, "value": "original"}, {"id": 2, "value": "original2"}]
    _write_to_delta(initial_data, temp_delta_path)

    # Upsert: update id=1, insert id=3
    upsert_data = [{"id": 1, "value": "updated"}, {"id": 3, "value": "new"}]
    _write_to_delta(
        upsert_data,
        temp_delta_path,
        mode=SaveMode.UPSERT,
        upsert_kwargs={"join_cols": ["id"]},
    )

    # Verify results
    rows = _read_from_delta(temp_delta_path)
    rows_dict = {row["id"]: row["value"] for row in rows}
    assert rows_dict[1] == "updated"  # Updated
    assert rows_dict[2] == "original2"  # Unchanged
    assert rows_dict[3] == "new"  # Inserted


def test_write_delta_upsert_requires_existing_table(
    ray_start_regular_shared, temp_delta_path
):
    """Test that upsert mode requires an existing table."""
    from ray.data import SaveMode

    # Upsert on non-existent table should fail
    with pytest.raises(ValueError, match="requires an existing Delta table"):
        _write_to_delta(
            [{"id": 1}],
            temp_delta_path,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["id"]},
        )


def test_write_delta_upsert_requires_join_cols(
    ray_start_regular_shared, temp_delta_path
):
    """Test that upsert mode requires join_cols."""
    from ray.data import SaveMode

    # Create table first
    _write_to_delta([{"id": 1, "value": "original"}], temp_delta_path)

    # Upsert without join_cols should fail
    with pytest.raises(ValueError, match="requires join_cols"):
        _write_to_delta([{"id": 2}], temp_delta_path, mode=SaveMode.UPSERT)


def test_write_delta_upsert_kwargs_validation(
    ray_start_regular_shared, temp_delta_path
):
    """Test that upsert_kwargs requires upsert mode."""
    from ray.data import SaveMode

    # upsert_kwargs with non-upsert mode should fail
    with pytest.raises(ValueError, match="can only be specified with SaveMode.UPSERT"):
        _write_to_delta(
            [{"id": 1}],
            temp_delta_path,
            mode=SaveMode.APPEND,
            upsert_kwargs={"join_cols": ["id"]},
        )


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


def test_write_delta_large_dataset(ray_start_regular_shared, temp_delta_path):
    """Test writing larger dataset to verify partitioning and file handling."""
    ds = ray.data.range(10000)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 10000


def test_write_delta_multiple_blocks(ray_start_regular_shared, temp_delta_path):
    """Test writing dataset with multiple blocks."""
    # Create dataset with multiple blocks
    ds = ray.data.range(100, override_num_blocks=10)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 100


def test_write_delta_nested_type_unsupported(ray_start_regular_shared, temp_delta_path):
    """Test that nested types in partition columns are rejected."""
    # Nested types as partition columns should fail
    data = [{"id": 1, "nested": {"a": 1}}, {"id": 2, "nested": {"a": 2}}]
    ds = ray.data.from_items(data)

    with pytest.raises(ValueError, match="nested type|Nested types"):
        ds.write_delta(temp_delta_path, partition_cols=["nested"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
