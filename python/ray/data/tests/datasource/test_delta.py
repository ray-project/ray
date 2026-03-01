import os
from typing import Any, Dict, List
from unittest.mock import Mock

import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Skip all tests in this module if deltalake is not installed
pytest.importorskip("deltalake")


@pytest.fixture
def temp_delta_path(tmp_path):
    """Fixture to provide a clean temporary path for Delta tables."""
    return os.path.join(str(tmp_path), "delta_table")


def _write_to_delta(data: List[Dict[str, Any]], path: str, **kwargs: Any) -> None:
    """Write data to a Delta table."""
    ray.data.from_items(data).write_delta(path, **kwargs)


def _read_from_delta(path: str, **kwargs: Any) -> List[Dict[str, Any]]:
    """Read data from a Delta table."""
    return ray.data.read_delta(path, **kwargs).take_all()


def _delta_log_exists(path: str) -> bool:
    """Return True if a local delta log directory exists."""
    return os.path.isdir(os.path.join(path, "_delta_log"))


# =============================================================================
# Basic Write/Read Tests
# =============================================================================


def test_write_delta_basic(ray_start_regular_shared, tmp_path):
    path = os.path.join(str(tmp_path), "delta_table")
    ds = ray.data.range(100)
    ds.write_delta(path)
    ds_read = ray.data.read_delta(path)
    assert ds_read.count() == 100


def test_write_delta_append_mode(ray_start_regular_shared, temp_delta_path):
    ray.data.range(50).write_delta(temp_delta_path, mode="append")
    ray.data.range(30).write_delta(temp_delta_path, mode="append")
    assert ray.data.read_delta(temp_delta_path).count() == 80


def test_write_delta_overwrite_mode(ray_start_regular_shared, temp_delta_path):
    ray.data.range(50).write_delta(temp_delta_path, mode="append")
    ray.data.range(30).write_delta(temp_delta_path, mode="overwrite")
    assert ray.data.read_delta(temp_delta_path).count() == 30


def test_write_delta_error_mode(ray_start_regular_shared, temp_delta_path):
    ray.data.range(50).write_delta(temp_delta_path, mode="append")
    with pytest.raises(ValueError, match=r"already exists"):
        ray.data.range(30).write_delta(temp_delta_path, mode="error")


def test_write_delta_ignore_mode(ray_start_regular_shared, temp_delta_path):
    ray.data.range(50).write_delta(temp_delta_path, mode="append")
    ray.data.range(30).write_delta(temp_delta_path, mode="ignore")
    assert ray.data.read_delta(temp_delta_path).count() == 50


def test_write_delta_empty_new_table_requires_schema(
    ray_start_regular_shared, temp_delta_path
):
    """Your implementation requires schema to create a new empty table."""
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    empty = pa.table({"id": [], "value": []}, schema=schema)
    ray.data.from_arrow(empty).write_delta(temp_delta_path, schema=schema)
    assert _delta_log_exists(temp_delta_path)


# =============================================================================
# Partitioning Tests
# =============================================================================


def test_write_delta_partitioning(ray_start_regular_shared, temp_delta_path):
    data = [
        {"year": 2024, "month": 1, "value": 100},
        {"year": 2024, "month": 2, "value": 200},
        {"year": 2023, "month": 12, "value": 300},
    ]
    ray.data.from_items(data).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )
    assert ray.data.read_delta(temp_delta_path).count() == 3

    # Local path check (tests run on local tmp dir).
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2024"))
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2023"))


def test_write_delta_partition_error_missing_column(
    ray_start_regular_shared, temp_delta_path
):
    ds = ray.data.range(10)
    with pytest.raises(ValueError, match=r"Missing partition columns"):
        ds.write_delta(temp_delta_path, partition_cols=["nonexistent"])


def test_write_delta_partition_mismatch_existing(
    ray_start_regular_shared, temp_delta_path
):
    data = [{"year": 2024, "value": 1}]
    ray.data.from_items(data).write_delta(temp_delta_path, partition_cols=["year"])

    with pytest.raises(ValueError, match=r"Partition columns mismatch"):
        ray.data.from_items(data).write_delta(
            temp_delta_path, partition_cols=["year", "month"]
        )


def test_read_delta_with_partition_filters(ray_start_regular_shared, temp_delta_path):
    data = [
        {"year": 2023, "month": 1, "value": 100},
        {"year": 2023, "month": 2, "value": 200},
        {"year": 2024, "month": 1, "value": 300},
    ]
    ray.data.from_items(data).write_delta(temp_delta_path, partition_cols=["year"])

    # Your DeltaDatasource normalizes values to strings internally; passing int is fine.
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
    data = [{"id": 1, "value": "a"}, {"id": 2, "value": None}]
    ray.data.from_items(data).write_delta(temp_delta_path)
    rows = ray.data.read_delta(temp_delta_path).take_all()
    assert any(r["value"] is None for r in rows)


def test_write_delta_various_types(ray_start_regular_shared, temp_delta_path):
    data = [
        {"int_col": 1, "float_col": 1.5, "str_col": "a", "bool_col": True},
        {"int_col": 2, "float_col": 2.5, "str_col": "b", "bool_col": False},
    ]
    ray.data.from_items(data).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 2


def test_write_delta_timestamp_data(ray_start_regular_shared, temp_delta_path):
    import datetime

    data = [
        {"id": 1, "ts": datetime.datetime(2024, 1, 15, 10, 30, 0)},
        {"id": 2, "ts": datetime.datetime(2024, 2, 20, 14, 45, 0)},
    ]
    ray.data.from_items(data).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 2


def test_write_delta_decimal_data(ray_start_regular_shared, temp_delta_path):
    from decimal import Decimal

    schema = pa.schema([("id", pa.int64()), ("amount", pa.decimal128(10, 2))])
    tbl = pa.table(
        {"id": [1, 2], "amount": [Decimal("123.45"), Decimal("678.90")]}, schema=schema
    )
    ray.data.from_arrow(tbl).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 2


def test_write_delta_string_data(ray_start_regular_shared, temp_delta_path):
    data = [
        {"id": 1, "text": "hello world"},
        {"id": 2, "text": "special chars: @#$%^&*()"},
        {"id": 3, "text": "unicode: café"},
    ]
    ray.data.from_items(data).write_delta(temp_delta_path)
    rows = ray.data.read_delta(temp_delta_path).take_all()
    assert len(rows) == 3
    texts = {r["text"] for r in rows}
    assert "hello world" in texts
    assert "special chars: @#$%^&*()" in texts
    assert "unicode: café" in texts


def test_write_delta_complex_types_maps_structs(ray_start_regular_shared, temp_delta_path):
    """Test that Delta Lake supports maps and structs (complex types)."""
    # Create data with maps and structs
    # Note: PyArrow maps are represented as list of key-value pairs
    # Structs are represented as dictionaries
    schema = pa.schema([
        ("id", pa.int64()),
        ("metadata", pa.map_(pa.string(), pa.string())),  # Map type
        ("address", pa.struct([  # Struct type
            ("street", pa.string()),
            ("city", pa.string()),
            ("zip", pa.int32()),
        ])),
    ])

    # Create PyArrow table with complex types
    map_data = [
        [("key1", "value1"), ("key2", "value2")],
        [("key3", "value3")],
    ]
    struct_data = [
        {"street": "123 Main St", "city": "San Francisco", "zip": 94102},
        {"street": "456 Oak Ave", "city": "New York", "zip": 10001},
    ]

    table = pa.table({
        "id": [1, 2],
        "metadata": map_data,
        "address": struct_data,
    }, schema=schema)

    # Write to Delta
    ray.data.from_arrow(table).write_delta(temp_delta_path)

    # Read back and verify
    ds = ray.data.read_delta(temp_delta_path)
    assert ds.count() == 2

    rows = ds.take_all()
    assert len(rows) == 2

    # Verify map data
    assert len(rows[0]["metadata"]) == 2
    assert rows[0]["metadata"][0]["key"] == "key1"
    assert rows[0]["metadata"][0]["value"] == "value1"

    # Verify struct data
    assert rows[0]["address"]["street"] == "123 Main St"
    assert rows[0]["address"]["city"] == "San Francisco"
    assert rows[0]["address"]["zip"] == 94102


# =============================================================================
# Read Tests
# =============================================================================


def test_read_delta_column_projection(ray_start_regular_shared, temp_delta_path):
    data = [{"id": i, "value": i * 2} for i in range(5)]
    ray.data.from_items(data).write_delta(temp_delta_path)

    rows = ray.data.read_delta(temp_delta_path, columns=["id"]).take_all()
    assert all(set(r.keys()) == {"id"} for r in rows)
    assert sorted(r["id"] for r in rows) == list(range(5))


def test_read_delta_time_travel_version(ray_start_regular_shared, temp_delta_path):
    # version 0
    ray.data.from_items([{"id": 1, "value": "v0"}]).write_delta(
        temp_delta_path, mode="append"
    )
    # version 1
    ray.data.from_items([{"id": 2, "value": "v1"}]).write_delta(
        temp_delta_path, mode="append"
    )

    assert ray.data.read_delta(temp_delta_path).count() == 2

    ds_v0 = ray.data.read_delta(temp_delta_path, version=0)
    assert ds_v0.count() == 1
    rows_v0 = ds_v0.take_all()
    assert rows_v0[0]["value"] == "v0"


# =============================================================================
# Write Mode Validation Tests
# =============================================================================


def test_write_delta_invalid_mode(ray_start_regular_shared, temp_delta_path):
    ds = ray.data.range(10)
    # Your implementation raises: "Invalid mode '...'. Supported: [...]"
    with pytest.raises(ValueError, match=r"Invalid mode"):
        ds.write_delta(temp_delta_path, mode="invalid_mode")


# =============================================================================
# Upsert Mode Tests
# =============================================================================


def test_write_delta_upsert_basic(ray_start_regular_shared, temp_delta_path):
    from ray.data import SaveMode

    _write_to_delta(
        [{"id": 1, "value": "original"}, {"id": 2, "value": "original2"}],
        temp_delta_path,
        mode=SaveMode.APPEND,
    )

    _write_to_delta(
        [{"id": 1, "value": "updated"}, {"id": 3, "value": "new"}],
        temp_delta_path,
        mode=SaveMode.UPSERT,
        upsert_kwargs={"join_cols": ["id"]},
    )

    rows = _read_from_delta(temp_delta_path)
    rows_dict = {r["id"]: r["value"] for r in rows}
    assert rows_dict[1] == "updated"
    assert rows_dict[2] == "original2"
    assert rows_dict[3] == "new"


def test_write_delta_upsert_requires_existing_table(
    ray_start_regular_shared, temp_delta_path
):
    from ray.data import SaveMode

    with pytest.raises(ValueError, match=r"requires an existing Delta table"):
        _write_to_delta(
            [{"id": 1, "value": "x"}],
            temp_delta_path,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["id"]},
        )


def test_write_delta_upsert_requires_join_cols(
    ray_start_regular_shared, temp_delta_path
):
    from ray.data import SaveMode

    _write_to_delta(
        [{"id": 1, "value": "original"}], temp_delta_path, mode=SaveMode.APPEND
    )

    with pytest.raises(
        ValueError, match=r"requires join_cols|requires join_cols in upsert_kwargs"
    ):
        _write_to_delta(
            [{"id": 2, "value": "x"}], temp_delta_path, mode=SaveMode.UPSERT
        )


def test_write_delta_upsert_kwargs_validation(
    ray_start_regular_shared, temp_delta_path
):
    from ray.data import SaveMode

    with pytest.raises(
        ValueError, match=r"upsert_kwargs can only be specified with SaveMode\.UPSERT"
    ):
        _write_to_delta(
            [{"id": 1, "value": "x"}],
            temp_delta_path,
            mode=SaveMode.APPEND,
            upsert_kwargs={"join_cols": ["id"]},
        )


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


def test_write_delta_large_dataset(ray_start_regular_shared, temp_delta_path):
    ray.data.range(10_000).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 10_000


def test_write_delta_multiple_blocks(ray_start_regular_shared, temp_delta_path):
    ray.data.range(100, override_num_blocks=10).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 100


def test_write_delta_nested_type_unsupported(ray_start_regular_shared, temp_delta_path):
    # Partition columns must not be nested/dictionary
    data = [{"id": 1, "nested": {"a": 1}}, {"id": 2, "nested": {"a": 2}}]
    ds = ray.data.from_items(data)

    with pytest.raises(ValueError, match=r"nested type|nested"):
        ds.write_delta(temp_delta_path, partition_cols=["nested"])


def test_write_delta_invalid_schema_mode(ray_start_regular_shared, temp_delta_path):
    """Test that invalid schema_mode values raise errors."""
    # Create initial table
    data1 = [{"id": 1, "value": 100}]
    ray.data.from_items(data1).write_delta(temp_delta_path)

    # Try to write with new column and invalid schema_mode
    data2 = [{"id": 1, "value": 100, "new_col": 200}]
    with pytest.raises(ValueError, match=r"Invalid schema_mode"):
        ray.data.from_items(data2).write_delta(
            temp_delta_path, schema_mode="invalid_mode"
        )

    # Test typo in schema_mode
    with pytest.raises(ValueError, match=r"Invalid schema_mode"):
        ray.data.from_items(data2).write_delta(
            temp_delta_path, schema_mode="errror"  # typo
        )


# =============================================================================
# Partition Overwrite Mode Tests
# =============================================================================


def test_write_delta_overwrite_static_mode(ray_start_regular_shared, temp_delta_path):
    """Test static partition overwrite mode (default) - deletes all data."""
    # Create partitioned table with multiple partitions
    data1 = [
        {"year": 2023, "month": 1, "value": 100},
        {"year": 2023, "month": 2, "value": 200},
        {"year": 2024, "month": 1, "value": 300},
    ]
    ray.data.from_items(data1).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )
    assert ray.data.read_delta(temp_delta_path).count() == 3

    # Overwrite with static mode (default) - should delete all partitions
    data2 = [{"year": 2024, "month": 3, "value": 400}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year", "month"],
        partition_overwrite_mode="static",
    )
    # Should only have new data
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 1
    assert result[0]["year"] == 2024
    assert result[0]["month"] == 3


def test_write_delta_overwrite_dynamic_mode(ray_start_regular_shared, temp_delta_path):
    """Test dynamic partition overwrite mode - only deletes written partitions."""
    # Create partitioned table with multiple partitions
    data1 = [
        {"year": 2023, "month": 1, "value": 100},
        {"year": 2023, "month": 2, "value": 200},
        {"year": 2024, "month": 1, "value": 300},
        {"year": 2024, "month": 2, "value": 400},
    ]
    ray.data.from_items(data1).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )
    assert ray.data.read_delta(temp_delta_path).count() == 4

    # Overwrite only year=2024, month=1 partition with dynamic mode
    data2 = [{"year": 2024, "month": 1, "value": 999}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year", "month"],
        partition_overwrite_mode="dynamic",
    )
    # Should have: updated 2024/1, plus original 2023/1, 2023/2, 2024/2
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 4
    # Check that 2024/1 was updated
    year_2024_month_1 = [r for r in result if r["year"] == 2024 and r["month"] == 1]
    assert len(year_2024_month_1) == 1
    assert year_2024_month_1[0]["value"] == 999
    # Check other partitions still exist
    assert any(r["year"] == 2023 and r["month"] == 1 for r in result)
    assert any(r["year"] == 2023 and r["month"] == 2 for r in result)
    assert any(r["year"] == 2024 and r["month"] == 2 for r in result)


def test_write_delta_overwrite_dynamic_multiple_partitions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode overwriting multiple partitions."""
    # Create initial data
    data1 = [
        {"year": 2023, "month": 1, "value": 100},
        {"year": 2023, "month": 2, "value": 200},
        {"year": 2024, "month": 1, "value": 300},
        {"year": 2024, "month": 2, "value": 400},
    ]
    ray.data.from_items(data1).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )

    # Overwrite multiple partitions (2023/1 and 2024/2)
    data2 = [
        {"year": 2023, "month": 1, "value": 111},
        {"year": 2024, "month": 2, "value": 444},
    ]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year", "month"],
        partition_overwrite_mode="dynamic",
    )
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 4
    # Check updated values
    assert any(
        r["year"] == 2023 and r["month"] == 1 and r["value"] == 111 for r in result
    )
    assert any(
        r["year"] == 2024 and r["month"] == 2 and r["value"] == 444 for r in result
    )
    # Check unchanged partitions
    assert any(
        r["year"] == 2023 and r["month"] == 2 and r["value"] == 200 for r in result
    )
    assert any(
        r["year"] == 2024 and r["month"] == 1 and r["value"] == 300 for r in result
    )


def test_write_delta_overwrite_dynamic_no_partitions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode with non-partitioned table falls back to static."""
    # Create non-partitioned table
    ray.data.range(50).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 50

    # Overwrite with dynamic mode (should use static since no partitions)
    ray.data.range(30).write_delta(
        temp_delta_path, mode="overwrite", partition_overwrite_mode="dynamic"
    )
    # Should delete all and replace
    assert ray.data.read_delta(temp_delta_path).count() == 30


def test_write_delta_overwrite_dynamic_new_partitions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode when writing to new partitions."""
    # Create initial data
    data1 = [{"year": 2023, "month": 1, "value": 100}]
    ray.data.from_items(data1).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )

    # Write to new partition (2024/1) - should not delete existing partition
    data2 = [{"year": 2024, "month": 1, "value": 200}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year", "month"],
        partition_overwrite_mode="dynamic",
    )
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 2
    assert any(r["year"] == 2023 and r["month"] == 1 for r in result)
    assert any(r["year"] == 2024 and r["month"] == 1 for r in result)


def test_write_delta_overwrite_dynamic_invalid_mode(
    ray_start_regular_shared, temp_delta_path
):
    """Test that invalid partition_overwrite_mode raises error."""
    ray.data.range(10).write_delta(temp_delta_path)
    with pytest.raises(ValueError, match=r"partition_overwrite_mode"):
        ray.data.range(5).write_delta(
            temp_delta_path,
            mode="overwrite",
            partition_overwrite_mode="invalid",
        )


def test_write_delta_overwrite_dynamic_with_nulls(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode handles NULL partition values correctly."""
    # Create table with NULL partition values
    data1 = [
        {"year": 2023, "month": 1, "category": "A", "value": 100},
        {"year": 2023, "month": None, "category": "B", "value": 200},
    ]
    ray.data.from_items(data1).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )

    # Overwrite partition with NULL month
    data2 = [{"year": 2023, "month": None, "category": "B", "value": 999}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year", "month"],
        partition_overwrite_mode="dynamic",
    )
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 2
    # Check NULL partition was updated
    null_partition = [r for r in result if r["month"] is None]
    assert len(null_partition) == 1
    assert null_partition[0]["value"] == 999
    # Check other partition unchanged
    assert any(r["month"] == 1 and r["value"] == 100 for r in result)


def test_write_delta_overwrite_dynamic_integer_partitions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode works correctly with integer partition values."""
    # Create table with integer partition columns
    data1 = [
        {"year": 2023, "value": 100},
        {"year": 2024, "value": 200},
    ]
    ray.data.from_items(data1).write_delta(temp_delta_path, partition_cols=["year"])
    assert ray.data.read_delta(temp_delta_path).count() == 2

    # Overwrite year=2024 partition
    data2 = [{"year": 2024, "value": 999}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
    )
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 2
    # Check year=2024 was updated
    year_2024 = [r for r in result if r["year"] == 2024]
    assert len(year_2024) == 1
    assert year_2024[0]["value"] == 999
    # Check year=2023 unchanged
    assert any(r["year"] == 2023 and r["value"] == 100 for r in result)


def test_write_delta_overwrite_dynamic_string_partitions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode works correctly with string partition values."""
    # Create table with string partition columns
    data1 = [
        {"region": "us-west", "value": 100},
        {"region": "us-east", "value": 200},
    ]
    ray.data.from_items(data1).write_delta(temp_delta_path, partition_cols=["region"])
    assert ray.data.read_delta(temp_delta_path).count() == 2

    # Overwrite us-west partition
    data2 = [{"region": "us-west", "value": 999}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["region"],
        partition_overwrite_mode="dynamic",
    )
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 2
    # Check us-west was updated
    us_west = [r for r in result if r["region"] == "us-west"]
    assert len(us_west) == 1
    assert us_west[0]["value"] == 999
    # Check us-east unchanged
    assert any(r["region"] == "us-east" and r["value"] == 200 for r in result)


def test_write_delta_overwrite_dynamic_empty_file_actions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode with empty file actions falls back to delete all."""
    # Create partitioned table
    data1 = [{"year": 2023, "value": 100}]
    ray.data.from_items(data1).write_delta(temp_delta_path, partition_cols=["year"])

    # Empty overwrite with dynamic mode (edge case - shouldn't happen in practice)
    # This tests the fallback logic
    schema = pa.schema([("year", pa.int64()), ("value", pa.int64())])
    empty = pa.table({"year": [], "value": []}, schema=schema)
    ray.data.from_arrow(empty).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
        schema=schema,
    )
    # Should delete all (fallback behavior)
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 0


def test_write_delta_overwrite_dynamic_all_null_partitions(
    ray_start_regular_shared, temp_delta_path
):
    """Test dynamic mode correctly handles all-NULL partitions."""
    # Create table with NULL and non-NULL partitions
    data1 = [
        {"year": 2023, "month": 1, "value": 100},
        {"year": 2023, "month": None, "value": 200},
        {"year": None, "month": None, "value": 300},  # All-NULL partition
    ]
    ray.data.from_items(data1).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )
    assert ray.data.read_delta(temp_delta_path).count() == 3

    # Overwrite only the all-NULL partition
    data2 = [{"year": None, "month": None, "value": 999}]
    ray.data.from_items(data2).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year", "month"],
        partition_overwrite_mode="dynamic",
    )
    result = ray.data.read_delta(temp_delta_path).take_all()
    assert len(result) == 3
    # Check all-NULL partition was updated
    all_null = [r for r in result if r["year"] is None and r["month"] is None]
    assert len(all_null) == 1
    assert all_null[0]["value"] == 999
    # Check other partitions unchanged
    assert any(
        r["year"] == 2023 and r["month"] == 1 and r["value"] == 100 for r in result
    )
    assert any(
        r["year"] == 2023 and r["month"] is None and r["value"] == 200 for r in result
    )


def test_build_partition_delete_predicate_unit():
    """Unit test for _build_partition_delete_predicate function."""
    from ray.data._internal.datasource.delta.committer import (
        _build_partition_delete_predicate,
    )

    # Test with single partition column
    action1 = Mock()
    action1.partition_values = {"year": "2024"}
    action2 = Mock()
    action2.partition_values = {"year": "2023"}
    pred = _build_partition_delete_predicate([action1, action2], ["year"])
    assert pred is not None
    assert "year" in pred
    assert "2024" in pred or "'2024'" in pred
    assert "2023" in pred or "'2023'" in pred

    # Test with multiple partition columns
    action3 = Mock()
    action3.partition_values = {"year": "2024", "month": "1"}
    action4 = Mock()
    action4.partition_values = {"year": "2024", "month": "2"}
    pred = _build_partition_delete_predicate([action3, action4], ["year", "month"])
    assert pred is not None
    assert "year" in pred
    assert "month" in pred
    assert "1" in pred or "'1'" in pred
    assert "2" in pred or "'2'" in pred

    # Test with NULL partition values
    action5 = Mock()
    action5.partition_values = {"year": "2024", "month": None}
    pred = _build_partition_delete_predicate([action5], ["year", "month"])
    assert pred is not None
    assert "year" in pred
    assert "month" in pred
    # NULL values must be explicitly checked with IS NULL to avoid over-deletion
    assert "IS NULL" in pred or "is null" in pred.lower()

    # Test with empty file actions
    pred = _build_partition_delete_predicate([], ["year"])
    assert pred is None

    # Test with no partition columns
    pred = _build_partition_delete_predicate([action1], [])
    assert pred is None

    # Test with missing partition_values (defensive)
    action6 = Mock()
    action6.partition_values = {}
    pred = _build_partition_delete_predicate([action6], ["year"])
    assert pred is None

    # Test with None partition_values
    action7 = Mock()
    action7.partition_values = None
    pred = _build_partition_delete_predicate([action7], ["year"])
    assert pred is None

    # Test with all-NULL partition (critical: must not be filtered out)
    action8 = Mock()
    action8.partition_values = {"year": None, "month": None}
    pred = _build_partition_delete_predicate([action8], ["year", "month"])
    assert pred is not None
    assert "year" in pred
    assert "month" in pred
    assert "IS NULL" in pred or "is null" in pred.lower()
    # Should have both columns with IS NULL
    assert pred.count("IS NULL") == 2 or pred.lower().count("is null") == 2

    # Test with NaN partition value (critical: must use != comparison, not = 'NaN')
    # Schema is required to distinguish float NaN from string "NaN"
    schema = pa.schema([("value", pa.float64())])
    action9 = Mock()
    action9.partition_values = {"value": "NaN"}
    pred = _build_partition_delete_predicate([action9], ["value"], schema=schema)
    assert pred is not None
    assert "value" in pred
    # Should use != comparison for NaN (col != col), not string comparison
    assert "!=" in pred
    assert "'NaN'" not in pred  # Should not use string literal

    # Test with NaN partition value WITHOUT schema (should fall back to string "NaN")
    # This verifies that without schema, the code treats "NaN" as a string value
    action10 = Mock()
    action10.partition_values = {"value": "NaN"}
    pred_no_schema = _build_partition_delete_predicate([action10], ["value"])
    assert pred_no_schema is not None
    assert "value" in pred_no_schema
    # Without schema, should treat as string "NaN"
    assert "'NaN'" in pred_no_schema or '"NaN"' in pred_no_schema


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
