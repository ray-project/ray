import os

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


def test_write_delta_empty(ray_start_regular_shared, temp_delta_path):
    """Test writing empty dataset."""
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    ds = ray.data.from_arrow(pa.table({"id": [], "value": []}, schema=schema))
    ds.write_delta(temp_delta_path, schema=schema)
    assert os.path.exists(os.path.join(temp_delta_path, "_delta_log"))


def test_write_delta_nulls(ray_start_regular_shared, temp_delta_path):
    """Test writing data with NULL values."""
    data = [{"id": 1, "value": "a"}, {"id": 2, "value": None}]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path)
    ds_read = ray.data.read_delta(temp_delta_path)
    rows = ds_read.take_all()
    assert any(row["value"] is None for row in rows)


def test_write_delta_invalid_mode(ray_start_regular_shared, temp_delta_path):
    """Test that invalid mode raises ValueError."""
    ds = ray.data.range(10)
    with pytest.raises(ValueError, match="Invalid mode"):
        ds.write_delta(temp_delta_path, mode="invalid_mode")


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


def test_read_delta_column_projection(ray_start_regular_shared, temp_delta_path):
    """Read with column projection returns only requested columns."""
    data = [{"id": i, "value": i * 2} for i in range(5)]
    ray.data.from_items(data).write_delta(temp_delta_path)

    ds = ray.data.read_delta(temp_delta_path, columns=["id"])
    rows = ds.take_all()
    assert all(set(row.keys()) == {"id"} for row in rows)
    assert sorted(row["id"] for row in rows) == list(range(5))


def test_delta_datasink_local_path_distributed_writes(ray_start_regular_shared):
    """Local paths should not support distributed writes."""
    from ray.data._internal.datasource.delta import DeltaDatasink

    # Local paths should not support distributed writes
    datasink = DeltaDatasink("/tmp/test_delta_table", mode="append")
    assert datasink.supports_distributed_writes is False

    # Paths with file:// scheme should also not support distributed writes
    datasink_file = DeltaDatasink("file:///tmp/test_delta_table", mode="append")
    assert datasink_file.supports_distributed_writes is False


def test_delta_datasink_preserves_table_uri():
    """Verify that table_uri preserves the original URI with scheme."""
    from ray.data._internal.datasource.delta import DeltaDatasink

    # Test with local path
    local_datasink = DeltaDatasink("/tmp/test_table", mode="append")
    assert local_datasink.table_uri == "/tmp/test_table"

    # Test with s3:// path (mock - won't actually connect)
    s3_datasink = DeltaDatasink("s3://bucket/path/to/table", mode="append")
    assert s3_datasink.table_uri == "s3://bucket/path/to/table"
    # Resolved path should have scheme stripped for PyArrow
    assert not s3_datasink.path.startswith("s3://")


def test_delta_datasink_repr(ray_start_regular_shared):
    """Test __repr__ for debugging."""
    from ray.data._internal.datasource.delta import DeltaDatasink

    datasink = DeltaDatasink(
        "/tmp/test_table", mode="overwrite", partition_cols=["year", "month"]
    )
    repr_str = repr(datasink)
    assert "DeltaDatasink" in repr_str
    assert "overwrite" in repr_str
    assert "year" in repr_str
    assert "month" in repr_str


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
    # Verify types are preserved
    assert any(row["int_col"] == 1 and row["float_col"] == 1.5 for row in rows)


def test_write_delta_large_dataset(ray_start_regular_shared, temp_delta_path):
    """Test writing larger dataset to verify partitioning and file handling."""
    ds = ray.data.range(10000)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 10000


def test_write_delta_nested_type_unsupported(ray_start_regular_shared, temp_delta_path):
    """Test that nested types in partition columns are rejected."""
    # Nested types as partition columns should fail
    data = [{"id": 1, "nested": {"a": 1}}, {"id": 2, "nested": {"a": 2}}]
    ds = ray.data.from_items(data)

    with pytest.raises(ValueError, match="nested type|Nested types"):
        ds.write_delta(temp_delta_path, partition_cols=["nested"])


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


def test_delta_datasource_repr():
    """Test DeltaDatasource __repr__ for debugging."""
    from ray.data._internal.datasource.delta import DeltaDatasource

    # Basic repr
    ds = DeltaDatasource("/tmp/test_table")
    repr_str = repr(ds)
    assert "DeltaDatasource" in repr_str
    assert "/tmp/test_table" in repr_str

    # With version
    ds_version = DeltaDatasource("/tmp/test_table", version=5)
    repr_version = repr(ds_version)
    assert "version=5" in repr_version

    # With columns
    ds_cols = DeltaDatasource("/tmp/test_table", columns=["id", "name"])
    repr_cols = repr(ds_cols)
    assert "columns=" in repr_cols


def test_write_delta_multiple_blocks(ray_start_regular_shared, temp_delta_path):
    """Test writing dataset with multiple blocks."""
    # Create dataset with multiple blocks
    ds = ray.data.range(100, override_num_blocks=10)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 100


def test_write_delta_string_data(ray_start_regular_shared, temp_delta_path):
    """Test writing string data with special characters."""
    data = [
        {"id": 1, "text": "hello world"},
        {"id": 2, "text": "special chars: @#$%^&*()"},
        {"id": 3, "text": "unicode: cafe"},
    ]
    ds = ray.data.from_items(data)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == 3

    rows = ds_read.take_all()
    texts = {row["text"] for row in rows}
    assert "hello world" in texts
    assert "special chars: @#$%^&*()" in texts


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


def test_delta_write_mode_validation(ray_start_regular_shared):
    """Test that invalid write modes are rejected."""
    from ray.data._internal.datasource.delta import DeltaDatasink
    from ray.data._internal.savemode import SaveMode

    with pytest.raises(ValueError, match="Invalid mode"):
        DeltaDatasink("/tmp/test", mode="invalid")

    # Valid string modes should work
    for mode in ["append", "overwrite", "error", "ignore", "upsert"]:
        datasink = DeltaDatasink("/tmp/test", mode=mode)
        assert datasink.mode.value == mode

    # SaveMode enum should also work
    datasink = DeltaDatasink("/tmp/test", mode=SaveMode.APPEND)
    assert datasink.mode == SaveMode.APPEND


def test_delta_upsert_requires_existing_table(
    ray_start_regular_shared, temp_delta_path
):
    """Test that upsert mode requires an existing table."""
    from ray.data._internal.datasource.delta import UPSERT_JOIN_COLS, DeltaDatasink
    from ray.data._internal.savemode import SaveMode

    datasink = DeltaDatasink(
        temp_delta_path,
        mode=SaveMode.UPSERT,
        upsert_kwargs={UPSERT_JOIN_COLS: ["id"]},
    )

    # on_write_start should fail because table doesn't exist
    with pytest.raises(ValueError, match="requires an existing Delta table"):
        datasink.on_write_start()


def test_delta_upsert_requires_join_cols(ray_start_regular_shared, temp_delta_path):
    """Test that upsert mode requires join_cols."""
    from ray.data._internal.datasource.delta import DeltaDatasink
    from ray.data._internal.savemode import SaveMode

    # Create table first
    ds = ray.data.from_items([{"id": 1, "value": "original"}])
    ds.write_delta(temp_delta_path)

    # Create datasink without join_cols
    datasink = DeltaDatasink(temp_delta_path, mode=SaveMode.UPSERT)

    # on_write_start should fail
    with pytest.raises(ValueError, match="requires join_cols"):
        datasink.on_write_start()


def test_delta_upsert_kwargs_validation(ray_start_regular_shared):
    """Test that upsert_kwargs requires upsert mode."""
    from ray.data._internal.datasource.delta import UPSERT_JOIN_COLS, DeltaDatasink
    from ray.data._internal.savemode import SaveMode

    # upsert_kwargs with non-upsert mode should fail
    with pytest.raises(ValueError, match="can only be specified with SaveMode.UPSERT"):
        DeltaDatasink(
            "/tmp/test",
            mode=SaveMode.APPEND,
            upsert_kwargs={UPSERT_JOIN_COLS: ["id"]},
        )


def test_delta_write_result_dataclass():
    """Test DeltaWriteResult dataclass structure."""
    from ray.data._internal.datasource.delta import DeltaWriteResult

    # Default empty result
    result = DeltaWriteResult()
    assert result.add_actions == []
    assert result.upsert_keys is None
    assert result.schemas == []

    # Result with data
    result = DeltaWriteResult(
        add_actions=[],
        upsert_keys=None,
        schemas=[pa.schema([("id", pa.int64())])],
    )
    assert len(result.schemas) == 1


def test_delta_empty_path_validation():
    """Test that empty paths are rejected."""
    from ray.data._internal.datasource.delta import DeltaDatasink, DeltaDatasource

    with pytest.raises(ValueError, match="cannot be empty"):
        DeltaDatasink("", mode="append")

    with pytest.raises(ValueError, match="cannot be empty"):
        DeltaDatasink("   ", mode="append")

    with pytest.raises(ValueError, match="cannot be empty"):
        DeltaDatasource("")


def test_delta_partition_column_validation():
    """Test partition column name validation."""
    from ray.data._internal.datasource.delta.utils import (
        validate_partition_column_names,
    )

    # Valid columns
    assert validate_partition_column_names(["year", "month"]) == ["year", "month"]

    # Empty list is valid
    assert validate_partition_column_names([]) == []

    # Invalid: contains path separator
    with pytest.raises(ValueError, match="Invalid characters"):
        validate_partition_column_names(["year/month"])

    # Invalid: contains equals sign
    with pytest.raises(ValueError, match="Invalid characters"):
        validate_partition_column_names(["year=2024"])

    # Invalid: duplicate columns
    with pytest.raises(ValueError, match="Duplicate"):
        validate_partition_column_names(["year", "year"])

    # Invalid: empty string
    with pytest.raises(ValueError, match="Invalid partition column name"):
        validate_partition_column_names([""])


def test_delta_partition_value_validation():
    """Test partition value validation."""
    from ray.data._internal.datasource.delta.utils import (
        validate_partition_value,
    )

    # Valid values
    validate_partition_value("value")
    validate_partition_value(123)
    validate_partition_value(None)
    validate_partition_value(float("nan"))

    # Invalid: contains path separator
    with pytest.raises(ValueError, match="invalid characters"):
        validate_partition_value("a/b")

    # Invalid: contains path traversal
    with pytest.raises(ValueError, match="invalid characters"):
        validate_partition_value("a..b")


def test_delta_file_path_validation():
    """Test file path validation."""
    from ray.data._internal.datasource.delta.utils import validate_file_path

    # Valid paths
    validate_file_path("year=2024/file.parquet")

    # Invalid: empty
    with pytest.raises(ValueError, match="cannot be empty"):
        validate_file_path("")

    # Invalid: path traversal
    with pytest.raises(ValueError, match="contains '..'"):
        validate_file_path("../file.parquet")

    # Invalid: absolute path
    with pytest.raises(ValueError, match="Absolute path"):
        validate_file_path("/absolute/path.parquet")

    # Invalid: Windows-invalid characters
    with pytest.raises(ValueError, match="invalid character"):
        validate_file_path("file<name>.parquet")


def test_delta_schema_conversion():
    """Test schema conversion utilities."""
    from ray.data._internal.datasource.delta.utils import (
        convert_schema_to_delta,
        infer_partition_type,
        types_compatible,
    )

    # Test type compatibility
    assert types_compatible(pa.int64(), pa.int64()) is True
    assert types_compatible(pa.int64(), pa.int32()) is True  # narrower fits wider
    assert (
        types_compatible(pa.int32(), pa.int64()) is False
    )  # wider doesn't fit narrower
    assert types_compatible(pa.float64(), pa.float32()) is True
    assert types_compatible(pa.string(), pa.large_string()) is True

    # Test partition type inference with actual Python types
    assert infer_partition_type(123) == pa.int64()
    assert infer_partition_type(1.5) == pa.float64()
    assert infer_partition_type(True) == pa.bool_()
    assert infer_partition_type("value") == pa.string()
    assert infer_partition_type(None) == pa.string()

    # Test partition type inference from strings (as stored in Delta metadata)
    # This is critical because AddAction.partition_values stores all values as strings
    assert infer_partition_type("2024") == pa.int64()  # Integer string
    assert infer_partition_type("3.14") == pa.float64()  # Float string
    assert infer_partition_type("true") == pa.bool_()  # Boolean string
    assert infer_partition_type("false") == pa.bool_()  # Boolean string
    assert infer_partition_type("hello") == pa.string()  # Plain string

    # Test schema conversion with None
    with pytest.raises(ValueError, match="Cannot convert None"):
        convert_schema_to_delta(None)


def test_delta_storage_options_auto_detection():
    """Test storage options auto-detection."""
    from ray.data._internal.datasource.delta.utils import get_storage_options

    # Local paths don't add any options
    opts = get_storage_options("/local/path")
    assert opts == {}

    # User-provided options are preserved
    opts = get_storage_options("/local/path", {"key": "value"})
    assert opts == {"key": "value"}


def test_delta_to_pyarrow_schema():
    """Test to_pyarrow_schema utility."""
    from ray.data._internal.datasource.delta.utils import to_pyarrow_schema

    # Already a PyArrow schema
    schema = pa.schema([("id", pa.int64())])
    assert to_pyarrow_schema(schema) == schema

    # None raises error
    with pytest.raises(ValueError, match="Cannot convert None"):
        to_pyarrow_schema(None)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
