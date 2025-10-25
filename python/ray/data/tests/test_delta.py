import os
import shutil

import pyarrow as pa
import pytest

import ray
from ray.data import Schema
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_read_basic(tmp_path, batch_size, write_mode):
    import pandas as pd
    from deltalake import write_deltalake

    # Parse the data path.
    path = os.path.join(tmp_path, "tmp_test_delta")

    # Create a sample Delta Lake table
    # Convert pandas DataFrame to PyArrow table for deltalake
    df = pd.DataFrame(
        {"x": [42] * batch_size, "y": ["a"] * batch_size, "z": [3.14] * batch_size}
    )
    table = pa.Table.from_pandas(df)

    if write_mode == "append":
        write_deltalake(path, table, mode=write_mode)
        write_deltalake(path, table, mode=write_mode)
    elif write_mode == "overwrite":
        write_deltalake(path, table, mode=write_mode)

    # Read the Delta Lake table
    ds = ray.data.read_delta(path)

    if write_mode == "append":
        assert ds.count() == batch_size * 2
    elif write_mode == "overwrite":
        assert ds.count() == batch_size

    assert ds.schema() == Schema(
        pa.schema(
            {
                "x": pa.int64(),
                "y": pa.string(),
                "z": pa.float64(),
            }
        )
    )

    if batch_size > 0:
        assert ds.take(1)[0] == {"x": 42, "y": "a", "z": 3.14}
    assert ds.schema().names == ["x", "y", "z"]


@pytest.fixture
def temp_delta_path(tmp_path):
    """Fixture to provide a clean temporary path for Delta tables."""
    delta_path = os.path.join(tmp_path, "delta_table")
    yield delta_path
    # Cleanup after test
    if os.path.exists(delta_path):
        shutil.rmtree(delta_path)


class TestDeltaWrite:
    """Test suite for ray.data.write_delta functionality."""

    def test_write_delta_basic_append(self, temp_delta_path):
        """Test basic write with append mode."""
        ds = ray.data.range(100)
        ds.write_delta(temp_delta_path, mode="append")

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 100

    def test_write_delta_append_twice(self, temp_delta_path):
        """Test appending to existing Delta table twice."""
        ds = ray.data.range(50)
        ds.write_delta(temp_delta_path, mode="append")
        ds.write_delta(temp_delta_path, mode="append")

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 100

    def test_write_delta_overwrite(self, temp_delta_path):
        """Test overwrite mode replaces existing data."""
        ds1 = ray.data.range(100)
        ds1.write_delta(temp_delta_path, mode="append")

        ds2 = ray.data.range(50)
        ds2.write_delta(temp_delta_path, mode="overwrite")

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 50

    def test_write_delta_error_mode(self, temp_delta_path):
        """Test error mode raises exception if table exists."""
        ds = ray.data.range(100)
        ds.write_delta(temp_delta_path, mode="append")

        with pytest.raises(ValueError, match="already exists"):
            ds.write_delta(temp_delta_path, mode="error")

    def test_write_delta_ignore_mode(self, temp_delta_path):
        """Test ignore mode skips write if table exists."""
        ds1 = ray.data.range(100)
        ds1.write_delta(temp_delta_path, mode="append")

        ds2 = ray.data.range(50)
        ds2.write_delta(temp_delta_path, mode="ignore")

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 100

    def test_write_delta_with_partitioning(self, temp_delta_path):
        """Test writing with partition columns."""
        data = [
            {"year": 2024, "month": 1, "value": 100},
            {"year": 2024, "month": 2, "value": 200},
            {"year": 2024, "month": 3, "value": 300},
            {"year": 2023, "month": 12, "value": 400},
        ]
        ds = ray.data.from_items(data)
        ds.write_delta(temp_delta_path, partition_cols=["year", "month"])

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 4

        # Verify partition directories exist
        assert os.path.exists(os.path.join(temp_delta_path, "year=2024"))
        assert os.path.exists(os.path.join(temp_delta_path, "year=2023"))

    def test_write_delta_single_partition_column(self, temp_delta_path):
        """Test writing with single partition column."""
        data = [
            {"category": "A", "value": 1},
            {"category": "B", "value": 2},
            {"category": "A", "value": 3},
        ]
        ds = ray.data.from_items(data)
        ds.write_delta(temp_delta_path, partition_cols=["category"])

        # Verify partition directories
        assert os.path.exists(os.path.join(temp_delta_path, "category=A"))
        assert os.path.exists(os.path.join(temp_delta_path, "category=B"))

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 3

    def test_write_delta_missing_partition_column(self, temp_delta_path):
        """Test error when partition column doesn't exist."""
        ds = ray.data.range(10)

        with pytest.raises(ValueError, match="Partition columns.*not found"):
            ds.write_delta(temp_delta_path, partition_cols=["nonexistent_column"])

    def test_write_delta_schema_preservation(self, temp_delta_path):
        """Test that schema is preserved correctly."""
        data = [
            {
                "int_col": 42,
                "float_col": 3.14,
                "str_col": "hello",
                "bool_col": True,
            }
        ]
        ds = ray.data.from_items(data)
        ds.write_delta(temp_delta_path)

        ds_read = ray.data.read_delta(temp_delta_path)
        schema = ds_read.schema()

        assert "int_col" in schema.names
        assert "float_col" in schema.names
        assert "str_col" in schema.names
        assert "bool_col" in schema.names

    def test_write_delta_empty_dataset(self, temp_delta_path):
        """Test writing empty dataset."""
        schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        ds = ray.data.from_arrow(pa.table({"id": [], "value": []}, schema=schema))
        ds.write_delta(temp_delta_path, schema=schema)

        # Verify table was created (even if empty)
        assert os.path.exists(os.path.join(temp_delta_path, "_delta_log"))

    def test_write_delta_large_dataset(self, temp_delta_path):
        """Test writing larger dataset with multiple blocks."""
        ds = ray.data.range(10000)
        ds.write_delta(temp_delta_path)

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 10000

    def test_write_delta_with_nulls(self, temp_delta_path):
        """Test writing data with NULL values."""
        data = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": None},
            {"id": 3, "value": "c"},
        ]
        ds = ray.data.from_items(data)
        ds.write_delta(temp_delta_path)

        ds_read = ray.data.read_delta(temp_delta_path)
        rows = ds_read.take_all()
        assert any(row["value"] is None for row in rows)

    def test_write_delta_string_partition_values(self, temp_delta_path):
        """Test partitioning with string values."""
        data = [
            {"region": "us-west", "count": 100},
            {"region": "us-east", "count": 200},
            {"region": "eu-west", "count": 300},
        ]
        ds = ray.data.from_items(data)
        ds.write_delta(temp_delta_path, partition_cols=["region"])

        # Verify partition directories with string values
        assert os.path.exists(os.path.join(temp_delta_path, "region=us-west"))
        assert os.path.exists(os.path.join(temp_delta_path, "region=us-east"))
        assert os.path.exists(os.path.join(temp_delta_path, "region=eu-west"))

    def test_write_delta_transaction_log(self, temp_delta_path):
        """Test that Delta transaction log is created correctly."""
        ds = ray.data.range(100)
        ds.write_delta(temp_delta_path)

        # Verify _delta_log directory exists
        log_dir = os.path.join(temp_delta_path, "_delta_log")
        assert os.path.exists(log_dir)

        # Verify at least one transaction file exists
        log_files = os.listdir(log_dir)
        json_files = [f for f in log_files if f.endswith(".json")]
        assert len(json_files) > 0

    def test_write_delta_multiple_partitions(self, temp_delta_path):
        """Test writing with multiple partition levels."""
        data = [
            {"year": 2024, "month": 1, "day": 1, "value": 100},
            {"year": 2024, "month": 1, "day": 2, "value": 200},
            {"year": 2024, "month": 2, "day": 1, "value": 300},
        ]
        ds = ray.data.from_items(data)
        ds.write_delta(temp_delta_path, partition_cols=["year", "month", "day"])

        # Verify nested partition structure
        assert os.path.exists(
            os.path.join(temp_delta_path, "year=2024", "month=1", "day=1")
        )
        assert os.path.exists(
            os.path.join(temp_delta_path, "year=2024", "month=1", "day=2")
        )
        assert os.path.exists(
            os.path.join(temp_delta_path, "year=2024", "month=2", "day=1")
        )

    def test_write_delta_compression(self, temp_delta_path):
        """Test writing with different compression codecs."""
        ds = ray.data.range(1000)
        ds.write_delta(temp_delta_path, compression="gzip")

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 1000

    def test_write_delta_custom_metadata(self, temp_delta_path):
        """Test writing with custom table metadata."""
        ds = ray.data.range(100)
        ds.write_delta(
            temp_delta_path,
            name="test_table",
            description="Test Delta table for unit tests",
            configuration={"delta.enableChangeDataFeed": "true"},
        )

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 100

    def test_write_delta_invalid_mode(self, temp_delta_path):
        """Test that invalid mode raises ValueError."""
        ds = ray.data.range(10)

        with pytest.raises(ValueError, match="Invalid mode"):
            ds.write_delta(temp_delta_path, mode="invalid_mode")

    def test_write_delta_concurrent_append(self, temp_delta_path):
        """Test multiple sequential appends work correctly."""
        for i in range(3):
            ds = ray.data.range(10)
            ds.write_delta(temp_delta_path, mode="append")

        ds_read = ray.data.read_delta(temp_delta_path)
        assert ds_read.count() == 30

        # Verify multiple transaction files exist
        log_dir = os.path.join(temp_delta_path, "_delta_log")
        json_files = [f for f in os.listdir(log_dir) if f.endswith(".json")]
        assert len(json_files) >= 3


@pytest.mark.parametrize("num_rows", [10, 100, 1000])
def test_write_delta_various_sizes(temp_delta_path, num_rows):
    """Test writing datasets of various sizes."""
    ds = ray.data.range(num_rows)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == num_rows


@pytest.mark.parametrize(
    "data_type,values",
    [
        (pa.int8(), [1, 2, 3]),
        (pa.int16(), [100, 200, 300]),
        (pa.int32(), [1000, 2000, 3000]),
        (pa.int64(), [10000, 20000, 30000]),
        (pa.float32(), [1.1, 2.2, 3.3]),
        (pa.float64(), [10.5, 20.5, 30.5]),
        (pa.string(), ["a", "b", "c"]),
        (pa.bool_(), [True, False, True]),
    ],
)
def test_write_delta_various_types(temp_delta_path, data_type, values):
    """Test writing various Arrow data types."""
    table = pa.table({"col": pa.array(values, type=data_type)})
    ds = ray.data.from_arrow(table)
    ds.write_delta(temp_delta_path)

    ds_read = ray.data.read_delta(temp_delta_path)
    assert ds_read.count() == len(values)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
