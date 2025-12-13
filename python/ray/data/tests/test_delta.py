import os

import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


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
    with pytest.raises(ValueError, match="Partition columns.*not found"):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
