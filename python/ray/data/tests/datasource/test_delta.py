import os
from typing import Any, Dict, List

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


# =============================================================================
# Write Mode Tests (PR 2)
# =============================================================================


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


def test_write_delta_invalid_mode(ray_start_regular_shared, temp_delta_path):
    ds = ray.data.range(10)
    with pytest.raises(ValueError, match=r"Invalid mode"):
        ds.write_delta(temp_delta_path, mode="invalid_mode")


# =============================================================================
# Empty Table Tests
# =============================================================================


def test_write_delta_empty_new_table_requires_schema(
    ray_start_regular_shared, temp_delta_path
):
    """Writing an empty dataset to a new table requires an explicit schema."""
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    empty = pa.table({"id": [], "value": []}, schema=schema)
    ray.data.from_arrow(empty).write_delta(temp_delta_path, schema=schema)
    assert _delta_log_exists(temp_delta_path)


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


# =============================================================================
# Read Tests
# =============================================================================


def test_read_delta_column_projection(ray_start_regular_shared, temp_delta_path):
    data = [{"id": i, "value": i * 2} for i in range(5)]
    ray.data.from_items(data).write_delta(temp_delta_path)

    rows = ray.data.read_delta(temp_delta_path, columns=["id"]).take_all()
    assert all(set(r.keys()) == {"id"} for r in rows)
    assert sorted(r["id"] for r in rows) == list(range(5))


# =============================================================================
# Edge Cases
# =============================================================================


def test_write_delta_large_dataset(ray_start_regular_shared, temp_delta_path):
    ray.data.range(10_000).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 10_000


def test_write_delta_multiple_blocks(ray_start_regular_shared, temp_delta_path):
    ray.data.range(100, override_num_blocks=10).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
