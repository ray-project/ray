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
    rows = ray.data.read_delta(path).take_all()
    assert len(rows) == 100
    assert sorted(r["id"] for r in rows) == list(range(100))


def test_write_delta_append_mode(ray_start_regular_shared, temp_delta_path):
    ray.data.range(50).write_delta(temp_delta_path)
    ray.data.range(30).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 80


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
    rows = ray.data.read_delta(temp_delta_path).take_all()
    assert len(rows) == 2
    by_int = sorted(rows, key=lambda r: r["int_col"])
    assert by_int[0]["str_col"] == "a"
    assert by_int[1]["bool_col"] is False


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
# Edge Cases and Error Handling
# =============================================================================


def test_write_delta_large_dataset(ray_start_regular_shared, temp_delta_path):
    ray.data.range(10_000).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 10_000


def test_write_delta_multiple_blocks(ray_start_regular_shared, temp_delta_path):
    ray.data.range(100, override_num_blocks=10).write_delta(temp_delta_path)
    assert ray.data.read_delta(temp_delta_path).count() == 100


def test_write_delta_compression(ray_start_regular_shared, temp_delta_path):
    ray.data.range(10).write_delta(temp_delta_path, compression="zstd")
    assert ray.data.read_delta(temp_delta_path).count() == 10


def test_read_delta_empty_table(ray_start_regular_shared, temp_delta_path):
    """Reading an empty Delta table should return a dataset with correct schema."""
    schema = pa.schema([("name", pa.string()), ("age", pa.int64())])
    empty = pa.table({"name": [], "age": []}, schema=schema)
    ray.data.from_arrow(empty).write_delta(temp_delta_path, schema=schema)

    ds = ray.data.read_delta(temp_delta_path)
    assert ds.count() == 0


def test_write_delta_empty_without_schema_errors(
    ray_start_regular_shared, tmp_path
):
    """Writing empty dataset to new table without schema should fail."""
    path = os.path.join(str(tmp_path), "no_schema_table")
    schema = pa.schema([("id", pa.int64())])
    empty = pa.table({"id": []}, schema=schema)
    # No schema= provided, and no data to infer from, so this should raise
    with pytest.raises(ValueError, match="schema"):
        ray.data.from_arrow(empty).write_delta(path)


def test_read_delta_nonexistent_table(ray_start_regular_shared, tmp_path):
    """Reading a non-existent table should raise an error."""
    path = os.path.join(str(tmp_path), "nonexistent")
    with pytest.raises(Exception):
        ray.data.read_delta(path).take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
