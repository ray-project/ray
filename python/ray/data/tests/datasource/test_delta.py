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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
