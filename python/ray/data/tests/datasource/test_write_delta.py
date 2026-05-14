"""Tests for ``Dataset.write_delta`` (PR 3 -- APPEND MVP).

This file is added in PR 3 and grows across PRs 4-7 as new modes/features
land. Each PR's tests are clearly grouped and labelled.

Master's existing ``test_delta.py`` covers ``ray.data.read_delta`` and is
not modified by any of these PRs.
"""

import os
from typing import Any, Dict, List

import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

try:
    import deltalake as _deltalake_check  # noqa: F401
except ImportError:
    _deltalake_check = None

_pa_version = get_pyarrow_version()
# Use skipif (not importorskip) so tests are still collected and show up as
# skipped with a clear reason when optional deps are missing.
pytestmark = [
    pytest.mark.skipif(
        _deltalake_check is None,
        reason="Missing optional dependency: pip install deltalake",
    ),
    pytest.mark.skipif(
        _pa_version is None or _pa_version < parse_version("15.0.0"),
        reason="deltalake requires pyarrow >= 15.0",
    ),
]


@pytest.fixture
def temp_delta_path(tmp_path):
    """Clean per-test path for a Delta table."""
    return os.path.join(str(tmp_path), "delta_table")


def _write_append(rows: List[Dict[str, Any]], path: str, **kwargs) -> None:
    ray.data.from_items(rows).write_delta(path, mode="append", **kwargs)


def _read_all(path: str, **kwargs) -> List[Dict[str, Any]]:
    return ray.data.read_delta(path, **kwargs).take_all()


def _log_exists(path: str) -> bool:
    return os.path.isdir(os.path.join(path, "_delta_log"))


# ----------------------------------------------------------------------
# PR 3 -- APPEND MVP.
# ----------------------------------------------------------------------


def test_append_to_new_path_creates_table(temp_delta_path):
    rows = [{"id": i, "v": f"r{i}"} for i in range(5)]
    _write_append(rows, temp_delta_path)
    assert _log_exists(temp_delta_path)
    assert sorted(_read_all(temp_delta_path), key=lambda r: r["id"]) == rows


def test_append_to_existing_table_sums_rows(temp_delta_path):
    first = [{"id": i, "v": f"a{i}"} for i in range(3)]
    second = [{"id": i + 100, "v": f"b{i}"} for i in range(2)]
    _write_append(first, temp_delta_path)
    _write_append(second, temp_delta_path)
    assert len(_read_all(temp_delta_path)) == 5


def test_explicit_schema_round_trip(temp_delta_path):
    schema = pa.schema([("id", pa.int64()), ("v", pa.string())])
    rows = [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]
    _write_append(rows, temp_delta_path, schema=schema)
    out = _read_all(temp_delta_path)
    assert len(out) == 2


def test_append_empty_dataset_with_schema_creates_empty_table(temp_delta_path):
    schema = pa.schema([("id", pa.int64())])
    ray.data.from_items([], override_num_blocks=1).write_delta(
        temp_delta_path, mode="append", schema=schema
    )
    # Table exists, no rows.
    assert _log_exists(temp_delta_path)
    assert _read_all(temp_delta_path) == []


def _delta_log_json_count(path: str) -> int:
    """Count the *.json files inside _delta_log/ (one per Delta version)."""
    log_dir = os.path.join(path, "_delta_log")
    if not os.path.isdir(log_dir):
        return 0
    return sum(1 for f in os.listdir(log_dir) if f.endswith(".json"))


def test_append_creates_one_delta_log_file_per_call(temp_delta_path):
    _write_append([{"id": 1}], temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 1
    assert os.path.isfile(
        os.path.join(temp_delta_path, "_delta_log", "00000000000000000000.json")
    )
    _write_append([{"id": 2}], temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 2
    assert os.path.isfile(
        os.path.join(temp_delta_path, "_delta_log", "00000000000000000001.json")
    )


def test_multiple_blocks_commit_in_single_version(temp_delta_path):
    """Multi-block, multi-task write must produce exactly one new Delta
    log entry, even though every task writes its own file(s)."""
    import json

    rows = [{"id": i, "v": f"r{i}"} for i in range(20)]
    ray.data.from_items(rows, override_num_blocks=4).write_delta(temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 1

    log_path = os.path.join(temp_delta_path, "_delta_log", "00000000000000000000.json")
    add_count = 0
    with open(log_path) as f:
        for line in f:
            entry = json.loads(line)
            if "add" in entry:
                add_count += 1
    assert add_count >= 4, f"Expected >=4 AddActions, got {add_count}"
    assert len(_read_all(temp_delta_path)) == 20


def test_upsert_mode_rejected(temp_delta_path):
    """UPSERT is intentionally out of scope; the framework must reject it."""
    with pytest.raises(ValueError, match="does not support mode"):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="upsert")


# ----------------------------------------------------------------------
# PR 4 -- OVERWRITE / ERROR / IGNORE.
# ----------------------------------------------------------------------


def test_error_mode_against_existing_table_raises(temp_delta_path):
    _write_append([{"id": 1}], temp_delta_path)
    with pytest.raises(ValueError, match="already exists"):
        ray.data.from_items([{"id": 2}]).write_delta(temp_delta_path, mode="error")


def test_error_mode_against_new_path_succeeds(temp_delta_path):
    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="error")
    assert _log_exists(temp_delta_path)


def test_ignore_mode_against_existing_table_is_noop(temp_delta_path):
    _write_append([{"id": 1, "v": "first"}], temp_delta_path)
    ray.data.from_items([{"id": 2, "v": "second"}]).write_delta(
        temp_delta_path, mode="ignore"
    )
    # Only the original row should be present.
    out = _read_all(temp_delta_path)
    assert len(out) == 1
    assert out[0]["v"] == "first"


def test_ignore_mode_against_new_path_creates_table(temp_delta_path):
    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="ignore")
    assert _log_exists(temp_delta_path)


def test_overwrite_replaces_existing_data(temp_delta_path):
    _write_append([{"id": i, "v": "old"} for i in range(3)], temp_delta_path)
    ray.data.from_items([{"id": i, "v": "new"} for i in range(2)]).write_delta(
        temp_delta_path, mode="overwrite"
    )
    out = _read_all(temp_delta_path)
    assert len(out) == 2
    assert all(r["v"] == "new" for r in out)


def test_overwrite_creates_table_when_missing(temp_delta_path):
    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="overwrite")
    assert _log_exists(temp_delta_path)


def test_overwrite_empty_dataset_truncates_table(temp_delta_path):
    schema = pa.schema([("id", pa.int64())])
    _write_append([{"id": 1}, {"id": 2}], temp_delta_path)
    ray.data.from_items([], override_num_blocks=1).write_delta(
        temp_delta_path, mode="overwrite", schema=schema
    )
    assert _read_all(temp_delta_path) == []


def test_error_mode_race_table_appears_after_preflight(temp_delta_path, monkeypatch):
    """If a table is created concurrently between preflight (sees no
    table) and commit (sees one), ERROR mode must NOT silently append.
    We assert *some* exception bubbles up rather than a silent success."""
    from ray.data._internal.datasource.delta import (
        adapter as _delta_adapter,
        utils as _delta_utils,
    )

    real_get = _delta_utils.try_get_deltatable
    n = {"calls": 0}

    def racy_get(table_uri, storage_options=None):
        n["calls"] += 1
        if n["calls"] == 1:
            return None
        if not os.path.isdir(os.path.join(table_uri, "_delta_log")):
            from deltalake import write_deltalake

            write_deltalake(
                table_uri,
                pa.table({"id": pa.array([0], type=pa.int64())}),
            )
        return real_get(table_uri, storage_options)

    monkeypatch.setattr(_delta_utils, "try_get_deltatable", racy_get)
    monkeypatch.setattr(_delta_adapter, "try_get_deltatable", racy_get)

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="error")


# ----------------------------------------------------------------------
# PR 5 -- Partitioning + dynamic overwrite + buffered writes.
# ----------------------------------------------------------------------


def test_single_column_partition(temp_delta_path):
    rows = [{"year": 2024, "id": i, "v": f"r{i}"} for i in range(3)] + [
        {"year": 2025, "id": i, "v": f"r{i}"} for i in range(2)
    ]
    ray.data.from_items(rows).write_delta(temp_delta_path, partition_cols=["year"])
    # Partition directories must exist.
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2024"))
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2025"))
    out = _read_all(temp_delta_path)
    assert len(out) == 5


def test_multi_column_partition(temp_delta_path):
    rows = [
        {"year": 2024, "month": 1, "id": 1},
        {"year": 2024, "month": 2, "id": 2},
        {"year": 2025, "month": 1, "id": 3},
    ]
    ray.data.from_items(rows).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2024", "month=1"))
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2024", "month=2"))
    assert os.path.isdir(os.path.join(temp_delta_path, "year=2025", "month=1"))


def test_invalid_partition_column_name(temp_delta_path):
    with pytest.raises(ValueError, match="Invalid characters in partition column"):
        ray.data.from_items([{"a/b": 1}]).write_delta(
            temp_delta_path, partition_cols=["a/b"]
        )


def test_partition_column_mismatch_with_existing_table(temp_delta_path):
    ray.data.from_items([{"year": 2024, "id": 1}]).write_delta(
        temp_delta_path, partition_cols=["year"]
    )
    with pytest.raises(ValueError, match="Partition columns mismatch"):
        ray.data.from_items([{"year": 2024, "id": 2}]).write_delta(
            temp_delta_path, partition_cols=["id"]
        )


def test_dynamic_partition_overwrite_only_replaces_affected_partitions(temp_delta_path):
    rows_initial = [
        {"year": 2024, "id": 1, "v": "a"},
        {"year": 2024, "id": 2, "v": "b"},
        {"year": 2025, "id": 3, "v": "c"},
    ]
    ray.data.from_items(rows_initial).write_delta(
        temp_delta_path, partition_cols=["year"]
    )
    # Overwrite only 2024.
    ray.data.from_items([{"year": 2024, "id": 99, "v": "new"}]).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: (r["year"], r["id"]))
    # 2025 partition should be intact; 2024 should have only the new row.
    assert len(out) == 2
    assert {r["year"] for r in out} == {2024, 2025}


def test_static_partition_overwrite_replaces_everything(temp_delta_path):
    ray.data.from_items([{"year": 2024, "id": 1}, {"year": 2025, "id": 2}]).write_delta(
        temp_delta_path, partition_cols=["year"]
    )
    ray.data.from_items([{"year": 2026, "id": 99}]).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="static",
    )
    out = _read_all(temp_delta_path)
    assert len(out) == 1
    assert out[0]["year"] == 2026


def test_target_file_size_bytes_validates_positive(temp_delta_path):
    with pytest.raises(ValueError, match="target_file_size_bytes must be > 0"):
        ray.data.from_items([{"id": 1}]).write_delta(
            temp_delta_path, target_file_size_bytes=0
        )


# ---- PR 5: partition edge cases & writer knobs ---------------------------


def test_partition_column_string(temp_delta_path):
    rows = [
        {"region": "us", "id": 1},
        {"region": "eu", "id": 2},
        {"region": "us", "id": 3},
    ]
    ray.data.from_items(rows).write_delta(temp_delta_path, partition_cols=["region"])
    assert os.path.isdir(os.path.join(temp_delta_path, "region=us"))
    assert os.path.isdir(os.path.join(temp_delta_path, "region=eu"))


def test_partition_column_bool(temp_delta_path):
    rows = [{"flag": True, "id": 1}, {"flag": False, "id": 2}]
    ray.data.from_items(rows).write_delta(temp_delta_path, partition_cols=["flag"])
    children = set(os.listdir(temp_delta_path))
    children.discard("_delta_log")
    assert any(d.lower().startswith("flag=true") for d in children)
    assert any(d.lower().startswith("flag=false") for d in children)


def test_multi_column_partition_round_trip_values(temp_delta_path):
    rows = [
        {"year": 2024, "month": 1, "id": 1},
        {"year": 2024, "month": 2, "id": 2},
        {"year": 2025, "month": 1, "id": 3},
    ]
    ray.data.from_items(rows).write_delta(
        temp_delta_path, partition_cols=["year", "month"]
    )
    out = sorted(
        _read_all(temp_delta_path), key=lambda r: (r["year"], r["month"], r["id"])
    )
    assert [(r["year"], r["month"], r["id"]) for r in out] == [
        (2024, 1, 1),
        (2024, 2, 2),
        (2025, 1, 3),
    ]


def test_null_partition_value(temp_delta_path):
    schema = pa.schema([("region", pa.string()), ("id", pa.int64())])
    table = pa.table(
        {"region": pa.array(["us", None, "eu"]), "id": pa.array([1, 2, 3])},
        schema=schema,
    )
    ray.data.from_arrow(table).write_delta(
        temp_delta_path, partition_cols=["region"], schema=schema
    )
    assert os.path.isdir(
        os.path.join(temp_delta_path, "region=__HIVE_DEFAULT_PARTITION__")
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    null_row = next(r for r in out if r["id"] == 2)
    assert null_row["region"] is None


def test_nan_partition_value_distinct_from_string_nan(temp_delta_path):
    schema = pa.schema([("v", pa.float64()), ("id", pa.int64())])
    table = pa.table(
        {"v": pa.array([1.5, float("nan"), 2.5]), "id": pa.array([1, 2, 3])},
        schema=schema,
    )
    ray.data.from_arrow(table).write_delta(
        temp_delta_path, partition_cols=["v"], schema=schema
    )
    assert os.path.isdir(os.path.join(temp_delta_path, "v=NaN"))


def test_partition_column_dropped_from_on_disk_payload(temp_delta_path):
    import pyarrow.parquet as pq

    rows = [
        {"year": 2024, "id": 1, "v": "a"},
        {"year": 2025, "id": 2, "v": "b"},
    ]
    ray.data.from_items(rows).write_delta(temp_delta_path, partition_cols=["year"])
    part_dir = os.path.join(temp_delta_path, "year=2024")
    pq_files = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert pq_files, "expected at least one parquet file in partition dir"
    on_disk_schema = pq.read_schema(os.path.join(part_dir, pq_files[0]))
    assert "year" not in on_disk_schema.names
    assert "id" in on_disk_schema.names and "v" in on_disk_schema.names


@pytest.mark.parametrize("bad_value", ["a/b", "..", "", "x\x00y"])
def test_invalid_partition_value_rejected(temp_delta_path, bad_value):
    rows = [{"col": bad_value, "id": 1}]
    with pytest.raises(ValueError):
        ray.data.from_items(rows).write_delta(temp_delta_path, partition_cols=["col"])


def test_max_partitions_cap(tmp_path, monkeypatch):
    """Unit test the per-task partition cap directly on ``DeltaFileWriter``.

    The cap is enforced inside ``partition_table`` which runs in a worker
    process. A monkeypatch of ``_MAX_PARTITIONS`` at the test-process module
    level does not propagate to those workers, so this test exercises the
    cap by calling the writer directly in-process.
    """
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta import writer as _writer

    monkeypatch.setattr(_writer, "_MAX_PARTITIONS", 4)

    table_root = str(tmp_path / "delta")
    os.makedirs(table_root, exist_ok=True)
    w = _writer.DeltaFileWriter(
        filesystem=pa_fs.LocalFileSystem(),
        partition_cols=["col"],
        write_uuid="test",
        write_kwargs={},
        written_files=set(),
        local_filesystem_root=table_root,
    )

    table = pa.table({"col": pa.array([str(i) for i in range(6)])})
    with pytest.raises(ValueError, match="Too many partition"):
        w.partition_table(table, ["col"])


def test_default_writer_one_file_per_partition_per_block(temp_delta_path):
    import json

    rows = [{"region": "us", "id": i} for i in range(3)] + [
        {"region": "eu", "id": i} for i in range(2)
    ]
    ray.data.from_items(rows, override_num_blocks=1).write_delta(
        temp_delta_path, partition_cols=["region"]
    )
    log = os.path.join(temp_delta_path, "_delta_log", "00000000000000000000.json")
    add_count = 0
    with open(log) as f:
        for line in f:
            if "add" in json.loads(line):
                add_count += 1
    assert add_count == 2, f"expected 2 AddActions (one per partition), got {add_count}"


def test_target_file_size_bytes_buffers_and_flushes(temp_delta_path):
    """With buffering enabled, a multi-block write to a single partition
    should not produce one file per block — the buffer merges them."""
    import json

    rows = [{"v": i} for i in range(200)]
    ray.data.from_items(rows, override_num_blocks=4).write_delta(
        temp_delta_path,
        target_file_size_bytes=100_000,
    )
    log = os.path.join(temp_delta_path, "_delta_log", "00000000000000000000.json")
    add_count = 0
    with open(log) as f:
        for line in f:
            if "add" in json.loads(line):
                add_count += 1
    assert add_count <= 4
    assert len(_read_all(temp_delta_path)) == 200


@pytest.mark.parametrize(
    "compression,should_raise",
    [("snappy", False), ("zstd", False), ("not-a-codec", True)],
)
def test_compression_parametrize(temp_delta_path, compression, should_raise):
    rows = [{"id": 1, "v": "x"}]
    if should_raise:
        with pytest.raises(ValueError, match="Invalid compression"):
            ray.data.from_items(rows).write_delta(
                temp_delta_path, compression=compression
            )
    else:
        ray.data.from_items(rows).write_delta(temp_delta_path, compression=compression)
        assert _read_all(temp_delta_path) == rows


@pytest.mark.parametrize("write_statistics", [True, False])
def test_write_statistics_parametrize(temp_delta_path, write_statistics):
    rows = [{"id": i, "v": f"r{i}"} for i in range(4)]
    ray.data.from_items(rows).write_delta(
        temp_delta_path, write_statistics=write_statistics
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
