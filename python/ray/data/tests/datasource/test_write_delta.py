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


# ----------------------------------------------------------------------
# PR 6 -- Schema evolution.
# ----------------------------------------------------------------------


def test_schema_mode_error_rejects_new_column(temp_delta_path):
    _write_append([{"id": 1, "v": "x"}], temp_delta_path)
    schema_with_extra = pa.schema(
        [("id", pa.int64()), ("v", pa.string()), ("extra", pa.string())]
    )
    with pytest.raises(ValueError, match="New columns detected"):
        ray.data.from_items([{"id": 2, "v": "y", "extra": "z"}]).write_delta(
            temp_delta_path, schema=schema_with_extra, schema_mode="error"
        )


def test_schema_mode_merge_adds_new_column(temp_delta_path):
    _write_append([{"id": 1, "v": "x"}], temp_delta_path)
    schema_with_extra = pa.schema(
        [("id", pa.int64()), ("v", pa.string()), ("extra", pa.string())]
    )
    ray.data.from_items([{"id": 2, "v": "y", "extra": "z"}]).write_delta(
        temp_delta_path, schema=schema_with_extra, schema_mode="merge"
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert len(out) == 2
    assert "extra" in out[1]


def test_invalid_schema_mode_value_rejected(temp_delta_path):
    _write_append([{"id": 1}], temp_delta_path)
    extra = pa.schema([("id", pa.int64()), ("x", pa.string())])
    with pytest.raises(ValueError, match="Invalid schema_mode"):
        ray.data.from_items([{"id": 2, "x": "v"}]).write_delta(
            temp_delta_path, schema=extra, schema_mode="bogus"
        )


def test_existing_column_type_mismatch_rejected(temp_delta_path):
    _write_append([{"id": 1, "v": "string"}], temp_delta_path)
    mismatched = pa.schema([("id", pa.int64()), ("v", pa.int64())])
    with pytest.raises(ValueError, match="type mismatches"):
        ray.data.from_items([{"id": 2, "v": 99}]).write_delta(
            temp_delta_path, schema=mismatched, schema_mode="merge"
        )


# ---- PR 6: schema-evolution edge cases -----------------------------------


def test_block_missing_required_column_raises(temp_delta_path):
    schema = pa.schema([("id", pa.int64()), ("v", pa.string())])
    table = pa.table({"id": pa.array([1, 2], type=pa.int64())})  # missing "v"
    with pytest.raises(ValueError, match="Missing columns"):
        ray.data.from_arrow(table).write_delta(temp_delta_path, schema=schema)


def test_all_null_column_writes_to_nullable_existing_column(temp_delta_path):
    _write_append([{"id": 1, "v": "x"}], temp_delta_path)
    table = pa.table(
        {
            "id": pa.array([2, 3], type=pa.int64()),
            "v": pa.array([None, None], type=pa.null()),
        }
    )
    ray.data.from_arrow(table).write_delta(temp_delta_path)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert len(out) == 3
    assert all(r["v"] is None for r in out if r["id"] in (2, 3))


def test_complex_types_round_trip(temp_delta_path):
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("tags", pa.list_(pa.string())),
            ("nested", pa.struct([("a", pa.int64()), ("b", pa.string())])),
        ]
    )
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "tags": pa.array([["x", "y"], ["z"]], type=pa.list_(pa.string())),
            "nested": pa.array(
                [{"a": 1, "b": "p"}, {"a": 2, "b": "q"}],
                type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
            ),
        },
        schema=schema,
    )
    ray.data.from_arrow(table).write_delta(temp_delta_path, schema=schema)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert len(out) == 2
    assert out[0]["tags"] == ["x", "y"]
    assert out[0]["nested"] == {"a": 1, "b": "p"}
    assert out[1]["nested"] == {"a": 2, "b": "q"}


def test_cross_worker_type_promotion_int32_int64(temp_delta_path):
    """Two workers emit blocks with int32 and int64 columns named `v`.

    The framework's schema-reconciliation at commit time promotes to int64
    (the wider type). The per-block validator only fires when the user
    declared an explicit schema, so we declare ``int64`` so that the
    narrower int32 block is accepted (narrower-into-wider is always
    compatible) and the wider int64 block matches the declared type.
    """
    block_a = pa.table({"v": pa.array([1, 2, 3], type=pa.int32())})
    block_b = pa.table({"v": pa.array([10, 20], type=pa.int64())})
    declared = pa.schema([("v", pa.int64())])
    ds = ray.data.from_arrow(block_a).union(ray.data.from_arrow(block_b))
    ds.write_delta(temp_delta_path, schema=declared)
    out = _read_all(temp_delta_path)
    assert sorted(r["v"] for r in out) == [1, 2, 3, 10, 20]


# ----------------------------------------------------------------------
# PR 7 -- Cloud + idempotency + orphan cleanup.
# ----------------------------------------------------------------------


def test_storage_options_passthrough(temp_delta_path):
    """Caller-supplied storage_options must be preserved (and overlaid on
    any auto-detected cloud creds)."""
    ds = ray.data.from_items([{"id": 1}])
    # Local path so no cloud auto-detection runs; storage_options should
    # be threaded through cleanly.
    ds.write_delta(temp_delta_path, storage_options={"FOO": "bar"})
    assert _log_exists(temp_delta_path)


def test_app_transaction_id_added_to_commit_properties(temp_delta_path):
    """Writes should embed a deterministic app_transactions entry so a
    retry of the same write doesn't double-commit."""
    from ray.data._internal.datasource.delta.utils import create_app_transaction_id

    txn = create_app_transaction_id("abc")
    assert txn.app_id == "ray.data.write_delta:abc"
    assert txn.version == 1

    # Also exercise the no-uuid fallback.
    txn2 = create_app_transaction_id(None)
    assert txn2.app_id.startswith("ray.data.write_delta:ray_data_write_")


def test_normalize_commit_properties_validation():
    from ray.data._internal.datasource.delta.utils import normalize_commit_properties

    assert normalize_commit_properties(None) is None
    props = normalize_commit_properties({"k": "v", "max_commit_retries": "3"})
    assert props is not None
    with pytest.raises(TypeError):
        normalize_commit_properties({"k": 1})
    with pytest.raises(TypeError):
        normalize_commit_properties("not a dict")


def test_adapter_pickle_round_trip(temp_delta_path):
    """The adapter must survive pickling (workers receive a pickled copy)."""
    import pickle

    from ray.data._internal.datasource.delta.adapter import DeltaAdapter

    adapter = DeltaAdapter(temp_delta_path)
    restored = pickle.loads(pickle.dumps(adapter))
    assert restored.table_uri == temp_delta_path
    assert restored.filesystem is None  # rebuilt lazily on the worker.


# ---- Mocked cloud-credential coverage (no network, no real cloud) ---------


def test_get_storage_options_s3_auto_detects_aws_creds(monkeypatch):
    """boto3 credentials are picked up for s3:// paths; caller-supplied
    options take precedence."""
    import sys
    import types

    fake_boto3 = types.ModuleType("boto3")

    class _Creds:
        access_key = "AKIA-FAKE"
        secret_key = "SECRET-FAKE"
        token = "TOKEN-FAKE"

    class _Session:
        region_name = "us-west-2"

        def get_credentials(self):
            return _Creds()

    fake_boto3.Session = _Session
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    from ray.data._internal.datasource.delta.utils import get_storage_options

    opts = get_storage_options("s3://bucket/table", {"AWS_REGION": "caller-wins"})
    assert opts["AWS_ACCESS_KEY_ID"] == "AKIA-FAKE"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "SECRET-FAKE"
    assert opts["AWS_SESSION_TOKEN"] == "TOKEN-FAKE"
    # Caller value wins for keys present in both.
    assert opts["AWS_REGION"] == "caller-wins"


def test_get_storage_options_local_path_is_passthrough():
    """Local paths must never trigger cloud-credential detection."""
    from ray.data._internal.datasource.delta.utils import get_storage_options

    assert get_storage_options("/tmp/local", {"X": "y"}) == {"X": "y"}
    assert get_storage_options("/tmp/local", None) == {}


def test_get_storage_options_missing_boto3_falls_back_cleanly(monkeypatch):
    """Missing boto3 must not raise for s3:// paths -- the caller-supplied
    options pass through as-is."""
    import builtins

    real_import = builtins.__import__

    def _fail_import(name, *args, **kwargs):
        if name == "boto3" or name.startswith("boto3."):
            raise ImportError("boto3 not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fail_import)

    from ray.data._internal.datasource.delta.utils import get_storage_options

    opts = get_storage_options("s3://bucket/table", {"AWS_REGION": "us-east-1"})
    assert opts == {"AWS_REGION": "us-east-1"}


def test_create_filesystem_from_storage_options_s3_with_creds():
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta.utils import (
        create_filesystem_from_storage_options,
    )

    fs = create_filesystem_from_storage_options(
        "s3://bucket/table",
        {
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
            "AWS_REGION": "us-west-2",
        },
    )
    assert isinstance(fs, pa_fs.S3FileSystem)


def test_create_filesystem_from_storage_options_returns_none_when_no_match():
    """Non-cloud paths and empty options must return None so PyArrow's
    default resolution takes over."""
    from ray.data._internal.datasource.delta.utils import (
        create_filesystem_from_storage_options,
    )

    assert create_filesystem_from_storage_options("/tmp/x", {"foo": "bar"}) is None
    assert create_filesystem_from_storage_options("s3://b/t", {}) is None
    assert create_filesystem_from_storage_options("s3://b/t", None) is None


# ---- Additional PR 7 tests folded in from the test-coverage audit --------


def test_single_delta_log_version_per_write_delta_call(temp_delta_path):
    """A single write_delta call must produce exactly one new _delta_log
    json entry, regardless of block count."""
    rows = [{"id": i} for i in range(8)]
    ray.data.from_items(rows, override_num_blocks=3).write_delta(temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == 1


def test_app_transaction_idempotence_on_commit_retry(temp_delta_path, monkeypatch):
    """If the underlying delta-rs commit fails transiently, retrying must
    not produce two new versions."""
    from ray.data._internal.datasource.delta import committer as _committer

    _write_append([{"id": 0}], temp_delta_path)
    before = _delta_log_json_count(temp_delta_path)

    real_commit = _committer.commit_to_existing_table
    state = {"raised": False}

    def flaky_commit(inputs, table, file_actions, schema, filesystem):
        if not state["raised"]:
            state["raised"] = True
            raise RuntimeError("transient delta-rs commit failure")
        return real_commit(inputs, table, file_actions, schema, filesystem)

    monkeypatch.setattr(_committer, "commit_to_existing_table", flaky_commit)
    monkeypatch.setattr(
        "ray.data._internal.datasource.delta.adapter.commit_to_existing_table",
        flaky_commit,
    )

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == before

    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path)
    assert _delta_log_json_count(temp_delta_path) == before + 1


def test_max_commit_retries_propagated(temp_delta_path):
    """max_commit_retries from write_kwargs must reach CommitProperties."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter

    adapter = DeltaAdapter(temp_delta_path, max_commit_retries=7)
    adapter._aggregated_write_uuid = "ut-fixed"
    kwargs = adapter._build_commit_kwargs()
    assert kwargs["commit_properties"].max_commit_retries == 7


def test_worker_failure_cleans_orphan_files_delta_end_to_end(temp_delta_path):
    """Adapter.on_failure must delete orphan files reported by failed tasks.

    Unit test (not an end-to-end Ray run) because monkeypatches on
    ``pq.write_table`` don't propagate to Ray worker processes. We exercise
    the same cleanup code path by:

    1. Creating a fake "orphan" parquet file under the table path.
    2. Invoking ``DeltaAdapter.on_failure`` with the orphan's relative path.
    3. Asserting the file is deleted and no _delta_log was created.
    """
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter

    os.makedirs(temp_delta_path, exist_ok=True)
    orphan_rel = "orphan-task0.parquet"
    orphan_abs = os.path.join(temp_delta_path, orphan_rel)
    with open(orphan_abs, "wb") as f:
        f.write(b"not a real parquet but exists on disk")
    assert os.path.exists(orphan_abs)

    adapter = DeltaAdapter(temp_delta_path)
    adapter.on_failure([orphan_rel])

    assert not os.path.exists(orphan_abs)
    # No commit ever happened, so the transaction log must not exist.
    assert not _log_exists(temp_delta_path)


def test_commit_failure_leaves_files_and_no_new_version(temp_delta_path, monkeypatch):
    """If commit raises, no new version appears in _delta_log; existing
    parquet files must remain (deleting them risks destroying committed
    data when the commit actually succeeded silently)."""
    _write_append([{"id": 0}], temp_delta_path)
    before_versions = _delta_log_json_count(temp_delta_path)

    monkeypatch.setattr(
        "ray.data._internal.datasource.delta.adapter.commit_to_existing_table",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("commit boom")),
    )

    parquet_before = sum(
        1 for _, _, fs in os.walk(temp_delta_path) for f in fs if f.endswith(".parquet")
    )

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path)

    assert _delta_log_json_count(temp_delta_path) == before_versions

    parquet_after = sum(
        1 for _, _, fs in os.walk(temp_delta_path) for f in fs if f.endswith(".parquet")
    )
    assert parquet_after >= parquet_before


def test_explicit_filesystem_bypasses_auto_detection(temp_delta_path):
    """Passing filesystem= must short-circuit make_fs_config so the
    local path doesn't get a local_filesystem_root."""
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta.adapter import DeltaAdapter

    adapter = DeltaAdapter(temp_delta_path, filesystem=pa_fs.LocalFileSystem())
    assert adapter._fs_config.local_filesystem_root is None
    assert adapter.filesystem is not None


@pytest.mark.parametrize(
    "uri",
    ["/tmp/somepath", "file:///tmp/somepath", "local:///tmp/somepath"],
)
def test_local_path_supports_distributed_writes_false(uri):
    """Local schemes must declare supports_distributed_writes=False so the
    write planner pins tasks to the driver. Bare relative paths are not
    valid table URIs (pa.fs.FileSystem.from_uri rejects them) so they
    are excluded from this parametrize."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter

    adapter = DeltaAdapter(uri)
    assert adapter.supports_distributed_writes is False


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_table_deleted_between_preflight_and_commit(temp_delta_path, monkeypatch, mode):
    """If the table is deleted between preflight and commit, APPEND must
    raise; OVERWRITE must succeed by creating a new table."""
    from ray.data._internal.datasource.delta import (
        adapter as _delta_adapter,
        utils as _utils,
    )

    _write_append([{"id": 0}], temp_delta_path)
    real_get = _utils.try_get_deltatable
    n = {"calls": 0}

    def deleting_get(table_uri, storage_options=None):
        n["calls"] += 1
        if n["calls"] == 1:
            return real_get(table_uri, storage_options)
        return None

    monkeypatch.setattr(_utils, "try_get_deltatable", deleting_get)
    monkeypatch.setattr(_delta_adapter, "try_get_deltatable", deleting_get)

    if mode == "append":
        with pytest.raises(Exception):
            ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode=mode)
    else:
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode=mode)


def test_read_parquet_against_delta_root_documented_behaviour(temp_delta_path):
    """Calling read_parquet against a Delta table root is not supported --
    it sees raw parquet files (including tombstoned ones) and ignores the
    transaction log. This test documents that. Users should use
    read_delta."""
    _write_append([{"id": i} for i in range(3)], temp_delta_path)
    out = ray.data.read_parquet(temp_delta_path).take_all()
    assert len(out) >= 3


# ---------------------------------------------------------------------------
# PR 7 (amend) -- CommitProperties propagation + driver-side _with_retry.
# ---------------------------------------------------------------------------


def test_commit_properties_propagate_to_create_write_transaction(
    temp_delta_path, monkeypatch
):
    """The CommitProperties built by _build_commit_kwargs must actually
    reach DeltaTable.create_write_transaction (existing-table path)."""
    from ray.data._internal.datasource.delta import committer as _committer

    _write_append([{"id": 0}], temp_delta_path)

    captured = {}
    real_commit = _committer.commit_to_existing_table

    def spy_commit(inputs, table, file_actions, schema, filesystem):
        # Capture kwargs by wrapping create_write_transaction on this table.
        real_cwt = table.create_write_transaction

        def spy_cwt(**kwargs):
            captured["kwargs"] = kwargs
            return real_cwt(**kwargs)

        table.create_write_transaction = spy_cwt
        return real_commit(inputs, table, file_actions, schema, filesystem)

    monkeypatch.setattr(_committer, "commit_to_existing_table", spy_commit)
    monkeypatch.setattr(
        "ray.data._internal.datasource.delta.adapter.commit_to_existing_table",
        spy_commit,
    )

    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, max_commit_retries=9)

    assert "commit_properties" in captured["kwargs"]
    props = captured["kwargs"]["commit_properties"]
    assert props is not None, "CommitProperties must be plumbed through to delta-rs"
    assert props.max_commit_retries == 9
    assert props.app_transactions is not None and len(props.app_transactions) >= 1


def test_commit_properties_propagate_to_create_table_with_add_actions(
    temp_delta_path, monkeypatch
):
    """The CommitProperties must also reach create_table_with_add_actions
    on the new-table code path. Note: committer.py imports the symbol
    lazily inside ``create_table_with_files``, so the only patchable site
    is its source module ``deltalake.transaction``."""
    import deltalake.transaction as _dlt

    captured = {}
    real = _dlt.create_table_with_add_actions

    def spy(**kwargs):
        captured["kwargs"] = kwargs
        return real(**kwargs)

    monkeypatch.setattr(_dlt, "create_table_with_add_actions", spy)

    ray.data.from_items([{"id": 0}]).write_delta(temp_delta_path, max_commit_retries=4)

    assert "commit_properties" in captured["kwargs"]
    props = captured["kwargs"]["commit_properties"]
    assert props is not None
    assert props.max_commit_retries == 4


def test_with_retry_retries_on_transient_io_error(temp_delta_path, monkeypatch):
    """Driver-side _with_retry must retry on substring-matched transient
    errors and eventually succeed."""
    from ray.data._internal.datasource.delta import committer as _committer

    _write_append([{"id": 0}], temp_delta_path)

    real_commit = _committer.commit_to_existing_table
    attempts = {"n": 0}

    def flaky(inputs, table, file_actions, schema, filesystem):
        attempts["n"] += 1
        if attempts["n"] < 3:
            # Substring "Connection reset" is in DEFAULT_DELTA_COMMIT_RETRIED_ERRORS.
            raise IOError("Connection reset by peer")
        return real_commit(inputs, table, file_actions, schema, filesystem)

    monkeypatch.setattr(_committer, "commit_to_existing_table", flaky)
    monkeypatch.setattr(
        "ray.data._internal.datasource.delta.adapter.commit_to_existing_table",
        flaky,
    )

    before = _delta_log_json_count(temp_delta_path)
    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path)
    after = _delta_log_json_count(temp_delta_path)

    assert attempts["n"] == 3, "should have retried twice then succeeded"
    assert after == before + 1, "exactly one new delta-log version expected"


def test_with_retry_does_not_retry_on_logical_error(temp_delta_path, monkeypatch):
    """_with_retry must NOT retry on errors that don't match the
    configured substring set (e.g. schema mismatch)."""
    from ray.data._internal.datasource.delta import committer as _committer

    _write_append([{"id": 0}], temp_delta_path)

    attempts = {"n": 0}

    def boom(inputs, table, file_actions, schema, filesystem):
        attempts["n"] += 1
        raise ValueError("schema mismatch -- a logical error, not a transient one")

    monkeypatch.setattr(_committer, "commit_to_existing_table", boom)
    monkeypatch.setattr(
        "ray.data._internal.datasource.delta.adapter.commit_to_existing_table",
        boom,
    )

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path)

    assert attempts["n"] == 1, "logical errors must not trigger retry"


# ---------------------------------------------------------------------------
# Guardrails -- public API stability, UPSERT exclusion, retry configurability.
# ---------------------------------------------------------------------------


def test_write_delta_signature_unchanged():
    """Lock down Dataset.write_delta's public signature so accidental
    additions or renames are caught immediately."""
    import inspect

    from ray.data import Dataset

    sig = inspect.signature(Dataset.write_delta)
    names = list(sig.parameters.keys())
    expected = [
        "self",
        "path",
        "mode",
        "partition_cols",
        "filesystem",
        "schema",
        "schema_mode",
        "ray_remote_args",
        "concurrency",
        "write_kwargs",  # **write_kwargs catchall.
    ]
    assert (
        names == expected
    ), f"Dataset.write_delta signature drift: got {names}, expected {expected}"
    # The catchall must be VAR_KEYWORD.
    assert (
        sig.parameters["write_kwargs"].kind == inspect.Parameter.VAR_KEYWORD
    ), "write_kwargs must remain **kwargs"
    # No upsert_kwargs ever.
    assert "upsert_kwargs" not in names


def test_upsert_mode_rejected_on_write_delta(temp_delta_path):
    """write_delta must reject UPSERT mode -- the feature is out of scope
    for this delivery train. Guards against accidental re-introduction."""
    with pytest.raises(ValueError) as excinfo:
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="upsert")
    msg = str(excinfo.value).lower()
    # Either mode-rejection or supported-modes-listed phrasing is OK.
    assert "upsert" in msg or "supported" in msg or "mode" in msg


def test_retry_config_per_call_override(temp_delta_path, monkeypatch):
    """Per-call retry kwargs via **write_kwargs must override DataContext
    defaults for that call only."""
    from ray.data._internal.datasource.delta import committer as _committer

    _write_append([{"id": 0}], temp_delta_path)

    attempts = {"n": 0}

    def always_transient(inputs, table, file_actions, schema, filesystem):
        attempts["n"] += 1
        raise IOError("Connection reset")

    monkeypatch.setattr(_committer, "commit_to_existing_table", always_transient)
    monkeypatch.setattr(
        "ray.data._internal.datasource.delta.adapter.commit_to_existing_table",
        always_transient,
    )

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 1}]).write_delta(
            temp_delta_path,
            commit_retry_max_attempts=2,
            commit_retry_max_backoff_s=0,
        )

    # Override of 2 must win over DataContext default of 5.
    assert attempts["n"] == 2


def test_retry_config_global_default(temp_delta_path):
    """Without per-call or delta_config overrides, _resolved_retry_config
    must fall through to ``DataContext.table_write_config`` (the shared
    cross-format defaults)."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter
    from ray.data.context import DataContext

    adapter = DeltaAdapter(temp_delta_path)
    max_attempts, max_backoff_s, retried = adapter._resolved_retry_config()
    shared = DataContext.get_current().table_write_config
    # delta_config retry fields default to None now, so the fallback chain
    # lands on the shared TableWriteConfig.
    assert max_attempts == shared.max_attempts
    assert max_backoff_s == shared.max_backoff_s
    assert list(retried) == list(shared.retried_errors)


# ---------------------------------------------------------------------------
# Cross-format retry precedence chain (per-call > delta_config > shared).
# ---------------------------------------------------------------------------


def test_resolved_retry_config_per_call_wins_over_delta_config(temp_delta_path):
    """Per-call kwarg must win over both DeltaConfig and TableWriteConfig."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    original_delta = ctx.delta_config.commit_max_attempts
    original_shared = ctx.table_write_config.max_attempts
    try:
        ctx.delta_config.commit_max_attempts = 99
        ctx.table_write_config.max_attempts = 77
        adapter = DeltaAdapter(temp_delta_path, commit_retry_max_attempts=3)
        max_attempts, _, _ = adapter._resolved_retry_config()
        assert max_attempts == 3, "per-call kwarg must win"
    finally:
        ctx.delta_config.commit_max_attempts = original_delta
        ctx.table_write_config.max_attempts = original_shared


def test_resolved_retry_config_delta_config_wins_over_shared(temp_delta_path):
    """If DeltaConfig.commit_max_attempts is set, it wins over
    TableWriteConfig but loses to per-call kwargs."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    original_delta = ctx.delta_config.commit_max_attempts
    original_shared = ctx.table_write_config.max_attempts
    try:
        ctx.delta_config.commit_max_attempts = 11
        ctx.table_write_config.max_attempts = 77
        adapter = DeltaAdapter(temp_delta_path)  # no per-call override
        max_attempts, _, _ = adapter._resolved_retry_config()
        assert max_attempts == 11, "DeltaConfig must win when set"
    finally:
        ctx.delta_config.commit_max_attempts = original_delta
        ctx.table_write_config.max_attempts = original_shared


def test_resolved_retry_config_falls_through_to_shared(temp_delta_path):
    """When DeltaConfig.commit_max_attempts is None (default), the chain
    falls through to TableWriteConfig.max_attempts."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    original_delta = ctx.delta_config.commit_max_attempts
    original_shared = ctx.table_write_config.max_attempts
    try:
        ctx.delta_config.commit_max_attempts = None
        ctx.table_write_config.max_attempts = 42
        adapter = DeltaAdapter(temp_delta_path)
        max_attempts, _, _ = adapter._resolved_retry_config()
        assert max_attempts == 42, "must fall through to shared TableWriteConfig"
    finally:
        ctx.delta_config.commit_max_attempts = original_delta
        ctx.table_write_config.max_attempts = original_shared


def test_resolved_retry_config_retried_errors_chain(temp_delta_path):
    """The same three-level chain applies to retried_errors."""
    from ray.data._internal.datasource.delta.adapter import DeltaAdapter
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    original_delta = ctx.delta_config.commit_retried_errors
    original_shared = ctx.table_write_config.retried_errors
    try:
        ctx.delta_config.commit_retried_errors = None
        ctx.table_write_config.retried_errors = ["custom-shared"]

        adapter1 = DeltaAdapter(temp_delta_path)
        _, _, errors1 = adapter1._resolved_retry_config()
        assert errors1 == ["custom-shared"], "shared wins when delta_config is None"

        ctx.delta_config.commit_retried_errors = ["custom-delta"]
        adapter2 = DeltaAdapter(temp_delta_path)
        _, _, errors2 = adapter2._resolved_retry_config()
        assert errors2 == ["custom-delta"], "delta_config wins when set"

        adapter3 = DeltaAdapter(
            temp_delta_path, commit_retried_errors=["custom-per-call"]
        )
        _, _, errors3 = adapter3._resolved_retry_config()
        assert errors3 == ["custom-per-call"], "per-call wins"
    finally:
        ctx.delta_config.commit_retried_errors = original_delta
        ctx.table_write_config.retried_errors = original_shared


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
