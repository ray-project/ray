"""Tests for ``Dataset.write_delta`` -- core write path.

Covers APPEND / OVERWRITE / ERROR / IGNORE modes, Hive-style partitioning
(incl. dynamic partition overwrite and buffered writes), and schema
evolution.

Master's existing ``test_delta.py`` covers ``ray.data.read_delta`` and is
not modified here.
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
# APPEND.
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


def test_append_empty_dataset_against_existing_table_is_noop(temp_delta_path):
    """Regression test: a genuinely empty ``Dataset`` never triggers
    ``on_write_start`` (see ``test_dynamic_overwrite_empty_dataset_is_a_noop``
    for why), so ``_commit_append``'s empty-``file_actions`` branch must
    re-check table existence at commit time rather than trust a driver-side
    flag set only in ``on_write_start``. Before that fix this branch believed
    the table didn't exist and tried to (re)create it in place of a no-op."""
    schema = pa.schema([("id", pa.int64())])
    _write_append([{"id": 1}, {"id": 2}], temp_delta_path)
    ray.data.from_items([], override_num_blocks=1).write_delta(
        temp_delta_path, mode="append", schema=schema
    )
    assert sorted(_read_all(temp_delta_path), key=lambda r: r["id"]) == [
        {"id": 1},
        {"id": 2},
    ]


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
    """UPSERT is intentionally out of scope; the datasink must reject it."""
    with pytest.raises(ValueError, match="does not support mode"):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="upsert")


# ----------------------------------------------------------------------
# OVERWRITE / ERROR / IGNORE.
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


def test_static_overwrite_failure_does_not_delete_existing_data(
    temp_delta_path, monkeypatch
):
    """Regression test: static overwrite must replace the table's data in a
    single atomic transaction. Previously it did a separate `table.delete()`
    followed by `create_write_transaction(mode="append")` -- if the second
    call failed, the table was left permanently empty. Simulate that
    failure and confirm the original data survives."""
    from deltalake import DeltaTable

    _write_append([{"id": 1, "v": "original"}], temp_delta_path)

    def raise_on_commit(self, *args, **kwargs):
        raise IOError("AWS Error NETWORK_CONNECTION: simulated failure")

    monkeypatch.setattr(DeltaTable, "create_write_transaction", raise_on_commit)

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 2, "v": "new"}]).write_delta(
            temp_delta_path, mode="overwrite"
        )

    monkeypatch.undo()
    out = _read_all(temp_delta_path)
    assert out == [{"id": 1, "v": "original"}]


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


def test_dynamic_overwrite_empty_dataset_is_a_noop(temp_delta_path):
    """Dynamic partition overwrite only replaces partitions present in the
    new data. An empty write has no partitions in it, so -- unlike static
    overwrite -- it must leave all existing data untouched (mirrors Spark's
    dynamic partitionOverwriteMode, which has no way to know which partition
    an empty write meant to target).

    Regression test: for a genuinely empty ``Dataset``, Ray Data never calls
    ``on_write_start`` at all (zero blocks means zero write tasks means the
    Write operator's first-bundle callback never fires), so a driver-side
    flag set only in ``on_write_start`` is unreliable here. ``_commit_append``
    / ``_commit_overwrite`` must instead rely on a fresh ``try_get_deltatable``
    check made at commit time -- see ``datasink.py``."""
    schema = pa.schema([("year", pa.int64()), ("id", pa.int64())])
    rows_initial = [{"year": 2024, "id": 1}, {"year": 2025, "id": 2}]
    ray.data.from_items(rows_initial).write_delta(
        temp_delta_path, partition_cols=["year"]
    )
    ray.data.from_items([], override_num_blocks=1).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
        schema=schema,
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == rows_initial


def test_error_mode_race_table_appears_after_preflight(temp_delta_path, monkeypatch):
    """If a table is created concurrently between preflight (sees no
    table) and commit (sees one), ERROR mode must NOT silently append.
    We assert *some* exception bubbles up rather than a silent success."""
    from ray.data._internal.datasource.delta import (
        datasink as _delta_datasink,
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
    monkeypatch.setattr(_delta_datasink, "try_get_deltatable", racy_get)

    with pytest.raises(Exception):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode="error")


# ----------------------------------------------------------------------
# Partitioning + dynamic overwrite + buffered writes.
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


def test_dynamic_overwrite_skips_delete_when_predicate_unavailable(
    temp_delta_path, monkeypatch
):
    """Regression test: when neither the atomic single-partition filter nor
    the partition-scoped delete predicate can be built, dynamic partition
    overwrite must skip the delete entirely rather than falling back to an
    unconditional `table.delete()`, which would wipe partitions the write
    never asked to touch."""
    from ray.data._internal.datasource.delta import committer as _committer

    rows_initial = [
        {"year": 2024, "id": 1, "v": "a"},
        {"year": 2025, "id": 2, "v": "b"},
    ]
    ray.data.from_items(rows_initial).write_delta(
        temp_delta_path, partition_cols=["year"]
    )

    # Force past the atomic single-partition path too, so this exercises the
    # delete()-based fallback specifically.
    monkeypatch.setattr(
        _committer, "_build_single_partition_filter", lambda *a, **kw: None
    )
    monkeypatch.setattr(
        _committer, "_build_partition_delete_predicate", lambda *a, **kw: None
    )

    ray.data.from_items([{"year": 2024, "id": 99, "v": "new"}]).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
    )
    out = _read_all(temp_delta_path)
    # 2025 must survive -- the old code's unconditional table.delete() would
    # have wiped it even though the write never touched that partition.
    assert any(r["year"] == 2025 and r["id"] == 2 for r in out)


def test_dynamic_overwrite_single_partition_is_atomic(temp_delta_path, monkeypatch):
    """Regression test: when a dynamic overwrite touches exactly one
    partition combination (the common case), it must replace that
    partition's data in the SAME transaction as the append -- not a
    separate `table.delete()` followed by a second commit, which would
    leave the partition empty if the second commit failed."""
    import deltalake

    rows_initial = [
        {"year": 2024, "id": 1, "v": "old"},
        {"year": 2025, "id": 2, "v": "b"},
    ]
    ray.data.from_items(rows_initial).write_delta(
        temp_delta_path, partition_cols=["year"]
    )

    delete_calls = {"n": 0}
    real_delete = deltalake.DeltaTable.delete

    def spy_delete(self, *args, **kwargs):
        delete_calls["n"] += 1
        return real_delete(self, *args, **kwargs)

    monkeypatch.setattr(deltalake.DeltaTable, "delete", spy_delete)

    ray.data.from_items([{"year": 2024, "id": 99, "v": "new"}]).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
    )
    assert delete_calls["n"] == 0, "single-partition dynamic overwrite must be atomic"
    out = sorted(_read_all(temp_delta_path), key=lambda r: (r["year"], r["id"]))
    assert out == [
        {"year": 2024, "id": 99, "v": "new"},
        {"year": 2025, "id": 2, "v": "b"},
    ]


def test_dynamic_overwrite_multiple_partitions_in_one_write(temp_delta_path):
    """Dynamic overwrite touching more than one distinct partition value in
    a single write can't use the atomic single-partition path (delta-rs's
    partition_filters doesn't support an OR across combinations); it must
    still correctly replace every touched partition via the delete()
    fallback, leaving untouched partitions alone."""
    rows_initial = [
        {"year": 2023, "id": 1, "v": "a"},
        {"year": 2024, "id": 2, "v": "b"},
        {"year": 2025, "id": 3, "v": "c"},
    ]
    ray.data.from_items(rows_initial).write_delta(
        temp_delta_path, partition_cols=["year"]
    )
    ray.data.from_items(
        [
            {"year": 2024, "id": 99, "v": "new"},
            {"year": 2025, "id": 100, "v": "new"},
        ]
    ).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["year"],
        partition_overwrite_mode="dynamic",
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: (r["year"], r["id"]))
    assert out == [
        {"year": 2023, "id": 1, "v": "a"},
        {"year": 2024, "id": 99, "v": "new"},
        {"year": 2025, "id": 100, "v": "new"},
    ]


def test_dynamic_overwrite_null_partition_value(temp_delta_path):
    """A NULL partition value can't be expressed in delta-rs's
    partition_filters (verified empirically), so this must fall back to
    the delete()-based path rather than erroring or silently no-op'ing."""
    rows_initial = [
        {"region": None, "id": 1, "v": "a"},
        {"region": "us", "id": 2, "v": "b"},
    ]
    ray.data.from_items(rows_initial).write_delta(
        temp_delta_path, partition_cols=["region"]
    )
    ray.data.from_items([{"region": None, "id": 99, "v": "new"}]).write_delta(
        temp_delta_path,
        mode="overwrite",
        partition_cols=["region"],
        partition_overwrite_mode="dynamic",
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: (str(r["region"]), r["id"]))
    assert out == [
        {"region": None, "id": 99, "v": "new"},
        {"region": "us", "id": 2, "v": "b"},
    ]


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


# ---- Partition edge cases & writer knobs ---------------------------------


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
    should not produce one file per block -- the buffer merges them."""
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


def test_get_file_info_with_retry_retries_transient_failure(tmp_path):
    """Regression test for the `writer.py` call-site fix: the post-write
    size check must go through `get_file_info_with_retry` (which retries
    transient I/O errors), not a bare `filesystem.get_file_info` call.

    `write_parquet_with_retry`'s write happens through a real
    `pyarrow.fs.FileSystem` (pq.write_table type-checks the `filesystem`
    argument and rejects duck-typed wrappers), so this tests the retry
    helper directly rather than through a distributed write -- the actual
    Parquet write runs on a Ray worker in a separate process, where a
    driver-process monkeypatch wouldn't be visible anyway.
    """
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta.utils import get_file_info_with_retry

    real_fs = pa_fs.LocalFileSystem()
    path = str(tmp_path / "probe.txt")
    with open(path, "w") as f:
        f.write("x")
    calls = {"n": 0}
    real_get_file_info = real_fs.get_file_info

    class FlakyFs:
        def get_file_info(self, p):
            calls["n"] += 1
            if calls["n"] == 1:
                raise IOError("AWS Error NETWORK_CONNECTION: simulated failure")
            return real_get_file_info(p)

    info = get_file_info_with_retry(FlakyFs(), path)
    assert calls["n"] == 2
    assert info.size > 0


# ----------------------------------------------------------------------
# Schema evolution.
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


def test_schema_mode_error_rejects_new_column_when_schema_inferred(temp_delta_path):
    """Regression test: schema_mode must be enforced even when the schema
    comes from Ray's inference (no explicit `schema=`), not just when an
    explicit schema is passed. Evolution now happens at commit time in
    `_aggregate_and_commit`, using the worker-reconciled schema, rather than
    only in `on_write_start` against an explicit `schema=`."""
    _write_append([{"id": 1, "v": "x"}], temp_delta_path)
    with pytest.raises(ValueError, match="New columns detected"):
        ray.data.from_items([{"id": 2, "v": "y", "extra": "z"}]).write_delta(
            temp_delta_path, mode="append"
        )


def test_schema_mode_merge_adds_new_column_when_schema_inferred(temp_delta_path):
    _write_append([{"id": 1, "v": "x"}], temp_delta_path)
    ray.data.from_items([{"id": 2, "v": "y", "extra": "z"}]).write_delta(
        temp_delta_path, mode="append", schema_mode="merge"
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert len(out) == 2
    assert out[1]["extra"] == "z"


def test_ignore_mode_does_not_evolve_schema_of_existing_table(temp_delta_path):
    """IGNORE mode against an existing table must be a complete no-op --
    including not evolving the table's schema, even though workers still
    emit an Arrow schema for the (discarded) incoming data."""
    _write_append([{"id": 1, "v": "x"}], temp_delta_path)
    ray.data.from_items([{"id": 2, "v": "y", "extra": "z"}]).write_delta(
        temp_delta_path, mode="ignore", schema_mode="merge"
    )
    out = _read_all(temp_delta_path)
    assert out == [{"id": 1, "v": "x"}]
    assert "extra" not in out[0]


# ---- Schema-evolution edge cases ------------------------------------------


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


def test_non_microsecond_timestamp_units_coerced(temp_delta_path):
    """Regression test: Delta Lake requires microsecond-precision
    timestamps. Only `timestamp("s")` was previously coerced to
    `timestamp("us")`; `ns`/`ms` inputs (common from pandas/PyArrow) must
    be coerced too."""
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "ts_ns": pa.array([1_000_000_000, 2_000_000_000], type=pa.timestamp("ns")),
            "ts_ms": pa.array([1_000, 2_000], type=pa.timestamp("ms")),
        }
    )
    ray.data.from_arrow(table).write_delta(temp_delta_path)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert len(out) == 2


def test_cross_worker_type_promotion_int32_int64(temp_delta_path):
    """Two workers emit blocks with int32 and int64 columns named `v`.

    Schema-reconciliation at commit time promotes to int64 (the wider
    type). The per-block validator only fires when the user declared an
    explicit schema, so we declare ``int64`` so that the narrower int32
    block is accepted (narrower-into-wider is always compatible) and the
    wider int64 block matches the declared type.
    """
    block_a = pa.table({"v": pa.array([1, 2, 3], type=pa.int32())})
    block_b = pa.table({"v": pa.array([10, 20], type=pa.int64())})
    declared = pa.schema([("v", pa.int64())])
    ds = ray.data.from_arrow(block_a).union(ray.data.from_arrow(block_b))
    ds.write_delta(temp_delta_path, schema=declared)
    out = _read_all(temp_delta_path)
    assert sorted(r["v"] for r in out) == [1, 2, 3, 10, 20]


# ---- Filesystem plumbing ---------------------------------------------------


def test_worker_filesystem_carries_explicit_filesystem_through_pickle():
    """Regression test: an explicit `filesystem=` must reach the worker
    exactly as given, not be silently dropped in favor of a
    from_uri(table_uri) reconstruction (which would ignore the caller's
    credentials or non-default rooting, e.g. a SubTreeFileSystem)."""
    import pickle

    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta.fs import (
        make_fs_config,
        worker_filesystem,
    )

    explicit_fs = pa_fs.SubTreeFileSystem("/some/root", pa_fs.LocalFileSystem())
    config, driver_fs = make_fs_config("s3://bucket/table", explicit_fs, {})
    assert driver_fs is explicit_fs
    assert config.local_filesystem_root is None

    # Simulate shipping the config to a worker.
    restored_config = pickle.loads(pickle.dumps(config))
    worker_fs = worker_filesystem(restored_config)
    assert isinstance(worker_fs, pa_fs.SubTreeFileSystem)
    assert worker_fs.base_path == explicit_fs.base_path


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
