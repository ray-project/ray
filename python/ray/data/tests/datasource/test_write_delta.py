"""Tests for the prototype ``Dataset.write_delta`` implementation.

Covers only what this prototype actually supports: ``SaveMode.APPEND`` and
``SaveMode.OVERWRITE`` (no ``ERROR``/``IGNORE``, no dynamic partition
overwrite), Hive-style partitioning, ``storage_options`` passthrough to the
driver-side commit calls, and ``schema_mode``-governed schema evolution on
APPEND (adding new columns only -- never changing an existing column's type).

Master's existing ``test_delta.py`` covers ``ray.data.read_delta`` and is not
modified here.
"""

import os
from typing import Any, Callable, Dict, List, Optional
from unittest import mock

import pytest
from packaging.version import parse as parse_version

import ray
from ray.data import SaveMode
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.tests.catalog_test_utils import FakeCatalog
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

try:
    import deltalake as _deltalake_check  # noqa: F401
except ImportError:
    _deltalake_check = None

_pa_version = get_pyarrow_version()
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

# Whether ``read_delta`` gives partition columns back with their declared types.
#
# A Delta reader takes a partitioned column's value from metadata rather than
# from the Parquet file (the writer omits it there), so restoring the declared
# type is the reader's job. ``read_delta`` delegates that to PyArrow -- see the
# comment on ``partition_columns=[]`` in ``ParquetDatasource.from_pyarrow_dataset``
# -- and on pyarrow<19 it doesn't happen: values come back as the raw Hive path
# strings instead, so ``2024`` reads as ``"2024"`` and a null partition as the
# literal ``"__HIVE_DEFAULT_PARTITION__"``.
#
# That's a gap in the *read* path, which this file doesn't own; the writes
# themselves are fine on every version. Rather than weaken the assertions
# everywhere, the round-trip *value* checks below skip on old pyarrow while the
# on-disk layout checks keep running. 19 is the lowest version observed to
# round-trip correctly (17 fails, 19 and 20 pass); 18 is simply untested.
_PARTITION_VALUES_ROUND_TRIP = _pa_version is not None and _pa_version >= parse_version(
    "19.0.0"
)
_NO_TYPED_PARTITION_VALUES = (
    "read_delta returns partition values as untyped Hive path strings on pyarrow<19"
)
_skip_without_typed_partition_values = pytest.mark.skipif(
    not _PARTITION_VALUES_ROUND_TRIP, reason=_NO_TYPED_PARTITION_VALUES
)


@pytest.fixture
def temp_delta_path(tmp_path):
    """Clean per-test path for a Delta table."""
    return os.path.join(str(tmp_path), "delta_table")


def _write_append(rows: List[Dict[str, Any]], path: str, **kwargs) -> None:
    ray.data.from_items(rows).write_delta(path, **kwargs)


def _read_all(path: str) -> List[Dict[str, Any]]:
    return ray.data.read_delta(path).take_all()


def _log_exists(path: str) -> bool:
    return os.path.isdir(os.path.join(path, "_delta_log"))


def _delta_log_json_count(path: str) -> int:
    """Count the *.json files inside _delta_log/ (one per Delta version)."""
    log_dir = os.path.join(path, "_delta_log")
    if not os.path.isdir(log_dir):
        return 0
    return sum(1 for f in os.listdir(log_dir) if f.endswith(".json"))


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


def test_multiple_blocks_commit_correctly(temp_delta_path):
    """A dataset split across several blocks/write tasks must still commit
    all rows in a single, correctly-aggregated transaction."""
    rows = [{"id": i, "v": f"r{i}"} for i in range(20)]
    ray.data.from_items(rows, override_num_blocks=4).write_delta(temp_delta_path)
    assert sorted(_read_all(temp_delta_path), key=lambda r: r["id"]) == rows


def test_read_parquet_sourced_write_succeeds(tmp_path, temp_delta_path):
    """Regression test: a Dataset sourced from a file-listing-based read
    (e.g. ``read_parquet``) must write successfully. ``on_write_start``
    can receive Ray Data's internal file-listing metadata schema (e.g. a
    null-typed ``__file_chunk_metadata`` column) rather than the actual
    row schema for such datasets -- ``_resolve_schema`` must prefer the
    schema workers actually wrote over that driver-side capture. Every
    other test in this file uses ``from_items``, which doesn't go through
    this code path and so never exercised this."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    source_path = str(tmp_path / "source.parquet")
    rows = [{"id": i, "v": f"r{i}"} for i in range(10)]
    pq.write_table(pa.Table.from_pylist(rows), source_path)

    ray.data.read_parquet(source_path).write_delta(temp_delta_path)
    assert sorted(_read_all(temp_delta_path), key=lambda r: r["id"]) == rows


# ----------------------------------------------------------------------
# OVERWRITE.
# ----------------------------------------------------------------------


def test_overwrite_replaces_existing_data(temp_delta_path):
    _write_append([{"id": i, "v": "old"} for i in range(3)], temp_delta_path)
    ray.data.from_items([{"id": i, "v": "new"} for i in range(2)]).write_delta(
        temp_delta_path, mode=SaveMode.OVERWRITE
    )
    out = _read_all(temp_delta_path)
    assert len(out) == 2
    assert all(r["v"] == "new" for r in out)


def test_overwrite_creates_table_when_missing(temp_delta_path):
    ray.data.from_items([{"id": 1}]).write_delta(
        temp_delta_path, mode=SaveMode.OVERWRITE
    )
    assert _log_exists(temp_delta_path)
    assert _read_all(temp_delta_path) == [{"id": 1}]


def test_overwrite_empty_dataset_against_existing_table_truncates(temp_delta_path):
    """An empty OVERWRITE against an already-existing table is still a
    well-defined operation (truncate it) even though no worker writes any
    rows to infer a schema from. Regression test: before the fix, this
    raised "Cannot write a Delta table without a schema" because no write
    tasks ran (so there was no worker schema) and the resolver had no
    fallback to the table's own existing schema."""
    _write_append([{"id": i, "v": "old"} for i in range(3)], temp_delta_path)
    ray.data.from_items([]).write_delta(temp_delta_path, mode=SaveMode.OVERWRITE)
    assert _read_all(temp_delta_path) == []


# ----------------------------------------------------------------------
# Unsupported modes -- this prototype only implements APPEND/OVERWRITE.
# ----------------------------------------------------------------------


@pytest.mark.parametrize("mode", [SaveMode.ERROR, SaveMode.IGNORE, SaveMode.UPSERT])
def test_unsupported_mode_rejected(temp_delta_path, mode):
    with pytest.raises(ValueError, match="only supports"):
        ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode=mode)


# ----------------------------------------------------------------------
# Partitioning.
# ----------------------------------------------------------------------


def test_single_column_partition(temp_delta_path):
    rows = [{"year": 2024, "id": 1}, {"year": 2025, "id": 2}]
    ray.data.from_items(rows).write_delta(temp_delta_path, partition_by=["year"])
    assert set(os.listdir(temp_delta_path)) >= {"year=2024", "year=2025"}

    if not _PARTITION_VALUES_ROUND_TRIP:
        pytest.skip(_NO_TYPED_PARTITION_VALUES)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == rows


@_skip_without_typed_partition_values
def test_multi_column_partition(temp_delta_path):
    rows = [
        {"year": 2024, "month": 1, "id": 1},
        {"year": 2024, "month": 2, "id": 2},
        {"year": 2025, "month": 1, "id": 3},
    ]
    ray.data.from_items(rows).write_delta(
        temp_delta_path, partition_by=["year", "month"]
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == rows


def test_null_partition_value_round_trips_as_none(temp_delta_path):
    """Regression test: PyArrow's Hive-flavor writer encodes a NULL
    partition value as the literal directory name
    ``__HIVE_DEFAULT_PARTITION__`` -- this must round-trip back as Python
    ``None``, not that literal string."""
    rows = [{"year": 2024, "id": 1}, {"year": None, "id": 2}]
    ray.data.from_items(rows).write_delta(temp_delta_path, partition_by=["year"])
    assert "year=__HIVE_DEFAULT_PARTITION__" in os.listdir(temp_delta_path)

    if not _PARTITION_VALUES_ROUND_TRIP:
        pytest.skip(_NO_TYPED_PARTITION_VALUES)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == rows


def test_parse_partition_values_maps_hive_default_to_none():
    from ray.data._internal.datasource.delta_datasink import _parse_partition_values

    assert _parse_partition_values(
        "year=__HIVE_DEFAULT_PARTITION__/part-0.parquet", ["year"]
    ) == {"year": None}


def test_parse_partition_values_unquotes_keys():
    """Partition column names, like values, must be URL-unquoted. PyArrow's
    current Hive writer doesn't appear to actually quote keys in practice,
    but this is a cheap, harmless, symmetric fix with the value-unquoting
    that's already done -- exercised here directly since a real write can't
    easily be made to produce a quoted key."""
    from ray.data._internal.datasource.delta_datasink import _parse_partition_values

    assert _parse_partition_values("col%20name=val/part-0.parquet", ["col name"]) == {
        "col name": "val"
    }


def test_append_without_partition_by_inherits_existing_partitioning(temp_delta_path):
    """Regression test: appending to an already-partitioned table without
    re-specifying partition_by used to write files flat (no Hive partition
    directory) with an empty AddAction.partition_values -- Delta's reader
    derives a partitioned column's value from that metadata, not from the
    (still-present) column in the physical Parquet file, so the real value
    was silently lost entirely (read back as None), not just misplaced on
    disk."""
    rows1 = [{"year": 2024, "id": 1}]
    ray.data.from_items(rows1).write_delta(temp_delta_path, partition_by=["year"])
    assert "year=2024" in os.listdir(temp_delta_path)

    rows2 = [{"year": 2025, "id": 2}]
    ray.data.from_items(rows2).write_delta(temp_delta_path)  # no partition_by

    assert "year=2025" in os.listdir(temp_delta_path)

    if not _PARTITION_VALUES_ROUND_TRIP:
        pytest.skip(_NO_TYPED_PARTITION_VALUES)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [{"year": 2024, "id": 1}, {"year": 2025, "id": 2}]


def test_append_without_partition_by_on_unpartitioned_table_unaffected(
    temp_delta_path,
):
    """An append with no partition_by to a table that was never partitioned
    must behave exactly as before -- no partition inheritance to do."""
    _write_append([{"id": 1, "v": "a"}], temp_delta_path)
    _write_append([{"id": 2, "v": "b"}], temp_delta_path)
    assert not any(name.startswith("id=") for name in os.listdir(temp_delta_path))
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


@pytest.mark.parametrize(
    "create_partition_by,write_partition_by",
    [
        pytest.param(["year", "month"], ["year"], id="subset"),
        pytest.param(["year"], ["year", "month"], id="superset"),
        pytest.param(["month", "year"], ["year", "month"], id="reordered"),
        pytest.param(None, ["year"], id="unpartitioned-to-partitioned"),
    ],
)
def test_partition_by_mismatch_rejected(
    temp_delta_path, create_partition_by, write_partition_by
):
    """A partition_by that disagrees with the table's partition columns is
    rejected before anything is written.

    Delta takes a partitioned column's value from AddAction metadata, so a
    mismatched layout either reads those values back as None (subset, or
    partitioning a previously-unpartitioned table) or commits a version that
    can't be read at all (superset). Both used to happen silently.
    """
    from deltalake import DeltaTable

    first = [{"year": 2024, "month": 1, "id": 1}]
    ray.data.from_items(first).write_delta(
        temp_delta_path, partition_by=create_partition_by
    )
    log_before = _delta_log_json_count(temp_delta_path)

    with pytest.raises(ValueError, match="can't change an existing table's"):
        ray.data.from_items([{"year": 2025, "month": 2, "id": 2}]).write_delta(
            temp_delta_path, partition_by=write_partition_by
        )

    # Nothing was committed and the table is untouched -- the assertions that
    # actually guard against the corruption this test exists for.
    assert _delta_log_json_count(temp_delta_path) == log_before
    assert DeltaTable(temp_delta_path).metadata().partition_columns == (
        create_partition_by or []
    )

    # Only the baseline tables that are themselves partitioned depend on
    # partition values surviving the read; the unpartitioned case is unaffected.
    if create_partition_by and not _PARTITION_VALUES_ROUND_TRIP:
        pytest.skip(_NO_TYPED_PARTITION_VALUES)
    assert _read_all(temp_delta_path) == first


@_skip_without_typed_partition_values
def test_partition_by_matching_existing_is_allowed(temp_delta_path):
    """Passing exactly the table's own partition columns is fine."""
    ray.data.from_items([{"year": 2024, "id": 1}]).write_delta(
        temp_delta_path, partition_by=["year"]
    )
    ray.data.from_items([{"year": 2025, "id": 2}]).write_delta(
        temp_delta_path, partition_by=["year"]
    )
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [{"year": 2024, "id": 1}, {"year": 2025, "id": 2}]


def test_overwrite_without_partition_by_inherits_existing_partitioning(
    temp_delta_path,
):
    """An OVERWRITE inherits the existing table's partitioning just like an
    APPEND does. Regression test: overwriting an already-partitioned table
    without re-specifying partition_by used to write flat files with empty
    AddAction.partition_values while the table's partition_columns metadata
    stayed intact -- verified empirically that
    ``create_write_transaction(partition_by=None)`` does NOT reset that
    metadata -- leaving the table self-contradictory and every partition
    value reading back as None."""
    ray.data.from_items([{"year": 2024, "id": 1}]).write_delta(
        temp_delta_path, partition_by=["year"]
    )
    ray.data.from_items([{"year": 2025, "id": 2}]).write_delta(
        temp_delta_path, mode=SaveMode.OVERWRITE
    )

    assert "year=2025" in os.listdir(temp_delta_path)

    if not _PARTITION_VALUES_ROUND_TRIP:
        pytest.skip(_NO_TYPED_PARTITION_VALUES)
    assert _read_all(temp_delta_path) == [{"year": 2025, "id": 2}]


@pytest.mark.parametrize("mode", [SaveMode.APPEND, SaveMode.OVERWRITE])
def test_write_missing_partition_column_raises_clear_error(temp_delta_path, mode):
    """Writing data that lacks one of the table's partition columns must
    raise a clear error naming the missing column. Those columns can be
    inherited from the existing table rather than passed by the caller, so
    the bare ``KeyError('Field "year" does not exist in schema')`` PyArrow
    raises from inside a worker task gives the user nothing to go on."""
    ray.data.from_items([{"year": 2024, "id": 1}]).write_delta(
        temp_delta_path, partition_by=["year"]
    )
    with pytest.raises(Exception, match="missing partition column"):
        ray.data.from_items([{"other": "x"}]).write_delta(temp_delta_path, mode=mode)


# ----------------------------------------------------------------------
# Empty dataset against an existing table (APPEND is a no-op).
# ----------------------------------------------------------------------


def test_empty_append_against_existing_table_is_noop(temp_delta_path):
    """An empty-dataset APPEND against an already-existing table must be a
    pure no-op: no schema resolution needed, no empty commit added to the
    Delta log, and existing data left untouched."""
    rows = [{"id": 1}, {"id": 2}]
    ray.data.from_items(rows).write_delta(temp_delta_path)
    before = _delta_log_json_count(temp_delta_path)

    ray.data.from_items([]).write_delta(temp_delta_path)

    assert _delta_log_json_count(temp_delta_path) == before
    assert sorted(_read_all(temp_delta_path), key=lambda r: r["id"]) == rows


# ----------------------------------------------------------------------
# Relative path computation across filesystems.
# ----------------------------------------------------------------------


def test_written_file_relative_path_handles_leading_slash_mismatch(
    temp_delta_path, monkeypatch
):
    """Regression test: some cloud filesystems (e.g. S3) can return a table
    root without a leading slash from ``FileSystem.from_uri`` while written-
    file paths from that same filesystem still carry one (verified
    empirically against a real S3-compatible server). Before the fix,
    ``posixpath.relpath`` would walk upward (``../../bucket/...``) instead of
    the intended relative path, corrupting the Delta log's ``AddAction.path``.
    """
    import types

    import pyarrow as pa
    import pyarrow.fs as pafs

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    real_from_uri = pafs.FileSystem.from_uri

    class _FakeFileSystem:
        @staticmethod
        def from_uri(path):
            fs, root = real_from_uri(path)
            # Simulate the mismatch: root lacks a leading slash even though
            # written-file paths (below) still carry one.
            return fs, root.lstrip("/")

    def fake_write_dataset(
        table,
        *,
        base_dir,
        file_visitor: Optional[Callable[[Any], None]] = None,
        **kwargs,
    ):
        written_file = types.SimpleNamespace(
            path="/" + base_dir + "/part-0.parquet",
            size=123,
            metadata=types.SimpleNamespace(num_rows=table.num_rows),
        )
        # The visitor is how ``_write_parquet`` collects AddAction paths.
        assert file_visitor is not None
        file_visitor(written_file)

    monkeypatch.setattr("pyarrow.fs.FileSystem", _FakeFileSystem)
    monkeypatch.setattr("pyarrow.dataset.write_dataset", fake_write_dataset)

    datasink = DeltaDatasink(temp_delta_path)

    class _FakeTaskContext:
        task_idx = 0

    # pyrefly: ignore[bad-argument-type]
    add_actions = datasink._write_parquet(pa.table({"id": [1]}), _FakeTaskContext())
    assert add_actions[0].path == "part-0.parquet"


def test_create_dir_skipped_only_for_cloud_filesystems(monkeypatch):
    """Regression test: cloud object stores have no real directories --
    pyarrow's write_dataset defaults to create_dir=True, which for an
    S3FileSystem issues a PutObject to a single marker key representing the
    table root. Under a large write spanning many concurrent tasks that all
    target the same brand-new prefix, every task racing to PUT that same key
    can trigger AWS S3 throttling ("SLOW_DOWN") for no functional benefit.
    Local filesystems still need the directory actually created (verified
    separately: create_dir=False raises FileNotFoundError against a
    not-yet-existing local directory), so the skip must be filesystem-type-
    aware, not unconditional."""
    import pyarrow as pa

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    captured = {}

    def fake_write_dataset(table, *, create_dir, **kwargs):
        captured["create_dir"] = create_dir

    monkeypatch.setattr("pyarrow.dataset.write_dataset", fake_write_dataset)

    class _FakeTaskContext:
        task_idx = 0

    s3_datasink = DeltaDatasink("s3://bucket/table")
    # pyrefly: ignore[bad-argument-type]
    s3_datasink._write_parquet(pa.table({"id": [1]}), _FakeTaskContext())
    assert captured["create_dir"] is False


# ----------------------------------------------------------------------
# Filesystem root derivation.
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "path,expected",
    [
        # Azure: the storage account lives on the AzureFileSystem object, not in
        # the path, so the "@account.dfs.core.windows.net" host must be dropped.
        # Regression: keeping it pointed writes at a container literally named
        # "container@account..." and made every AddAction relative path wrong.
        ("abfss://container@acct.dfs.core.windows.net/tbl", "container/tbl"),
        ("abfs://container@acct.dfs.core.windows.net/a/b", "container/a/b"),
        # Object stores: bucket/key.
        ("s3://bucket/tbl", "bucket/tbl"),
        ("s3a://bucket/tbl", "bucket/tbl"),
        ("gs://bucket/a/b", "bucket/a/b"),
        ("gcs://bucket/a/b", "bucket/a/b"),
        # Local paths pass through.
        ("/tmp/local/tbl", "/tmp/local/tbl"),
        ("file:///tmp/x", "/tmp/x"),
    ],
)
def test_filesystem_root_from_uri_matches_conventions(path, expected):
    """``_filesystem_root_from_uri`` must return exactly what each filesystem expects as a
    path -- i.e. what ``FileSystem.from_uri`` would return -- without building a
    filesystem, since doing so resolves an S3 bucket against real AWS and so
    fails against any S3-compatible endpoint."""
    from ray.data.datasource.path_util import _filesystem_root_from_uri

    assert _filesystem_root_from_uri(path) == expected


def test_filesystem_root_from_uri_agrees_with_from_uri():
    """Pins the parity claim above for every scheme ``from_uri`` can resolve
    without network access (it can't for ``s3://``, which is the whole reason
    ``_filesystem_root_from_uri`` exists)."""
    import pyarrow.fs as pafs

    from ray.data.datasource.path_util import _filesystem_root_from_uri

    for path in [
        "abfss://container@acct.dfs.core.windows.net/tbl",
        "abfs://container@acct.dfs.core.windows.net/a/b",
        "gs://bucket/tbl",
        "gcs://bucket/a/b",
        "/tmp/local/tbl",
        "file:///tmp/x",
    ]:
        _, from_uri_path = pafs.FileSystem.from_uri(path)
        assert _filesystem_root_from_uri(path) == from_uri_path, path


# ----------------------------------------------------------------------
# storage_options reaching the worker-side filesystem.
# ----------------------------------------------------------------------


def test_explicit_filesystem_from_storage_options_s3():
    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    fs = _explicit_filesystem_from_storage_options(
        "s3://bucket/table",
        {
            "AWS_ACCESS_KEY_ID": "AKIA-FAKE",
            "AWS_SECRET_ACCESS_KEY": "SECRET-FAKE",
            "AWS_REGION": "us-west-2",
        },
    )
    import pyarrow.fs as pafs

    assert isinstance(fs, pafs.S3FileSystem)


def test_explicit_filesystem_from_storage_options_s3_falls_back_to_default_region():
    """Regression test: callers who set AWS_DEFAULT_REGION (a common
    boto-style key) instead of AWS_REGION must still get a correctly-regioned
    worker filesystem -- previously only AWS_REGION was read, so the worker's
    S3FileSystem silently got region=None even though the driver's commit
    path (which forwards the full storage_options dict to deltalake) could
    honor AWS_DEFAULT_REGION fine."""
    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    fs = _explicit_filesystem_from_storage_options(
        "s3://bucket/table",
        {
            "AWS_ACCESS_KEY_ID": "AKIA-FAKE",
            "AWS_SECRET_ACCESS_KEY": "SECRET-FAKE",
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    )
    assert fs is not None
    assert fs.region == "us-west-2"

    # AWS_REGION still takes precedence when both are set.
    fs = _explicit_filesystem_from_storage_options(
        "s3://bucket/table",
        {
            "AWS_ACCESS_KEY_ID": "AKIA-FAKE",
            "AWS_SECRET_ACCESS_KEY": "SECRET-FAKE",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    )
    assert fs is not None
    assert fs.region == "us-east-1"


def test_explicit_filesystem_from_storage_options_returns_none_without_options():
    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    assert _explicit_filesystem_from_storage_options("s3://bucket/table", None) is None
    assert _explicit_filesystem_from_storage_options("s3://bucket/table", {}) is None
    # Unrecognized scheme: no match even with options set.
    assert (
        _explicit_filesystem_from_storage_options(
            "hdfs://cluster/table", {"AWS_ACCESS_KEY_ID": "x"}
        )
        is None
    )


def test_explicit_filesystem_from_storage_options_azure_sas_key():
    """Regression test: the SAS token key must match ray.data.catalog's
    vended-credential key (AZURE_STORAGE_SAS_TOKEN), not a different name --
    a mismatch here means workers silently build an uncredentialed
    filesystem even when the driver's commit works fine.

    ``pyarrow.fs.AzureFileSystem`` is an immutable Cython extension type
    (its ``__init__`` can't be monkeypatched directly), so this replaces
    the class itself via ``mock.patch`` to capture the constructor kwargs.
    """
    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    with mock.patch("pyarrow.fs.AzureFileSystem") as mock_azure_fs:
        _explicit_filesystem_from_storage_options(
            "abfss://container@account.dfs.core.windows.net/table",
            {
                "AZURE_STORAGE_ACCOUNT_NAME": "account",
                "AZURE_STORAGE_SAS_TOKEN": "sv=fake-sas",
            },
        )

    mock_azure_fs.assert_called_once_with(
        account_name="account", sas_token="sv=fake-sas", account_key=None
    )


def test_explicit_filesystem_from_storage_options_azure_account_key():
    """Regression test: an account key is a separate, mutually exclusive
    auth method from a SAS token (both deltalake and
    pyarrow.fs.AzureFileSystem accept either) -- a caller authenticating
    via account key instead of SAS token must still get a credentialed
    worker filesystem, not one that silently falls back to ambient auth."""
    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    with mock.patch("pyarrow.fs.AzureFileSystem") as mock_azure_fs:
        _explicit_filesystem_from_storage_options(
            "abfss://container@account.dfs.core.windows.net/table",
            {
                "AZURE_STORAGE_ACCOUNT_NAME": "account",
                "AZURE_STORAGE_ACCOUNT_KEY": "fake-account-key",
            },
        )

    mock_azure_fs.assert_called_once_with(
        account_name="account", sas_token=None, account_key="fake-account-key"
    )


def test_explicit_filesystem_from_storage_options_gcs_restores_env(monkeypatch):
    """Regression test: constructing a GCS filesystem from a service-account
    path must not leak GOOGLE_APPLICATION_CREDENTIALS -- a Ray worker
    process can be reused for other tasks (different or no credentials)
    afterward, so the env var must be restored to whatever it was before."""
    import os

    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/previous/creds.json")
    _explicit_filesystem_from_storage_options(
        "gs://bucket/table",
        {"GOOGLE_SERVICE_ACCOUNT": "/new/creds.json"},
    )
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/previous/creds.json"

    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    _explicit_filesystem_from_storage_options(
        "gs://bucket/table",
        {"GOOGLE_SERVICE_ACCOUNT": "/new/creds.json"},
    )
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ


# ----------------------------------------------------------------------
# Multi-block tasks.
# ----------------------------------------------------------------------


def test_write_returns_one_schema_and_file_per_block(temp_delta_path):
    """Each block is written to its own Parquet file and reports its own
    schema; the driver unifies them at commit time."""
    import pyarrow as pa

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    class _FakeTaskContext:
        task_idx = 0

    datasink = DeltaDatasink(temp_delta_path)
    block_a = pa.table({"v": pa.array([1, 2], type=pa.int64())})
    block_b = pa.table({"v": pa.array([None], type=pa.null())})

    # pyrefly: ignore[bad-argument-type]
    result = datasink.write([block_a, block_b], _FakeTaskContext())

    assert len(result.add_actions) == 2
    assert result.schemas == [block_a.schema, block_b.schema]


def test_multi_block_task_with_job_write_uuid_does_not_collide(temp_delta_path):
    """Blocks in one task must not overwrite each other's output file.

    The write UUID is fixed once per job (so a retry reuses the same
    filenames), which means it is identical for every block in a task, and
    ``basename_template``'s ``{i}`` restarts at 0 on each ``write_dataset``
    call. Without a per-block component in the name, the second block would
    write to the same path as the first and -- under
    ``existing_data_behavior="overwrite_or_ignore"`` -- silently replace it,
    while the commit reported the same path twice.

    The other multi-block tests can't catch this: they leave ``ctx.kwargs``
    empty, so they take the per-call ``uuid4()`` fallback rather than the
    production path exercised here."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME

    class _FakeTaskContext:
        task_idx = 0
        kwargs = {WRITE_UUID_KWARG_NAME: "fixedjobuuid"}

    datasink = DeltaDatasink(temp_delta_path)
    block_a = pa.table({"id": [1, 2]})
    block_b = pa.table({"id": [3, 4]})

    # pyrefly: ignore[bad-argument-type]
    result = datasink.write([block_a, block_b], _FakeTaskContext())

    paths = [action.path for action in result.add_actions]
    assert len(paths) == len(set(paths)), f"duplicate AddAction paths: {paths}"
    written = sorted(f for f in os.listdir(temp_delta_path) if f.endswith(".parquet"))
    assert len(written) == 2, written
    # Every row survived -- nothing was overwritten.
    total = 0
    for name in written:
        total += pq.read_table(os.path.join(temp_delta_path, name)).num_rows
    assert total == 4


def test_multi_block_task_unifies_schema_across_blocks(temp_delta_path):
    """Blocks in one task can have different but unifiable schemas (here a
    null-typed column against a concrete one). The committed table schema
    is the unification, and every row round-trips."""
    import pyarrow as pa

    block_a = pa.table({"id": pa.array([1]), "v": pa.array(["x"])})
    block_b = pa.table({"id": pa.array([2]), "v": pa.array([None], type=pa.null())})
    ray.data.from_arrow([block_a, block_b]).write_delta(temp_delta_path)

    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [{"id": 1, "v": "x"}, {"id": 2, "v": None}]


def test_multi_block_task_with_differing_columns(temp_delta_path):
    """Blocks with genuinely different column sets (one missing a column
    another has) round-trip, with the absent value read back as None."""
    import pyarrow as pa

    block_a = pa.table({"id": [1], "name": ["a"]})
    block_b = pa.table({"id": [2], "name": ["b"], "extra": ["x"]})
    ray.data.from_arrow([block_a, block_b]).write_delta(temp_delta_path)

    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [
        {"id": 1, "name": "a", "extra": None},
        {"id": 2, "name": "b", "extra": "x"},
    ]


# ----------------------------------------------------------------------
# Table metadata.
# ----------------------------------------------------------------------


def test_name_and_description_recorded(temp_delta_path):
    from deltalake import DeltaTable

    ray.data.from_items([{"id": 1}]).write_delta(
        temp_delta_path, name="my_table", description="my description"
    )
    metadata = DeltaTable(temp_delta_path).metadata()
    assert metadata.name == "my_table"
    assert metadata.description == "my description"


# ----------------------------------------------------------------------
# Empty dataset.
# ----------------------------------------------------------------------


def test_empty_dataset_without_schema_raises(temp_delta_path):
    """This prototype has no ``schema=`` parameter to pre-declare a schema, so
    a genuinely empty ``Dataset`` (no rows, no schema to infer) must raise a
    clear error rather than commit an empty/malformed table."""
    with pytest.raises(ValueError, match="without a schema"):
        ray.data.from_items([]).write_delta(temp_delta_path)


# ----------------------------------------------------------------------
# storage_options passthrough to the driver-side commit calls.
# ----------------------------------------------------------------------


def test_storage_options_passed_to_create_table_with_add_actions(
    temp_delta_path, monkeypatch
):
    """storage_options must reach create_table_with_add_actions when the
    table doesn't exist yet (new-table path)."""
    from deltalake.transaction import create_table_with_add_actions as real_create

    captured = {}

    def spy_create(*args, **kwargs):
        captured.update(kwargs)
        return real_create(*args, **kwargs)

    with mock.patch("deltalake.transaction.create_table_with_add_actions", spy_create):
        ray.data.from_items([{"id": 1}]).write_delta(
            temp_delta_path, storage_options={"AWS_REGION": "us-west-2"}
        )

    assert captured.get("storage_options") == {"AWS_REGION": "us-west-2"}
    assert _read_all(temp_delta_path) == [{"id": 1}]


def test_storage_options_passed_to_create_write_transaction(
    temp_delta_path, monkeypatch
):
    """storage_options must reach the DeltaTable constructor used for the
    commit when the table already exists (existing-table path)."""
    from deltalake import DeltaTable

    _write_append([{"id": 1}], temp_delta_path)

    captured = {}
    real_init = DeltaTable.__init__

    def spy_init(self, table_uri, *args, **kwargs):
        captured["storage_options"] = kwargs.get("storage_options")
        return real_init(self, table_uri, *args, **kwargs)

    with mock.patch.object(DeltaTable, "__init__", spy_init):
        ray.data.from_items([{"id": 2}]).write_delta(
            temp_delta_path, storage_options={"AWS_REGION": "us-west-2"}
        )

    assert captured.get("storage_options") == {"AWS_REGION": "us-west-2"}
    assert len(_read_all(temp_delta_path)) == 2


# ----------------------------------------------------------------------
# Schema reconciliation on APPEND: schema_mode="merge" (default) evolves
# the table's schema to add new columns; schema_mode="error" rejects them.
# A type-incompatible existing column always raises, regardless of mode.
# ----------------------------------------------------------------------


def test_append_new_column_evolves_schema_by_default(temp_delta_path):
    """Appending a row with a column the existing table doesn't have adds
    that column to the table (schema_mode="merge" is the default) --
    existing rows read back with None for it, the new row has its real
    value. Before schema evolution was added, this used to silently drop
    the column's data instead."""
    _write_append([{"id": 1, "name": "a"}], temp_delta_path)
    _write_append([{"id": 2, "name": "b", "extra": "x"}], temp_delta_path)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [
        {"id": 1, "name": "a", "extra": None},
        {"id": 2, "name": "b", "extra": "x"},
    ]


def test_append_multiple_new_columns_evolves_schema(temp_delta_path):
    """More than one new column in a single append are all added."""
    _write_append([{"id": 1}], temp_delta_path)
    _write_append([{"id": 2, "a": "x", "b": 5}], temp_delta_path)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [
        {"id": 1, "a": None, "b": None},
        {"id": 2, "a": "x", "b": 5},
    ]


def test_append_multiple_new_columns_preserves_incoming_column_order(temp_delta_path):
    """New columns are added in the incoming schema's own column order.

    Regression test: they used to be collected via a set difference over
    field names, and string hash randomization makes set iteration order
    vary per process -- so the table's final column order came out
    different on every run (verified empirically: three runs, three
    orders). Enough columns are used here that a set-ordered
    implementation would essentially never match by chance.
    """
    from deltalake import DeltaTable

    _write_append([{"id": 1}], temp_delta_path)
    _write_append(
        [
            {
                "id": 2,
                "alpha": "a",
                "beta": "b",
                "gamma": "c",
                "delta": "d",
                "epsilon": "e",
            }
        ],
        temp_delta_path,
    )

    assert [f.name for f in DeltaTable(temp_delta_path).schema().fields] == [
        "id",
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
    ]


def test_append_new_column_and_incompatible_type_leaves_schema_unchanged(
    temp_delta_path,
):
    """Regression test: an append that both adds a new column AND has an
    incompatible type on an existing column must reject the whole write
    without evolving the schema first. Before this fix, the new column
    would be permanently added (a real, committed ALTER TABLE) before the
    type-compatibility check ran, so a rejected write still left the
    table's schema permanently changed."""
    import pyarrow as pa
    from deltalake import DeltaTable

    _write_append([{"id": 1, "name": "a"}], temp_delta_path)
    bad_schema = pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("name", pa.string()),
            pa.field("extra", pa.string()),
        ]
    )
    table = pa.table({"id": ["2"], "name": ["b"], "extra": ["x"]}, schema=bad_schema)
    with pytest.raises(ValueError, match="not compatible with the table's existing"):
        ray.data.from_arrow(table).write_delta(temp_delta_path)

    # The table's schema must be exactly as it was before the rejected
    # write -- no "extra" column, and the original row untouched.
    dt = DeltaTable(temp_delta_path)
    assert {f.name for f in dt.schema().fields} == {"id", "name"}
    assert _read_all(temp_delta_path) == [{"id": 1, "name": "a"}]


def test_append_new_column_forced_nullable(temp_delta_path):
    """A new column is always added as nullable, even if the incoming data
    declares it non-nullable -- every row already in the table has no
    value for a brand-new column, so a non-nullable declaration would make
    the table's schema self-contradictory."""
    import pyarrow as pa
    from deltalake import DeltaTable

    _write_append([{"id": 1}], temp_delta_path)
    non_nullable_schema = pa.schema(
        [pa.field("id", pa.int64()), pa.field("extra", pa.string(), nullable=False)]
    )
    table = pa.table({"id": [2], "extra": ["x"]}, schema=non_nullable_schema)
    ray.data.from_arrow(table).write_delta(temp_delta_path)

    dt = DeltaTable(temp_delta_path)
    field = next(f for f in dt.schema().fields if f.name == "extra")
    assert field.nullable is True


def test_append_extra_column_rejected_with_schema_mode_error(temp_delta_path):
    """schema_mode="error" is the opt-out: reject the extra column instead
    of evolving the schema, leaving the table untouched."""
    _write_append([{"id": 1, "name": "a"}], temp_delta_path)
    with pytest.raises(ValueError, match="not present in the table's existing schema"):
        _write_append(
            [{"id": 2, "name": "b", "extra": "x"}],
            temp_delta_path,
            schema_mode="error",
        )
    # The rejected write must not have changed the table.
    assert _read_all(temp_delta_path) == [{"id": 1, "name": "a"}]


def test_invalid_schema_mode_rejected(temp_delta_path):
    with pytest.raises(ValueError, match="schema_mode"):
        ray.data.from_items([{"id": 1}]).write_delta(
            temp_delta_path, schema_mode="bogus"
        )


def test_append_incompatible_type_rejected(temp_delta_path):
    """Regression test: appending a column with a type incompatible with
    the existing table's (e.g. string where the table has int64) used to
    commit successfully but leave the table unreadable on the very next
    read. Must raise clearly at write time instead, and must not corrupt
    the table. Unlike a brand-new column, this always raises regardless of
    schema_mode -- changing an existing column's type is never supported,
    only adding a new one is."""
    _write_append([{"id": 1, "name": "a"}], temp_delta_path)
    with pytest.raises(ValueError, match="not compatible with the table's existing"):
        _write_append([{"id": "not-an-int", "name": "b"}], temp_delta_path)
    # The table must still be readable after the rejected write.
    assert _read_all(temp_delta_path) == [{"id": 1, "name": "a"}]


@pytest.mark.parametrize("exc_name", ["ArrowTypeError", "ArrowInvalid", "KeyError"])
def test_unifiable_schema_failures_are_wrapped(temp_delta_path, monkeypatch, exc_name):
    """Every way ``unify_schemas`` can fail is reported as a clear Delta
    error. It raises ArrowTypeError or ArrowInvalid depending on the types
    involved, and a bare KeyError for a few shapes it can't reconcile (e.g.
    duplicate field names) -- none of those should reach the caller raw."""
    import pyarrow as pa
    from deltalake import DeltaTable

    from ray.data._internal.datasource import delta_datasink as dd

    exc = {
        "ArrowTypeError": pa.ArrowTypeError("boom"),
        "ArrowInvalid": pa.ArrowInvalid("boom"),
        "KeyError": KeyError("boom"),
    }[exc_name]

    _write_append([{"id": 1}], temp_delta_path)
    datasink = dd.DeltaDatasink(temp_delta_path)

    def _raise(*args, **kwargs):
        raise exc

    monkeypatch.setattr(dd, "unify_schemas", _raise)
    with pytest.raises(ValueError, match="not compatible with the table's existing"):
        datasink._reconcile_schema_with_existing_table(
            pa.schema([("id", pa.int64())]), DeltaTable(temp_delta_path)
        )


def test_append_compatible_schema_still_succeeds(temp_delta_path):
    """A same-typed but otherwise not-byte-identical schema (e.g. a
    nullable-flag difference) append against an existing table must still
    succeed -- the new validation must not reject legitimately compatible
    writes, only genuinely incompatible ones."""
    import pyarrow as pa

    _write_append([{"id": 1, "tag": "x"}], temp_delta_path)
    non_nullable_schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("tag", pa.string())]
    )
    table = pa.table({"id": [2], "tag": ["y"]}, schema=non_nullable_schema)
    ray.data.from_arrow(table).write_delta(temp_delta_path, mode=SaveMode.APPEND)
    out = sorted(_read_all(temp_delta_path), key=lambda r: r["id"])
    assert out == [{"id": 1, "tag": "x"}, {"id": 2, "tag": "y"}]


def test_overwrite_with_different_schema_still_works(temp_delta_path):
    """OVERWRITE intentionally replaces the schema wholesale -- the new
    APPEND-only schema-compatibility check must not apply to it."""
    _write_append([{"id": 1, "name": "a"}], temp_delta_path)
    ray.data.from_items([{"completely": "different", "cols": 1}]).write_delta(
        temp_delta_path, mode=SaveMode.OVERWRITE
    )
    assert _read_all(temp_delta_path) == [{"completely": "different", "cols": 1}]


# ----------------------------------------------------------------------
# S3 custom endpoint forwarding (review-comment fix).
# ----------------------------------------------------------------------


def test_explicit_filesystem_from_storage_options_s3_endpoint_override():
    """Regression test: a custom S3-compatible endpoint (e.g. MinIO, a test
    double like moto) set via AWS_ENDPOINT_URL must reach the worker's
    S3FileSystem -- previously only access_key/secret_key/session_token/
    region were forwarded, so workers would silently target real AWS
    instead of the custom endpoint the driver's commit path used."""
    from ray.data._internal.datasource.delta_datasink import (
        _explicit_filesystem_from_storage_options,
    )

    fs = _explicit_filesystem_from_storage_options(
        "s3://bucket/table",
        {
            "AWS_ACCESS_KEY_ID": "AKIA-FAKE",
            "AWS_SECRET_ACCESS_KEY": "SECRET-FAKE",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
        },
    )
    # S3FileSystem doesn't expose endpoint_override as a public attribute;
    # __reduce__ carries the constructor kwargs it was built with.
    assert fs is not None
    ctor_kwargs = fs.__reduce__()[1][0]
    assert ctor_kwargs["endpoint_override"] == "http://localhost:9000"


# ----------------------------------------------------------------------
# ``_SUPPORTED_MODES`` set + ``.value`` usage (review-comment fix).
# ----------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_string_mode_still_accepted(temp_delta_path, mode):
    """Regression test: ``SaveMode`` is a ``(str, Enum)``, so a plain string
    like ``"append"`` is value-equal to ``SaveMode.APPEND`` in membership
    checks but isn't an actual enum instance. The datasink must normalize
    it so downstream ``.value`` access doesn't crash on a plain string."""
    ray.data.from_items([{"id": 1}]).write_delta(temp_delta_path, mode=mode)
    assert _read_all(temp_delta_path) == [{"id": 1}]


def test_unsupported_mode_error_message_uses_plain_strings():
    """The validation error must render supported/requested modes as plain
    strings (e.g. "append", not "SaveMode.APPEND"), since ``SaveMode``
    members don't override ``__str__``."""
    with pytest.raises(ValueError, match=r"\['append', 'overwrite'\]"):
        ray.data.from_items([{"id": 1}]).write_delta("/tmp/unused", mode=SaveMode.ERROR)


# ----------------------------------------------------------------------
# Deterministic write filenames survive a retry.
# ----------------------------------------------------------------------


def test_write_parquet_retry_reuses_same_write_uuid(temp_delta_path, monkeypatch):
    """Regression test: a worker's write_uuid must be fixed across retries of
    the same task (baked into ctx.kwargs[WRITE_UUID_KWARG_NAME] once per write
    job, not regenerated per attempt) -- otherwise a retried write leaks
    orphan files under a fresh random name instead of overwriting the failed
    attempt's files."""
    import pyarrow as pa
    import pyarrow.dataset as pds

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME

    real_write_dataset = pds.write_dataset
    captured_templates = []
    call_count = {"n": 0}

    def fake_write_dataset(table, *, basename_template, **kwargs):
        captured_templates.append(basename_template)
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise IOError("Connection reset")
        real_write_dataset(table, basename_template=basename_template, **kwargs)

    monkeypatch.setattr("pyarrow.dataset.write_dataset", fake_write_dataset)

    class _FakeTaskContext:
        task_idx = 0
        kwargs = {WRITE_UUID_KWARG_NAME: "fixed-uuid-1234"}

    datasink = DeltaDatasink(temp_delta_path)
    table = pa.table({"id": [1, 2, 3]})
    # pyrefly: ignore[bad-argument-type]
    add_actions = datasink._write_parquet(table, _FakeTaskContext())

    assert call_count["n"] == 2
    assert captured_templates[0] == captured_templates[1]
    assert "fixed-uuid-1234" in captured_templates[0]
    assert len(add_actions) == 1
    assert "fixed-uuid-1234" in add_actions[0].path


# ----------------------------------------------------------------------
# is_auth_error classification.
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "message,expected",
    [
        ("An error occurred (ExpiredToken) when calling ListObjectsV2", True),
        ("AWS Error AccessDenied during HeadObject operation", True),
        ("AuthenticationFailed: Server failed to authenticate the request", True),
        ("invalid_grant: Bad Request", True),
        ("HTTP 403 Forbidden", True),
        ("Some other unrelated transient network blip", False),
        ("Connection reset by peer", False),
    ],
)
def test_is_auth_error(message, expected):
    from ray.data._internal.cloud_auth import is_auth_error

    assert is_auth_error(Exception(message)) is expected


# ----------------------------------------------------------------------
# Driver-side retry + credential refresh.
# ----------------------------------------------------------------------


# Shared with test_catalog.py, and importable (not defined in a test module) so
# it survives pickling to a Ray worker -- see catalog_test_utils.
_FakeCatalog = FakeCatalog


def test_with_retry_retries_transient_error_without_catalog(temp_delta_path):
    """Without a catalog, an auth error is still retried -- a plain retry is
    itself the refresh mechanism for ambient credentials (see the class
    docstring), so no explicit refresh call is needed for it to succeed."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    datasink = DeltaDatasink(temp_delta_path)
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise Exception("AWS Error ExpiredToken during PutObject")
        return "ok"

    assert datasink._with_retry(flaky, description="test") == "ok"
    assert attempts["n"] == 2


def test_with_retry_refreshes_via_catalog_on_auth_error(temp_delta_path):
    """With a catalog configured, an auth error triggers exactly one
    catalog.resolve() re-call before the next retry attempt, and the
    freshly-resolved storage_options are applied."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import CatalogAccessMode, ReaderFormat, ResolvedSource

    catalog = _FakeCatalog(
        ResolvedSource(storage_options={"AWS_SESSION_TOKEN": "fresh-token"})
    )
    datasink = DeltaDatasink(
        temp_delta_path, catalog=catalog, table_identifier="main.db.tbl"
    )

    attempts = {"n": 0}

    def flaky_commit():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise Exception("AWS Error ExpiredToken during PutObject")
        return "committed"

    assert (
        datasink._with_retry(
            flaky_commit,
            description="test commit",
            # Same call shape the driver commit uses.
            refresh=datasink._refresh_driver_filesystem,
        )
        == "committed"
    )
    assert attempts["n"] == 2
    assert catalog.calls == [
        ("main.db.tbl", ReaderFormat.DELTA, CatalogAccessMode.WRITE)
    ]
    assert datasink._storage_options is not None
    assert datasink._storage_options["AWS_SESSION_TOKEN"] == "fresh-token"


def test_with_retry_refresh_preserves_user_storage_options_override(
    temp_delta_path,
):
    """Regression test: a user-supplied static storage_options value must
    survive a catalog-triggered refresh. The fresh catalog value applies for
    keys the user didn't set (e.g. a rotated session token), but a key the
    user explicitly overrode -- which the catalog also has a default for --
    must keep the user's value, not the catalog's, on every refresh, not just
    the first resolution."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import ResolvedSource

    catalog = _FakeCatalog(
        ResolvedSource(
            storage_options={
                "AWS_SESSION_TOKEN": "fresh-token",
                "AWS_REGION": "catalog-default-region",
            }
        )
    )
    datasink = DeltaDatasink(
        temp_delta_path,
        catalog=catalog,
        table_identifier="main.db.tbl",
        storage_options={"AWS_REGION": "user-override-region"},
        user_storage_options={"AWS_REGION": "user-override-region"},
    )

    attempts = {"n": 0}

    def flaky_commit():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise Exception("AWS Error ExpiredToken during PutObject")
        return "committed"

    datasink._with_retry(
        flaky_commit,
        description="test",
        refresh=datasink._refresh_driver_filesystem,
    )

    assert datasink._storage_options is not None
    assert datasink._storage_options["AWS_SESSION_TOKEN"] == "fresh-token"
    assert datasink._storage_options["AWS_REGION"] == "user-override-region"


def test_catalog_refresh_advances_token_without_user_storage_options(temp_delta_path):
    """Regression test: a refresh must pick up freshly vended credentials when
    the caller passed no ``storage_options`` at all -- the common
    ``write_delta(path, catalog=...)`` call.

    ``Dataset.write_delta`` forwards ``user_storage_options=None`` in that
    case, and the datasink used to fall back to its own (already
    catalog-merged) ``storage_options``. That froze the *first* vended token as
    if the caller had supplied it, so it won every later merge and the refresh
    never actually changed the token that had expired. Goes through
    ``write_delta`` rather than constructing the datasink directly so the
    catalog merge under test is the real one.
    """
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import ResolvedSource

    catalog = _FakeCatalog(
        [
            ResolvedSource(
                path=temp_delta_path, storage_options={"AWS_SESSION_TOKEN": "token-1"}
            ),
            ResolvedSource(
                path=temp_delta_path, storage_options={"AWS_SESSION_TOKEN": "token-2"}
            ),
        ]
    )

    with mock.patch(
        "ray.data.dataset.DeltaDatasink"
    ) as datasink_cls, mock.patch.object(ray.data.Dataset, "write_datasink"):
        ray.data.from_items([{"id": 1}]).write_delta("main.db.tbl", catalog=catalog)
    _, kwargs = datasink_cls.call_args
    assert kwargs["user_storage_options"] is None

    datasink = DeltaDatasink(**kwargs)
    assert datasink._storage_options == {"AWS_SESSION_TOKEN": "token-1"}

    assert datasink._refresh_driver_filesystem() is True
    assert datasink._storage_options == {"AWS_SESSION_TOKEN": "token-2"}


def test_on_write_start_retries_transient_failure(temp_delta_path, monkeypatch):
    """``on_write_start``'s Delta log reads are retried like the commit's own.

    They are the same kind of driver-side call ``on_write_complete`` already
    wraps, so an expired token or a throttled request here used to fail the
    whole write job outright, before any retry or credential refresh could
    happen.
    """
    from deltalake import DeltaTable

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    ray.data.from_items([{"year": 2024, "id": 1}]).write_delta(
        temp_delta_path, partition_by=["year"]
    )

    real_is_deltatable = DeltaTable.is_deltatable
    attempts = {"n": 0}

    def flaky_is_deltatable(table_uri, storage_options=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise Exception("AWS Error ExpiredToken during GetObject")
        return real_is_deltatable(table_uri, storage_options=storage_options)

    monkeypatch.setattr(DeltaTable, "is_deltatable", staticmethod(flaky_is_deltatable))

    datasink = DeltaDatasink(temp_delta_path)
    datasink.on_write_start()

    assert attempts["n"] == 2
    # Retried, so partitioning was still inherited rather than lost.
    assert datasink._partition_by == ["year"]


def test_on_write_start_does_not_retry_partition_mismatch(temp_delta_path, monkeypatch):
    """A partition mismatch is a logical error, so it must be raised straight
    out of ``on_write_start`` rather than retried -- only the Delta log reads
    are inside the retry."""
    from deltalake import DeltaTable

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    ray.data.from_items([{"year": 2024, "id": 1}]).write_delta(
        temp_delta_path, partition_by=["year"]
    )

    real_is_deltatable = DeltaTable.is_deltatable
    calls = {"n": 0}

    def counting_is_deltatable(table_uri, storage_options=None):
        calls["n"] += 1
        return real_is_deltatable(table_uri, storage_options=storage_options)

    monkeypatch.setattr(
        DeltaTable, "is_deltatable", staticmethod(counting_is_deltatable)
    )

    datasink = DeltaDatasink(temp_delta_path, partition_by=["month"])
    with pytest.raises(ValueError, match="can't change an existing table's"):
        datasink.on_write_start()

    assert calls["n"] == 1


def test_with_retry_opt_out_does_not_retry_auth_errors(temp_delta_path):
    """DeltaConfig.credential_refresh_enabled=False disables the extra
    auth-error retry/refresh entirely -- the first failure propagates."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    original = ctx.delta_config.credential_refresh_enabled
    ctx.delta_config.credential_refresh_enabled = False
    try:
        datasink = DeltaDatasink(temp_delta_path)
        attempts = {"n": 0}

        def always_fails():
            attempts["n"] += 1
            raise Exception("AWS Error ExpiredToken during PutObject")

        with pytest.raises(Exception, match="ExpiredToken"):
            datasink._with_retry(always_fails, description="test")
        assert attempts["n"] == 1
    finally:
        ctx.delta_config.credential_refresh_enabled = original


# ----------------------------------------------------------------------
# Worker-side retry + credential refresh (AWS-only catalog scope).
# ----------------------------------------------------------------------


def test_worker_refresh_via_catalog_succeeds_for_aws_shaped_response(temp_delta_path):
    """A catalog response carrying an explicit filesystem (confirmed true for
    AWS-backed Delta writes) lets a worker refresh successfully."""
    import pyarrow.fs as pafs

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import CatalogAccessMode, ReaderFormat, ResolvedSource

    fresh_fs = pafs.S3FileSystem(access_key="a", secret_key="b", region="us-west-2")
    stale_fs = pafs.S3FileSystem(access_key="c", secret_key="d", region="us-west-2")
    catalog = _FakeCatalog(ResolvedSource(filesystem=fresh_fs))
    # ``write_delta`` always hands the datasink the catalog's own filesystem for
    # this shape, because the driver resolved before constructing it. That
    # already-present filesystem is what tells a worker a refresh is safe --
    # see ``_can_refresh_worker_credentials``.
    datasink = DeltaDatasink(
        temp_delta_path,
        catalog=catalog,
        table_identifier="main.db.tbl",
        filesystem=stale_fs,
    )

    assert datasink._refresh_worker_filesystem() is True
    assert datasink._filesystem is fresh_fs
    assert catalog.calls == [
        ("main.db.tbl", ReaderFormat.DELTA, CatalogAccessMode.WRITE)
    ]


def test_worker_refresh_restores_environment_mutated_by_resolve(temp_delta_path):
    """A worker-side ``catalog.resolve()`` must not leave credentials behind in
    the worker process's environment.

    Unity Catalog delivers vended credentials by writing them into
    ``os.environ`` (``_apply_env``) and never restores them. A Ray worker
    process outlives the task and is reused for unrelated ones, so anything
    left there leaks into whatever runs next. The refreshed filesystem carries
    the credentials the write actually uses, so nothing needs them in the
    environment afterwards.
    """
    import pyarrow.fs as pafs

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import Catalog, CatalogAccessMode, ResolvedSource

    key = "RAY_TEST_VENDED_CREDENTIAL"

    class _EnvMutatingCatalog(Catalog):
        """Mimics ``DatabricksUnityCatalog._resolve_storage``: mutates the
        environment as a side effect and returns an explicit filesystem."""

        def __init__(self, filesystem):
            self._filesystem = filesystem
            self.calls = 0

        def resolve(self, table, *, reader, mode=CatalogAccessMode.READ):
            self.calls += 1
            os.environ[key] = "vended-secret"
            return ResolvedSource(filesystem=self._filesystem)

    fresh_fs = pafs.S3FileSystem(access_key="a", secret_key="b", region="us-west-2")
    stale_fs = pafs.S3FileSystem(access_key="c", secret_key="d", region="us-west-2")
    catalog = _EnvMutatingCatalog(fresh_fs)
    datasink = DeltaDatasink(
        temp_delta_path,
        catalog=catalog,
        table_identifier="main.db.tbl",
        filesystem=stale_fs,
    )

    os.environ.pop(key, None)
    try:
        assert datasink._refresh_worker_filesystem() is True
        assert datasink._filesystem is fresh_fs
        assert catalog.calls == 1
        assert key not in os.environ
    finally:
        os.environ.pop(key, None)


def test_worker_refresh_via_catalog_declines_for_azure_shaped_response(
    temp_delta_path,
):
    """A catalog whose credentials a worker can't rebuild from (today's shape
    for Azure Unity Catalog vending, which returns no explicit filesystem)
    declines the refresh -- and must decline *without calling* ``resolve()``.

    ``resolve()`` delivers Azure's SAS token by mutating this process's
    environment and never restoring it, so calling it merely to discover the
    shape is unusable would leak live credentials into a worker Ray then
    reuses. That makes the check a precondition, not a post-hoc test of the
    result. Locks in the documented AWS-only scope for this PR."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import ResolvedSource

    catalog = _FakeCatalog(
        ResolvedSource(storage_options={"AZURE_STORAGE_SAS_TOKEN": "x"})
    )
    datasink = DeltaDatasink(
        temp_delta_path, catalog=catalog, table_identifier="main.db.tbl"
    )

    assert datasink._refresh_worker_filesystem() is False
    assert datasink._filesystem is None
    assert catalog.calls == []


def test_worker_retry_retries_transient_error_without_catalog(temp_delta_path):
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    datasink = DeltaDatasink(temp_delta_path)
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise Exception("AWS Error ExpiredToken during PutObject")
        return "ok"

    assert (
        datasink._with_retry(
            flaky,
            description="test",
            refresh=datasink._refresh_worker_filesystem,
            retry_auth_errors=datasink._worker_retry_can_change_credentials(),
        )
        == "ok"
    )
    assert attempts["n"] == 2


def test_worker_retry_does_not_retry_unrefreshable_auth_error(temp_delta_path):
    """End-to-end: an auth error a worker can't refresh from raises on the
    *first* attempt rather than being retried.

    With an Azure-shaped catalog response there is no fresh credential a worker
    can apply, so retrying can only fail identically -- and each attempt would
    otherwise re-call ``catalog.resolve()``, hitting the catalog's REST API
    repeatedly and (before the precondition check) re-leaking credentials into
    the worker's environment every time. The error propagating immediately is
    also the behavior this feature promises for unsupported shapes: the same as
    if credential refresh didn't exist."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import ResolvedSource

    catalog = _FakeCatalog(
        ResolvedSource(storage_options={"AZURE_STORAGE_SAS_TOKEN": "x"})
    )
    datasink = DeltaDatasink(
        temp_delta_path, catalog=catalog, table_identifier="main.db.tbl"
    )

    attempts = {"n": 0}

    def always_fails():
        attempts["n"] += 1
        raise Exception("AuthenticationFailed: token expired")

    with pytest.raises(Exception, match="AuthenticationFailed"):
        datasink._with_retry(
            always_fails,
            description="test",
            refresh=datasink._refresh_worker_filesystem,
            retry_auth_errors=datasink._worker_retry_can_change_credentials(),
        )
    assert attempts["n"] == 1
    assert catalog.calls == []


def test_worker_retry_does_not_retry_auth_error_with_fixed_filesystem(
    temp_delta_path,
):
    """An auth error must not be retried when the caller pinned a filesystem.

    Retrying a worker auth error is only useful because each attempt re-runs
    ``_resolve_worker_filesystem``, rebuilding the filesystem and so
    re-resolving the ambient cloud SDK credential chain. A caller-supplied
    ``filesystem=`` is returned as-is on every attempt, so its credentials
    can never change and retrying just burns the full backoff schedule before
    failing with the same error."""
    import pyarrow.fs as pafs

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    datasink = DeltaDatasink(
        temp_delta_path,
        filesystem=pafs.S3FileSystem(
            access_key="a", secret_key="b", region="us-west-2"
        ),
    )
    attempts = {"n": 0}

    def always_fails():
        attempts["n"] += 1
        raise Exception("AWS Error ExpiredToken during PutObject")

    with pytest.raises(Exception, match="ExpiredToken"):
        datasink._with_retry(
            always_fails,
            description="test",
            refresh=datasink._refresh_worker_filesystem,
            retry_auth_errors=datasink._worker_retry_can_change_credentials(),
        )
    assert attempts["n"] == 1


def test_worker_retry_still_retries_auth_error_without_fixed_filesystem(
    temp_delta_path,
):
    """The complement of the test above: with nothing pinned, each attempt
    rebuilds the filesystem, so an auth error stays retryable."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink

    datasink = DeltaDatasink(temp_delta_path)
    assert datasink._worker_retry_can_change_credentials() is True

    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise Exception("AWS Error ExpiredToken during PutObject")
        return "ok"

    assert (
        datasink._with_retry(
            flaky,
            description="test",
            refresh=datasink._refresh_worker_filesystem,
            retry_auth_errors=datasink._worker_retry_can_change_credentials(),
        )
        == "ok"
    )
    assert attempts["n"] == 2


# ----------------------------------------------------------------------
# catalog= interaction at the datasink level.
#
# The ``catalog=`` *wiring* on ``Dataset.write_delta`` (resolve, merge,
# reject-conflicting-filesystem) is covered in ``test_catalog.py`` alongside
# the equivalent write_parquet/write_iceberg tests. What's left here is
# datasink-internal behavior that has no analogue in the other writers.
# ----------------------------------------------------------------------


def test_datasink_with_catalog_is_picklable(temp_delta_path):
    """The datasink must survive being pickled to a worker with a live
    catalog reference attached (the mechanism that lets workers re-resolve
    catalog-vended credentials on their own)."""
    import pickle

    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import ResolvedSource

    catalog = _FakeCatalog(ResolvedSource(path=temp_delta_path))
    datasink = DeltaDatasink(
        temp_delta_path, catalog=catalog, table_identifier="main.db.tbl"
    )

    restored = pickle.loads(pickle.dumps(datasink))

    assert restored._catalog is not None
    assert restored._table_identifier == "main.db.tbl"


def test_catalog_without_table_identifier_rejected(temp_delta_path):
    """``catalog`` needs the identifier the catalog resolves, which ``path``
    isn't -- by then it's the physical location resolution produced.

    There used to be a silent fallback (``table or path``), which just deferred
    the failure into the refresh path as ``resolve('<physical URI>')``. Fail at
    construction instead."""
    from ray.data._internal.datasource.delta_datasink import DeltaDatasink
    from ray.data.catalog import ResolvedSource

    catalog = _FakeCatalog(ResolvedSource(path=temp_delta_path))
    with pytest.raises(ValueError, match="table_identifier is required"):
        DeltaDatasink(temp_delta_path, catalog=catalog)

    # Without a catalog it's simply unused, so omitting it is fine.
    assert DeltaDatasink(temp_delta_path)._table_identifier is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
