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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
