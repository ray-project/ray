"""Unit tests for the Delta Lake file listing (no Ray cluster).

Listing through ``read_delta`` (the log at the pinned version, path decoding,
shuffle, resume) is tested in ``tests/datasource/test_delta_v2.py``. These cover
what ``read_delta`` can't reach.
"""

import datetime
import os
import sys
from types import SimpleNamespace
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pytest

from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    PARTITION_VALUES_COLUMN,
    DeltaFileIndexer,
    DeltaPartitionPruner,
    DeltaTableSnapshot,
    _files_selected_by_partition_filters,
    partition_values_as_strings,
    resolve_data_file_path,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.datasource.partitioning import (
    HIVE_DEFAULT_PARTITION,
    _broadcast_partition_value,
)
from ray.data.expressions import col

deltalake = pytest.importorskip("deltalake")


def _indexer(path: str, version: Optional[int] = None) -> DeltaFileIndexer:
    """The indexer ``DeltaDatasourceV2`` builds, reading a local table."""
    table = deltalake.DeltaTable(path, version=version, without_files=True)
    schema = pa.schema(table.schema().to_arrow())
    return DeltaFileIndexer(
        snapshot=DeltaTableSnapshot(table_uri=table.table_uri, version=table.version()),
        filesystem=pa_fs.SubTreeFileSystem(path, pa_fs.LocalFileSystem()),
        partition_schema=pa.schema(
            [schema.field(name) for name in table.metadata().partition_columns]
        ),
    )


def _listed_paths(indexer, **kwargs) -> List[str]:
    return [
        info.path for info in indexer.list_file_infos(pa.array(["label"]), **kwargs)
    ]


@pytest.fixture
def partitioned_table(tmp_path) -> str:
    """``grp`` holds a null and the string Delta names a null's directory after,
    which share that directory."""
    path = str(tmp_path / "partitioned")
    deltalake.write_deltalake(
        path,
        pa.table({"id": [1, 2, 3], "grp": ["a b", None, HIVE_DEFAULT_PARTITION]}),
        partition_by=["grp"],
    )
    return path


@pytest.mark.parametrize(
    "logged_path, expected",
    [
        ("part-0.parquet", "part-0.parquet"),
        # A colon in a partition value isn't a URI scheme.
        ("ts=10:30/part-0.parquet", "ts=10:30/part-0.parquet"),
    ],
)
def test_resolve_data_file_path(logged_path, expected):
    assert resolve_data_file_path(logged_path) == expected


@pytest.mark.parametrize(
    "logged_path",
    ["/abs/part-0.parquet", "s3://bucket/part-0.parquet", "file:///t/part-0.parquet"],
)
def test_resolve_data_file_path_rejects_absolute_paths(logged_path):
    with pytest.raises(ValueError, match="absolute path"):
        resolve_data_file_path(logged_path)


def test_partition_values_come_from_the_log(partitioned_table):
    """The null and the string share a directory; only the log tells them apart."""
    indexer = _indexer(partitioned_table)

    paths = _listed_paths(indexer)

    values = sorted(
        (indexer._partition_values_by_path[path]["grp"] for path in paths),
        key=str,
    )
    assert values == [None, HIVE_DEFAULT_PARTITION, "a b"]


def test_lists_files_outside_partition_directories(partitioned_table, monkeypatch):
    """The Delta protocol doesn't require partition directories."""
    monkeypatch.setattr(
        DeltaTableSnapshot,
        "add_actions",
        lambda self: pa.table(
            {"path": ["part-0.parquet"], "size_bytes": [10], "partition.grp": ["x"]}
        ),
    )
    indexer = _indexer(partitioned_table)

    assert _listed_paths(indexer) == ["part-0.parquet"]
    assert indexer._partition_values_by_path == {"part-0.parquet": {"grp": "x"}}


@pytest.mark.parametrize(
    "logged",
    [
        pa.array([1, None], pa.int8()),
        pa.array([0.1, None], pa.float64()),
        pa.array([True, False], pa.bool_()),
        pa.array([datetime.date(2024, 1, 2), None], pa.date32()),
        pa.array(
            [datetime.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc)],
            pa.timestamp("us", tz="UTC"),
        ),
    ],
    ids=lambda array: str(array.type),
)
def test_partition_values_as_strings_cast_back_exactly(logged):
    """The reader casts each string back to the column's type."""
    schema = pa.schema([pa.field("p", logged.type)])

    strings = partition_values_as_strings(pa.table({"partition.p": logged}), schema)

    read_back = [
        _broadcast_partition_value("p", values["p"], 1, schema)[0].as_py()
        for values in strings
    ]
    assert read_back == logged.to_pylist()


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (col("grp") == "a b", ["a b"]),
        (col("grp").is_null(), [None]),
        (col("grp") == HIVE_DEFAULT_PARTITION, [HIVE_DEFAULT_PARTITION]),
    ],
    ids=["equal", "is_null", "literal_null_string"],
)
def test_partition_pruner_uses_the_log_values(partitioned_table, predicate, expected):
    indexer = _indexer(partitioned_table)
    pruner = DeltaPartitionPruner(predicate, schema=pa.schema([("grp", pa.string())]))

    paths = _listed_paths(indexer, pruners=[pruner])

    assert [indexer._partition_values_by_path[p]["grp"] for p in paths] == expected


def test_lists_sizes_from_the_log(partitioned_table):
    """Without a ``get_file_info`` per file."""
    infos = list(_indexer(partitioned_table).list_file_infos(pa.array(["label"])))

    assert [info.size for info in infos] == [
        os.path.getsize(os.path.join(partitioned_table, info.path)) for info in infos
    ]


def test_applies_path_pruners(partitioned_table):
    class _DropAll:
        def should_include(self, path: str) -> bool:
            return False

    assert _listed_paths(_indexer(partitioned_table), pruners=[_DropAll()]) == []


def test_manifests_carry_the_partition_values(partitioned_table):
    indexer = _indexer(partitioned_table)
    paths = _listed_paths(indexer)
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * len(paths), chunk_metadatas=[None] * len(paths)
    )

    (with_values,) = indexer._with_partition_values([manifest])

    extra_columns = with_values.extra_columns
    assert with_values.paths.tolist() == paths
    assert extra_columns is not None
    assert extra_columns[PARTITION_VALUES_COLUMN].to_pylist() == [
        indexer._partition_values_by_path[path] for path in paths
    ]


def _log_paths(path: str, version: Optional[int] = None) -> List[str]:
    table = deltalake.DeltaTable(path, version=version)
    return sorted(
        resolve_data_file_path(p)
        for p in pa.table(table.get_add_actions(flatten=True))["path"].to_pylist()
    )


@pytest.mark.parametrize(
    "predicate, expected_files",
    [
        (None, [0, 1, 2]),
        (col("x") >= 20, [2]),
        (col("x") < 5, [0]),
        ((col("x") > 12) & (col("x") < 18), [1]),
        (col("x") > 100, []),
        (col("x") != 5, [0, 1, 2]),
    ],
    ids=str,
)
def test_listing_skips_files_the_log_statistics_rule_out(
    tmp_path, predicate, expected_files
):
    """Three commits, one file each, with x in [0, 9], [10, 19] and [20, 29]."""
    path = str(tmp_path / "table")
    for start in (0, 10, 20):
        mode = "append" if start else "error"
        deltalake.write_deltalake(
            path, pa.table({"x": list(range(start, start + 10))}), mode=mode
        )
    files_by_commit = []
    for version in range(3):
        (new_file,) = set(_log_paths(path, version=version)) - set(files_by_commit)
        files_by_commit.append(new_file)
    indexer = _indexer(path).as_whole_file_indexer()

    manifests = indexer.list_files(
        pa.array(["label"]), filesystem=None, predicate=predicate
    )

    listed = sorted(p for m in manifests for p in m.paths.tolist())
    assert listed == sorted(files_by_commit[i] for i in expected_files)


def test_partition_filters_fail_on_uris_that_dont_match_the_log():
    """A ``deltalake`` URI form the listing doesn't expect fails the read, rather
    than dropping the files it couldn't match."""
    table: Any = SimpleNamespace(file_uris=lambda *args: ["s3://b/t/p=x/a.parquet"])

    with pytest.raises(ValueError, match="Couldn't match the data files"):
        _files_selected_by_partition_filters(
            table, [("p", "=", "x")], ["p%3Dx/a.parquet"]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
