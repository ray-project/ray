"""Unit tests for the Delta Lake file listing (no Ray cluster)."""

import os
import posixpath
import sys
from typing import List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pytest

import ray.cloudpickle as cloudpickle
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    DeltaFileIndexer,
    DeltaTableSnapshot,
    DeltaWholeFileIndexer,
    make_partitioning,
    resolve_data_file_path,
)
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import HIVE_DEFAULT_PARTITION, PartitionStyle

deltalake = pytest.importorskip("deltalake")


def _write(path: str, table: pa.Table, **kwargs) -> None:
    deltalake.write_deltalake(path, table, **kwargs)


def _indexer(path: str, version: Optional[int] = None, **kwargs) -> DeltaFileIndexer:
    """The indexer ``DeltaDatasourceV2`` builds, reading a local table."""
    table = deltalake.DeltaTable(path, version=version, without_files=True)
    return DeltaFileIndexer(
        snapshot=DeltaTableSnapshot(table_uri=table.table_uri, version=table.version()),
        filesystem=pa_fs.SubTreeFileSystem(path, pa_fs.LocalFileSystem()),
        partition_columns=table.metadata().partition_columns,
        **kwargs,
    )


def _listed_paths(indexer, **kwargs) -> List[str]:
    return [
        info.path for info in indexer.list_file_infos(pa.array(["label"]), **kwargs)
    ]


def _log_paths(path: str, version: Optional[int] = None) -> List[str]:
    table = deltalake.DeltaTable(path, version=version)
    return sorted(
        resolve_data_file_path(p)
        for p in pa.table(table.get_add_actions(flatten=True))["path"].to_pylist()
    )


@pytest.fixture
def partitioned_table(tmp_path) -> str:
    path = str(tmp_path / "partitioned")
    _write(
        path,
        pa.table(
            {
                "id": [1, 2, 3, 4],
                "grp": ["a b", "e=f", None, "a b"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        partition_by=["grp"],
    )
    return path


@pytest.mark.parametrize(
    "logged_path, expected",
    [
        ("part-0.parquet", "part-0.parquet"),
        # The log encodes the on-disk name once more; decode exactly once.
        ("grp=e%253Df/part-0.parquet", "grp=e%3Df/part-0.parquet"),
        ("grp=a%2520b/part-0.parquet", "grp=a%20b/part-0.parquet"),
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


def test_make_partitioning():
    assert make_partitioning([]) is None

    partitioning = make_partitioning(["year", "month"])

    assert partitioning is not None
    assert partitioning.style == PartitionStyle.HIVE
    assert partitioning.field_names == ["year", "month"]
    assert partitioning.null_fallback == HIVE_DEFAULT_PARTITION


def test_lists_only_the_files_the_log_names(tmp_path):
    """An overwrite leaves the old version's files in the table directory."""
    path = str(tmp_path / "table")
    _write(path, pa.table({"x": [1, 2]}))
    _write(path, pa.table({"x": [3]}), mode="overwrite")
    on_disk = [f for f in os.listdir(path) if f.endswith(".parquet")]
    assert len(on_disk) == 2

    indexer = _indexer(path).as_whole_file_indexer()

    assert _listed_paths(indexer) == _log_paths(path)
    assert len(_listed_paths(indexer)) == 1


def test_lists_the_pinned_version(tmp_path):
    path = str(tmp_path / "table")
    _write(path, pa.table({"x": [1, 2]}))
    _write(path, pa.table({"x": [3]}), mode="append")

    indexer = _indexer(path, version=0).as_whole_file_indexer()

    assert _listed_paths(indexer) == _log_paths(path, version=0)


def test_lists_relative_decoded_sorted_paths_with_log_sizes(partitioned_table):
    indexer = _indexer(partitioned_table).as_whole_file_indexer()

    infos = list(indexer.list_file_infos(pa.array(["label"])))

    paths = [info.path for info in infos]
    assert paths == sorted(paths)
    # The on-disk directory names, which hold one level of URL encoding.
    assert {posixpath.dirname(p) for p in paths} == {
        "grp=a%20b",
        "grp=e%3Df",
        f"grp={HIVE_DEFAULT_PARTITION}",
    }
    for info in infos:
        assert info.size == os.path.getsize(os.path.join(partitioned_table, info.path))


def test_applies_pruners(partitioned_table):
    class _DropNullPartition:
        def should_include(self, path: str) -> bool:
            return HIVE_DEFAULT_PARTITION not in path

    paths = _listed_paths(_indexer(partitioned_table), pruners=[_DropNullPartition()])

    assert len(paths) == 2
    assert all(HIVE_DEFAULT_PARTITION not in p for p in paths)


def test_rejects_data_files_outside_partition_directories(
    partitioned_table, monkeypatch
):
    """The reader takes partition values from the path, so a partitioned table
    whose file isn't under partition directories must fail, not read nulls."""
    monkeypatch.setattr(
        DeltaTableSnapshot,
        "add_actions",
        lambda self: pa.table({"path": ["part-0.parquet"], "size_bytes": [10]}),
    )

    with pytest.raises(ValueError, match="Hive-style directories"):
        _listed_paths(_indexer(partitioned_table))


def test_rejects_the_literal_null_partition_string(tmp_path):
    """It shares a directory with null values, so the reader would read it as
    null; only the log tells them apart."""
    path = str(tmp_path / "literal")
    _write(
        path,
        pa.table({"id": [1, 2], "grp": [HIVE_DEFAULT_PARTITION, "a"]}),
        partition_by=["grp"],
    )
    indexer = _indexer(path)

    with pytest.raises(ValueError, match="can't tell the string from null"):
        _listed_paths(indexer)
    with pytest.raises(ValueError, match="can't tell the string from null"):
        _listed_paths(indexer.as_whole_file_indexer())


@pytest.mark.parametrize("excluded_index", [0, 1])
def test_whole_file_listing_honors_excluded_read_units(tmp_path, excluded_index):
    path = str(tmp_path / "table")
    _write(path, pa.table({"x": [1, 2]}))
    _write(path, pa.table({"x": [3]}), mode="append")
    indexer = _indexer(path).as_whole_file_indexer()
    paths = _log_paths(path)

    manifests = list(
        indexer.list_files(
            pa.array(["label"]),
            filesystem=None,
            excluded_read_unit_ids={paths[excluded_index]},
        )
    )

    listed = [p for m in manifests for p in m.paths.tolist()]
    assert listed == [p for i, p in enumerate(paths) if i != excluded_index]


def test_whole_file_indexer_lists_from_the_log(partitioned_table):
    """Not ``FooterFileIndexer``'s, which walks the table directory."""
    indexer = _indexer(partitioned_table)

    whole_file = indexer.as_whole_file_indexer()

    assert isinstance(whole_file, DeltaWholeFileIndexer)
    assert whole_file.as_whole_file_indexer() is whole_file
    assert _listed_paths(whole_file) == _listed_paths(indexer)


def test_listing_applies_the_read_shuffle(tmp_path):
    path = str(tmp_path / "table")
    for i in range(8):
        _write(path, pa.table({"x": [i]}), mode="append")
    footer_indexer = _indexer(path)
    indexer = DeltaWholeFileIndexer(
        snapshot=footer_indexer._snapshot,
        filesystem=footer_indexer._filesystem,
        partition_columns=[],
        shuffle_config=FileShuffleConfig(seed=7),
    )

    shuffled = [
        p
        for m in indexer.list_files(pa.array(["label"]), filesystem=None)
        for p in m.paths.tolist()
    ]

    assert sorted(shuffled) == _log_paths(path)
    assert shuffled != _log_paths(path)


def test_indexer_survives_worker_serialization(partitioned_table):
    """Listing tasks receive the indexer pickled, and reopen the table."""
    indexer = cloudpickle.loads(cloudpickle.dumps(_indexer(partitioned_table)))

    assert _listed_paths(indexer) == _log_paths(partitioned_table)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
