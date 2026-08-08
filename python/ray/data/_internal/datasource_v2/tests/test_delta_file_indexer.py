"""Tests for :class:`DeltaFileIndexer`.

The indexer answers "which files does this query need?" from the Delta
transaction log alone -- no directory walk, no Parquet footers. These tests
exercise it directly, without a Ray cluster.
"""

import os
from typing import List, Optional

import pyarrow as pa
import pyarrow.fs
import pytest

from ray.data._internal.datasource_v2.listing.delta_file_indexer import DeltaFileIndexer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data.expressions import Expr, col, lit

deltalake = pytest.importorskip("deltalake")


@pytest.fixture
def partitioned_table(tmp_path) -> str:
    """A table partitioned by ``region``, with per-file ``val`` ranges.

    region=US: val 1, 2      region=EU: val 100, 200
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "partitioned")
    write_deltalake(
        path,
        pa.table(
            {
                "region": ["US", "US", "EU", "EU"],
                "val": [1, 2, 100, 200],
                "name": ["a", "b", "c", "d"],
            }
        ),
        partition_by=["region"],
    )
    return path


@pytest.fixture
def unpartitioned_table(tmp_path) -> str:
    """Two files with disjoint ``val`` ranges and no partitioning.

    This is the shape the issue is about: pruning has to come from the log's
    min/max statistics because there are no partition directories to match.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "unpartitioned")
    write_deltalake(path, pa.table({"val": [1, 2, 3], "name": ["a", "b", "c"]}))
    write_deltalake(
        path, pa.table({"val": [100, 200, 300], "name": ["x", "y", "z"]}), mode="append"
    )
    return path


def _list(
    indexer: DeltaFileIndexer,
    path: str,
    *,
    predicate: Optional[Expr] = None,
    partition_predicate: Optional[Expr] = None,
) -> List[FileManifest]:
    """List with the pushdown state ``DeriveListFilesPushdown`` would supply."""
    return list(
        indexer.list_files(
            pa.array([path]),
            filesystem=pyarrow.fs.LocalFileSystem(),
            predicate=predicate,
            partition_predicate=partition_predicate,
        )
    )


def _paths(indexer: DeltaFileIndexer, path: str, **pushdown) -> List[str]:
    paths: List[str] = []
    for manifest in _list(indexer, path, **pushdown):
        paths.extend(manifest.paths.tolist())
    return paths


def _sizes(indexer: DeltaFileIndexer, path: str, **pushdown) -> List[int]:
    sizes: List[int] = []
    for manifest in _list(indexer, path, **pushdown):
        sizes.extend(int(s) for s in manifest.file_sizes.tolist())
    return sizes


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


def test_lists_every_file_when_unfiltered(partitioned_table):
    paths = _paths(DeltaFileIndexer(), partitioned_table)
    assert len(paths) == 2
    assert all(os.path.exists(p) for p in paths), paths


def test_sizes_come_from_the_log_and_match_disk(partitioned_table):
    indexer = DeltaFileIndexer()
    paths = _paths(indexer, partitioned_table)
    sizes = _sizes(indexer, partitioned_table)
    assert sizes == [os.path.getsize(p) for p in paths]


def test_paths_match_delta_file_uris(partitioned_table):
    from deltalake import DeltaTable

    expected = set(DeltaTable(partitioned_table).file_uris())
    assert set(_paths(DeltaFileIndexer(), partitioned_table)) == expected


def test_special_characters_in_partition_values(tmp_path):
    """Add-action paths are URL-encoded; the on-disk names are not.

    A partition value containing ``=``, ``/`` or a space appears in the log as
    a doubly-encoded path (``grp=e%253Df``) while the directory on disk is
    singly encoded (``grp=e%3Df``). Joining the raw log path onto the table
    URI yields a path that does not exist.
    """
    from deltalake import DeltaTable, write_deltalake

    path = os.path.join(tmp_path, "encoded")
    write_deltalake(
        path,
        pa.table({"grp": ["a b", "c/d", "e=f", "plain"], "v": [1, 2, 3, 4]}),
        partition_by=["grp"],
    )

    paths = _paths(DeltaFileIndexer(), path)
    assert len(paths) == 4
    assert set(paths) == set(DeltaTable(path).file_uris())
    for p in paths:
        assert os.path.exists(p), p


def test_reads_a_pinned_version(tmp_path):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "versioned")
    write_deltalake(path, pa.table({"val": [1]}))
    write_deltalake(path, pa.table({"val": [2]}), mode="append")

    assert len(_paths(DeltaFileIndexer(version=0), path)) == 1
    assert len(_paths(DeltaFileIndexer(), path)) == 2


def test_empty_table_yields_no_manifests(tmp_path):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "empty")
    write_deltalake(path, pa.table({"val": pa.array([], type=pa.int64())}))

    assert _list(DeltaFileIndexer(), path) == []


# ---------------------------------------------------------------------------
# Pruning
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "predicate,expected_regions,description",
    [
        (col("region") == lit("US"), {"US"}, "equality on a partition column"),
        (col("region") != lit("US"), {"EU"}, "inequality on a partition column"),
        (
            (col("region") == lit("US")) | (col("region") == lit("EU")),
            {"US", "EU"},
            "a disjunction covering every partition keeps every file",
        ),
    ],
)
def test_partition_predicate_prunes(
    partitioned_table, predicate: Expr, expected_regions: set, description: str
):
    paths = _paths(DeltaFileIndexer(), partitioned_table, partition_predicate=predicate)
    regions = {p.split("region=")[1].split("/")[0] for p in paths}
    assert regions == expected_regions, description


@pytest.mark.parametrize(
    "predicate,expected_files,description",
    [
        (col("val") > lit(50), 1, "only the high-range file can match"),
        (col("val") < lit(50), 1, "only the low-range file can match"),
        (col("val") > lit(1000), 0, "no file's range can match"),
        (col("val") > lit(0), 2, "every file's range can match"),
        (col("val") != lit(1), 2, "an unprovable predicate prunes nothing"),
    ],
)
def test_statistics_prune_without_partitioning(
    unpartitioned_table, predicate: Expr, expected_files: int, description: str
):
    paths = _paths(DeltaFileIndexer(), unpartitioned_table, predicate=predicate)
    assert len(paths) == expected_files, description


def test_partition_and_statistics_prune_together(partitioned_table):
    paths = _paths(
        DeltaFileIndexer(),
        partitioned_table,
        partition_predicate=col("region") == lit("EU"),
        predicate=col("val") > lit(150),
    )
    assert len(paths) == 1
    assert "region=EU" in paths[0]


def test_honors_file_pruners(partitioned_table):
    class _RejectEverything(FilePruner):
        def should_include(self, path: str) -> bool:
            return False

    manifests = list(
        DeltaFileIndexer().list_files(
            pa.array([partitioned_table]),
            filesystem=pyarrow.fs.LocalFileSystem(),
            pruners=[_RejectEverything()],
        )
    )
    assert manifests == []


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_pushdown_state_does_not_persist_across_calls(partitioned_table):
    """Predicates are per call, so one listing can't bias the next.

    A logical operator may be re-optimized, and the same indexer instance is
    reused across listing tasks. Holding predicates on the instance risked
    pruning against a stale plan; taking them as arguments makes that
    unrepresentable. This pins that.
    """
    indexer = DeltaFileIndexer()

    pruned = _paths(
        indexer, partitioned_table, partition_predicate=col("region") == lit("US")
    )
    unpruned = _paths(indexer, partitioned_table)

    assert len(pruned) == 1
    assert len(unpruned) == 2


@pytest.mark.parametrize("version", [None, 0])
def test_construction_options_survive_pushdown(tmp_path, version: Optional[int]):
    """Per-call predicates must not disturb the snapshot the indexer is pinned to."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "opts")
    write_deltalake(path, pa.table({"val": [1]}))
    write_deltalake(path, pa.table({"val": [2]}), mode="append")

    indexer = DeltaFileIndexer(version=version)
    expected = 1 if version == 0 else 2
    assert len(_paths(indexer, path, predicate=col("val") > lit(0))) == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
