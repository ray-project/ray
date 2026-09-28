"""Partition pruning on a ``FileScanner`` that has no filter pushdown.

Partition values come from file paths, so any file format gets partition
pruning from ``FileScanner`` itself. These tests use a scanner that
implements nothing beyond the two abstract methods and check that a
predicate on a partition column still reaches it.
"""
from dataclasses import dataclass
from typing import Iterator

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.logical_optimizers import (
    SupportsFilterPushdown,
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data._internal.logical.operators import Filter, ListFiles, ReadFiles
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col

SCHEMA = pa.schema([("a", pa.int64()), ("country", pa.string())])
PARTITIONING = Partitioning(
    PartitionStyle.HIVE, base_dir="/root", field_names=["country"]
)


class _NoopReader(Reader[FileManifest]):
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        yield from ()


@dataclass(frozen=True)
class _PartitionOnlyScanner(FileScanner):
    """A file scanner with no pushdown of its own."""

    def read_schema(self) -> pa.Schema:
        return SCHEMA

    def create_reader(self) -> Reader[FileManifest]:
        return _NoopReader()


def _read_files(scanner: FileScanner) -> ReadFiles:
    list_files = ListFiles(
        paths=["/root"],
        file_indexer=NonSamplingFileIndexer(ignore_missing_paths=False),
        filesystem=LocalFileSystem(),
        source_paths=["/root"],
    )
    return ReadFiles(
        datasource_name="test",
        scanner=scanner,
        schema=SCHEMA,
        parallelism=-1,
        input_dependencies=[list_files],
    )


def _manifest(*paths: str) -> FileManifest:
    return FileManifest(
        pa.table(
            {
                PATH_COLUMN_NAME: list(paths),
                FILE_SIZE_COLUMN_NAME: [1] * len(paths),
                FILE_CHUNK_METADATA_COLUMN_NAME: [None] * len(paths),
            }
        )
    )


def test_scanner_without_filter_pushdown_still_takes_partition_predicates():
    scanner = _PartitionOnlyScanner(partitioning=PARTITIONING)
    assert not isinstance(scanner, SupportsFilterPushdown)
    op = _read_files(scanner)

    assert op.supports_predicate_pushdown() is True

    result = op.apply_predicate(col("country") == "US")

    assert isinstance(result, ReadFiles) and result is not op
    assert isinstance(result.scanner, _PartitionOnlyScanner)
    assert result.scanner.partition_predicate is not None
    assert result.scanner.partition_predicate.structurally_equals(
        col("country") == "US"
    )


def test_data_conjuncts_stay_in_a_filter_above_the_read():
    op = _read_files(_PartitionOnlyScanner(partitioning=PARTITIONING))

    result = op.apply_predicate((col("a") > 0) & (col("country") == "US"))

    assert isinstance(result, Filter)
    assert result.predicate_expr is not None
    assert result.predicate_expr.structurally_equals(col("a") > 0)
    new_read = result.input_dependencies[0]
    assert isinstance(new_read, ReadFiles)
    new_scanner = new_read.scanner
    assert isinstance(new_scanner, _PartitionOnlyScanner)
    assert new_scanner.partition_predicate is not None
    assert new_scanner.partition_predicate.structurally_equals(col("country") == "US")


@pytest.mark.parametrize(
    "scanner",
    [
        _PartitionOnlyScanner(partitioning=PARTITIONING),
        _PartitionOnlyScanner(),
    ],
    ids=["data_only_predicate", "no_partitioning_spec"],
)
def test_nothing_to_push_keeps_the_original_filter(scanner):
    op = _read_files(scanner)

    # ``PredicatePushdown`` keeps its ``Filter`` when ``apply_predicate``
    # hands back the same operator.
    assert op.apply_predicate(col("a") > 0) is op


def test_listing_gets_the_same_pruner_as_the_reader():
    scanner = _PartitionOnlyScanner(partitioning=PARTITIONING).prune_partitions(
        col("country") == "US"
    )

    pushdown = derive_list_files_pushdown(scanner)

    assert isinstance(pushdown.partition_pruner, PartitionPredicatePruner)
    assert pushdown.predicate is None


def test_prune_input_split_drops_files_by_path():
    scanner = _PartitionOnlyScanner(partitioning=PARTITIONING).prune_partitions(
        col("country") == "US"
    )
    manifest = _manifest(
        "/root/country=US/a.csv", "/root/country=CA/b.csv", "/root/country=US/c.csv"
    )

    kept = scanner.prune_input_split(manifest)

    assert list(kept.paths) == ["/root/country=US/a.csv", "/root/country=US/c.csv"]
    # Without a spec there is nothing to parse, so the split passes through.
    assert _PartitionOnlyScanner().prune_input_split(manifest) is manifest


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
