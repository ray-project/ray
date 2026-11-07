"""Unit tests for :meth:`FileScanner.prune_input_split`."""

from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col, lit


class _NoListingParquetScanner(ParquetScanner):
    def pushed_partition_pruner(self) -> Optional[FilePruner]:
        return None


class _NoListingOrcScanner(OrcScanner):
    def pushed_partition_pruner(self) -> Optional[FilePruner]:
        return None


def test_prune_input_split_matching_no_file():
    """Pruning away every file is an ordinary outcome, not an error."""
    manifest = FileManifest(
        pa.table(
            {
                PATH_COLUMN_NAME: ["/root/year=2020/data.parquet"],
                FILE_SIZE_COLUMN_NAME: [1],
                FILE_CHUNK_METADATA_COLUMN_NAME: [None],
            }
        )
    )
    scanner = ParquetScanner(
        schema=pa.schema([("x", pa.int64())]),
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir="/root"),
        partition_predicate=col("year") == "2029",
    )

    assert len(scanner.prune_input_split(manifest)) == 0


@pytest.mark.parametrize(
    "predicate, accepted, residual",
    [
        (col("path") == "P", None, col("path") == "P"),
        (col("x") == 2, col("x") == 2, None),
        (
            (col("path") == "P") & (col("x") == 2),
            col("x") == 2,
            col("path") == "P",
        ),
        (
            (col("path") == "P") | (col("x") == 2),
            None,
            (col("path") == "P") | (col("x") == 2),
        ),
        (
            ~((col("path") == "P") & (col("x") == 2)),
            None,
            ~((col("path") == "P") & (col("x") == 2)),
        ),
        (
            ((col("path") == "P") | (col("x") == 2)) & (col("x") > 0),
            col("x") > 0,
            (col("path") == "P") | (col("x") == 2),
        ),
        (col("path") == col("x"), None, col("path") == col("x")),
        (lit(True), lit(True), None),
    ],
    ids=["path", "physical", "and", "or", "not", "nested", "cross_column", "literal"],
)
@pytest.mark.parametrize("partitioning", [None, Partitioning(PartitionStyle.HIVE)])
def test_push_filters_retains_synthesized_residual(
    predicate, accepted, residual, partitioning
):
    columns = (PathColumn(),)
    original = col("x") < 10
    scanner = ParquetScanner(
        schema=pa.schema([("x", pa.int64()), ("path", pa.string())]),
        synthesized_columns=columns,
        partitioning=partitioning,
        predicate=original,
    )

    pushed, declined = scanner.push_filters(predicate)

    assert scanner.predicate is original
    assert pushed.synthesized_columns is columns
    if accepted is None:
        assert pushed is scanner
    else:
        assert pushed is not scanner
        assert pushed.predicate is not None
        assert pushed.predicate.structurally_equals(original & accepted)
    if residual is None:
        assert declined is None
    else:
        assert declined is not None and declined.structurally_equals(residual)
    reader = pushed.create_reader()
    assert isinstance(reader, ParquetFileReader)
    assert reader._synthesized_columns is columns
    assert reader._predicate is pushed.predicate


def test_push_filters_classification_survives_projection():
    class SourceFileColumn(PathColumn):
        name = "source_file"

    scanner = ParquetScanner(
        schema=pa.schema([("x", pa.int64()), ("source_file", pa.string())]),
        synthesized_columns=(SourceFileColumn(),),
    ).prune_columns(["x"])
    predicate = col("source_file") == "P"

    pushed, residual = scanner.push_filters(predicate)

    assert pushed is scanner
    assert residual is predicate
    assert scanner.predicate is None


def test_disabled_synthesized_column_keeps_physical_filter():
    scanner = ParquetScanner(schema=pa.schema([("path", pa.string())]))
    predicate = col("path") == "physical"

    pushed, residual = scanner.push_filters(predicate)

    assert pushed.predicate is predicate
    assert residual is None


@pytest.mark.parametrize("columns", [None, ["id"], []])
@pytest.mark.parametrize(
    "scanner_cls",
    [ParquetScanner, _NoListingParquetScanner, OrcScanner, _NoListingOrcScanner],
)
@pytest.mark.parametrize(
    "predicate,expected_indices",
    [
        (col("year") > 2022, [0, 1, 2, 3]),
        (col("year") == 2024, [1, 2]),
        (col("year") > 2025, []),
    ],
)
def test_partition_pruning_uses_full_schema_and_preserves_chunk_rows(
    columns, scanner_cls, predicate, expected_indices
):
    paths = [
        "/root/year=2023/a.parquet",
        "/root/year=2024/b.parquet",
        "/root/year=2024/b.parquet",
        "/root/year=2025/c.parquet",
    ]
    manifest = FileManifest(
        pa.table(
            {
                PATH_COLUMN_NAME: paths,
                FILE_SIZE_COLUMN_NAME: [10, 20, 30, 40],
                FILE_CHUNK_METADATA_COLUMN_NAME: [
                    FileChunk(
                        unit_ids=(i,), num_rows=i + 1, size_bytes=size
                    ).to_metadata()
                    for i, size in enumerate([10, 20, 30, 40])
                ],
            }
        )
    )
    scanner = scanner_cls(
        schema=pa.schema([("id", pa.int64()), ("year", pa.int32())]),
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir="/root", field_names=["year"]
        ),
        partition_predicate=predicate,
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)

    pruner = scanner.pushed_partition_pruner()
    expected_paths = [paths[i] for i in expected_indices]
    if pruner is not None:
        assert [path for path in paths if pruner.should_include(path)] == expected_paths
    else:
        pushdown = derive_list_files_pushdown(scanner.push_limit(1))
        assert pushdown.partition_pruner is None
        assert pushdown.limit is None
    kept = scanner.prune_input_split(manifest)

    assert list(kept.paths) == expected_paths
    original_block = manifest.as_block()
    kept_block = kept.as_block()
    assert isinstance(original_block, pa.Table)
    assert isinstance(kept_block, pa.Table)
    expected_block = original_block.take(pa.array(expected_indices, type=pa.int64()))
    assert kept_block.equals(expected_block)


@pytest.mark.parametrize(
    "scanner_cls",
    [ParquetScanner, _NoListingParquetScanner, OrcScanner, _NoListingOrcScanner],
)
def test_partition_pruning_empty_manifest(scanner_cls):
    block = pa.table(
        {
            PATH_COLUMN_NAME: pa.array([], type=pa.string()),
            FILE_SIZE_COLUMN_NAME: pa.array([], type=pa.int64()),
            FILE_CHUNK_METADATA_COLUMN_NAME: pa.array([], type=pa.null()),
        }
    )
    scanner = scanner_cls(
        schema=pa.schema([("year", pa.int64())]),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
        partition_predicate=col("year") == 2024,
    )

    kept_block = scanner.prune_input_split(FileManifest(block)).as_block()
    assert isinstance(kept_block, pa.Table)
    assert kept_block.equals(block)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
