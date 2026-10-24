"""Unit tests for :meth:`FileScanner.prune_input_split`."""
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
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
    FileManifest,
)
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col, lit


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
