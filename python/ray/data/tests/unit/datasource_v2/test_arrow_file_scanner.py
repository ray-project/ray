"""Unit tests for :meth:`ArrowFileScanner.prune_manifest`."""
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def test_prune_manifest_matching_no_file():
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

    assert len(scanner.prune_manifest(manifest)) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
