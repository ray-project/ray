"""Unit tests for :meth:`ArrowFileScanner.prune_manifest`."""

from dataclasses import replace
from typing import List

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import col

_BASE_DIR = "/root"
_PATHS = [f"{_BASE_DIR}/year={year}/data.parquet" for year in (2020, 2021, 2022)]


def _manifest(paths: List[str]) -> FileManifest:
    return FileManifest(
        pa.table(
            {
                PATH_COLUMN_NAME: paths,
                FILE_SIZE_COLUMN_NAME: [1] * len(paths),
                FILE_CHUNK_METADATA_COLUMN_NAME: [None] * len(paths),
            }
        )
    )


@pytest.fixture
def scanner() -> ParquetScanner:
    return ParquetScanner(
        schema=pa.schema([("x", pa.int64())]),
        partitioning=Partitioning("hive", base_dir=_BASE_DIR),
    )


@pytest.mark.parametrize(
    "predicate, expected_years",
    [
        pytest.param(None, [2020, 2021, 2022], id="no-predicate"),
        pytest.param(col("year") != "1999", [2020, 2021, 2022], id="keeps-all"),
        pytest.param(col("year") == "2021", [2021], id="keeps-some"),
        pytest.param(col("year") == "2029", [], id="keeps-none"),
    ],
)
def test_prune_manifest(scanner, predicate, expected_years):
    """Pruning to zero files is an ordinary outcome, not an error.

    ``take`` on an untyped empty index list infers a null-typed array and
    raises ``ArrowNotImplementedError``, so a predicate matching no partition
    used to fail the read instead of returning no rows.
    """
    pruned = replace(scanner, partition_predicate=predicate).prune_manifest(
        _manifest(_PATHS)
    )

    assert list(pruned.paths) == [
        f"{_BASE_DIR}/year={year}/data.parquet" for year in expected_years
    ]
    assert len(pruned) == len(expected_years)


def test_prune_manifest_keeps_schema_when_empty(scanner):
    """An emptied manifest keeps its columns so downstream reads still bind."""
    manifest = _manifest(_PATHS)
    pruned = replace(scanner, partition_predicate=col("year") == "2029").prune_manifest(
        manifest
    )

    expected = pa.Table.from_batches([], schema=manifest.as_block().schema)
    assert pruned.as_block().schema == expected.schema


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
