"""Regression tests for limit pushdown in the shared FileReader."""

import gzip
from pathlib import Path

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.common.file_reader import FileFormat, FileReader
from ray.data._internal.datasource_v2.common.line_chunking import (
    split_into_line_chunks,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def test_file_reader_limit_zero_reads_nothing(tmp_path):
    broken = tmp_path / "broken.csv.gz"
    broken.write_bytes(b"invalid gzip data")
    reader = FileReader(format=FileFormat.CSV, limit=0)

    assert list(reader.read(_manifest(broken))) == []


def test_file_reader_limit_one_stops_before_later_fragment_error(tmp_path):
    good = tmp_path / "good.csv.gz"
    broken = tmp_path / "broken.csv.gz"
    good.write_bytes(gzip.compress(b"id\n42\n43\n"))
    broken.write_bytes(b"invalid gzip data")
    reader = FileReader(format=FileFormat.CSV, limit=1)

    result = pa.concat_tables(list(reader.read(_manifest(good, broken))))
    assert result.to_pylist() == [{"id": 42}]


def test_file_reader_rejects_a_row_with_chunk_metadata(tmp_path):
    path = tmp_path / "a.csv"
    path.write_text("id\n1\n2\n")
    chunk = next(iter(split_into_line_chunks(path.stat().st_size, chunk_size=4)))
    manifest = FileManifest.construct_manifest(
        paths=[str(path)],
        sizes=[path.stat().st_size],
        chunk_metadatas=[chunk.to_metadata()],
    )
    reader = FileReader(format=FileFormat.CSV)

    with pytest.raises(NotImplementedError, match="chunk metadata"):
        list(reader.read(manifest))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
