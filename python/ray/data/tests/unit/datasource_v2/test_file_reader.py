"""Regression tests for limit pushdown in the shared FileReader."""

import gzip
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.dataset as pds
import pytest

from ray.data._internal.datasource_v2.common.file_reader import FileFormat, FileReader
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def test_file_reader_limit_zero_skips_dataset_construction(tmp_path, monkeypatch):
    path = tmp_path / "data.csv"
    path.write_text("id\n42\n", encoding="utf-8")
    dataset = Mock(wraps=pds.dataset)
    monkeypatch.setattr(pds, "dataset", dataset)

    reader = FileReader(format=FileFormat.CSV, limit=0)

    assert list(reader.read(_manifest(path))) == []
    dataset.assert_not_called()


def test_file_reader_limit_one_stops_before_later_fragment_error(tmp_path):
    good = tmp_path / "good.csv.gz"
    broken = tmp_path / "broken.csv.gz"
    good.write_bytes(gzip.compress(b"id\n42\n43\n"))
    broken.write_bytes(b"invalid gzip data")
    reader = FileReader(format=FileFormat.CSV, limit=1)

    result = pa.concat_tables(list(reader.read(_manifest(good, broken))))
    assert result.to_pylist() == [{"id": 42}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
