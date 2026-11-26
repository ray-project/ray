"""Unit tests for the shared byte-stream reader, through a minimal format."""

from pathlib import Path
from typing import Any, Iterator, List, Optional
from unittest.mock import Mock

import pyarrow as pa
import pytest
from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.common import (
    file_stream_reader as file_stream_reader_mod,
)
from ray.data._internal.datasource_v2.common.file_stream_datasource_v2 import (
    FileStreamDatasourceV2,
)
from ray.data._internal.datasource_v2.common.file_stream_reader import (
    FileStreamReader,
)
from ray.data._internal.datasource_v2.common.file_stream_scanner import (
    FileStreamScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle


class LineReader(FileStreamReader):
    """Yields each line of a file as its own one-row table, plus an empty one."""

    def _read_stream(self, stream: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        yield pa.table({"line": pa.array([], type=pa.string())})
        for line in stream.readall().decode().splitlines():
            yield pa.table({"line": [line]})


class ThreadedLineReader(LineReader):
    num_threads_per_task = 2


class LineScanner(FileStreamScanner):
    def create_reader(self) -> LineReader:
        return LineReader(**self._reader_kwargs())


class LineDatasource(FileStreamDatasourceV2):
    """Leaves the schema to the first read block, as a format whose types are
    only known after decoding would."""

    def __init__(self, paths: List[str], **kwargs: Any):
        super().__init__(paths, name="Lines", **kwargs)

    def _infer_file_schema(self, sample: Optional[FileManifest]) -> Optional[pa.Schema]:
        return None

    def create_scanner(
        self,
        schema: Optional[pa.Schema],
        filesystem: Optional[FileSystem] = None,
        **options: Any,
    ) -> LineScanner:
        return LineScanner(**self._scanner_kwargs(schema, filesystem, options))


class OffsetColumn(SynthesizedColumn):
    """Records where each table sits in its file."""

    name = "offset"
    type = pa.int64()

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        return pa.array(
            [position.rows_before + i for i in range(num_rows)], type=pa.int64()
        )


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def _rows(reader: FileStreamReader, *paths: Path) -> List[dict]:
    tables = list(reader.read(_manifest(*paths)))
    assert all(table.num_rows > 0 for table in tables)
    return pa.concat_tables(tables).to_pylist() if tables else []


def test_reader_tracks_rows_before_across_the_tables_of_one_file(tmp_path):
    first = _write(tmp_path / "a.txt", "a\nb\n")
    second = _write(tmp_path / "b.txt", "c\n")

    rows = _rows(LineReader(synthesized_columns=(OffsetColumn(),)), first, second)

    assert rows == [
        {"line": "a", "offset": 0},
        {"line": "b", "offset": 1},
        {"line": "c", "offset": 0},
    ]


def test_reader_limit_slices_tables_and_stops_opening_files(tmp_path):
    first = _write(tmp_path / "a.txt", "a\nb\nc\n")
    missing = tmp_path / "missing.txt"
    manifest = FileManifest.construct_manifest(
        paths=[str(first), str(missing)], sizes=[6, 1], chunk_metadatas=[None, None]
    )

    tables = list(LineReader(limit=2).read(manifest))

    assert pa.concat_tables(tables).column("line").to_pylist() == ["a", "b"]


def test_reader_retries_by_reopening_the_file(tmp_path, monkeypatch):
    path = _write(tmp_path / "a.txt", "a\nb\n")
    monkeypatch.setattr(
        DataContext.get_current(), "retried_io_errors", ["flaky storage"]
    )
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)
    opened = []

    class FlakyReader(LineReader):
        def _read_stream(self, stream, path):
            opened.append(path)
            yield from super()._read_stream(stream, path)
            if len(opened) == 1:
                raise OSError("flaky storage")

    rows = _rows(FlakyReader(), path)

    assert [row["line"] for row in rows] == ["a", "b"]
    assert len(opened) == 2


def test_reader_rejects_chunked_manifests(tmp_path):
    path = _write(tmp_path / "a.txt", "a\n")
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[2], chunk_metadatas=[{"size_bytes": 2}]
    )

    with pytest.raises(ValueError, match="reads whole files"):
        list(LineReader().read(manifest))


def test_reader_bad_file_fails_the_read_after_earlier_files(tmp_path):
    good = _write(tmp_path / "a.txt", "a\n")
    bad = _write(tmp_path / "b.txt", "b\n")

    class CorruptReader(LineReader):
        def _read_stream(self, stream, path):
            if path == str(bad):
                raise ValueError(f"corrupt file {path}")
            yield from super()._read_stream(stream, path)

    tables = CorruptReader().read(_manifest(good, bad))

    assert next(tables).column("line").to_pylist() == ["a"]
    with pytest.raises(ValueError, match="corrupt file"):
        next(tables)


def test_reader_threads_keep_manifest_order_and_limit(tmp_path):
    files = [_write(tmp_path / f"{i}.txt", f"{i}\n") for i in range(5)]

    rows = _rows(ThreadedLineReader(), *files)
    limited = list(ThreadedLineReader(limit=2).read(_manifest(*files)))

    assert [row["line"] for row in rows] == ["0", "1", "2", "3", "4"]
    assert pa.concat_tables(limited).column("line").to_pylist() == ["0", "1"]


def test_reader_reads_sequentially_under_preserve_order(tmp_path, monkeypatch):
    files = [_write(tmp_path / f"{i}.txt", f"{i}\n") for i in range(3)]
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    monkeypatch.setattr(
        file_stream_reader_mod,
        "make_async_gen",
        Mock(side_effect=AssertionError("threads under preserve_order")),
    )

    rows = _rows(ThreadedLineReader(), *files)

    assert [row["line"] for row in rows] == ["0", "1", "2"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))


def test_datasource_may_leave_the_schema_to_the_first_block(tmp_path):
    path = _write(tmp_path / "year=2024" / "a.txt", "a\n")
    datasource = LineDatasource(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        include_paths=True,
    )

    assert datasource.infer_schema(_manifest(path)) is None
    scanner = datasource.create_scanner(None).prune_columns(["line", "year"])
    assert scanner.read_schema() is None

    rows = _rows(scanner.create_reader(), path)

    assert rows == [{"line": "a", "year": "2024"}]
