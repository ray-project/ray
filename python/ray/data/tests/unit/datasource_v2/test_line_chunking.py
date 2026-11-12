"""Unit tests for byte-range line chunking of CSV and JSONL on DataSourceV2."""
import collections
import json
import random

import fsspec
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.json as pajson
import pytest
from pyarrow.fs import FSSpecHandler, LocalFileSystem, PyFileSystem

from ray.data._internal.datasource_v2.common import line_chunking
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    LineChunkFileIndexer,
)
from ray.data._internal.datasource_v2.common.line_chunkable_file_reader import (
    LineChunkableFileReader,
    one_block_size,
)
from ray.data._internal.datasource_v2.common.line_chunking import (
    line_chunk_byte_range,
    read_line_aligned,
    split_into_line_chunks,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)


def _read_chunks(data: bytes, chunk_size: int):
    file = pa.BufferReader(data)
    chunks = []
    for chunk in split_into_line_chunks(len(data), chunk_size=chunk_size):
        start, end = line_chunk_byte_range(
            index=chunk.unit_ids[0], chunk_size=chunk_size, file_size=len(data)
        )
        chunks.append(
            bytes(read_line_aligned(file, start=start, end=end, file_size=len(data)))
        )
    return chunks


def test_a_line_belongs_to_the_chunk_holding_its_first_byte():
    assert _read_chunks(b"a\nbb\nc\n", chunk_size=3) == [b"a\nbb\n", b"c\n", b""]


@pytest.mark.parametrize("probe_bytes", [1, 4, 64 * 1024])
def test_chunks_join_back_into_the_file(monkeypatch, probe_bytes):
    # A small probe sends lines that cross a chunk's end through the
    # follow-up reads in ``_read_through_newline``.
    monkeypatch.setattr(line_chunking, "_LINE_PROBE_BYTES", probe_bytes)
    rng = random.Random(0)
    for _ in range(300):
        data = bytes(rng.choice(b"ab\n\r") for _ in range(rng.randint(1, 40)))
        for chunk_size in range(1, len(data) + 2):
            chunks = _read_chunks(data, chunk_size)
            assert b"".join(chunks) == data, (data, chunk_size, chunks)
            # Every non-empty chunk ends a line, except the file's last line.
            non_empty = [c for c in chunks if c]
            assert all(c.endswith(b"\n") for c in non_empty[:-1])


@pytest.mark.parametrize(
    "path,expected_rows",
    [
        ("data.csv", 4),
        ("data.jsonl", 4),
        ("data.csv.gz", 1),
        ("data.CSV.GZ", 1),
        ("data.csv.br", 1),
        ("data.csv.zst", 1),
        ("data.csv.snappy", 1),
    ],
)
def test_indexer_splits_only_uncompressed_files(path, expected_rows):
    indexer = LineChunkFileIndexer(chunk_size=10, ignore_missing_paths=False)
    metadatas = indexer._chunk_metadatas_of_file(FileInfo(path=path, size=35))
    assert len(metadatas) == expected_rows
    if expected_rows > 1:
        chunks = [FileChunk.from_metadata(md) for md in metadatas if md is not None]
        assert [c.unit_ids for c in chunks] == [(0,), (1,), (2,), (3,)]
        assert [c.size_bytes for c in chunks] == [10, 10, 10, 5]


def test_indexer_keeps_a_file_that_fits_in_one_chunk_whole():
    indexer = LineChunkFileIndexer(chunk_size=10, ignore_missing_paths=False)
    assert indexer._chunk_metadatas_of_file(FileInfo(path="a.csv", size=10)) == [None]


class _JsonlReader(LineChunkableFileReader):
    def _make_chunk_fragment(self, lines, *, starts_at_file_start, path):
        read_options = pajson.ReadOptions(block_size=one_block_size(lines, 1 << 20))
        return pds.JsonFileFormat(read_options=read_options).make_fragment(
            pa.py_buffer(lines)
        )


def _write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return str(path)


def _read(paths, *, chunk_size, schema, columns=None, filesystem=None):
    indexer = LineChunkFileIndexer(chunk_size=chunk_size, ignore_missing_paths=False)
    rows_paths, sizes, metadatas = [], [], []
    for path in paths:
        size = LocalFileSystem().get_file_info(path).size
        for metadata in indexer._chunk_metadatas_of_file(FileInfo(path, size)):
            rows_paths.append(path)
            sizes.append(size)
            metadatas.append(metadata)
    manifest = FileManifest.construct_manifest(
        paths=rows_paths, sizes=sizes, chunk_metadatas=metadatas
    )
    reader = _JsonlReader(
        FileFormat.JSON,
        chunk_size=chunk_size,
        filesystem=filesystem or LocalFileSystem(),
        schema=schema,
        columns=columns,
    )
    return manifest, list(reader.read(manifest))


@pytest.mark.parametrize("chunk_size", [7, 20, 64, 10_000])
def test_reader_reads_every_row_exactly_once(tmp_path, chunk_size):
    path = _write_jsonl(tmp_path / "a.jsonl", [{"id": i} for i in range(200)])
    manifest, tables = _read(
        [path], chunk_size=chunk_size, schema=pa.schema([("id", pa.int64())])
    )
    ids = [row["id"] for table in tables for row in table.to_pylist()]
    assert sorted(ids) == list(range(200))
    if chunk_size < 10_000:
        assert len(manifest) > 1


def test_reader_infers_types_per_chunk_and_null_fills_missing_columns(tmp_path):
    rows = [{"x": 1}] * 5 + [{"x": 1.5, "y": "new"}] * 5
    path = _write_jsonl(tmp_path / "a.jsonl", rows)
    schema = pa.schema([("x", pa.int64()), ("z", pa.string())])
    _, tables = _read([path], chunk_size=40, schema=schema)

    x_types = {table.schema.field("x").type for table in tables}
    assert x_types == {pa.int64(), pa.float64()}
    assert all(table.column("z").null_count == len(table) for table in tables)
    merged = pa.concat_tables(tables, promote_options="permissive")
    assert merged.column("x").to_pylist() == [1] * 5 + [1.5] * 5


def test_reader_projects_a_column_one_chunk_lacks(tmp_path):
    path = _write_jsonl(tmp_path / "a.jsonl", [{"a": 1}] * 5 + [{"b": 2}] * 5)
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
    _, tables = _read([path], chunk_size=30, schema=schema, columns=["b"])
    values = [v for table in tables for v in table.column("b").to_pylist()]
    assert values == [None] * 5 + [2] * 5


def test_reader_projects_a_column_one_chunk_lacks_without_a_schema(tmp_path):
    path = _write_jsonl(tmp_path / "a.jsonl", [{"a": 1}] * 5 + [{"b": 2}] * 5)
    _, tables = _read([path], chunk_size=30, schema=None, columns=["b"])
    values = [v for table in tables for v in table.column("b").to_pylist()]
    assert values == [None] * 5 + [2] * 5


# Rows past the first MiB, the default ``block_size`` pyarrow infers types from.
_EARLY_ROWS = [{"i": i, "x": 1, "pad": "y" * 100} for i in range(12_000)]
_LATE_ROWS = [{"i": i, "x": 1.5, "pad": "y" * 100, "k": i} for i in range(12_000)]


@pytest.mark.parametrize("chunk_size", [2 << 20, 8 << 20], ids=["split", "whole"])
def test_reader_reads_a_key_first_seen_after_the_first_mib(tmp_path, chunk_size):
    path = _write_jsonl(tmp_path / "a.jsonl", _EARLY_ROWS + _LATE_ROWS)
    schema = pa.schema([("i", pa.int64()), ("x", pa.int64()), ("pad", pa.string())])
    _, tables = _read([path], chunk_size=chunk_size, schema=schema)
    merged = pa.concat_tables(tables, promote_options="permissive")
    assert merged.column("k").drop_null().to_pylist() == list(range(12_000))


@pytest.mark.parametrize("chunk_size", [2 << 20, 8 << 20], ids=["split", "whole"])
def test_reader_reads_whole_numbers_then_decimals(tmp_path, chunk_size):
    path = _write_jsonl(tmp_path / "a.jsonl", _EARLY_ROWS + _LATE_ROWS)
    schema = pa.schema([("i", pa.int64()), ("x", pa.int64()), ("pad", pa.string())])
    _, tables = _read([path], chunk_size=chunk_size, schema=schema)
    merged = pa.concat_tables(tables, promote_options="permissive")
    assert collections.Counter(merged.column("x").to_pylist()) == {
        1: 12_000,
        1.5: 12_000,
    }


class _CountingHandler(FSSpecHandler):
    def __init__(self):
        super().__init__(fsspec.filesystem("file"))
        self.opened = []

    def open_input_file(self, path):
        self.opened.append(path)
        return super().open_input_file(path)


def test_reader_opens_a_small_file_once(tmp_path):
    paths = [
        _write_jsonl(tmp_path / f"{i}.jsonl", [{"id": i}, {"id": i}]) for i in range(3)
    ]
    handler = _CountingHandler()
    _, tables = _read(
        paths,
        chunk_size=10_000,
        schema=pa.schema([("id", pa.int64())]),
        filesystem=PyFileSystem(handler),
    )
    assert sum(len(table) for table in tables) == 6
    assert sorted(handler.opened) == sorted(paths)


def test_reader_reads_a_file_that_fits_in_one_chunk_whole(tmp_path):
    path = _write_jsonl(tmp_path / "a.jsonl", [{"id": i} for i in range(3)])
    manifest, tables = _read(
        [path], chunk_size=10_000, schema=pa.schema([("id", pa.int64())])
    )
    assert list(manifest.file_chunk_metadatas) == [None]
    assert pa.concat_tables(tables).column("id").to_pylist() == [0, 1, 2]


def test_chunk_units_name_their_byte_offset(tmp_path):
    path = _write_jsonl(tmp_path / "a.jsonl", [{"id": i} for i in range(20)])
    manifest, _ = _read([path], chunk_size=50, schema=pa.schema([("id", pa.int64())]))
    reader = _JsonlReader(FileFormat.JSON, chunk_size=50, filesystem=LocalFileSystem())
    dataset = pds.dataset([path], format="json", filesystem=LocalFileSystem())
    units = reader._get_fragments_to_read(dataset, manifest)
    assert [u.unit.id for u in units] == [f"{path}#chunk{i}" for i in range(len(units))]
    assert [u.unit_start_row for u in units] == [50 * i for i in range(len(units))]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
