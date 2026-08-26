import gzip

import pyarrow as pa
import pytest
from pyarrow import csv

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    LineDelimitedFileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.csv_datasource_v2 import CSVDatasourceV2
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.csv_file_reader import CSVFileReader
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning


def _chunk(start: int, end: int):
    return {
        "chunk_byte_start_idx": start,
        "chunk_byte_end_idx": end,
    }


class _SmallLineDelimitedFileChunker(LineDelimitedFileChunker):
    _CHUNK_BYTE_SIZE = 8


def test_infer_schema_and_create_scanner(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,a\n2,b\n")

    datasource = CSVDatasourceV2([str(path)], include_paths=True)
    sample = FileManifest.construct_manifest([str(path)], [path.stat().st_size], [None])

    schema = datasource.infer_schema(sample)
    assert schema == pa.schema(
        [("id", pa.int64()), ("value", pa.string()), ("path", pa.string())]
    )
    assert datasource.create_scanner(schema).read_schema() == schema


def test_infer_schema_with_hive_partition(tmp_path):
    directory = tmp_path / "year=2026"
    directory.mkdir()
    path = directory / "data.csv"
    path.write_text("id\n1\n")

    datasource = CSVDatasourceV2([str(path)], partitioning=Partitioning("hive"))
    sample = FileManifest.construct_manifest([str(path)], [path.stat().st_size], [None])

    assert datasource.infer_schema(sample).names == ["id", "year"]


def test_default_and_fallback_chunkers(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")

    default = CSVDatasourceV2([str(path)])
    assert default._get_file_indexer().requires_file_io
    chunks = list(
        default._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    )
    assert chunks == [(_chunk(0, path.stat().st_size), path.stat().st_size)]

    multiline = CSVDatasourceV2(
        [str(path)],
        arrow_csv_args={
            "parse_options": csv.ParseOptions(newlines_in_values=True),
        },
    )
    assert isinstance(multiline._get_file_indexer().file_chunker, WholeFileChunker)
    assert not multiline._get_file_indexer().requires_file_io

    projected = CSVDatasourceV2(
        [str(path)],
        arrow_csv_args={
            "convert_options": csv.ConvertOptions(include_columns=["id"]),
        },
    )
    assert isinstance(projected._get_file_indexer().file_chunker, WholeFileChunker)

    skipped = CSVDatasourceV2(
        [str(path)],
        arrow_csv_args={"read_options": csv.ReadOptions(skip_rows=1)},
    )
    assert isinstance(skipped._get_file_indexer().file_chunker, WholeFileChunker)


def test_explicit_none_csv_options_use_pyarrow_defaults(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")

    datasource = CSVDatasourceV2(
        [str(path)],
        arrow_csv_args={
            "read_options": None,
            "parse_options": None,
            "convert_options": None,
        },
    )
    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    )

    assert len(chunks) == 1


def test_compressed_file_is_not_chunked(tmp_path):
    path = tmp_path / "data.csv.gz"
    with gzip.open(path, "wt") as file:
        file.write("id\n1\n")

    datasource = CSVDatasourceV2([str(path)])
    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    )
    assert chunks == [(None, path.stat().st_size)]

    reader = CSVFileReader(schema=pa.schema([("id", pa.int64())]))
    manifest = FileManifest.construct_manifest(
        [str(path)], [path.stat().st_size], [None]
    )
    assert pa.concat_tables(list(reader.read(manifest))).to_pylist() == [{"id": 1}]


def test_reader_handles_header_only_file(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n")
    reader = CSVFileReader(
        schema=pa.schema([("id", pa.int64()), ("value", pa.string())])
    )
    manifest = FileManifest.construct_manifest(
        [str(path)], [path.stat().st_size], [None]
    )

    assert list(reader.read(manifest)) == []


def test_reader_assigns_one_large_record_to_one_chunk(tmp_path):
    path = tmp_path / "data.csv"
    large_value = "x" * 256
    path.write_text(f"id,value\n1,{large_value}\n2,end\n")
    file_size = path.stat().st_size
    manifest = FileManifest.construct_manifest(
        [str(path)] * 3,
        [32, 64, file_size - 96],
        [_chunk(0, 32), _chunk(32, 96), _chunk(96, file_size)],
    )
    reader = CSVFileReader(
        schema=pa.schema([("id", pa.int64()), ("value", pa.string())])
    )

    table = pa.concat_tables(list(reader.read(manifest)))
    assert table.to_pylist() == [
        {"id": 1, "value": large_value},
        {"id": 2, "value": "end"},
    ]


def test_reader_reads_aligned_chunks_without_duplicates(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text('id,value\n1,"a,b"\n2,c\n3,d\n4,e\n')
    file_size = path.stat().st_size
    manifest = FileManifest.construct_manifest(
        [str(path)] * 3,
        [12, 10, file_size - 22],
        [_chunk(0, 12), _chunk(12, 22), _chunk(22, file_size)],
    )
    reader = CSVFileReader(
        schema=pa.schema([("id", pa.int64()), ("value", pa.string())])
    )

    table = pa.concat_tables(list(reader.read(manifest)))
    assert table.to_pylist() == [
        {"id": 1, "value": "a,b"},
        {"id": 2, "value": "c"},
        {"id": 3, "value": "d"},
        {"id": 4, "value": "e"},
    ]


def test_reader_preserves_headerless_read_options_across_chunks(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("1,a\n2,b\n3,c\n")
    file_size = path.stat().st_size
    manifest = FileManifest.construct_manifest(
        [str(path), str(path)],
        [5, file_size - 5],
        [_chunk(0, 5), _chunk(5, file_size)],
    )
    reader = CSVFileReader(
        schema=pa.schema([("id", pa.int64()), ("value", pa.string())]),
        read_options=csv.ReadOptions(column_names=["id", "value"]),
    )

    table = pa.concat_tables(list(reader.read(manifest)))
    assert table.to_pylist() == [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
        {"id": 3, "value": "c"},
    ]


def test_reader_accepts_none_convert_options_for_nonzero_chunk(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,a\n2,b\n")
    file_size = path.stat().st_size
    physical_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    second_chunk = _chunk(11, file_size)
    manifest = FileManifest.construct_manifest(
        [str(path)], [file_size - 11], [second_chunk]
    )
    reader = CSVFileReader(
        schema=physical_schema,
        arrow_csv_args={"convert_options": None},
    )

    assert pa.concat_tables(list(reader.read(manifest))).to_pylist() == [
        {"id": 2, "value": "b"}
    ]


def test_reader_rejects_columns_not_present_in_sampled_schema(tmp_path):
    sampled_paths = []
    sampled_sizes = []
    for index in range(16):
        path = tmp_path / f"{index:02d}.csv"
        path.write_text(f"id\n{index}\n")
        sampled_paths.append(str(path))
        sampled_sizes.append(path.stat().st_size)

    late_path = tmp_path / "16.csv"
    late_path.write_text("id,extra\n16,preserved\n")
    datasource = CSVDatasourceV2(sampled_paths + [str(late_path)])
    sample = FileManifest.construct_manifest(
        sampled_paths, sampled_sizes, [None] * len(sampled_paths)
    )
    sampled_schema = datasource.infer_schema(sample)
    late_manifest = FileManifest.construct_manifest(
        [str(late_path)], [late_path.stat().st_size], [None]
    )

    with pytest.raises(ValueError, match="not present in the sampled schema.*extra"):
        list(
            datasource.create_scanner(sampled_schema)
            .create_reader()
            .read(late_manifest)
        )


def test_schema_validating_chunker_inspects_each_file_once(monkeypatch, tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,a\n2,b\n3,c\n")
    calls = 0
    inspect_schema = CSVFileReader.inspect_schema

    def count_inspections(self, inspected_path):
        nonlocal calls
        calls += 1
        return inspect_schema(self, inspected_path)

    monkeypatch.setattr(CSVFileReader, "inspect_schema", count_inspections)
    datasource = CSVDatasourceV2(
        [str(path)], file_chunker=_SmallLineDelimitedFileChunker()
    )
    sample = FileManifest.construct_manifest([str(path)], [path.stat().st_size], [None])
    datasource.infer_schema(sample)

    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    )

    assert len(chunks) > 1
    assert calls == 1
    assert all(set(chunk_metadata) == set(_chunk(0, 0)) for chunk_metadata, _ in chunks)


def test_schema_validating_chunker_falls_back_for_different_physical_columns(
    tmp_path,
):
    sampled_path = tmp_path / "sampled.csv"
    sampled_path.write_text("id,value\n1,a\n")
    different_path = tmp_path / "different.csv"
    different_path.write_text("id,extra\n1,a\n2,b\n3,c\n")
    datasource = CSVDatasourceV2(
        [str(sampled_path), str(different_path)],
        file_chunker=_SmallLineDelimitedFileChunker(),
    )
    sample = FileManifest.construct_manifest(
        [str(sampled_path)], [sampled_path.stat().st_size], [None]
    )
    datasource.infer_schema(sample)

    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(different_path), different_path.stat().st_size
        )
    )

    assert chunks == [(None, different_path.stat().st_size)]


def test_schema_validating_chunker_does_not_inspect_single_chunk_file(
    monkeypatch, tmp_path
):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")

    def fail_on_inspection(*_args):
        raise AssertionError("single-chunk files should not be inspected while listing")

    monkeypatch.setattr(CSVFileReader, "inspect_schema", fail_on_inspection)
    datasource = CSVDatasourceV2([str(path)])

    assert (
        len(
            list(
                datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
                    str(path), path.stat().st_size
                )
            )
        )
        == 1
    )


def test_csv_listing_emits_header_aware_manifests_frequently(tmp_path):
    paths = []
    for index in range(33):
        path = tmp_path / f"{index:02d}.csv"
        path.write_text(f"id,value\n{index},a\n")
        paths.append(str(path))

    datasource = CSVDatasourceV2(
        paths,
        file_chunker=_SmallLineDelimitedFileChunker(),
    )
    sample_path = paths[0]
    sample = FileManifest.construct_manifest(
        [sample_path], [tmp_path.joinpath("00.csv").stat().st_size], [None]
    )
    datasource.infer_schema(sample)

    manifests = list(
        datasource._get_file_indexer().list_files(
            pa.array([str(tmp_path)]),
            filesystem=datasource.filesystem,
            preserve_order=True,
        )
    )

    assert len(manifests) > 1
    assert max(map(len, manifests)) <= 64
    assert sum(map(len, manifests)) > 64


def test_reader_retry_does_not_duplicate_yielded_batches(monkeypatch):
    reader = CSVFileReader(schema=pa.schema([("id", pa.int64())]))
    manifest = FileManifest.construct_manifest(["unused.csv"], [1], [None])
    attempts = 0

    def flaky_read_path(*_args):
        nonlocal attempts
        attempts += 1
        yield pa.table({"id": [1]})
        if attempts == 1:
            raise OSError("transient CSV read")
        yield pa.table({"id": [2]})

    context = DataContext.get_current()
    original_retried_io_errors = context.retried_io_errors
    context.retried_io_errors = ["transient CSV read"]
    monkeypatch.setattr(reader, "_read_path", flaky_read_path)
    monkeypatch.setattr("ray.data._internal.util.random.random", lambda: 0)
    try:
        tables = list(reader.read(manifest))
    finally:
        context.retried_io_errors = original_retried_io_errors

    assert attempts == 2
    assert pa.concat_tables(tables).to_pylist() == [{"id": 1}, {"id": 2}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
