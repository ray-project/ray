import gzip
import io
import math
from unittest.mock import MagicMock

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
    sample = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )

    schema = datasource.infer_schema(sample)
    assert schema == pa.schema(
        [("id", pa.int64()), ("value", pa.string()), ("path", pa.string())]
    )
    assert datasource.create_scanner(schema).read_schema() is None


def test_partition_filter_cache_is_bounded_and_frozen():
    from ray.data._internal.datasource_v2.listing.listing_utils import (
        _CachedPathPartitionFilter,
    )

    delegate = MagicMock()
    delegate.apply.return_value = False
    cache = _CachedPathPartitionFilter(delegate, max_cached_paths=2)
    for index in range(100):
        assert cache.apply(str(index)) is False
    assert len(cache._decisions) == 2
    cache.freeze()
    calls = delegate.apply.call_count
    assert cache.apply("0") is False
    assert delegate.apply.call_count == calls
    for index in range(100, 200):
        assert cache.apply(str(index)) is False
    assert len(cache._decisions) == 2


@pytest.mark.parametrize("multi_chunk", [False, True])
def test_listing_does_not_parse_csv_headers(monkeypatch, tmp_path, multi_chunk):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,a\n2,b\n3,c\n")

    def fail_on_parse(*_args, **_kwargs):
        raise AssertionError("listing must not parse CSV headers")

    monkeypatch.setattr(CSVFileReader, "_open_csv", fail_on_parse)
    datasource = CSVDatasourceV2(
        [str(path)],
        file_chunker=_SmallLineDelimitedFileChunker() if multi_chunk else None,
    )

    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    )

    assert (len(chunks) > 1) == multi_chunk


def test_infer_schema_with_hive_partition(tmp_path):
    directory = tmp_path / "year=2026"
    directory.mkdir()
    path = directory / "data.csv"
    path.write_text("id\n1\n")

    datasource = CSVDatasourceV2([str(path)], partitioning=Partitioning("hive"))
    sample = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )

    assert datasource.infer_schema(sample).names == ["id", "year"]


def test_infer_schema_unions_hive_partition_fields(tmp_path):
    year_directory = tmp_path / "year=2026"
    year_directory.mkdir()
    first_path = year_directory / "first.csv"
    first_path.write_text("id\n1\n")
    month_directory = year_directory / "month=09"
    month_directory.mkdir()
    second_path = month_directory / "second.csv"
    second_path.write_text("id\n2\n")
    paths = [str(first_path), str(second_path)]
    datasource = CSVDatasourceV2(paths, partitioning=Partitioning("hive"))
    sample = FileManifest.construct_manifest(
        paths=paths,
        sizes=[first_path.stat().st_size, second_path.stat().st_size],
        chunk_metadatas=[None, None],
    )

    schema = datasource.infer_schema(sample)

    assert schema.names == ["id", "year", "month"]
    assert datasource.resolve_partitioning(sample).field_names is None


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
    assert chunks == [(None, path.stat().st_size)]

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

    stream_args = CSVDatasourceV2(
        [str(path)],
        open_stream_args={"buffer_size": 1},
    )
    assert isinstance(stream_args._get_file_indexer().file_chunker, WholeFileChunker)


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

    reader = CSVFileReader()
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    assert pa.concat_tables(list(reader.read(manifest))).to_pylist() == [{"id": 1}]


def test_reader_handles_header_only_file(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n")
    reader = CSVFileReader()
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )

    assert list(reader.read(manifest)) == []


@pytest.mark.parametrize("headerless", [False, True])
@pytest.mark.parametrize("terminator", ["\n", "\r\n"])
def test_reader_preserves_bom_in_noninitial_chunk(tmp_path, headerless, terminator):
    prefix = ("" if headerless else f"value{terminator}") + f"plain{terminator}"
    data = (prefix + f"\ufeffretained{terminator}").encode("utf-8")
    path = tmp_path / "data.csv"
    path.write_bytes(data)
    read_options = csv.ReadOptions(column_names=["value"] if headerless else [])
    reader = CSVFileReader(read_options=read_options)
    boundary = len(prefix.encode("utf-8"))
    whole = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[len(data)], chunk_metadatas=[None]
    )
    chunks = FileManifest.construct_manifest(
        paths=[str(path)] * 2,
        sizes=[boundary, len(data) - boundary],
        chunk_metadatas=[_chunk(0, boundary), _chunk(boundary, len(data))],
    )
    expected = [{"value": "plain"}, {"value": "\ufeffretained"}]
    assert pa.concat_tables(list(reader.read(whole))).to_pylist() == expected
    assert pa.concat_tables(list(reader.read(chunks))).to_pylist() == expected


def test_single_chunk_uses_stream_only_filesystem(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n2\n")
    datasource = CSVDatasourceV2([str(path)])
    schema = pa.schema([("id", pa.int64())])
    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    )

    class StreamOnlyFilesystem:
        def open_input_file(self, _path):
            raise pa.ArrowNotImplementedError("random access unavailable")

        def open_input_stream(self, _path, **kwargs):
            return pa.BufferReader(path.read_bytes())

    manifest = FileManifest.construct_manifest(
        paths=[str(path)],
        sizes=[size for _, size in chunks],
        chunk_metadatas=[metadata for metadata, _ in chunks],
    )
    reader = datasource.create_scanner(
        schema, filesystem=StreamOnlyFilesystem()
    ).create_reader()
    assert pa.concat_tables(list(reader.read(manifest))).to_pylist() == [
        {"id": 1},
        {"id": 2},
    ]


@pytest.mark.parametrize("hadoop", [False, True])
def test_snappy_schema_sampling_is_incremental(hadoop):
    import snappy

    from ray.data.datasource.file_based_datasource import _SnappyInputStream

    payload = b"value\n" + b"abcdefgh\n" * (1 << 20)
    compressed = io.BytesIO()
    compressor_cls = (
        snappy.HadoopStreamCompressor if hadoop else snappy.StreamCompressor
    )
    snappy.stream_compress(
        io.BytesIO(payload), compressed, compressor_cls=compressor_cls
    )

    class CountingFile(io.BytesIO):
        bytes_read = 0

        def read(self, size=-1):
            assert 0 < size <= 64 * 1024
            data = super().read(size)
            self.bytes_read += len(data)
            return data

    file = CountingFile(compressed.getvalue())
    decompressor_cls = (
        snappy.HadoopStreamDecompressor if hadoop else snappy.StreamDecompressor
    )
    with pa.PythonFile(
        _SnappyInputStream(file, decompressor_cls()), mode="r"
    ) as stream:
        reader = csv.open_csv(stream)
        assert reader.schema == pa.schema([("value", pa.string())])
        assert file.bytes_read < len(compressed.getvalue())
        assert reader.read_all().num_rows == 1 << 20
    assert file.closed


def test_reader_ignores_manifest_file_size(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")
    reader = CSVFileReader()
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[None], chunk_metadatas=[None]
    )

    assert pa.concat_tables(list(reader.read(manifest))).to_pylist() == [{"id": 1}]


def test_reader_assigns_one_large_record_to_one_chunk(monkeypatch, tmp_path):
    from ray.data._internal.datasource_v2 import csv_datasource_v2

    path = tmp_path / "data.csv"
    large_value = "x" * 4096
    path.write_text(f"id,value\n1,{large_value}\n2,end\n")
    boundary_scans = 0
    boundary_bytes_read = 0
    boundary_read_calls = 0
    find_record_boundary = csv_datasource_v2._find_record_boundary

    def count_boundary_scans(*args):
        nonlocal boundary_scans
        boundary_scans += 1
        return find_record_boundary(*args)

    monkeypatch.setattr(
        csv_datasource_v2, "_find_record_boundary", count_boundary_scans
    )
    datasource = CSVDatasourceV2(
        [str(path)],
        partitioning=None,
        file_chunker=_SmallLineDelimitedFileChunker(),
    )
    file_size = path.stat().st_size
    sample = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[file_size], chunk_metadatas=[None]
    )
    schema = datasource.infer_schema(sample)

    class CountingRandomAccessFile:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._inner.close()

        def size(self):
            return self._inner.size()

        def read_at(self, size, offset):
            nonlocal boundary_bytes_read, boundary_read_calls
            data = self._inner.read_at(size, offset)
            boundary_bytes_read += len(data)
            boundary_read_calls += 1
            return data

    class CountingFilesystem:
        def open_input_file(self, file_path):
            return CountingRandomAccessFile(pa.OSFile(file_path, "r"))

    datasource._get_file_indexer().file_chunker._filesystem = CountingFilesystem()
    chunks = list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), file_size
        )
    )
    manifest = FileManifest.construct_manifest(
        paths=[str(path)] * len(chunks),
        sizes=[chunk_size for _, chunk_size in chunks],
        chunk_metadatas=[metadata for metadata, _ in chunks],
    )
    reader = datasource.create_scanner(schema).create_reader()

    table = pa.concat_tables(list(reader.read(manifest)))
    assert table.to_pylist() == [
        {"id": 1, "value": large_value},
        {"id": 2, "value": "end"},
    ]
    # Header, huge row, and trailing row are each scanned once instead of
    # emitting every nominal 8-byte chunk that falls inside the huge row.
    assert len(chunks) <= 3
    assert boundary_scans <= 2
    assert boundary_bytes_read <= file_size * 2
    assert boundary_read_calls < 20


def test_reader_reads_aligned_chunks_without_duplicates(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text('id,value\n1,"a,b"\n2,c\n3,d\n4,e\n')
    file_size = path.stat().st_size
    manifest = FileManifest.construct_manifest(
        paths=[str(path)] * 3,
        sizes=[12, 10, file_size - 22],
        chunk_metadatas=[_chunk(0, 12), _chunk(12, 22), _chunk(22, file_size)],
    )
    reader = CSVFileReader()

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
        paths=[str(path), str(path)],
        sizes=[5, file_size - 5],
        chunk_metadatas=[_chunk(0, 5), _chunk(5, file_size)],
    )
    reader = CSVFileReader(
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
    second_chunk = _chunk(11, file_size)
    manifest = FileManifest.construct_manifest(
        paths=[str(path)],
        sizes=[file_size - 11],
        chunk_metadatas=[second_chunk],
    )
    reader = CSVFileReader(arrow_csv_args={"convert_options": None})

    assert pa.concat_tables(list(reader.read(manifest))).to_pylist() == [
        {"id": 2, "value": "b"}
    ]


def test_reader_preserves_columns_not_present_in_sampled_schema(tmp_path):
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
        paths=sampled_paths,
        sizes=sampled_sizes,
        chunk_metadatas=[None] * len(sampled_paths),
    )
    sampled_schema = datasource.infer_schema(sample)
    late_manifest = FileManifest.construct_manifest(
        paths=[str(late_path)],
        sizes=[late_path.stat().st_size],
        chunk_metadatas=[None],
    )

    tables = list(
        datasource.create_scanner(sampled_schema).create_reader().read(late_manifest)
    )
    assert pa.concat_tables(tables).to_pylist() == [{"id": 16, "extra": "preserved"}]


def test_reader_rejects_duplicate_columns_in_late_file(tmp_path):
    path = tmp_path / "duplicate.csv"
    path.write_text("a,a\n1,2\n")
    reader = CSVFileReader()
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )

    with pytest.raises(ValueError, match="duplicate column names.*a"):
        list(reader.read(manifest))


def test_snappy_stream_uses_shared_decompressor(monkeypatch):
    filesystem = MagicMock()
    compressed_stream = object()
    decompressed_stream = object()
    filesystem.open_input_stream.return_value = compressed_stream
    decompress = MagicMock(return_value=decompressed_stream)
    monkeypatch.setattr(
        "ray.data._internal.datasource_v2.readers.csv_file_reader."
        "_file_to_snappy_stream",
        decompress,
    )
    reader = CSVFileReader(
        filesystem=filesystem,
        open_stream_args={"compression": "snappy"},
    )

    assert reader._open_input_stream("data.csv.snappy") is decompressed_stream
    filesystem.open_input_stream.assert_called_once()
    assert filesystem.open_input_stream.call_args.kwargs["compression"] is None
    decompress.assert_called_once_with(compressed_stream, filesystem)


def _chunked_manifest(chunker, path):
    chunks = list(chunker.generate_chunk_metadatas(str(path), path.stat().st_size))
    manifest = FileManifest.construct_manifest(
        paths=[str(path)] * len(chunks),
        sizes=[chunk_size for _, chunk_size in chunks],
        chunk_metadatas=[metadata for metadata, _ in chunks],
    )
    return chunks, manifest


def test_chunked_file_with_different_columns_keeps_its_own_columns(tmp_path):
    sampled_path = tmp_path / "sampled.csv"
    sampled_path.write_text("id,value\n1,a\n")
    different_path = tmp_path / "different.csv"
    different_path.write_text("id,extra\n1,a\n2,b\n3,c\n")
    datasource = CSVDatasourceV2(
        [str(sampled_path), str(different_path)],
        file_chunker=_SmallLineDelimitedFileChunker(),
    )
    sample = FileManifest.construct_manifest(
        paths=[str(sampled_path)],
        sizes=[sampled_path.stat().st_size],
        chunk_metadatas=[None],
    )
    sampled_schema = datasource.infer_schema(sample)

    chunks, manifest = _chunked_manifest(
        datasource._get_file_indexer().file_chunker, different_path
    )
    table = pa.concat_tables(
        list(datasource.create_scanner(sampled_schema).create_reader().read(manifest))
    )

    assert sampled_schema.names == ["id", "value"]
    assert len(chunks) > 1
    assert table.to_pylist() == [
        {"id": 1, "extra": "a"},
        {"id": 2, "extra": "b"},
        {"id": 3, "extra": "c"},
    ]


def test_chunked_file_keeps_its_own_types(tmp_path):
    sampled_path = tmp_path / "sampled.csv"
    sampled_path.write_text("value\n1\n")
    promoted_path = tmp_path / "promoted.csv"
    promoted_path.write_text("value\n1.5\n2.5\n3.5\n")
    datasource = CSVDatasourceV2(
        [str(sampled_path), str(promoted_path)],
        file_chunker=_SmallLineDelimitedFileChunker(),
    )
    sample = FileManifest.construct_manifest(
        paths=[str(sampled_path)],
        sizes=[sampled_path.stat().st_size],
        chunk_metadatas=[None],
    )
    sampled_schema = datasource.infer_schema(sample)

    chunks, manifest = _chunked_manifest(
        datasource._get_file_indexer().file_chunker, promoted_path
    )
    table = pa.concat_tables(
        list(datasource.create_scanner(sampled_schema).create_reader().read(manifest))
    )

    assert sampled_schema == pa.schema([("value", pa.int64())])
    assert len(chunks) > 1
    assert table.schema == pa.schema([("value", pa.float64())])
    assert table.to_pylist() == [{"value": 1.5}, {"value": 2.5}, {"value": 3.5}]


def test_heterogeneous_files_are_all_chunked_with_their_own_types(tmp_path):
    paths = []
    for index in range(16):
        path = tmp_path / f"{index:02d}.csv"
        value = f"{index}.5" if index % 2 else str(index)
        path.write_text("id,value\n" + "".join(f"{row},{value}\n" for row in range(8)))
        paths.append(path)
    datasource = CSVDatasourceV2(
        [str(path) for path in paths], file_chunker=_SmallLineDelimitedFileChunker()
    )
    # Sample only integer-valued files so the planning schema disagrees with
    # the types Arrow infers for every other file.
    sample = FileManifest.construct_manifest(
        paths=[str(path) for path in paths[::2]],
        sizes=[path.stat().st_size for path in paths[::2]],
        chunk_metadatas=[None] * len(paths[::2]),
    )
    sampled_schema = datasource.infer_schema(sample)
    assert sampled_schema.field("value").type == pa.int64()

    chunker = datasource._get_file_indexer().file_chunker
    reader = datasource.create_scanner(sampled_schema).create_reader()
    for index, path in enumerate(paths):
        chunks, manifest = _chunked_manifest(chunker, path)
        assert len(chunks) > 1, path
        table = pa.concat_tables(list(reader.read(manifest)))
        expected_type = pa.float64() if index % 2 else pa.int64()
        assert table.schema.field("value").type == expected_type, path
        assert table.column("id").to_pylist() == list(range(8)), path


def test_chunk_read_leaves_null_inferred_columns_unpinned(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,note\n1,\n2,\n3,x\n")
    file_size = path.stat().st_size
    boundary = len("id,note\n1,\n2,\n")
    # A 12-byte block covers the header and the first row only, so the types
    # inferred from the file prefix mark ``note`` as null.
    reader = CSVFileReader(
        read_options=csv.ReadOptions(use_threads=False, block_size=12)
    )
    manifest = FileManifest.construct_manifest(
        paths=[str(path)] * 2,
        sizes=[boundary, file_size - boundary],
        chunk_metadatas=[_chunk(0, boundary), _chunk(boundary, file_size)],
    )

    tables = list(reader.read(manifest))

    assert tables[0].schema.field("note").type == pa.null()
    assert tables[-1].to_pylist() == [{"id": 3, "note": "x"}]


def test_chunk_reads_use_read_ahead_slabs(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id\n" + "".join(f"{index}\n" for index in range(20_000)))
    file_size = path.stat().st_size
    read_at_sizes = []

    class CountingFile:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._inner.close()

        def size(self):
            return self._inner.size()

        def read_at(self, size, offset):
            read_at_sizes.append(size)
            return self._inner.read_at(size, offset)

    class CountingFilesystem:
        def open_input_file(self, file_path):
            return CountingFile(pa.OSFile(file_path, "r"))

    read_ahead_size = 64 * 1024
    context = DataContext.get_current()
    original_read_ahead_size = context.streaming_read_buffer_size
    context.streaming_read_buffer_size = read_ahead_size
    try:
        reader = CSVFileReader(
            filesystem=CountingFilesystem(),
            read_options=csv.ReadOptions(use_threads=False, block_size=4 * 1024),
        )
        manifest = FileManifest.construct_manifest(
            paths=[str(path)], sizes=[file_size], chunk_metadatas=[_chunk(0, file_size)]
        )
        table = pa.concat_tables(list(reader.read(manifest)))
    finally:
        context.streaming_read_buffer_size = original_read_ahead_size

    assert table.num_rows == 20_000
    # Arrow requests 4 KiB blocks, but the file only sees 64 KiB slab reads.
    assert len(read_at_sizes) == math.ceil(file_size / read_ahead_size)
    assert max(read_at_sizes) <= read_ahead_size


def test_record_aligned_chunker_falls_back_without_random_access(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,a\n2,b\n3,c\n")
    datasource = CSVDatasourceV2(
        [str(path)], file_chunker=_SmallLineDelimitedFileChunker()
    )

    class StreamOnlyFilesystem:
        def open_input_file(self, _path):
            raise pa.ArrowNotImplementedError("random access is unavailable")

    datasource._get_file_indexer().file_chunker._filesystem = StreamOnlyFilesystem()

    assert list(
        datasource._get_file_indexer().file_chunker.generate_chunk_metadatas(
            str(path), path.stat().st_size
        )
    ) == [(None, path.stat().st_size)]


def test_csv_listing_emits_record_aligned_manifests_frequently(tmp_path):
    paths = []
    for index in range(33):
        path = tmp_path / f"{index:02d}.csv"
        path.write_text(f"id,value\n{index},a\n")
        paths.append(str(path))

    datasource = CSVDatasourceV2(
        paths,
        file_chunker=_SmallLineDelimitedFileChunker(),
    )
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
    reader = CSVFileReader()
    manifest = FileManifest.construct_manifest(
        paths=["unused.csv"], sizes=[1], chunk_metadatas=[None]
    )
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
