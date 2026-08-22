"""Unit tests for :class:`OrcDatasourceV2` and ORC stripe chunking."""

import os

import pyarrow as pa
import pyarrow.orc as orc
import pytest

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    OrcFileChunker,
    OrcFileChunkMetadata,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.orc_file_chunking_utils import (
    _calculate_stripe_range,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.orc_datasource_v2 import OrcDatasourceV2
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    OrcInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.readers.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.scanners.orc_scanner import OrcScanner
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _write_orc(path: str, table: pa.Table, *, rows_per_stripe: int = 20) -> None:
    # ``stripe_size`` is byte-based, so use a deliberately small value and
    # batch in row-sized pieces to create multiple stripes in tiny test files.
    orc.write_table(
        table,
        path,
        stripe_size=64,
        batch_size=rows_per_stripe,
    )


def _manifest_of(paths):
    sizes = [os.path.getsize(p) for p in paths]
    return FileManifest.construct_manifest(paths, sizes, [None] * len(paths))


def _write_multi_stripe_orc(path: str, num_rows: int, rows_per_stripe: int) -> None:
    _write_orc(
        path,
        pa.table(
            {
                "id": list(range(num_rows)),
                "value": [f"value-{i}" for i in range(num_rows)],
            }
        ),
        rows_per_stripe=rows_per_stripe,
    )
    assert orc.ORCFile(path).nstripes > 1


@pytest.mark.parametrize(
    "total_stripes,total_num_chunks,expected_ranges",
    [
        (10, 2, [(0, 5), (5, 10)]),
        (12, 3, [(0, 4), (4, 8), (8, 12)]),
        (10, 3, [(0, 4), (4, 7), (7, 10)]),
        (1, 1, [(0, 1)]),
        (1, 2, [(0, 1), None]),
        (0, 1, [None]),
        (5, 10, [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)] + [None] * 5),
    ],
)
def test_calculate_stripe_range_distribution(
    total_stripes, total_num_chunks, expected_ranges
):
    for chunk_idx in range(total_num_chunks):
        result = _calculate_stripe_range(chunk_idx, total_num_chunks, total_stripes)
        expected = (
            expected_ranges[chunk_idx] if chunk_idx < len(expected_ranges) else None
        )
        assert result == expected

    covered = set()
    for chunk_idx in range(total_num_chunks):
        result = _calculate_stripe_range(chunk_idx, total_num_chunks, total_stripes)
        if result is not None:
            start, end = result
            chunk_stripes = set(range(start, end))
            assert not (covered & chunk_stripes)
            covered.update(chunk_stripes)
    assert covered == set(range(total_stripes))


def test_infer_schema_unpartitioned(tmp_path):
    file_path = tmp_path / "data.orc"
    _write_orc(str(file_path), pa.table({"a": [1, 2], "b": ["x", "y"]}))

    datasource = OrcDatasourceV2([str(file_path)])
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))

    assert schema.names == ["a", "b"]
    assert schema.field("a").type == pa.int64()
    assert schema.field("b").type == pa.string()


def test_infer_schema_hive_partitioned(tmp_path):
    for part in ["a", "b"]:
        d = tmp_path / f"color={part}"
        d.mkdir()
        _write_orc(str(d / "data.orc"), pa.table({"x": [1, 2]}))

    first_file = str(tmp_path / "color=a" / "data.orc")
    datasource = OrcDatasourceV2(
        [str(tmp_path)], partitioning=Partitioning(PartitionStyle.HIVE)
    )
    schema = datasource.infer_schema(_manifest_of([first_file]))

    assert "x" in schema.names
    assert "color" in schema.names
    assert schema.field("color").type == pa.string()


def test_create_scanner_returns_orc_scanner(tmp_path):
    file_path = tmp_path / "data.orc"
    _write_orc(str(file_path), pa.table({"a": [1]}))

    datasource = OrcDatasourceV2([str(file_path)])
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))
    scanner = datasource.create_scanner(schema)

    assert isinstance(scanner, OrcScanner)
    assert scanner.schema == schema


def test_get_size_estimator_returns_orc_estimator(tmp_path):
    datasource = OrcDatasourceV2([str(tmp_path)])
    assert isinstance(datasource.get_size_estimator(), OrcInMemorySizeEstimator)


def test_datasource_defaults_to_orc_file_chunker(tmp_path):
    file_path = tmp_path / "data.orc"
    _write_orc(str(file_path), pa.table({"a": [1, 2]}))

    datasource = OrcDatasourceV2([str(file_path)])
    indexer = datasource._get_file_indexer()
    assert isinstance(indexer.file_chunker, OrcFileChunker)


def test_datasource_preserves_disabled_extension_filter(tmp_path):
    datasource = OrcDatasourceV2([str(tmp_path)], file_extensions=None)

    assert datasource.file_extensions is None


def test_orc_file_reader_reads_chunked_manifest(tmp_path):
    file_path = str(tmp_path / "data.orc")
    expected_rows = 200
    _write_multi_stripe_orc(file_path, expected_rows, rows_per_stripe=20)
    file_size = os.path.getsize(file_path)

    reader_whole = OrcFileReader()
    whole_manifest = FileManifest.construct_manifest([file_path], [file_size], [None])
    whole_rows = (
        pa.concat_tables(list(reader_whole.read(whole_manifest)))
        .column("id")
        .to_pylist()
    )

    chunker = OrcFileChunker(target_chunk_size=256)
    chunks = list(chunker.generate_chunk_metadatas(file_path, file_size))
    assert len(chunks) > 1, "test setup expects OrcFileChunker to chunk"

    chunked_manifest = FileManifest.construct_manifest(
        [file_path] * len(chunks),
        [size for _, size in chunks],
        [metadata for metadata, _ in chunks],
    )

    reader_chunked = OrcFileReader()
    chunked_rows = (
        pa.concat_tables(list(reader_chunked.read(chunked_manifest)))
        .column("id")
        .to_pylist()
    )

    assert sorted(chunked_rows) == sorted(whole_rows) == list(range(expected_rows))


def test_orc_file_reader_respects_batch_size(tmp_path):
    file_path = str(tmp_path / "data.orc")
    _write_orc(
        file_path,
        pa.table({"id": list(range(100))}),
        rows_per_stripe=100,
    )
    assert orc.ORCFile(file_path).nstripes == 1

    tables = list(OrcFileReader(batch_size=7).read(_manifest_of([file_path])))

    assert sum(table.num_rows for table in tables) == 100
    assert max(table.num_rows for table in tables) <= 7


def test_orc_file_reader_retries_read_unit_without_duplicates(tmp_path, monkeypatch):
    from ray.data._internal import util
    from ray.data.context import DataContext

    file_path = str(tmp_path / "data.orc")
    _write_orc(
        file_path,
        pa.table({"id": list(range(5))}),
        rows_per_stripe=5,
    )
    reader = OrcFileReader(batch_size=2)
    original_iter = reader._iter_fragment_tables
    attempts = 0

    def flaky_iter(read_unit, scanner_kwargs):
        nonlocal attempts
        attempts += 1
        for index, table in enumerate(original_iter(read_unit, scanner_kwargs)):
            yield table
            if attempts == 1 and index == 0:
                raise OSError("retryable ORC read")

    monkeypatch.setattr(reader, "_iter_fragment_tables", flaky_iter)
    monkeypatch.setattr(DataContext.get_current(), "retried_io_errors", ["retryable"])
    monkeypatch.setattr(util.time, "sleep", lambda _: None)

    tables = list(reader.read(_manifest_of([file_path])))

    assert attempts == 2
    assert pa.concat_tables(tables).column("id").to_pylist() == list(range(5))


def test_orc_file_reader_preserves_chunk_order(tmp_path, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    file_path = str(tmp_path / "data.orc")
    _write_multi_stripe_orc(file_path, num_rows=100, rows_per_stripe=10)
    file_size = os.path.getsize(file_path)
    chunks = list(
        OrcFileChunker(target_chunk_size=256).generate_chunk_metadatas(
            file_path, file_size
        )
    )
    assert len(chunks) > 1

    expected_chunks = []
    for metadata, chunk_size in chunks:
        manifest = FileManifest.construct_manifest(
            [file_path], [chunk_size], [metadata]
        )
        tables = list(OrcFileReader().read(manifest))
        expected_chunks.append(
            pa.concat_tables(tables).column("id").to_pylist() if tables else []
        )

    reversed_chunks = list(reversed(chunks))
    reversed_manifest = FileManifest.construct_manifest(
        [file_path] * len(reversed_chunks),
        [chunk_size for _, chunk_size in reversed_chunks],
        [metadata for metadata, _ in reversed_chunks],
    )
    actual = (
        pa.concat_tables(list(OrcFileReader().read(reversed_manifest)))
        .column("id")
        .to_pylist()
    )

    assert actual == [row for chunk in reversed(expected_chunks) for row in chunk]


def test_orc_file_reader_handles_out_of_range_chunks(tmp_path):
    file_path = str(tmp_path / "tiny.orc")
    _write_multi_stripe_orc(file_path, num_rows=5, rows_per_stripe=1)
    file_size = os.path.getsize(file_path)

    out_of_range = create_chunk_metadata(
        OrcFileChunkMetadata,
        chunk_idx=10,
        total_num_chunks=11,
    )
    manifest = FileManifest.construct_manifest([file_path], [file_size], [out_of_range])

    tables = list(OrcFileReader().read(manifest))
    assert sum(table.num_rows for table in tables) == 0


def test_orc_file_reader_filter_projection_drops_filter_columns(tmp_path):
    file_path = str(tmp_path / "data.orc")
    _write_orc(file_path, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    table = pa.concat_tables(
        list(
            OrcFileReader(columns=["b"], predicate=col("a") > 1).read(
                _manifest_of([file_path])
            )
        )
    )

    assert table.column_names == ["b"]
    assert table.to_pylist() == [{"b": "y"}, {"b": "z"}]


def test_orc_file_reader_null_fills_missing_projected_column(tmp_path):
    file_path = str(tmp_path / "data.orc")
    _write_orc(file_path, pa.table({"a": [1, 2]}))
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

    table = pa.concat_tables(
        list(
            OrcFileReader(columns=["a", "b"], schema=schema).read(
                _manifest_of([file_path])
            )
        )
    )

    assert table.schema == schema
    assert table.to_pylist() == [{"a": 1, "b": None}, {"a": 2, "b": None}]


def test_orc_file_reader_null_fills_missing_filter_column(tmp_path):
    without_b_path = str(tmp_path / "without_b.orc")
    with_b_path = str(tmp_path / "with_b.orc")
    _write_orc(without_b_path, pa.table({"a": [1]}))
    _write_orc(with_b_path, pa.table({"a": [2], "b": [20]}))
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])

    tables = list(
        OrcFileReader(columns=["a"], predicate=col("b") > 15, schema=schema).read(
            _manifest_of([without_b_path, with_b_path])
        )
    )

    assert pa.concat_tables(tables).to_pylist() == [{"a": 2}]


def test_orc_file_reader_empty_projection_uses_stub_column(tmp_path):
    file_path = str(tmp_path / "data.orc")
    _write_orc(file_path, pa.table({"a": [1, 2, 3]}))

    table = pa.concat_tables(
        list(OrcFileReader(columns=[]).read(_manifest_of([file_path])))
    )

    assert table.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
    assert table.num_rows == 3


def test_orc_file_reader_orders_synthetic_columns_by_projection(tmp_path):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    file_path = str(partition_dir / "data.orc")
    _write_orc(file_path, pa.table({"data": [1, 2]}))

    table = pa.concat_tables(
        list(
            OrcFileReader(
                columns=["year", "data"],
                partitioning=Partitioning(PartitionStyle.HIVE),
                schema=pa.schema([("data", pa.int64()), ("year", pa.string())]),
            ).read(_manifest_of([file_path]))
        )
    )

    assert table.column_names == ["year", "data"]
    assert table.to_pylist() == [
        {"year": "2024", "data": 1},
        {"year": "2024", "data": 2},
    ]


def test_orc_file_reader_overwrites_physical_path_with_source_path(tmp_path):
    file_path = str(tmp_path / "data.orc")
    _write_orc(file_path, pa.table({"path": ["not-a-source-path"]}))

    table = pa.concat_tables(
        list(
            OrcFileReader(columns=["path"], include_paths=True).read(
                _manifest_of([file_path])
            )
        )
    )

    assert table.column_names == ["path"]
    assert table.column("path").to_pylist() == [file_path]


def test_orc_file_reader_normalizes_to_unified_schema(tmp_path):
    first_path = str(tmp_path / "first.orc")
    second_path = str(tmp_path / "second.orc")
    _write_orc(first_path, pa.table({"a": pa.array([1], type=pa.int32())}))
    _write_orc(
        second_path,
        pa.table({"a": pa.array([2], type=pa.int64()), "b": ["x"]}),
    )
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

    tables = list(
        OrcFileReader(schema=schema).read(_manifest_of([first_path, second_path]))
    )

    assert [table.schema for table in tables] == [schema, schema]
    assert pa.concat_tables(tables).to_pylist() == [
        {"a": 1, "b": None},
        {"a": 2, "b": "x"},
    ]
