"""Unit tests for the binary DataSourceV2: reader, scanner and datasource."""

import gzip
import io
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest
import snappy
from pyarrow.fs import LocalFileSystem

import ray.data.read_api as read_api
from ray._private.utils import INT32_MAX
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.binary.binary_datasource_v2 import (
    BinaryDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.binary.binary_reader import (
    BINARY_SCHEMA,
    BYTES_COLUMN_NAME,
    BinaryReader,
)
from ray.data._internal.datasource_v2.formats.binary.binary_scanner import (
    BinaryScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _write(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def _read(reader: BinaryReader, *paths: Path) -> pa.Table:
    tables = list(reader.read(_manifest(*paths)))
    assert all(table.num_rows == 1 for table in tables)
    return pa.concat_tables(tables) if tables else BINARY_SCHEMA.empty_table()


def _snappy_stream_compress(data: bytes) -> bytes:
    out = io.BytesIO()
    snappy.stream_compress(io.BytesIO(data), out)
    return out.getvalue()


# -- reader --------------------------------------------------------------------


def test_reader_yields_one_row_per_file_in_manifest_order(tmp_path):
    first = _write(tmp_path / "a.bin", b"\x00\x01")
    second = _write(tmp_path / "b.bin", b"hello")
    empty = _write(tmp_path / "empty.bin", b"")

    table = _read(BinaryReader(), first, second, empty)

    assert table.schema == BINARY_SCHEMA
    assert table.column(BYTES_COLUMN_NAME).to_pylist() == [b"\x00\x01", b"hello", b""]


def test_reader_limit_does_not_open_later_files(tmp_path):
    first = _write(tmp_path / "a.bin", b"a")
    missing = tmp_path / "missing.bin"
    manifest = FileManifest.construct_manifest(
        paths=[str(first), str(missing)], sizes=[1, 1], chunk_metadatas=[None, None]
    )

    tables = list(BinaryReader(limit=1).read(manifest))

    assert pa.concat_tables(tables).column(BYTES_COLUMN_NAME).to_pylist() == [b"a"]


def test_reader_keeps_the_row_when_bytes_is_projected_away(tmp_path):
    path = _write(tmp_path / "a.bin", b"a")
    reader = BinaryScanner(schema=BINARY_SCHEMA).prune_columns([]).create_reader()

    tables = list(reader.read(_manifest(path)))

    assert [table.column_names for table in tables] == [
        [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
    ]
    assert tables[0].num_rows == 1


@pytest.mark.parametrize(
    "columns",
    [None, ["path", "year", BYTES_COLUMN_NAME], ["year"]],
    ids=["no_projection", "reordered", "partition_only"],
)
def test_reader_synthesizes_partition_and_path_columns(tmp_path, columns):
    path = _write(tmp_path / "year=2024" / "a.bin", b"a")
    schema = pa.schema(
        [(BYTES_COLUMN_NAME, pa.binary()), ("year", pa.int64()), ("path", pa.string())]
    )
    reader = BinaryReader(
        columns=columns,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
        ),
        synthesized_columns=(PathColumn(),),
        schema=schema,
    )

    rows = _read(reader, path).to_pylist()

    expected = {BYTES_COLUMN_NAME: b"a", "year": 2024, "path": str(path)}
    wanted = columns or [BYTES_COLUMN_NAME, "year", "path"]
    assert rows == [{name: expected[name] for name in wanted}]


def test_reader_null_fills_partition_columns_of_root_files(tmp_path):
    root = _write(tmp_path / "root.bin", b"r")
    nested = _write(tmp_path / "year=2024" / "a.bin", b"n")
    schema = pa.schema([(BYTES_COLUMN_NAME, pa.binary()), ("year", pa.string())])
    reader = BinaryReader(
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        schema=schema,
    )

    tables = list(reader.read(_manifest(root, nested)))

    assert [table.schema for table in tables] == [schema, schema]
    assert pa.concat_tables(tables).to_pylist() == [
        {BYTES_COLUMN_NAME: b"r", "year": None},
        {BYTES_COLUMN_NAME: b"n", "year": "2024"},
    ]


def test_reader_rejects_partition_field_named_bytes(tmp_path):
    path = _write(tmp_path / "bytes=a" / "a.bin", b"a")
    reader = BinaryReader(
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        schema=BINARY_SCHEMA,
    )

    with pytest.raises(ValueError, match="Partition field 'bytes'"):
        list(reader.read(_manifest(path)))


def test_reader_applies_partition_predicate_before_opening(tmp_path):
    root = _write(tmp_path / "root.bin", b"r")
    nested = _write(tmp_path / "year=2024" / "a.bin", b"n")
    schema = pa.schema([(BYTES_COLUMN_NAME, pa.binary()), ("year", pa.string())])
    opened = []

    class CountingReader(BinaryReader):
        def _open_stream(self, path):
            opened.append(path)
            return super()._open_stream(path)

    reader = CountingReader(
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        partition_predicate=col("year").is_null(),
        schema=schema,
    )

    assert _read(reader, root, nested).column(BYTES_COLUMN_NAME).to_pylist() == [b"r"]
    assert opened == [str(root)]


@pytest.mark.parametrize(
    "suffix, compress, open_stream_args",
    [
        pytest.param(".bin.gz", gzip.compress, None, id="gzip_by_extension"),
        pytest.param(".bin.snappy", _snappy_stream_compress, None, id="snappy_by_ext"),
        pytest.param(
            ".bin",
            _snappy_stream_compress,
            {"compression": "snappy"},
            id="snappy_by_open_stream_args",
        ),
        pytest.param(".bin.gz", gzip.compress, {"buffer_size": 1}, id="buffer_size"),
    ],
)
def test_reader_decompresses_by_extension_or_open_stream_args(
    tmp_path, suffix, compress, open_stream_args
):
    path = _write(tmp_path / f"data{suffix}", compress(b"hello, world"))

    table = _read(BinaryReader(open_stream_args=open_stream_args), path)

    assert table.column(BYTES_COLUMN_NAME).to_pylist() == [b"hello, world"]


def test_reader_rejects_a_file_larger_than_a_binary_cell():
    class HugeBytes:
        def __len__(self):
            return INT32_MAX + 1

    class HugeStream:
        def read_buffer(self):
            return HugeBytes()

    with pytest.raises(ValueError, match="'big.bin' is 2147483648 bytes"):
        list(BinaryReader()._read_stream(HugeStream(), "big.bin"))


def test_reader_counts_one_row_per_file_without_opening_any(tmp_path):
    manifest = FileManifest.construct_manifest(
        paths=[str(tmp_path / "missing1.bin"), str(tmp_path / "missing2.bin")],
        sizes=[1, None],
        chunk_metadatas=[None, None],
    )

    metadata = list(BinaryReader().read_metadata(manifest))

    assert [m.num_rows for m in metadata] == [1, 1]


# -- scanner -------------------------------------------------------------------


def test_scanner_counts_from_metadata_unless_a_pushdown_drops_rows():
    schema = pa.schema([(BYTES_COLUMN_NAME, pa.binary()), ("year", pa.int64())])
    scanner = BinaryScanner(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir="/data", field_names=["year"]
        ),
    )

    assert scanner.metadata_row_count_is_exact()
    assert scanner.prune_columns([]).metadata_row_count_is_exact()
    assert not scanner.push_limit(3).metadata_row_count_is_exact()
    assert not scanner.prune_partitions(
        col("year") == 2024
    ).metadata_row_count_is_exact()


def test_scanner_options_and_pushdowns_reach_the_reader(tmp_path):
    kept = _write(
        tmp_path / "year=2024" / "a.bin.snappy", _snappy_stream_compress(b"a")
    )
    dropped = _write(tmp_path / "year=2023" / "b.bin", b"b")
    second = _write(tmp_path / "year=2024" / "c.bin", b"c")
    schema = pa.schema(
        [(BYTES_COLUMN_NAME, pa.binary()), ("year", pa.int64()), ("path", pa.string())]
    )
    scanner = (
        BinaryScanner(
            schema=schema,
            synthesized_columns=(PathColumn(),),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
            ),
        )
        .prune_columns(["path", BYTES_COLUMN_NAME])
        .push_limit(1)
        .prune_partitions(col("year") == 2024)
    )

    pruned = scanner.prune_input_split(_manifest(kept, dropped, second))
    rows = pa.concat_tables(list(scanner.create_reader().read(pruned))).to_pylist()

    assert scanner.read_schema() == pa.schema(
        [("path", pa.string()), (BYTES_COLUMN_NAME, pa.binary())]
    )
    assert pruned.paths.tolist() == [str(kept), str(second)]
    assert rows == [{"path": str(kept), BYTES_COLUMN_NAME: b"a"}]


# -- datasource ----------------------------------------------------------------


def test_datasource_infer_schema_is_fixed_and_opens_no_files(tmp_path):
    sample = FileManifest.construct_manifest(
        paths=[str(tmp_path / "year=2024" / "missing.bin")],
        sizes=[1],
        chunk_metadatas=[None],
    )
    datasource = BinaryDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        include_paths=True,
    )

    assert datasource.schema_needs_file_sample
    assert datasource.infer_schema(sample) == pa.schema(
        [(BYTES_COLUMN_NAME, pa.binary()), ("year", pa.string()), ("path", pa.string())]
    )
    assert BinaryDatasourceV2([str(tmp_path)]).infer_schema(None) == BINARY_SCHEMA


def test_datasource_lists_empty_files(tmp_path):
    empty = _write(tmp_path / "empty.bin", b"")
    indexer = BinaryDatasourceV2([str(tmp_path)])._get_file_indexer()
    whole_file_indexer = indexer.as_whole_file_indexer()
    assert isinstance(indexer, NonSamplingFileIndexer)
    assert isinstance(whole_file_indexer, NonSamplingFileIndexer)

    listed = [
        [
            (info.path, info.size)
            for info in each.list_file_infos(
                pa.array([str(tmp_path)]), filesystem=LocalFileSystem()
            )
        ]
        for each in (indexer, whole_file_indexer)
    ]

    assert listed == [[(str(empty), 0)], [(str(empty), 0)]]


# -- read_api routing ----------------------------------------------------------


def test_read_binary_files_routes_to_v2_and_forwards_read_options(
    monkeypatch, tmp_path
):
    result = object()
    captured = {}

    def _read_datasource_v2(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs
        return result

    monkeypatch.setattr(read_api, "_read_datasource_v2", _read_datasource_v2)
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", True)
    partition_filter = Mock()
    path = _write(tmp_path / "data.bin", _snappy_stream_compress(b"a"))

    actual = read_api.read_binary_files(
        str(tmp_path),
        arrow_open_stream_args={"compression": "snappy"},
        partition_filter=partition_filter,
        include_paths=True,
        file_extensions=["bin"],
        concurrency=4,
        parallelism=2,
        override_num_blocks=3,
    )

    datasource = captured["datasource"]
    assert actual is result
    assert isinstance(datasource, BinaryDatasourceV2)
    assert datasource.paths == [str(tmp_path)]
    assert datasource.file_extensions == ["bin"]
    schema = datasource.infer_schema(None)
    reader = datasource.create_scanner(schema).create_reader()
    assert pa.concat_tables(list(reader.read(_manifest(path)))).to_pylist() == [
        {BYTES_COLUMN_NAME: b"a", "path": str(path)}
    ]
    assert captured["kwargs"]["parallelism"] == 2
    assert captured["kwargs"]["concurrency"] == 4
    assert captured["kwargs"]["partition_filter"] is partition_filter


def test_read_binary_files_routes_to_v1_when_v2_is_off(monkeypatch, tmp_path):
    result = object()
    captured = {}
    path = str(tmp_path)

    class FakeBinaryDatasource:
        def __init__(self, *args, **kwargs):
            captured["v1_init_args"] = args
            captured["v1_init_kwargs"] = kwargs

    def _read_datasource(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs
        return result

    def _unexpected_v2(*args, **kwargs):
        raise AssertionError("read_binary_files must not route to V2 here")

    monkeypatch.setattr(read_api, "BinaryDatasource", FakeBinaryDatasource)
    monkeypatch.setattr(read_api, "read_datasource", _read_datasource)
    monkeypatch.setattr(read_api, "_read_datasource_v2", _unexpected_v2)
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", False)

    actual = read_api.read_binary_files(path, include_paths=True, override_num_blocks=3)

    assert actual is result
    assert isinstance(captured["datasource"], FakeBinaryDatasource)
    assert captured["v1_init_args"] == (path,)
    assert captured["v1_init_kwargs"]["include_paths"] is True
    assert captured["kwargs"]["override_num_blocks"] == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
