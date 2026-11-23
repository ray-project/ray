"""Unit tests for the CSV datasource on DataSourceV2."""

import collections
import dataclasses
import gzip
import io
from typing import List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.csv as pcsv
import pytest
from pyarrow.fs import LocalFileSystem

import ray.data.read_api as read_api
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    LineChunkFileIndexer,
)
from ray.data._internal.datasource_v2.common.listing_utils import batch_file_infos
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.size_estimators import (
    OnDiskSizeEstimator,
)
from ray.data._internal.datasource_v2.formats.csv import csv_datasource_v2
from ray.data._internal.datasource_v2.formats.csv.csv_datasource_v2 import (
    CsvDatasourceV2,
    CsvFileIndexer,
)
from ray.data._internal.datasource_v2.formats.csv.csv_file_reader import (
    csv_file_format,
    invalid_csv_chunk_error,
)
from ray.data._internal.datasource_v2.formats.csv.header_reader import HeaderReader
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data._internal.datasource_v2.interfaces.read_units import (
    ReadUnit,
    ReadUnitFragment,
)
from ray.data._internal.util import RetryingPyFileSystem
from ray.data.context import DataContext
from ray.data.datasource.partitioning import (
    HIVE_DEFAULT_PARTITION,
    Partitioning,
    PartitionStyle,
)
from ray.data.expressions import col


def _write(path, text: str) -> str:
    path.write_text(text)
    return str(path)


def _read_tables(
    datasource: CsvDatasourceV2, paths, columns: Optional[Tuple[str, ...]] = None
) -> List[pa.Table]:
    """List, plan and read ``paths`` in-process, as one read task."""
    sample = FileManifest.construct_manifest(
        paths=paths,
        sizes=[datasource.filesystem.get_file_info(p).size for p in paths],
        chunk_metadatas=[None] * len(paths),
    )
    schema = datasource.infer_schema(sample)
    manifest = FileManifest.concat(
        list(
            datasource._get_file_indexer().list_files(
                pa.array(paths), filesystem=datasource.filesystem
            )
        )
    )
    scanner = datasource.create_scanner(
        schema=schema, partitioning=datasource.resolve_partitioning(sample)
    )
    if columns is not None:
        scanner = dataclasses.replace(scanner, columns=columns)
    reader = scanner.create_reader()
    return list(reader.read(manifest))


def _read_all(
    datasource: CsvDatasourceV2, paths, columns: Optional[Tuple[str, ...]] = None
) -> pa.Table:
    return pa.concat_tables(
        _read_tables(datasource, paths, columns), promote_options="permissive"
    )


@pytest.fixture(autouse=True)
def inline_header_reads(monkeypatch):
    # Header reads stay in this process; the actor pool is covered in
    # ``tests/datasource_v2/test_csv_header_listing.py``.
    monkeypatch.setenv("RAY_DATA_CSV_HEADER_NUM_ACTORS", "0")


@pytest.fixture
def small_chunks(monkeypatch):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", "64")


def test_reads_every_row_of_a_split_file_once(tmp_path, small_chunks):
    path = _write(
        tmp_path / "a.csv", "id,name\n" + "".join(f"{i},n{i}\n" for i in range(300))
    )
    datasource = CsvDatasourceV2([path])
    assert isinstance(datasource._get_file_indexer(), LineChunkFileIndexer)

    table = _read_all(datasource, [path])

    assert sorted(table.column("id").to_pylist()) == list(range(300))
    assert table.column_names == ["id", "name"]


def test_infers_types_per_chunk(tmp_path, small_chunks):
    path = _write(tmp_path / "a.csv", "x\n" + "1\n" * 60 + "1.5\n" * 60)
    # The sample sees only int64; the later chunks infer double, which
    # unification promotes the earlier int64 chunks to.
    table = _read_all(CsvDatasourceV2([path]), [path])
    assert table.column("x").to_pylist() == [1.0] * 60 + [1.5] * 60


@pytest.mark.parametrize(
    "read_options",
    [
        pcsv.ReadOptions(column_names=["id", "name"]),
        pcsv.ReadOptions(autogenerate_column_names=True),
    ],
)
def test_reads_headerless_files_in_chunks(tmp_path, small_chunks, read_options):
    path = _write(tmp_path / "a.csv", "".join(f"{i},n{i}\n" for i in range(200)))
    datasource = CsvDatasourceV2([path], arrow_csv_args={"read_options": read_options})
    assert type(datasource._get_file_indexer()) is LineChunkFileIndexer

    table = _read_all(datasource, [path])

    assert table.num_rows == 200
    assert sorted(table.column(0).to_pylist()) == list(range(200))


def test_names_a_split_file_from_its_own_header(tmp_path, small_chunks):
    first = _write(tmp_path / "a.csv", "id,name\n1,x\n")
    second = _write(
        tmp_path / "b.csv", "name,id\n" + "".join(f"n{i},{i}\n" for i in range(100))
    )

    table = _read_all(CsvDatasourceV2([first, second]), [first, second])

    rows = {(row["id"], row["name"]) for row in table.to_pylist()}
    assert rows == {(1, "x")} | {(i, f"n{i}") for i in range(100)}


@pytest.fixture
def provisioned_actors(monkeypatch):
    """Record the header actors a listing starts, without starting any."""
    provisioned = []

    class _FakeActorClass:
        @staticmethod
        def options(**_options):
            class _Builder:
                @staticmethod
                def remote(**kwargs):
                    provisioned.append(kwargs)
                    return object()

            return _Builder

    monkeypatch.setattr(csv_datasource_v2, "HeaderReaderActor", _FakeActorClass)
    monkeypatch.setenv("RAY_DATA_CSV_HEADER_NUM_ACTORS", "2")
    return provisioned


@pytest.mark.parametrize("chunk_bytes", ["1073741824", "64"], ids=["whole", "split"])
def test_starts_no_actors_for_few_split_files(
    tmp_path, monkeypatch, provisioned_actors, chunk_bytes
):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    monkeypatch.setenv("RAY_DATA_CSV_HEADER_BATCH_SIZE", "3")
    paths = [
        _write(tmp_path / f"{i}.csv", f"id{i},x\n" + "1,2\n" * 50) for i in range(3)
    ]
    datasource = CsvDatasourceV2(paths)

    manifest = FileManifest.concat(
        list(
            datasource._get_file_indexer().list_files(
                pa.array(paths), filesystem=datasource.filesystem
            )
        )
    )

    assert provisioned_actors == []
    names = {
        tuple(metadata["column_names"])
        for metadata in manifest.file_chunk_metadatas
        if metadata is not None
    }
    assert names == (
        set() if chunk_bytes != "64" else {(f"id{i}", "x") for i in range(3)}
    )


def test_lists_a_split_file_with_its_column_names(tmp_path, small_chunks):
    split = _write(tmp_path / "a.csv", '"order id",amount\n' + "1,2\n" * 100)
    whole = _write(tmp_path / "b.csv", "x\n1\n")
    datasource = CsvDatasourceV2([split, whole])
    indexer = datasource._get_file_indexer()
    assert isinstance(indexer, CsvFileIndexer)

    manifest = FileManifest.concat(
        list(
            indexer.list_files(
                pa.array([split, whole]), filesystem=datasource.filesystem
            )
        )
    )

    names = [
        None if metadata is None else tuple(metadata["column_names"])
        for metadata in manifest.file_chunk_metadatas
    ]
    assert names.count(None) == 1
    assert set(names) - {None} == {("order id", "amount")}


def test_header_reader_reads_each_files_header(tmp_path):
    # ``HeaderReader`` is a plain class, unit-tested here as ``FooterReader``
    # is; the indexer runs it as an actor pool.
    filesystem = LocalFileSystem()
    reader = HeaderReader(
        filesystem=filesystem,
        file_format=csv_file_format(
            pcsv.ReadOptions(), pcsv.ParseOptions(), pcsv.ConvertOptions()
        ),
        io_concurrency=2,
    )
    paths = [_write(tmp_path / f"{i}.csv", f"id{i},x\n1,2\n") for i in range(3)]
    files = [
        FileInfo(path=path, size=filesystem.get_file_info(path).size) for path in paths
    ]

    assert reader.read_header(files[0]) == ["id0", "x"]
    assert [reader.read_header(file) for file in files] == [
        [f"id{i}", "x"] for i in range(3)
    ]


@pytest.mark.parametrize(
    "chunk_bytes", [str(4 << 20), str(8 << 20)], ids=["split", "whole"]
)
def test_infers_types_from_every_row_of_a_large_chunk(
    tmp_path, monkeypatch, chunk_bytes
):
    # pyarrow infers a block's types from its first MiB, which has no notes.
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)

    def rows(note: str, price: str) -> str:
        return "".join(f"{i},{'x' * 60},{note},{price}\n" for i in range(30_000))

    path = _write(
        tmp_path / "a.csv", "id,pad,note,price\n" + rows("", "10") + rows("late", "7.5")
    )

    table = _read_all(CsvDatasourceV2([path]), [path])

    assert table.column("note").to_pylist().count("late") == 30_000
    assert collections.Counter(table.column("price").to_pylist()) == {
        10: 30_000,
        7.5: 30_000,
    }


@pytest.mark.parametrize("chunk_bytes", ["64", "1073741824"], ids=["split", "whole"])
@pytest.mark.parametrize("country_first", [False, True])
def test_reads_a_file_holding_its_partition_column(
    tmp_path, monkeypatch, chunk_bytes, country_first
):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    partition_dir = tmp_path / "country=US"
    partition_dir.mkdir()
    rows = [("country", "id")] + [("US", str(i)) for i in range(100)]
    path = _write(
        partition_dir / "a.csv",
        "".join(",".join(row if country_first else row[::-1]) + "\n" for row in rows),
    )

    table = _read_all(
        CsvDatasourceV2([path], partitioning=Partitioning(PartitionStyle.HIVE)),
        [path],
    )

    assert table.column_names == (
        ["country", "id"] if country_first else ["id", "country"]
    )
    assert sorted(table.column("id").to_pylist()) == list(range(100))
    assert set(table.column("country").to_pylist()) == {"US"}


@pytest.mark.parametrize("chunk_bytes", ["64", "1073741824"], ids=["split", "whole"])
def test_raises_when_a_file_column_differs_from_its_partition_value(
    tmp_path, monkeypatch, chunk_bytes
):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    partition_dir = tmp_path / "country=US"
    partition_dir.mkdir()
    rows = ["id,country"] + [f"{i},{'US' if i else 'CA'}" for i in range(100)]
    path = _write(partition_dir / "a.csv", "\n".join(rows) + "\n")

    with pytest.raises(ValueError, match="Partition column country exists"):
        _read_all(
            CsvDatasourceV2([path], partitioning=Partitioning(PartitionStyle.HIVE)),
            [path],
        )


@pytest.mark.parametrize("chunk_bytes", ["64", "1073741824"], ids=["split", "whole"])
def test_raises_when_a_projected_file_column_differs_from_its_partition_value(
    tmp_path, monkeypatch, chunk_bytes
):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    partition_dir = tmp_path / "country=US"
    partition_dir.mkdir()
    path = _write(
        partition_dir / "a.csv",
        "id,country\n" + "".join(f"{i},CA\n" for i in range(100)),
    )
    datasource = CsvDatasourceV2([path], partitioning=Partitioning(PartitionStyle.HIVE))

    with pytest.raises(ValueError, match="Partition column country exists"):
        _read_all(datasource, [path], columns=("country", "id"))


@pytest.mark.parametrize(
    "rows, matches",
    [("1,\n2,\n", True), ("1,a\n2,\n", False)],
    ids=["all-null-column", "partly-null-column"],
)
def test_null_partition_value_matches_only_an_all_null_file_column(
    tmp_path, rows, matches
):
    partition_dir = tmp_path / f"grp={HIVE_DEFAULT_PARTITION}"
    partition_dir.mkdir()
    path = _write(partition_dir / "a.csv", "id,grp\n" + rows)
    datasource = CsvDatasourceV2(
        [path],
        partitioning=Partitioning(
            PartitionStyle.HIVE, null_fallback=HIVE_DEFAULT_PARTITION
        ),
    )

    if matches:
        assert _read_all(datasource, [path]).column("grp").to_pylist() == [None, None]
    else:
        with pytest.raises(ValueError, match="Partition column grp exists"):
            _read_all(datasource, [path])


@pytest.mark.parametrize(
    "arrow_csv_args",
    [
        {"parse_options": pcsv.ParseOptions(newlines_in_values=True)},
        {"parse_options": pcsv.ParseOptions(escape_char="\\")},
        {"read_options": pcsv.ReadOptions(skip_rows=1)},
        {"read_options": pcsv.ReadOptions(skip_rows_after_names=1)},
        {"read_options": pcsv.ReadOptions(encoding="utf-16")},
    ],
)
def test_reads_files_whole_when_rows_may_not_end_at_newlines(arrow_csv_args):
    datasource = CsvDatasourceV2(["/tmp"], arrow_csv_args=arrow_csv_args)
    indexer = datasource._get_file_indexer()
    assert type(indexer) is NonSamplingFileIndexer


def test_hints_newlines_in_values_when_a_piece_fails_to_parse(tmp_path, monkeypatch):
    # The first piece ends inside a quoted value; the second piece's first
    # row then holds more fields than the header names, so pyarrow raises
    # and the reader points at ``newlines_in_values``. The chunk boundary
    # is placed right before the value's second half.
    head = "id,note\n" + "".join(f'{i},"short"\n' for i in range(4)) + '4,"multi\n'
    tail = 'w,x,y,z"\n' + "".join(f'{i},"short"\n' for i in range(5, 20))
    path = _write(tmp_path / "a.csv", head + tail)
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", str(len(head)))

    with pytest.raises(ValueError, match="newlines_in_values"):
        _read_all(CsvDatasourceV2([path]), [path])


@pytest.mark.parametrize(
    "message, hinted",
    [
        ("CSV parse error: Expected 2 columns, got 4: ...", True),
        ("CSV conversion error to int64: invalid value 'abc'", False),
    ],
    ids=["column_count", "conversion"],
)
def test_hints_newlines_in_values_only_for_a_column_count_error(message, hinted):
    error = invalid_csv_chunk_error("a.csv", pa.ArrowInvalid(message))

    assert message in str(error)
    assert ("newlines_in_values" in str(error)) == hinted


@pytest.mark.parametrize("chunk_bytes", ["64", "1073741824"], ids=["split", "whole"])
@pytest.mark.parametrize(
    "include_columns, names",
    [(["b"], ["b"]), (["c", "a"], ["c", "a"])],
    ids=["one", "reordered"],
)
def test_reads_only_the_included_columns(
    tmp_path, monkeypatch, chunk_bytes, include_columns, names
):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    path = _write(
        tmp_path / "a.csv", "a,b,c\n" + "".join(f"{i},{i},{i}\n" for i in range(100))
    )
    datasource = CsvDatasourceV2(
        [path],
        arrow_csv_args={
            "convert_options": pcsv.ConvertOptions(include_columns=include_columns)
        },
    )

    table = _read_all(datasource, [path])

    assert table.column_names == names
    assert sorted(table.column(names[0]).to_pylist()) == list(range(100))


def test_reads_compressed_files_whole(tmp_path, small_chunks):
    path = str(tmp_path / "a.csv.gz")
    with open(path, "wb") as f:
        f.write(gzip.compress(b"id\n" + b"".join(b"%d\n" % i for i in range(200))))
    table = _read_all(CsvDatasourceV2([path]), [path])
    assert sorted(table.column("id").to_pylist()) == list(range(200))


def test_streams_a_compressed_file_with_projection_filter_and_null_fill(tmp_path):
    path = str(tmp_path / "a.csv.gz")
    with open(path, "wb") as f:
        f.write(
            gzip.compress(
                b"id,name\n" + b"".join(b"%d,n%d\n" % (i, i) for i in range(20))
            )
        )
    datasource = CsvDatasourceV2([path])
    scanner = dataclasses.replace(
        datasource.create_scanner(schema=pa.schema([("id", pa.int64())])),
        columns=("name", "missing"),
        predicate=col("id") >= 18,
    )
    manifest = FileManifest.concat(
        list(
            datasource._get_file_indexer().list_files(
                pa.array([path]), filesystem=datasource.filesystem
            )
        )
    )

    table = pa.concat_tables(list(scanner.create_reader().read(manifest)))

    assert table.to_pylist() == [
        {"name": "n18", "missing": None},
        {"name": "n19", "missing": None},
    ]


def test_streams_a_compressed_file_block_by_block(tmp_path):
    path = str(tmp_path / "a.csv.gz")
    with open(path, "wb") as f:
        f.write(gzip.compress(b"id\n" + b"".join(b"%d\n" % i for i in range(5000))))
    datasource = CsvDatasourceV2(
        [path],
        arrow_csv_args={"read_options": pcsv.ReadOptions(block_size=1024)},
    )

    tables = _read_tables(datasource, [path])

    assert len(tables) > 1
    assert sum(len(t) for t in tables) == 5000


def test_reads_snappy_files_whole(tmp_path):
    snappy = pytest.importorskip("snappy")
    path = str(tmp_path / "a.csv.snappy")
    with open(path, "wb") as f:
        snappy.stream_compress(io.BytesIO(b"id,name\n1,a\n2,b\n"), f)

    table = _read_all(CsvDatasourceV2([path]), [path])

    assert table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_retries_filesystem_io_errors(tmp_path):
    path = _write(tmp_path / "a.csv", "id\n1\n")
    filesystem = CsvDatasourceV2([path]).filesystem
    assert isinstance(filesystem, RetryingPyFileSystem)
    assert filesystem.retryable_errors == DataContext.get_current().retried_io_errors


def test_wraps_parse_errors_like_v1(tmp_path):
    path = str(tmp_path / "a.csv")
    with open(path, "wb") as f:
        f.write(b"PAR1\x00\x01not,a\ncsv")
    with pytest.raises(ValueError, match="Failed to read CSV file"):
        _read_all(CsvDatasourceV2([path]), [path])


def test_weighs_a_chunk_by_its_own_bytes(tmp_path, small_chunks):
    path = _write(tmp_path / "a.csv", "id\n" + "1\n" * 100)
    datasource = CsvDatasourceV2([path])
    manifest = FileManifest.concat(
        list(
            datasource._get_file_indexer().list_files(
                pa.array([path]), filesystem=datasource.filesystem
            )
        )
    )
    assert OnDiskSizeEstimator().estimate_in_memory_sizes(manifest).tolist() == [
        64,
        64,
        64,
        11,
    ]


def test_batches_keep_files_of_unknown_size():
    infos = [FileInfo("a", None), FileInfo("b", 10), FileInfo("c", None)]
    assert [[info.path for info in batch] for batch in batch_file_infos(infos, 2)] == [
        ["a", "b"],
        ["c"],
    ]


def test_weighs_a_file_of_unknown_size_as_unknown(tmp_path):
    manifest = FileManifest.construct_manifest(
        paths=["a", "b"], sizes=[None, 10], chunk_metadatas=[None, None]
    )
    sizes = OnDiskSizeEstimator().estimate_in_memory_sizes(manifest)
    assert np.isnan(sizes[0])
    assert sizes[1] == 10


def test_reads_no_header_for_a_file_of_unknown_size(tmp_path, small_chunks):
    path = _write(tmp_path / "a.csv", "id\n" + "1\n" * 100)
    indexer = CsvFileIndexer(
        chunk_size=64,
        ignore_missing_paths=False,
        filesystem=LocalFileSystem(),
        read_options=pcsv.ReadOptions(),
        parse_options=pcsv.ParseOptions(),
    )
    assert indexer._is_chunked(FileInfo(path, 1000))
    assert not indexer._is_chunked(FileInfo(path, None))
    assert indexer._chunk_metadatas_of_file(FileInfo(path, None)) == [None]


def test_reads_a_file_of_unknown_size_whole_without_asking_for_its_size(tmp_path):
    path = _write(tmp_path / "a.csv", "id\n" + "".join(f"{i}\n" for i in range(5)))
    datasource = CsvDatasourceV2([path])
    sample = FileManifest.construct_manifest(
        paths=[path], sizes=[None], chunk_metadatas=[None]
    )
    reader = datasource.create_scanner(
        schema=datasource.infer_schema(sample)
    ).create_reader()

    table = pa.concat_tables(list(reader.read(sample)))

    assert table.column("id").to_pylist() == list(range(5))

    class _CannotOpen:
        def open(self):
            raise AssertionError("opened a file of unknown size for its size")

    unit = ReadUnitFragment(_CannotOpen(), ReadUnit(id=path, source=path))
    assert reader._read_whole_file(unit) is None


def test_preserve_order_fills_one_bucket_at_a_time(monkeypatch):
    ctx = DataContext.get_current()
    hints = PartitionHints(min_bucket_size=1, max_bucket_size=100, num_buckets=8)
    datasource = CsvDatasourceV2(["/tmp"])

    def num_buckets() -> int:
        partitioner = datasource.get_file_partitioner(hints=hints)
        assert isinstance(partitioner, RoundRobinPartitioner)
        return partitioner.num_buckets

    assert num_buckets() == 8
    monkeypatch.setattr(ctx.execution_options, "preserve_order", True)
    assert num_buckets() == 1


def test_read_csv_routes_to_v2(monkeypatch, tmp_path):
    captured = {}

    def _read_datasource_v2(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs

    monkeypatch.setattr(read_api, "_read_datasource_v2", _read_datasource_v2)
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)

    read_api.read_csv(
        str(tmp_path),
        parse_options=pcsv.ParseOptions(delimiter="\t"),
        override_num_blocks=3,
        concurrency=2,
    )

    assert isinstance(captured["datasource"], CsvDatasourceV2)
    assert captured["datasource"]._parse_options.delimiter == "\t"
    assert captured["kwargs"]["parallelism"] == 3
    assert captured["kwargs"]["concurrency"] == 2


@pytest.mark.parametrize(
    "use_datasource_v2,kwargs",
    [
        (False, {}),
        (True, {"arrow_open_stream_args": {"compression": "gzip"}}),
        (True, {"memory_pool": pa.default_memory_pool()}),
    ],
)
def test_read_csv_routes_to_v1(monkeypatch, tmp_path, use_datasource_v2, kwargs):
    captured = {}

    class FakeCSVDatasource:
        def __init__(self, *args, **kwargs):
            pass

    def _read_datasource(datasource, **_):
        captured["datasource"] = datasource

    monkeypatch.setattr(read_api, "CSVDatasource", FakeCSVDatasource)
    monkeypatch.setattr(read_api, "read_datasource", _read_datasource)
    monkeypatch.setattr(
        DataContext.get_current(), "use_datasource_v2", use_datasource_v2
    )
    read_api.read_csv(str(tmp_path), **kwargs)

    assert isinstance(captured["datasource"], FakeCSVDatasource)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
