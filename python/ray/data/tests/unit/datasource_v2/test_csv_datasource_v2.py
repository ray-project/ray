"""Unit tests for the CSV datasource on DataSourceV2."""
import gzip
import io
from typing import List

import pyarrow as pa
import pyarrow.csv as pcsv
import pytest

import ray.data.read_api as read_api
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    LineChunkFileIndexer,
)
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.size_estimators import (
    OnDiskSizeEstimator,
)
from ray.data._internal.datasource_v2.formats.csv.csv_datasource_v2 import (
    CsvDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle


def _write(path, text: str) -> str:
    path.write_text(text)
    return str(path)


def _read_tables(datasource: CsvDatasourceV2, paths) -> List[pa.Table]:
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
    reader = datasource.create_scanner(
        schema=schema, partitioning=datasource.resolve_partitioning(sample)
    ).create_reader()
    return list(reader.read(manifest))


def _read_all(datasource: CsvDatasourceV2, paths) -> pa.Table:
    return pa.concat_tables(
        _read_tables(datasource, paths), promote_options="permissive"
    )


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

    table = _read_all(datasource, [path])

    assert table.num_rows == 200
    assert sorted(table.column(0).to_pylist()) == list(range(200))


def test_rejects_a_split_file_whose_header_differs(tmp_path, small_chunks):
    first = _write(tmp_path / "a.csv", "id,name\n1,x\n")
    second = _write(
        tmp_path / "b.csv", "name,id\n" + "".join(f"n{i},{i}\n" for i in range(100))
    )
    with pytest.raises(ValueError, match=r"has columns \['name', 'id'\]"):
        _read_all(CsvDatasourceV2([first, second]), [first, second])


_SPLIT = pytest.param(
    "64",
    marks=pytest.mark.xfail(strict=True, reason="Known gap in split CSV reads."),
    id="split",
)


@pytest.mark.parametrize(
    "chunk_bytes", [pytest.param("1073741824", id="whole"), _SPLIT]
)
def test_reads_quoted_newlines_in_a_split_file(tmp_path, monkeypatch, chunk_bytes):
    # pyarrow parses a quoted newline without ``newlines_in_values``, so a cut
    # can land inside a value. Here it yields extra rows instead of an error.
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    path = _write(
        tmp_path / "a.csv",
        "id,score,text\n"
        + "".join(f'{i},{i},"hello\nworld, a, b"\n' for i in range(40)),
    )
    tables = _read_tables(CsvDatasourceV2([path]), [path])

    # Rows, not one concatenated table: the stray rows come back as strings.
    rows = [row for table in tables for row in table.to_pylist()]

    assert rows == [
        {"id": i, "score": i, "text": "hello\nworld, a, b"} for i in range(40)
    ]


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

    assert table.column_names == ["id", "country"]
    assert sorted(table.column("id").to_pylist()) == list(range(100))
    assert set(table.column("country").to_pylist()) == {"US"}


@pytest.mark.parametrize(
    "arrow_csv_args",
    [
        {"parse_options": pcsv.ParseOptions(newlines_in_values=True)},
        {"read_options": pcsv.ReadOptions(skip_rows=1)},
        {"read_options": pcsv.ReadOptions(skip_rows_after_names=1)},
        {"read_options": pcsv.ReadOptions(encoding="utf-16")},
    ],
)
def test_reads_files_whole_when_rows_may_not_end_at_newlines(arrow_csv_args):
    datasource = CsvDatasourceV2(["/tmp"], arrow_csv_args=arrow_csv_args)
    indexer = datasource._get_file_indexer()
    assert type(indexer) is NonSamplingFileIndexer


def test_reads_compressed_files_whole(tmp_path, small_chunks):
    path = str(tmp_path / "a.csv.gz")
    with open(path, "wb") as f:
        f.write(gzip.compress(b"id\n" + b"".join(b"%d\n" % i for i in range(200))))
    table = _read_all(CsvDatasourceV2([path]), [path])
    assert sorted(table.column("id").to_pylist()) == list(range(200))


def test_reads_snappy_files_whole(tmp_path):
    snappy = pytest.importorskip("snappy")
    path = str(tmp_path / "a.csv.snappy")
    with open(path, "wb") as f:
        snappy.stream_compress(io.BytesIO(b"id,name\n1,a\n2,b\n"), f)

    table = _read_all(CsvDatasourceV2([path]), [path])

    assert table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


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
