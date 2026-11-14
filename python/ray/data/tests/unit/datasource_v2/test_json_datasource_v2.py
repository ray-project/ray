"""Unit tests for the JSON datasource on DataSourceV2."""
import gzip
import io
import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as pajson
import pytest

import ray.data.read_api as read_api
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    LineChunkFileIndexer,
)
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.json import json_datasource_v2
from ray.data._internal.datasource_v2.formats.json.json_datasource_v2 import (
    JsonDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.context import DataContext


def _write_jsonl(path, rows) -> str:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return str(path)


def _read_all(datasource: JsonDatasourceV2, paths) -> pa.Table:
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
    reader = datasource.create_scanner(schema=schema).create_reader()
    return pa.concat_tables(list(reader.read(manifest)), promote_options="permissive")


@pytest.fixture
def small_chunks(monkeypatch):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", "64")


@pytest.mark.parametrize(
    "lines,path,num_rows",
    [
        (False, "a.jsonl", 4),
        (False, "a.JSONL", 4),
        (False, "a.json", 1),
        (True, "a.json", 4),
        (True, "a.jsonl.gz", 1),
    ],
)
def test_splits_only_json_lines_files(small_chunks, lines, path, num_rows):
    indexer = JsonDatasourceV2(["/tmp"], lines=lines)._get_file_indexer()
    assert isinstance(indexer, LineChunkFileIndexer)
    assert len(indexer._chunk_metadatas_of_file(FileInfo(path, 200))) == num_rows


def test_reads_files_whole_when_objects_may_span_lines(small_chunks):
    datasource = JsonDatasourceV2(
        ["/tmp"],
        lines=True,
        arrow_json_args={"parse_options": pajson.ParseOptions(newlines_in_values=True)},
    )
    assert type(datasource._get_file_indexer()) is NonSamplingFileIndexer


@pytest.mark.parametrize("lines", [False, True])
def test_reads_every_row_of_a_split_file_once(tmp_path, small_chunks, lines):
    path = _write_jsonl(tmp_path / "a.jsonl", [{"id": i} for i in range(300)])
    table = _read_all(JsonDatasourceV2([path], lines=lines), [path])
    assert sorted(table.column("id").to_pylist()) == list(range(300))


@pytest.mark.parametrize("chunk_bytes", ["2000000", "134217728"])
def test_keeps_a_key_first_seen_after_the_first_block(
    tmp_path, monkeypatch, chunk_bytes
):
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", chunk_bytes)
    rows = [{"a": i} for i in range(200_000)] + [{"a": -1, "late": "x"}]
    path = _write_jsonl(tmp_path / "a.jsonl", rows)

    table = _read_all(JsonDatasourceV2([path]), [path])

    assert table.num_rows == len(rows)
    # Units of one task are read concurrently, so rows needn't keep file order.
    assert table.filter(pc.field("a") == -1).to_pylist() == [{"a": -1, "late": "x"}]


def test_reads_a_line_longer_than_the_block_size(tmp_path):
    path = _write_jsonl(tmp_path / "a.json", [{"s": "x" * 5000}, {"s": "y"}])
    datasource = JsonDatasourceV2(
        [path], arrow_json_args={"read_options": pajson.ReadOptions(block_size=100)}
    )
    assert _read_all(datasource, [path]).column("s").to_pylist() == ["x" * 5000, "y"]


def test_falls_back_to_json_load_for_a_top_level_array(tmp_path):
    path = tmp_path / "a.json"
    path.write_text(json.dumps([{"one": [1]}, {"one": [1, 2]}]))
    table = _read_all(JsonDatasourceV2([str(path)]), [str(path)])
    assert table.to_pylist() == [{"one": [1]}, {"one": [1, 2]}]


_ROWS_OVER_ONE_MIB = [{"s": "x" * 1000}] * 1100


@pytest.mark.parametrize(
    "text,reads_whole_file",
    [
        pytest.param(
            "".join(json.dumps(row) + "\n" for row in _ROWS_OVER_ONE_MIB),
            False,
            id="json-lines",
        ),
        pytest.param(json.dumps(_ROWS_OVER_ONE_MIB), True, id="one-line-array"),
        pytest.param(json.dumps(_ROWS_OVER_ONE_MIB, indent=1), True, id="array"),
    ],
)
def test_infers_the_schema_from_a_prefix_unless_the_file_is_one_document(
    tmp_path, monkeypatch, text, reads_whole_file
):
    # A top-level array can't be parsed from a prefix, so planning reads it
    # whole on the driver.
    whole_file_reads = []
    original = json_datasource_v2.read_whole_json_file

    def read_whole_json_file(path, filesystem):
        whole_file_reads.append(path)
        return original(path, filesystem)

    monkeypatch.setattr(
        json_datasource_v2, "read_whole_json_file", read_whole_json_file
    )
    path = tmp_path / "a.json"
    path.write_text(text)

    schema = JsonDatasourceV2([str(path)]).infer_schema(
        FileManifest.construct_manifest(
            paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
        )
    )

    assert schema == pa.schema([("s", pa.string())])
    assert whole_file_reads == ([str(path)] if reads_whole_file else [])


def test_lines_true_reports_the_file_of_a_bad_line(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_text('{"a": 1}\n{"a":\n')
    with pytest.raises(ValueError, match=f"line-delimited JSON from file: {path}"):
        _read_all(JsonDatasourceV2([str(path)], lines=True), [str(path)])


def test_reads_compressed_files_whole(tmp_path, small_chunks):
    path = str(tmp_path / "a.jsonl.gz")
    with open(path, "wb") as f:
        f.write(gzip.compress(b"".join(b'{"id": %d}\n' % i for i in range(200))))
    table = _read_all(JsonDatasourceV2([path], lines=True), [path])
    assert sorted(table.column("id").to_pylist()) == list(range(200))


def test_reads_snappy_files_whole(tmp_path):
    snappy = pytest.importorskip("snappy")
    path = str(tmp_path / "a.json.snappy")
    with open(path, "wb") as f:
        snappy.stream_compress(io.BytesIO(b'{"id": 1}\n{"id": 2}\n'), f)
    table = _read_all(JsonDatasourceV2([path]), [path])
    assert table.to_pylist() == [{"id": 1}, {"id": 2}]


@pytest.mark.parametrize("lines", [False, True])
def test_read_json_routes_to_v2(monkeypatch, tmp_path, lines):
    captured = {}

    def _read_datasource_v2(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs

    monkeypatch.setattr(read_api, "_read_datasource_v2", _read_datasource_v2)
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)

    read_api.read_json(str(tmp_path), lines=lines, override_num_blocks=3)

    assert isinstance(captured["datasource"], JsonDatasourceV2)
    assert captured["datasource"]._lines is lines
    assert captured["kwargs"]["parallelism"] == 3


@pytest.mark.parametrize(
    "use_datasource_v2,kwargs",
    [
        (False, {}),
        (True, {"arrow_open_stream_args": {"compression": "gzip"}}),
        (True, {"memory_pool": pa.default_memory_pool()}),
    ],
)
def test_read_json_routes_to_v1(monkeypatch, tmp_path, use_datasource_v2, kwargs):
    captured = {}

    class FakeArrowJSONDatasource:
        def __init__(self, *args, **kwargs):
            pass

    def _read_datasource(datasource, **_):
        captured["datasource"] = datasource

    monkeypatch.setattr(read_api, "ArrowJSONDatasource", FakeArrowJSONDatasource)
    monkeypatch.setattr(read_api, "read_datasource", _read_datasource)
    monkeypatch.setattr(
        DataContext.get_current(), "use_datasource_v2", use_datasource_v2
    )

    read_api.read_json(str(tmp_path), **kwargs)

    assert isinstance(captured["datasource"], FakeArrowJSONDatasource)


def test_read_json_lines_still_rejects_a_filesystem(monkeypatch, tmp_path):
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    with pytest.raises(ValueError, match="`filesystem` is not supported"):
        read_api.read_json(str(tmp_path), lines=True, filesystem=object())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
