from dataclasses import replace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.file_metadata import (
    get_cached_file_metadata,
)
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
    OrcDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_TYPE,
    FILE_SIZE_COLUMN_NAME,
    FileManifest,
    FileMetadata,
)
from ray.data.context import DataContext
from ray.data.expressions import col
from ray.data.tests.schema_planning_utils import RecordingLocalFileSystem


def _plan(tmp_path, case=None):
    paths = [tmp_path / f"{i}.parquet" for i in range(2)]
    raw = RecordingLocalFileSystem(skip_instance_cache=True)
    fs = pafs.PyFileSystem(pafs.FSSpecHandler(raw))
    tables = [
        pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "empty_only": pa.array([], type=pa.string()),
            }
        ),
        pa.table(
            {
                "id": [0, 1, 2],
                "late": [1, 2, 3],
                "timestamp": pa.array([0, 1, 2], type=pa.timestamp("ns")),
            }
        ),
    ]
    if case in {"nan", "null"}:
        values = [35.0, 40.0, float("nan")] if case == "nan" else [35, 40, None]
        tables[1] = tables[1].set_column(0, "id", pa.array(values))
        tables[0] = tables[0].set_column(
            0, "id", pa.array([], type=tables[1].schema.field("id").type)
        )
    for path, table in zip(paths, tables):
        pq.write_table(
            table,
            path,
            row_group_size=3 if case in {"nan", "null", "no_statistics"} else 1,
            use_deprecated_int96_timestamps=True,
            write_statistics=case != "no_statistics",
        )
    sample = FileManifest.construct_manifest(
        paths=list(map(str, paths)),
        sizes=[p.stat().st_size for p in paths],
        chunk_metadatas=[None] * 2,
    )
    source = ParquetDatasourceV2(list(map(str, paths)), filesystem=fs)
    result = source.infer_full_schema_with_metadata(
        sample, ray_remote_args={"num_cpus": 0}
    )
    return result, sample, fs, raw


@pytest.mark.parametrize("source_type", [ParquetDatasourceV2, OrcDatasourceV2])
def test_metadata_hook_preserves_schema_override(mocker, source_type):
    hook = mocker.Mock(return_value=pa.schema([("custom", pa.int64())]))
    custom_type = type("CustomSource", (source_type,), {"infer_full_schema": hook})
    sample = FileManifest.construct_manifest(
        paths=["unused"], sizes=[1], chunk_metadatas=[None]
    )
    result = custom_type(["unused"]).infer_full_schema_with_metadata(
        sample, ray_remote_args={}
    )
    hook.assert_called_once_with(sample, ray_remote_args={})
    assert result.schema is hook.return_value and result.metadata is None


def test_metadata_hook_preserves_infer_schema_driver_fallback(
    mocker, monkeypatch, tmp_path
):
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 1)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", 8192)
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", 4096
    )
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    sample = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    hook = mocker.Mock(return_value=pa.schema([("custom", pa.int64())]))
    custom_type = type("CustomSource", (ParquetDatasourceV2,), {"infer_schema": hook})
    source = custom_type([str(path)], filesystem=pafs.LocalFileSystem())
    result = source.infer_full_schema_with_metadata(sample, ray_remote_args={})
    hook.assert_called_once_with(sample)
    assert result.schema is hook.return_value and result.metadata is None


@pytest.mark.parametrize("case", ["Parquet", "nan", "null", "no_statistics"])
@pytest.mark.parametrize("miss", [False, True])
def test_same_open_counts_and_footer_roundtrip(tmp_path, case, miss):
    result, sample, fs, raw = _plan(tmp_path, case)
    assert result.schema.names == ["id", "empty_only", "late", "timestamp"]
    metadata = result.metadata
    assert metadata is not None
    values = [FileMetadata.from_metadata(v) for v in metadata.to_pylist()]
    assert [v.num_rows for v in values] == [0, 3]
    assert len(raw.opens) == 2
    cached = [replace(v, num_rows=999, mtime_ns=0) for v in values] if miss else values
    manifest = FileManifest.construct_manifest(
        paths=sample.paths.tolist(),
        sizes=sample.file_sizes.tolist(),
        chunk_metadatas=[None] * 2,
        file_metadatas=cached,
    )
    reader = ParquetFileReader(filesystem=fs)
    assert [v.num_rows for v in reader.read_metadata(manifest)] == [0, 3]
    assert len(raw.opens) == (4 if miss else 2)
    path = str(sample.paths[-1])
    assert values[-1].parquet_footer is not None
    original = pq.read_metadata(path)
    assert original.schema.column(2).physical_type == "INT96"
    if case == "no_statistics":
        assert original.row_group(0).column(0).statistics is None
    assert pq.read_metadata(pa.BufferReader(values[-1].parquet_footer)).equals(original)
    footer = FooterReader(
        filesystem=fs,
        io_concurrency=1,
        filter_expr=col("id")
        >= (30 if case in {"nan", "null", "no_statistics"} else 1),
        projected_cols=["id"],
    )
    try:
        file = FileInfo(path, sample.file_sizes[-1])
        expected = footer._read_and_chunk(file)
        opens = len(raw.opens)
        actual = footer._read_and_chunk(replace(file, metadata=cached[-1]))
        assert actual == expected
        if case in {"nan", "null", "no_statistics"}:
            assert all(not group.fully_matched for group in actual.row_groups)
        assert len(raw.opens) == opens + int(miss)
        if case == "Parquet":
            unknown = footer._read_and_chunk(
                replace(file, size=None, metadata=cached[-1])
            )
            assert unknown.row_groups == expected.row_groups
            assert len(raw.opens) == opens + int(miss) + 1
    finally:
        footer.pool.shutdown()
    if case == "Parquet":
        block = manifest.as_block()
        assert isinstance(block, pa.Table)
        block = block.slice(1, 1)
        unknown_size = FileManifest(
            block.set_column(
                block.schema.get_field_index(FILE_SIZE_COLUMN_NAME),
                FILE_SIZE_COLUMN_NAME,
                pa.array([None], type=pa.int64()),
            )
        )
        opens = len(raw.opens)
        assert [v.num_rows for v in reader.read_metadata(unknown_size)] == [3]
        assert len(raw.opens) == opens + 1


@pytest.mark.parametrize(
    "total,task,batch,records",
    [
        (0, 4096, 2, 0),
        (4096, 0, 2, 0),
        (31, 4096, 2, 0),
        (32, 4096, 2, 1),
        (96, 4096, 2, 2),
        (4096, 32, 256, 1),
        (4096, 64, 1, 2),
    ],
)
def test_payload_budgets_and_large_batches(
    monkeypatch, tmp_path, total, task, batch, records
):
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", total
    )
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", task
    )
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", batch)
    result, sample, fs, raw = _plan(tmp_path)
    assert result.schema.names == ["id", "empty_only", "late", "timestamp"]
    metadata = result.metadata
    chunks = metadata.chunks if metadata is not None else []
    used = 0
    admitted = 0
    for chunk in chunks:
        values = [v for v in chunk.to_pylist() if v is not None]
        admitted += len(values)
        size = sum(32 + len(v["parquet_footer"] or b"") for v in values)
        assert size <= task
        used += size
    assert admitted == records and used <= total
    if (total, task, batch) == (96, 4096, 2):
        assert metadata is not None
        cached = FileMetadata.from_metadata(metadata.to_pylist()[-1])
        assert cached.num_rows == 3 and cached.parquet_footer is None
        footer = FooterReader(filesystem=fs, io_concurrency=1)
        try:
            file = FileInfo(str(sample.paths[-1]), sample.file_sizes[-1])
            expected = footer._read_and_chunk(file)
            opens = len(raw.opens)
            actual = footer._read_and_chunk(replace(file, metadata=cached))
            assert actual == expected
            assert len(raw.opens) == opens + 1
        finally:
            footer.pool.shutdown()


@pytest.mark.parametrize(
    "change", ["hit", "size", "mtime", "missing", "no_mtime", "empty"]
)
def test_cache_validation(tmp_path, change):
    path = tmp_path / "file"
    path.write_bytes(b"abc")
    fs = pafs.LocalFileSystem()
    info = fs.get_file_info(str(path))
    cached = FileMetadata(0, mtime_ns=info.mtime_ns)
    file = FileInfo(str(path), info.size, metadata=cached)
    if change == "size":
        path.write_bytes(b"longer")
    elif change == "mtime":
        file = replace(file, metadata=replace(cached, mtime_ns=0))
    elif change == "missing":
        file = replace(file, path=str(tmp_path / "missing"))
    elif change == "no_mtime":
        file = replace(file, metadata=replace(cached, mtime_ns=None))
    elif change == "empty":
        file = replace(file, metadata=FileMetadata())
    if change == "missing":
        with pytest.raises(FileNotFoundError):
            get_cached_file_metadata(file, fs, [])
    else:
        actual = get_cached_file_metadata(file, fs, [])
        assert actual == (file.metadata if change in {"hit", "no_mtime"} else None)


def test_completed_batches_release_credit_and_keep_candidate_positions(monkeypatch):
    for key, value in dict(
        min_files=0,
        max_tasks=4,
        batch_size=2,
        metadata_budget_bytes=512,
        task_metadata_budget_bytes=128,
    ).items():
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_" + key.upper(), value)
    paths = [str(i // 2) for i in range(12)]
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * 12, chunk_metadatas=[None] * 12
    )
    remote = Mock()
    remote.options.return_value = remote
    decode = Mock(wraps=pa.ipc.read_schema)
    state = dict(submitted=0, reserved=0, used=0)
    values = {}

    def submit(batch, sizes, fs, fmt, context, rows, credit, mtimes, **kwargs):
        assert rows * 32 + credit <= 128
        state["reserved"] += credit
        assert state["reserved"] + state["used"] <= 512 - 12 * 32
        ref = Mock(index=state["submitted"], credit=credit)
        state["submitted"] += 1
        size = min(8, credit // max(1, rows))
        name = "head" if ref.index == 0 else "tail"
        values[ref] = inference._SchemaBatchResult(
            [(0, pa.schema([(name, pa.int64())]).serialize().to_pybytes(), False)],
            pa.array(
                [
                    (
                        FileMetadata(
                            int(p), b"x" * size if size else None
                        ).to_metadata()
                        if i < rows
                        else None
                    )
                    for i, p in enumerate(batch)
                ],
                type=FILE_METADATA_TYPE,
            ),
            rows * size,
        )
        return ref

    def wait(refs, **kwargs):
        assert len(refs) <= 4
        candidates = [r for r in refs if r.index or state["submitted"] > 4]
        assert candidates, "Slow head must not prevent refill"
        ref = max(candidates, key=lambda r: r.index)
        return [ref], [r for r in refs if r is not ref]

    def get(ref):
        result = values[ref]
        state["reserved"] -= ref.credit
        state["used"] += result.footer_bytes
        assert state["reserved"] + state["used"] <= 128
        return result

    remote.remote.side_effect = submit
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", get)
    monkeypatch.setattr(pa.ipc, "read_schema", decode)
    result = inference.infer_full_schema_with_metadata_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        ray_remote_args={},
        supports_distributed_reads=True,
    )
    assert result is not None
    metadata = result.metadata
    assert metadata is not None
    assert result.schema.names == ["head", "tail"]
    assert [v["num_rows"] for v in metadata.to_pylist()] == list(map(int, paths))
    assert (state["submitted"], state["reserved"], decode.call_count) == (7, 0, 7)


@pytest.mark.parametrize("distributed", [False, True])
def test_deduplicated_schemas_keep_metadata_in_file_order(
    monkeypatch, tmp_path, distributed
):
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 4)
    schemas = [[], [], ["late"], [], ["last"], []]
    paths = [tmp_path / f"{i}.parquet" for i in range(len(schemas))]
    for i, (path, extra) in enumerate(zip(paths, schemas)):
        table = pa.table({key: list(range(i + 1)) for key in ["id", *extra]})
        pq.write_table(table, path)
    sample = FileManifest.construct_manifest(
        paths=list(map(str, paths)),
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )
    if distributed:
        remote = Mock()
        remote.options.return_value = remote
        remote.remote.side_effect = lambda *args, **kwargs: Mock(
            result=inference._read_schema_metadata_batch(*args, **kwargs)
        )
        monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
        monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
        monkeypatch.setattr(ray, "wait", lambda refs, **kw: ([refs[-1]], refs[:-1]))
        monkeypatch.setattr(ray, "get", lambda ref: ref.result)
    result = inference.infer_full_schema_with_metadata_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        ray_remote_args={},
        supports_distributed_reads=distributed,
    )
    assert result is not None
    metadata = result.metadata
    assert metadata is not None
    assert result.schema.names == ["id", "late", "last"]
    assert [value["num_rows"] for value in metadata.to_pylist()] == list(range(1, 7))
    assert [
        pq.read_metadata(pa.BufferReader(value["parquet_footer"])).num_rows
        for value in metadata.to_pylist()
    ] == list(range(1, 7))
    if distributed:
        assert remote.remote.call_count == 3


@pytest.mark.parametrize("permanent", [False, True])
def test_orc_metadata_read_retries_matching_errors(monkeypatch, tmp_path, permanent):
    path = tmp_path / "test.orc"
    orc.write_table(pa.table({"id": [1, 2]}), path)
    original = orc.ORCFile
    reader = Mock(
        side_effect=(
            OSError("permanent")
            if permanent
            else [OSError("temporary"), original(str(path))]
        )
    )
    monkeypatch.setattr(orc, "ORCFile", reader)
    monkeypatch.setattr("ray._common.retry.time.sleep", lambda _: None)
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "retried_io_errors", ["temporary"])
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    file_reader = OrcFileReader(
        format=FileFormat.ORC,
        filesystem=pafs.LocalFileSystem(),
        enable_metadata_count=True,
    )
    if permanent:
        with pytest.raises(OSError, match="permanent"):
            list(file_reader.read_metadata(manifest))
        assert reader.call_count == 1
    else:
        assert [m.num_rows for m in file_reader.read_metadata(manifest)] == [2]
        assert reader.call_count == 2


def test_metadata_timeout_cancels_before_driver_read(monkeypatch):
    for key, value in dict(min_files=0, batch_size=2, max_tasks=4).items():
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_" + key.upper(), value)
    sample = FileManifest.construct_manifest(
        paths=[str(i) for i in range(4)], sizes=[1] * 4, chunk_metadatas=[None] * 4
    )
    remote = Mock()
    remote.options.return_value = remote
    remote.remote.side_effect = lambda *a, **kw: Mock()
    events = []
    result = inference.FullSchemaInferenceResult(pa.schema([("id", pa.int64())]))
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", lambda refs, **kw: ([], refs))
    monkeypatch.setattr(ray, "cancel", lambda ref, **kw: events.append("cancel"))
    monkeypatch.setattr(
        inference,
        "_driver_read_all_metadata",
        lambda *a: events.append("driver") or result,
    )
    actual = inference.infer_full_schema_with_metadata_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        ray_remote_args={},
        supports_distributed_reads=True,
    )
    assert actual is result
    assert remote.remote.call_count == 3
    assert events == ["cancel", "cancel", "cancel", "driver"]


def test_driver_returns_unused_footer_credit(monkeypatch):
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 1)
    credits = []
    payload = pa.schema([("id", pa.int64())]).serialize().to_pybytes()

    def read(paths, sizes, fs, fmt, ctx, rows, credit, mtimes, **kwargs):
        credits.append(credit)
        return inference._SchemaBatchResult(
            [(0, payload, False)],
            pa.array([FileMetadata(1).to_metadata()], type=FILE_METADATA_TYPE),
            0,
        )

    monkeypatch.setattr(inference, "_read_schema_metadata_batch", read)
    inference._driver_read_all_metadata(
        ["a", "b"], [1, 1], pafs.LocalFileSystem(), "Parquet", 128, 128, None
    )
    assert credits == [64, 64]


def test_probe_batch_reserves_available_row_slots(monkeypatch):
    for key, value in dict(
        min_files=0,
        batch_size=2,
        max_tasks=4,
        metadata_budget_bytes=128,
        task_metadata_budget_bytes=32,
    ).items():
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_" + key.upper(), value)
    sample = FileManifest.construct_manifest(
        paths=[str(i) for i in range(4)], sizes=[1] * 4, chunk_metadatas=[None] * 4
    )
    remote = Mock()
    remote.options.return_value = remote
    payload = pa.schema([("id", pa.int64())]).serialize().to_pybytes()

    def submit(paths, sizes, fs, fmt, ctx, rows, credit, mtimes, **kwargs):
        values = [
            FileMetadata(int(p)).to_metadata() if i < rows else None
            for i, p in enumerate(paths)
        ]
        return Mock(
            result=inference._SchemaBatchResult(
                [(0, payload, False)], pa.array(values, type=FILE_METADATA_TYPE), 0
            )
        )

    remote.remote.side_effect = submit
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", lambda refs, **kw: ([refs[-1]], refs[:-1]))
    monkeypatch.setattr(ray, "get", lambda ref: ref.result)
    result = inference.infer_full_schema_with_metadata_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        ray_remote_args={},
        supports_distributed_reads=True,
    )
    assert result is not None
    metadata = result.metadata
    assert metadata is not None
    assert [v["num_rows"] if v else None for v in metadata.to_pylist()] == [
        0,
        1,
        None,
        3,
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
