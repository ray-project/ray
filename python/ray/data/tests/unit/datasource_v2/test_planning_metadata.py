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
    FileManifest,
    FileMetadata,
)
from ray.data.context import DataContext
from ray.data.expressions import col
from ray.data.tests.schema_planning_utils import RecordingLocalFileSystem


def _plan(tmp_path, name="Parquet", case=None):
    paths = [tmp_path / f"{i}.{name}" for i in range(2)]
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
        if name == "Parquet":
            pq.write_table(
                table,
                path,
                row_group_size=3 if case in {"nan", "null", "no_statistics"} else 1,
                use_deprecated_int96_timestamps=True,
                write_statistics=case != "no_statistics",
            )
        else:
            orc.write_table(table, path)
    sample = FileManifest.construct_manifest(
        paths=list(map(str, paths)),
        sizes=[p.stat().st_size for p in paths],
        chunk_metadatas=[None] * 2,
    )
    source = (ParquetDatasourceV2 if name == "Parquet" else OrcDatasourceV2)(
        list(map(str, paths)), filesystem=fs
    )
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


@pytest.mark.parametrize("case", ["Parquet", "nan", "null", "no_statistics", "ORC"])
@pytest.mark.parametrize("miss", [False, True])
def test_same_open_counts_and_footer_roundtrip(tmp_path, case, miss):
    name = "ORC" if case == "ORC" else "Parquet"
    result, sample, fs, raw = _plan(tmp_path, name, case)
    assert result.schema.names == ["id", "empty_only", "late", "timestamp"]
    assert result.metadata is not None
    values = [FileMetadata.from_metadata(v) for v in result.metadata.to_pylist()]
    assert [v.num_rows for v in values] == [0, 3]
    assert len(raw.opens) == 2
    cached = [replace(v, num_rows=999, mtime_ns=0) for v in values] if miss else values
    manifest = FileManifest.construct_manifest(
        paths=sample.paths.tolist(),
        sizes=sample.file_sizes.tolist(),
        chunk_metadatas=[None] * 2,
        file_metadatas=cached,
    )
    reader = (
        ParquetFileReader(filesystem=fs)
        if name == "Parquet"
        else OrcFileReader(
            format=FileFormat.ORC, filesystem=fs, reuse_file_metadata=True
        )
    )
    assert [v.num_rows for v in reader.read_metadata(manifest)] == [0, 3]
    assert len(raw.opens) == (4 if miss else 2)
    if name == "Parquet":
        path = str(sample.paths[-1])
        assert values[-1].parquet_footer is not None
        original = pq.read_metadata(path)
        assert original.schema.column(2).physical_type == "INT96"
        if case == "no_statistics":
            assert original.row_group(0).column(0).statistics is None
        assert pq.read_metadata(pa.BufferReader(values[-1].parquet_footer)).equals(
            original
        )
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
            actual = footer._read_and_chunk(replace(file, metadata=values[-1]))
            assert actual == expected
            if case in {"nan", "null", "no_statistics"}:
                assert all(not group.fully_matched for group in actual.row_groups)
            assert len(raw.opens) == opens
        finally:
            footer.pool.shutdown()
    else:
        assert all(v.parquet_footer is None for v in values)
        assert not OrcFileReader(format=FileFormat.ORC).available_metadata()
        assert not OrcFileReader(
            format=FileFormat.ORC, reuse_file_metadata=True, predicate=col("id") > 0
        ).available_metadata()


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
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "full_schema_inference_metadata_budget_bytes", total)
    monkeypatch.setattr(ctx, "full_schema_inference_task_metadata_budget_bytes", task)
    monkeypatch.setattr(ctx, "full_schema_inference_batch_size", batch)
    result, _, _, _ = _plan(tmp_path)
    assert result.schema.names == ["id", "empty_only", "late", "timestamp"]
    chunks = result.metadata.chunks if result.metadata is not None else []
    used = 0
    admitted = 0
    for chunk in chunks:
        values = [v for v in chunk.to_pylist() if v is not None]
        admitted += len(values)
        size = sum(32 + len(v["parquet_footer"] or b"") for v in values)
        assert size <= task
        used += size
    assert admitted == records and used <= total


@pytest.mark.parametrize(
    "suffix,value",
    [
        ("metadata_budget_bytes", -1),
        ("task_metadata_budget_bytes", -1),
        ("metadata_budget_bytes", True),
        ("task_metadata_budget_bytes", 1.5),
    ],
)
def test_invalid_metadata_budget(monkeypatch, suffix, value):
    monkeypatch.setattr(
        DataContext.get_current(), "full_schema_inference_" + suffix, value
    )
    sample = FileManifest.construct_manifest(paths=[], sizes=[], chunk_metadatas=[])
    with pytest.raises(ValueError, match=suffix):
        inference.infer_full_schema_with_metadata_from_files(
            sample,
            pafs.LocalFileSystem(),
            format_name="Parquet",
            ray_remote_args={},
            supports_distributed_reads=False,
        )


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
    ctx = DataContext.get_current()
    for key, value in dict(
        min_files=0,
        max_tasks=4,
        batch_size=2,
        metadata_budget_bytes=512,
        task_metadata_budget_bytes=128,
    ).items():
        monkeypatch.setattr(ctx, "full_schema_inference_" + key, value)
    paths = [str(i // 2) for i in range(12)]
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * 12, chunk_metadatas=[None] * 12
    )
    remote = Mock()
    remote.options.return_value = remote
    decode = Mock(wraps=pa.ipc.read_schema)
    state = dict(submitted=0, reserved=0, used=0)
    values = {}

    def submit(batch, fs, fmt, context, rows, credit, mtimes):
        assert rows * 32 + credit <= 128
        state["reserved"] += credit
        assert state["reserved"] + state["used"] <= 512 - 12 * 32
        ref = Mock(index=state["submitted"], credit=credit)
        state["submitted"] += 1
        size = min(8, credit // max(1, rows))
        name = "head" if ref.index == 0 else "tail"
        values[ref] = inference._SchemaBatchResult(
            [pa.schema([(name, pa.int64())]).serialize().to_pybytes() for _ in batch],
            pa.array(
                [
                    FileMetadata(int(p), b"x" * size if size else None).to_metadata()
                    if i < rows
                    else None
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
    assert result is not None and result.metadata is not None
    assert result.schema.names == ["head", "tail"]
    assert [v["num_rows"] for v in result.metadata.to_pylist()] == list(map(int, paths))
    assert (state["submitted"], state["reserved"], decode.call_count) == (6, 0, 2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
