from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    _read_parquet_metadata,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_TYPE,
    FileManifest,
    FileMetadata,
)


def test_completed_batches_refill_with_slow_head(monkeypatch):
    for key, value in dict(
        min_files=0,
        max_tasks=4,
        batch_size=2,
        metadata_budget_bytes=512,
        task_metadata_budget_bytes=128,
    ).items():
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_" + key.upper(), value)
    paths = [str(i) for i in range(12)]
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * 12, chunk_metadatas=[None] * 12
    )
    remote = Mock()
    remote.options.return_value = remote
    state = dict(submitted=0)
    values = {}

    def submit(batch, sizes, fs, fmt, context, rows, mtimes, **kwargs):
        assert rows * 32 <= 128
        ref = Mock(index=state["submitted"])
        state["submitted"] += 1
        name = "head" if ref.index == 0 else "tail"
        values[ref] = inference._SchemaBatchResult(
            [(0, pa.schema([(name, pa.int64())]).serialize().to_pybytes(), False)],
            pa.array(
                [FileMetadata(1).to_metadata()] * rows + [None] * (len(batch) - rows),
                type=FILE_METADATA_TYPE,
            ),
        )
        return ref

    def wait(refs, **kwargs):
        assert len(refs) <= 4
        candidates = [r for r in refs if r.index or state["submitted"] > 4]
        assert candidates, "Slow head must not prevent refill"
        ref = min(candidates, key=lambda r: r.index)
        return [ref], [r for r in refs if r is not ref]

    def get(ref):
        return values[ref]

    remote.remote.side_effect = submit
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", get)
    result = inference.infer_full_schema_with_metadata_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        read_metadata=_read_parquet_metadata,
        ray_remote_args={},
        supports_distributed_reads=True,
    )
    assert result is not None
    metadata = result.metadata
    assert metadata is not None
    assert result.schema.names == ["head", "tail"]
    records = metadata.to_pylist()
    assert [v["num_rows"] for v in records] == [1] * len(paths)
    assert state["submitted"] > 4


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
        lambda *a, **kw: events.append("driver") or result,
    )
    actual = inference.infer_full_schema_with_metadata_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        read_metadata=_read_parquet_metadata,
        ray_remote_args={},
        supports_distributed_reads=True,
    )
    assert actual is result
    assert remote.remote.call_count == 3
    assert events == ["cancel", "cancel", "cancel", "driver"]


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

    def submit(paths, sizes, fs, fmt, ctx, rows, mtimes, **kwargs):
        values = [
            FileMetadata(int(p)).to_metadata() if i < rows else None
            for i, p in enumerate(paths)
        ]
        return Mock(
            result=inference._SchemaBatchResult(
                [(0, payload, False)], pa.array(values, type=FILE_METADATA_TYPE)
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
        read_metadata=_read_parquet_metadata,
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
