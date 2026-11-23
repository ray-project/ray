import os
from dataclasses import replace
from typing import Any, Dict, cast
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.cluster_utils import Cluster
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.file_metadata import (
    get_cached_file_metadata,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
    _read_parquet_metadata,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_SIZE_COLUMN_NAME,
    FileManifest,
    FileMetadata,
)
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.read_operator import ReadFiles
from ray.data._internal.logical.rules.pushdown_count_files import PushdownCountFiles
from ray.data.tests.schema_planning_utils import (
    RecordingLocalFileSystem,
    read_schema_records as _records,
    recording_filesystem as _filesystem,
)
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.mark.parametrize("mode", ["distributed", "pg", "none"])
def test_metadata_count_preserves_storage_access(
    monkeypatch, tmp_path, ray_start_cluster, restore_data_context, mode
):
    cluster = ray_start_cluster
    zero = cluster.add_node(
        num_cpus=0, object_store_memory=100 * 1024 * 1024, labels={"pool": "zero"}
    )
    allowed = [
        cluster.add_node(
            num_cpus=1, object_store_memory=100 * 1024 * 1024, labels={"pool": "yes"}
        )
        for _ in range(2)
    ]
    cluster.add_node(
        num_cpus=1, object_store_memory=100 * 1024 * 1024, labels={"pool": "no"}
    )
    ray.init(address=cluster.address)
    ctx = restore_data_context
    config: Dict[str, Any] = dict(
        min_files=0,
        batch_size=2,
        max_tasks=2,
        num_cpus=1,
        io_concurrency=2,
        metadata_budget_bytes=8192,
        task_metadata_budget_bytes=4096,
    )
    ctx.use_datasource_v2 = True
    pg = None
    options: Dict[str, Any] = {
        "runtime_env": {"env_vars": {"RAY_METADATA_ACCESS": "enabled"}}
    }
    if mode == "pg":
        config["num_cpus"] = 0.05
        pg = placement_group([{"CPU": 0.1}])
        ray.get(pg.ready(), timeout=20)
        options.update(
            num_cpus=0.1,
            ray_remote_args={
                "scheduling_strategy": PlacementGroupSchedulingStrategy(
                    pg, placement_group_bundle_index=0
                )
            },
        )
    elif mode == "none":
        options["ray_remote_args"] = {"num_cpus": None}
        options["label_selector"] = {"pool": "zero"}
    else:
        options["ray_remote_args"] = {"scheduling_strategy": "SPREAD"}
    for key, value in config.items():
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_" + key.upper(), value)
    ctx.execution_options.label_selector = (
        {"pool": "yes"} if mode == "distributed" else None
    )
    paths = [tmp_path / f"f{i}.parquet" for i in range(6)]
    for i, path in enumerate(paths):
        pq.write_table(pa.table({"id": [i]}), path)
    trace = tmp_path / "reads.jsonl"
    try:
        monkeypatch.setenv("RAY_METADATA_ACCESS", "enabled")
        dataset = ray.data.read_parquet(
            list(map(str, paths)),
            merge_schema=True,
            filesystem=_filesystem(trace, require_env="RAY_METADATA_ACCESS"),
            **options,
        )
        before = _records(trace)
        assert len(before) == len(paths)
        if mode == "distributed":
            assert {r["node"] for r in before} == {node.node_id for node in allowed}
            assert all(r["pid"] != os.getpid() for r in before)
        else:
            assert all(r["pid"] == os.getpid() for r in before)
        count = (
            PushdownCountFiles()
            .apply(
                LogicalPlan(
                    Count(input_dependencies=[dataset._logical_plan.dag]),
                    dataset.context,
                )
            )
            .dag
        )
        assert isinstance(count, MapBatches)
        assert count.ray_remote_args == {
            "num_cpus": 0.5,
            **cast(ReadFiles, dataset._logical_plan.dag).ray_remote_args,
        }
        pq.write_table(pa.table({"id": list(range(100, 104))}), paths[-1])
        monkeypatch.delenv("RAY_METADATA_ACCESS")
        assert dataset.count() == 9
        misses = _records(trace)[len(before) :]
        assert misses and {r["path"] for r in misses} == {str(paths[-1])}
        assert all(r["pid"] != os.getpid() for r in misses)
        if mode != "pg":
            targets = (
                {zero.node_id} if mode == "none" else {node.node_id for node in allowed}
            )
            assert {r["node"] for r in misses} <= targets
    finally:
        if pg is not None:
            remove_placement_group(pg)


def test_cached_metadata_survives_new_cluster(
    monkeypatch, tmp_path, ray_start_cluster, restore_data_context
):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
    ray.init(address=cluster.address)
    restore_data_context.use_datasource_v2 = True
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 1)
    for i in range(3):
        pq.write_table(pa.table({"id": [i]}), tmp_path / f"f{i}.parquet")
    trace = tmp_path / "opens.jsonl"
    dataset = ray.data.read_parquet(
        str(tmp_path), merge_schema=True, filesystem=_filesystem(trace)
    )
    serialized = dataset.serialize_lineage()
    assert len(_records(trace)) == 3
    ray.shutdown()
    cluster.shutdown()
    pq.write_table(pa.table({"id": [99]}), tmp_path / "appended.parquet")
    other = Cluster()
    try:
        other.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
        ray.init(address=other.address)
        restored = ray.data.Dataset.deserialize_lineage(serialized)
        assert restored.count() == 3
        assert len(_records(trace)) == 3
        assert {r["path"] for r in _records(trace)} == {
            str(tmp_path / f"f{i}.parquet") for i in range(3)
        }
    finally:
        ray.shutdown()
        other.shutdown()


def _plan(tmp_path):
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
            }
        ),
    ]
    for path, table in zip(paths, tables):
        pq.write_table(table, path, row_group_size=1)
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


@pytest.mark.parametrize("miss", [False, True])
def test_planning_row_counts_hit_and_miss(tmp_path, miss):
    result, sample, fs, raw = _plan(tmp_path)
    assert result.schema.names == ["id", "empty_only", "late"]
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
    assert result.schema.names == ["id", "empty_only", "late"]
    metadata = result.metadata
    chunks = metadata.chunks if metadata is not None else []
    used = 0
    admitted = 0
    for chunk in chunks:
        values = [v for v in chunk.to_pylist() if v is not None]
        admitted += len(values)
        size = 32 * len(values)
        assert size <= task
        used += size
    assert admitted == records and used <= total


@pytest.mark.parametrize("change", ["size", "missing", "no_mtime", "empty"])
def test_cache_validation(tmp_path, change):
    path = tmp_path / "file"
    path.write_bytes(b"abc")
    fs = pafs.LocalFileSystem()
    info = fs.get_file_info(str(path))
    cached = FileMetadata(0, mtime_ns=info.mtime_ns)
    file = FileInfo(str(path), info.size, metadata=cached)
    if change == "size":
        path.write_bytes(b"longer")
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
        assert actual == (file.metadata if change == "no_mtime" else None)


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
            result=inference._read_schema_batch(*args, **kwargs)
        )
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
        supports_distributed_reads=distributed,
    )
    assert result is not None
    metadata = result.metadata
    assert metadata is not None
    assert result.schema.names == ["id", "late", "last"]
    assert [value["num_rows"] for value in metadata.to_pylist()] == list(range(1, 7))
    if distributed:
        assert remote.remote.call_count == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
