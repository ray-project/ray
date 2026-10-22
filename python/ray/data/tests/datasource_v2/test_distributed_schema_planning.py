"""Real-worker and public API checks for full-schema planning."""

import json
import os
from collections import Counter

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.cluster_utils import Cluster
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_COLUMN_NAME,
)
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.tests.schema_planning_utils import RecordingLocalFileSystem
from ray.exceptions import RayTaskError
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def _filesystem(**kwargs):
    backing = RecordingLocalFileSystem(skip_instance_cache=True, **kwargs)
    return pafs.PyFileSystem(pafs.FSSpecHandler(backing)), backing


def _sample(paths, filesystem):
    return sample_files(
        NonSamplingFileIndexer(ignore_missing_paths=False),
        list(map(str, paths)),
        filesystem,
        max_files=None,
    )


def _infer(sample, filesystem, *, format_name, distributed, remote_args):
    return inference.infer_full_schema_from_files(
        sample,
        filesystem,
        format_name=format_name,
        supports_distributed_reads=distributed,
        ray_remote_args=remote_args,
    )


@pytest.mark.parametrize(
    "format_name,write", [("Parquet", pq.write_table), ("ORC", orc.write_table)]
)
def test_real_workers_receive_runtime_env_and_files(
    monkeypatch, tmp_path, ray_start_2_cpus, format_name, write
):
    paths = [tmp_path / f"f{i}" for i in range(6)]
    for i, path in enumerate(paths):
        write(pa.table({"id": [i]}), path)
    trace = tmp_path / "opens.jsonl"
    filesystem, _ = _filesystem(trace_path=str(trace), require_env="RAY_SCHEMA_TEST")
    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 2)
    monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
    result = _infer(
        _sample(paths, filesystem),
        filesystem,
        format_name=format_name,
        distributed=True,
        remote_args={"runtime_env": {"env_vars": {"RAY_SCHEMA_TEST": "enabled"}}},
    )
    records = [json.loads(line) for line in trace.read_text().splitlines()]
    assert Counter(record["path"] for record in records) == {
        str(path): 1 for path in paths
    }
    assert all(record["pid"] != os.getpid() for record in records)
    assert result.schema == pa.schema([("id", pa.int64())])
    assert result.metadata is not None
    assert sum(value["num_rows"] for value in result.metadata.to_pylist()) == 6


@pytest.mark.parametrize(
    "format_name,write,read",
    [
        ("parquet", pq.write_table, ray.data.read_parquet),
        ("orc", orc.write_table, ray.data.read_orc),
    ],
)
def test_full_schema_uses_driver_for_fractional_cpu_placement_group(
    monkeypatch, tmp_path, ray_start_2_cpus, format_name, write, read
):
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    # Exercise task admission without creating thousands of small files.
    monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paths = [data_dir / f"f{i}.{format_name}" for i in range(3)]
    for i, path in enumerate(paths):
        table = pa.table({"id": [i]})
        if i == 2:
            table = table.append_column("late", pa.array(["value"]))
        write(table, path)
    trace = tmp_path / "schema-opens.jsonl"
    filesystem, _ = _filesystem(trace_path=str(trace))
    pg = placement_group([{"CPU": 0.1}])
    try:
        ray.get(pg.ready(), timeout=20)
        strategy = PlacementGroupSchedulingStrategy(pg, placement_group_bundle_index=0)
        dataset = read(
            str(data_dir),
            filesystem=filesystem,
            merge_schema=True,
            num_cpus=0.1,
            ray_remote_args={"scheduling_strategy": strategy},
            override_num_blocks=1,
        )
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        assert Counter(record["path"] for record in records) == {
            str(path): 1 for path in paths
        }
        assert all(record["pid"] == os.getpid() for record in records)
        assert dataset.schema().base_schema == pa.schema(
            [("id", pa.int64()), ("late", pa.string())]
        )
        read_op = dataset._logical_plan.dag
        assert read_op.ray_remote_args["num_cpus"] == 0.1
        retained_strategy = read_op.ray_remote_args["scheduling_strategy"]
        assert retained_strategy.placement_group == pg
        assert retained_strategy.placement_group_bundle_index == 0
        blocks = read_op.input_dependencies[0].prelisted_file_infos
        assert (
            sum(
                value["num_rows"]
                for block in blocks
                for value in block[FILE_METADATA_COLUMN_NAME].to_pylist()
            )
            == 3
        )
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == [
            {"id": 0, "late": None},
            {"id": 1, "late": None},
            {"id": 2, "late": "value"},
        ]
    finally:
        remove_placement_group(pg)


@pytest.mark.parametrize(
    "format_name,write,read",
    [
        ("parquet", pq.write_table, ray.data.read_parquet),
        ("orc", orc.write_table, ray.data.read_orc),
    ],
)
def test_merge_schema_gates_metadata_reuse_and_lineage(
    monkeypatch, tmp_path, ray_start_2_cpus, format_name, write, read
):
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    paths = [tmp_path / f"f{i}.{format_name}" for i in range(4)]
    for i, path in enumerate(paths):
        write(pa.table({"id": pa.array(list(range(i)), type=pa.int64())}), path)
    marker = tmp_path / "_deny"
    filesystem, _ = _filesystem(deny_open_path=str(marker))
    bounded = read(str(tmp_path), filesystem=filesystem)
    full = read(str(tmp_path), filesystem=filesystem, merge_schema=True)
    assert bounded._logical_plan.dag.input_dependencies[0].prelisted_file_infos is None
    blocks = full._logical_plan.dag.input_dependencies[0].prelisted_file_infos
    assert all(FILE_METADATA_COLUMN_NAME in block.column_names for block in blocks)
    serialized = full.serialize_lineage()
    marker.write_text("deny")
    assert full.count() == 6
    restored = ray.data.Dataset.deserialize_lineage(serialized)
    assert restored.count() == 6
    with pytest.raises(RayTaskError, match="Unexpected metadata read"):
        bounded.count()
    marker.write_text("allow")
    assert sorted(row["id"] for row in full.take_all()) == [0, 0, 0, 1, 1, 2]


@pytest.mark.parametrize(
    "format_name,write", [("Parquet", pq.write_table), ("ORC", orc.write_table)]
)
def test_full_schema_reads_span_two_nodes(
    monkeypatch, tmp_path, ray_start_cluster, format_name, write
):
    cluster = ray_start_cluster
    for _ in range(2):
        cluster.add_node(
            num_cpus=1,
            object_store_memory=100 * 1024 * 1024,
            labels={"schema-test": "enabled"},
        )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    paths = [tmp_path / f"f{i}" for i in range(12)]
    for i, path in enumerate(paths):
        write(pa.table({"id": [i]}), path)
    trace = tmp_path / "nodes.jsonl"
    filesystem, _ = _filesystem(trace_path=str(trace), require_env="RAY_SCHEMA_TEST")
    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 2)
    monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
    result = _infer(
        _sample(paths, filesystem),
        filesystem,
        format_name=format_name,
        distributed=True,
        remote_args={
            "scheduling_strategy": "SPREAD",
            "runtime_env": {"env_vars": {"RAY_SCHEMA_TEST": "enabled"}},
        },
    )
    records = [json.loads(line) for line in trace.read_text().splitlines()]
    assert len({record["node"] for record in records}) == 2
    assert Counter(record["path"] for record in records) == {
        str(path): 1 for path in paths
    }
    assert result.schema == pa.schema([("id", pa.int64())])


class _RegisteredBinaryType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "ray.tests.distributed_schema_binary")

    def __arrow_ext_serialize__(self):
        return b"v1"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


@pytest.mark.parametrize("case", ["custom_extension", "late_object"])
def test_distributed_files_preserve_extension_merge(
    monkeypatch, tmp_path, ray_start_2_cpus, case
):
    custom = _RegisteredBinaryType()
    pa.register_extension_type(custom)
    try:
        if case == "custom_extension":
            column = pa.ExtensionArray.from_storage(custom, pa.array([b"value"]))
            tables = [pa.table({"id": column})] * 3
        else:
            column = pa.ExtensionArray.from_storage(
                ArrowPythonObjectType(), pa.array([b"opaque"], type=pa.large_binary())
            )
            tables = [
                pa.table({"id": [1]}),
                pa.table({"id": ["one"]}),
                pa.table({"id": column}),
            ]
        paths = [tmp_path / f"f{i}.parquet" for i in range(3)]
        for path, table in zip(paths, tables):
            pq.write_table(table, path)
        expected = unify_schemas_with_validation(
            [pq.read_schema(path) for path in paths]
        )
        monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 2)
        monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
        result = _infer(
            _sample(paths, pafs.LocalFileSystem()),
            pafs.LocalFileSystem(),
            format_name="Parquet",
            distributed=True,
            remote_args={},
        )
        assert result.schema.equals(expected, check_metadata=True)
    finally:
        pa.unregister_extension_type(custom.extension_name)


def test_remote_planning_metadata_lineage_on_new_cluster(
    monkeypatch, tmp_path, ray_start_cluster
):
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 2)
    monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
    ray.init(address=cluster.address)
    for i in range(3):
        pq.write_table(pa.table({"id": [0, 1, 2]}), tmp_path / f"f{i}.parquet")
    marker = tmp_path / "_deny"
    filesystem, _ = _filesystem(deny_open_path=str(marker))
    dataset = ray.data.read_parquet(
        str(tmp_path), filesystem=filesystem, merge_schema=True
    )
    serialized = dataset.serialize_lineage()
    ray.shutdown()
    cluster.shutdown()
    pq.write_table(pa.table({"id": [99]}), tmp_path / "appended.parquet")
    marker.write_text("deny")
    other = Cluster()
    try:
        other.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
        ray.init(address=other.address)
        restored = ray.data.Dataset.deserialize_lineage(serialized)
        assert restored.count() == 9
    finally:
        ray.shutdown()
        other.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
