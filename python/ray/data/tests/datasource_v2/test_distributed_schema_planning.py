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
from ray.data.tests.schema_planning_utils import (
    RecordingLocalFileSystem,
    get_prelisted_blocks,
)
from ray.exceptions import RayTaskError
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

_READ_FORMATS = [
    ("parquet", pq.write_table, ray.data.read_parquet),
    ("orc", orc.write_table, ray.data.read_orc),
]


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


@pytest.mark.parametrize("format_name,write,read", _READ_FORMATS)
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
        assert dataset.count() == 3
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == [
            {"id": 0, "late": None},
            {"id": 1, "late": None},
            {"id": 2, "late": "value"},
        ]
    finally:
        remove_placement_group(pg)


@pytest.mark.parametrize("format_name,write,read", _READ_FORMATS)
def test_full_schema_uses_driver_for_hard_node_affinity(
    monkeypatch, tmp_path, ray_start_cluster, format_name, write, read
):
    cluster = ray_start_cluster
    target = cluster.add_node(num_cpus=0, object_store_memory=100 * 1024 * 1024)
    cluster.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    target_id = target.node_id
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
    paths = [tmp_path / f"f{i}.{format_name}" for i in range(3)]
    for i, path in enumerate(paths):
        table = pa.table({"id": [i]})
        if i == 2:
            table = table.append_column("late", pa.array(["value"]))
        write(table, path)
    trace = tmp_path / "opens.jsonl"
    filesystem, _ = _filesystem(trace_path=str(trace))
    dataset = read(
        [str(path) for path in paths],
        filesystem=filesystem,
        merge_schema=True,
        num_cpus=0,
        ray_remote_args={
            "scheduling_strategy": NodeAffinitySchedulingStrategy(target_id, soft=False)
        },
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
    remote_args = dataset._logical_plan.dag.ray_remote_args
    assert remote_args["num_cpus"] == 0
    strategy = remote_args["scheduling_strategy"]
    assert isinstance(strategy, NodeAffinitySchedulingStrategy)
    assert strategy.node_id == target_id
    assert not strategy.soft
    assert dataset.count() == 3
    read_trace_offset = len(trace.read_text().splitlines())
    assert sorted(dataset.take_all(), key=lambda row: row["id"]) == [
        {"id": 0, "late": None},
        {"id": 1, "late": None},
        {"id": 2, "late": "value"},
    ]
    records = [
        json.loads(line) for line in trace.read_text().splitlines()[read_trace_offset:]
    ]
    # ListFiles needs one CPU, so it cannot open files on this zero-CPU node.
    # Arrow I/O threads can lack task names; use process and node identity.
    worker_reads = [
        record
        for record in records
        if record["pid"] != os.getpid() and record["node"] == target_id
    ]
    assert {record["path"] for record in worker_reads} == {str(path) for path in paths}


@pytest.mark.parametrize(
    "cache_metadata", [True, False], ids=["cache_hit", "cache_miss"]
)
def test_orc_count_preserves_read_runtime_env(
    monkeypatch, tmp_path, ray_start_2_cpus, cache_metadata
):
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    if not cache_metadata:
        monkeypatch.setattr(inference, "_SCHEMA_METADATA_BUDGET", 0)
    env_key = "RAY_ORC_COUNT_ACCESS"
    # The cluster starts before setting the driver's environment, so default
    # workers cannot inherit this value from the raylet.
    monkeypatch.setenv(env_key, "enabled")
    paths = [tmp_path / f"f{i}.orc" for i in range(2)]
    for i, path in enumerate(paths):
        orc.write_table(pa.table({"id": [i]}), path)
    trace = tmp_path / "opens.jsonl"
    filesystem, _ = _filesystem(
        trace_path=str(trace), require_env=env_key, require_env_for_info=env_key
    )
    dataset = ray.data.read_orc(
        [str(path) for path in paths],
        filesystem=filesystem,
        merge_schema=True,
        runtime_env={"env_vars": {env_key: "enabled"}},
        override_num_blocks=1,
    )
    blocks = get_prelisted_blocks(dataset._logical_plan.dag)
    assert all(FILE_METADATA_COLUMN_NAME in block.column_names for block in blocks) == (
        cache_metadata
    )
    assert dataset.count() == 2
    records = [json.loads(line) for line in trace.read_text().splitlines()]
    assert sum(record["pid"] != os.getpid() for record in records) == (
        0 if cache_metadata else 2
    )


@pytest.mark.parametrize("format_name,write,read", _READ_FORMATS)
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
    assert all(record["pid"] != os.getpid() for record in records)
    assert result.schema == pa.schema([("id", pa.int64())])
    assert result.metadata is not None
    assert sum(value["num_rows"] for value in result.metadata.to_pylist()) == 12


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
