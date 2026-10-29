"""Real-worker and public API checks for full-schema planning."""

import json
import os
from collections import Counter
from typing import Any, Dict

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.tests.schema_planning_utils import (
    RecordingLocalFileSystem,
    get_prelisted_blocks,
)
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
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
    monkeypatch.setattr(DataContext.get_current(), "full_schema_inference_min_files", 0)
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
        blocks = get_prelisted_blocks(read_op)
        assert all(block.column_names == ["__path", "__file_size"] for block in blocks)
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == [
            {"id": 0, "late": None},
            {"id": 1, "late": None},
            {"id": 2, "late": "value"},
        ]
    finally:
        remove_placement_group(pg)


@pytest.mark.parametrize("format_name,write,read", _READ_FORMATS)
@pytest.mark.parametrize(
    "placement",
    ["hard_affinity", "label_selector", "node_label", "label_selector_none_cpu"],
)
def test_full_schema_uses_driver_for_zero_cpu_node(
    monkeypatch, tmp_path, ray_start_cluster, format_name, write, read, placement
):
    cluster = ray_start_cluster
    target = cluster.add_node(
        num_cpus=0,
        object_store_memory=100 * 1024 * 1024,
        labels={"schema-target": "zero"},
    )
    cluster.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    target_id = target.node_id
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    monkeypatch.setattr(DataContext.get_current(), "full_schema_inference_min_files", 0)
    paths = [tmp_path / f"f{i}.{format_name}" for i in range(3)]
    for i, path in enumerate(paths):
        table = pa.table({"id": [i]})
        if i == 2:
            table = table.append_column("late", pa.array(["value"]))
        write(table, path)
    trace = tmp_path / "opens.jsonl"
    filesystem, _ = _filesystem(trace_path=str(trace))
    read_cpu = None if placement == "label_selector_none_cpu" else 0
    if placement in ("label_selector", "label_selector_none_cpu"):
        read_options: Dict[str, Any] = {"label_selector": {"schema-target": "zero"}}
        if read_cpu is None:
            read_options["ray_remote_args"] = {"num_cpus": None}
    else:
        strategy = (
            NodeAffinitySchedulingStrategy(target_id, soft=False)
            if placement == "hard_affinity"
            else NodeLabelSchedulingStrategy({"schema-target": In("zero")})
        )
        read_options = {"ray_remote_args": {"scheduling_strategy": strategy}}
    dataset = read(
        [str(path) for path in paths],
        filesystem=filesystem,
        merge_schema=True,
        num_cpus=read_cpu,
        override_num_blocks=1,
        **read_options,
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
    assert remote_args["num_cpus"] == read_cpu
    if placement in ("label_selector", "label_selector_none_cpu"):
        assert remote_args["label_selector"] == {"schema-target": "zero"}
    else:
        strategy = remote_args["scheduling_strategy"]
        if placement == "hard_affinity":
            assert isinstance(strategy, NodeAffinitySchedulingStrategy)
            assert strategy.node_id == target_id
            assert not strategy.soft
        else:
            assert isinstance(strategy, NodeLabelSchedulingStrategy)
            assert len(strategy.hard) == 1
            assert strategy.hard[0].key == "schema-target"
            assert isinstance(strategy.hard[0].operator, In)
            assert strategy.hard[0].operator.values == ["zero"]
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
    "format_name,write", [("Parquet", pq.write_table), ("ORC", orc.write_table)]
)
def test_full_schema_reads_span_two_nodes(
    monkeypatch, tmp_path, ray_start_cluster, format_name, write
):
    cluster = ray_start_cluster
    eligible_nodes = []
    for _ in range(2):
        eligible_nodes.append(
            cluster.add_node(
                num_cpus=1,
                object_store_memory=100 * 1024 * 1024,
                labels={"schema-test": "enabled"},
            )
        )
    cluster.add_node(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        labels={"schema-test": "disabled"},
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    monkeypatch.setattr(
        DataContext.get_current().execution_options,
        "label_selector",
        {"schema-test": "enabled"},
    )
    paths = [tmp_path / f"f{i}" for i in range(12)]
    for i, path in enumerate(paths):
        write(pa.table({"id": [i]}), path)
    trace = tmp_path / "nodes.jsonl"
    config = dict(min_files=0, max_tasks=2, num_cpus=1, batch_size=2, io_concurrency=2)
    config = {f"full_schema_inference_{key}": value for key, value in config.items()}
    for name, value in config.items():
        monkeypatch.setattr(DataContext.get_current(), name, value)
    filesystem, _ = _filesystem(
        trace_path=str(trace), require_env="RAY_SCHEMA_TEST", schema_config=config
    )
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
    assert {record["node"] for record in records} == {
        node.node_id for node in eligible_nodes
    }
    assert Counter(record["path"] for record in records) == {
        str(path): 1 for path in paths
    }
    assert all(record["pid"] != os.getpid() for record in records)
    assert result == pa.schema([("id", pa.int64())])


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
        monkeypatch.setattr(
            DataContext.get_current(), "full_schema_inference_batch_size", 2
        )
        monkeypatch.setattr(
            DataContext.get_current(), "full_schema_inference_min_files", 0
        )
        result = _infer(
            _sample(paths, pafs.LocalFileSystem()),
            pafs.LocalFileSystem(),
            format_name="Parquet",
            distributed=True,
            remote_args={},
        )
        assert result.equals(expected, check_metadata=True)
    finally:
        pa.unregister_extension_type(custom.extension_name)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
