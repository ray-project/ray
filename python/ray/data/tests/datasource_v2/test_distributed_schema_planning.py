"""Public-API checks for distributed full-schema reads."""

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
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.tests.schema_planning_utils import RecordingLocalFileSystem
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

_FORMATS = [
    ("parquet", pq.write_table, ray.data.read_parquet),
    ("orc", orc.write_table, ray.data.read_orc),
]


def _filesystem(trace, **kwargs):
    return pafs.PyFileSystem(
        pafs.FSSpecHandler(
            RecordingLocalFileSystem(
                trace_path=str(trace), skip_instance_cache=True, **kwargs
            )
        )
    )


def _records(trace):
    return [json.loads(line) for line in trace.read_text().splitlines()]


@pytest.mark.parametrize("name,write,read", _FORMATS)
def test_schema_reads_use_selected_workers(
    monkeypatch, tmp_path, ray_start_cluster, name, write, read
):
    cluster = ray_start_cluster
    nodes = [
        cluster.add_node(
            num_cpus=1, object_store_memory=100 * 1024 * 1024, labels={"schema": "yes"}
        )
        for _ in range(2)
    ]
    cluster.add_node(
        num_cpus=1, object_store_memory=100 * 1024 * 1024, labels={"schema": "no"}
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "use_datasource_v2", True)
    monkeypatch.setattr(ctx.execution_options, "label_selector", {"schema": "yes"})
    config = {
        f"full_schema_inference_{key}": value
        for key, value in dict(
            min_files=0, max_tasks=2, num_cpus=1, batch_size=2, io_concurrency=2
        ).items()
    }
    for key, value in config.items():
        monkeypatch.setattr(ctx, key, value)
    paths = [tmp_path / f"f{i}.{name}" for i in range(12)]
    for i, path in enumerate(paths):
        table = (
            pa.table(
                {
                    "id": pa.array([], type=pa.int64()),
                    "empty_only": pa.array([], type=pa.string()),
                }
            )
            if i == 0
            else pa.table({"id": [i]})
        )
        write(table, path)
    trace = tmp_path / "reads.jsonl"
    dataset = read(
        [str(p) for p in paths],
        merge_schema=True,
        filesystem=_filesystem(
            trace, require_env="RAY_SCHEMA_TEST", schema_config=config
        ),
        runtime_env={"env_vars": {"RAY_SCHEMA_TEST": "enabled"}},
        ray_remote_args={"scheduling_strategy": "SPREAD"},
    )
    schema = dataset.schema(fetch_if_missing=False)
    assert schema is not None
    assert schema.base_schema == pa.schema(
        [("id", pa.int64()), ("empty_only", pa.string())]
    )
    records = _records(trace)
    assert {r["node"] for r in records} == {node.node_id for node in nodes}
    assert all(r["pid"] != os.getpid() for r in records)
    assert Counter(r["path"] for r in records) == {str(p): 1 for p in paths}


@pytest.mark.parametrize("name,write,read", _FORMATS)
@pytest.mark.parametrize("placement", ["pg", "zero", "none"])
def test_driver_schema_fallback_preserves_read_placement(
    monkeypatch, tmp_path, ray_start_cluster, name, write, read, placement
):
    cluster = ray_start_cluster
    target = cluster.add_node(
        num_cpus=0, object_store_memory=100 * 1024 * 1024, labels={"schema": "zero"}
    )
    cluster.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "use_datasource_v2", True)
    monkeypatch.setattr(ctx, "full_schema_inference_min_files", 0)
    paths = [tmp_path / f"f{i}.{name}" for i in range(3)]
    for i, path in enumerate(paths):
        write(
            pa.table({"id": [i]})
            if i < 2
            else pa.table({"id": [i], "late": ["value"]}),
            path,
        )
    pg = None
    options: Dict[str, Any]
    if placement == "pg":
        pg = placement_group([{"CPU": 0.1}])
        ray.get(pg.ready(), timeout=20)
        # CPU admission alone must not mask the PG fallback.
        monkeypatch.setattr(ctx, "full_schema_inference_num_cpus", 0.05)
        strategy = PlacementGroupSchedulingStrategy(pg, placement_group_bundle_index=0)
        options = {
            "num_cpus": 0.1,
            "ray_remote_args": {"scheduling_strategy": strategy},
        }
    else:
        options = {"num_cpus": 0, "label_selector": {"schema": "zero"}}
        if placement == "none":
            options.update(num_cpus=None, ray_remote_args={"num_cpus": None})
    trace = tmp_path / "reads.jsonl"
    try:
        dataset = read(
            [str(p) for p in paths],
            merge_schema=True,
            filesystem=_filesystem(trace),
            override_num_blocks=1,
            **options,
        )
        before = _records(trace)
        assert Counter(r["path"] for r in before) == {str(p): 1 for p in paths}
        assert all(r["pid"] == os.getpid() for r in before)
        args = dataset._logical_plan.dag.ray_remote_args
        assert args["num_cpus"] == options["num_cpus"]
        if pg is not None:
            assert args["scheduling_strategy"].placement_group == pg
        else:
            assert args["label_selector"] == {"schema": "zero"}
        assert sorted(dataset.take_all(), key=lambda row: row["id"]) == [
            {"id": 0, "late": None},
            {"id": 1, "late": None},
            {"id": 2, "late": "value"},
        ]
        if pg is None:
            reads = [
                r
                for r in _records(trace)[len(before) :]
                if r["pid"] != os.getpid() and r["node"] == target.node_id
            ]
            assert {r["path"] for r in reads} == {str(p) for p in paths}
    finally:
        if pg is not None:
            remove_placement_group(pg)


class _CustomType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "ray.tests.schema_reads")

    def __arrow_ext_serialize__(self):
        return b"v1"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


@pytest.mark.parametrize("case", ["custom_extension", "late_object"])
def test_worker_schemas_preserve_driver_unifier(
    monkeypatch, tmp_path, ray_start_2_cpus, case
):
    custom = _CustomType()
    pa.register_extension_type(custom)
    try:
        if case == "custom_extension":
            array = pa.ExtensionArray.from_storage(custom, pa.array([b"value"]))
            tables = [pa.table({"id": array})] * 3
        else:
            array = pa.ExtensionArray.from_storage(
                ArrowPythonObjectType(), pa.array([b"opaque"], type=pa.large_binary())
            )
            tables = [
                pa.table({"id": [1]}),
                pa.table({"id": ["one"]}),
                pa.table({"id": array}),
            ]
        paths = [tmp_path / f"f{i}.parquet" for i in range(3)]
        for path, table in zip(paths, tables):
            pq.write_table(table, path)
        file_schemas = [pq.read_schema(p) for p in paths]
        if case == "late_object":
            with pytest.raises((pa.ArrowTypeError, pa.ArrowInvalid)):
                unify_schemas_with_validation(file_schemas[:2])
        expected = unify_schemas_with_validation(file_schemas)
        assert isinstance(expected, pa.Schema)
        if case == "late_object":
            assert isinstance(expected.field("id").type, ArrowPythonObjectType)
        ctx = DataContext.get_current()
        for key, value in dict(
            use_datasource_v2=True,
            full_schema_inference_min_files=0,
            full_schema_inference_batch_size=2,
        ).items():
            monkeypatch.setattr(ctx, key, value)
        schema = ray.data.read_parquet(
            [str(p) for p in paths], merge_schema=True
        ).schema(fetch_if_missing=False)
        assert schema is not None and schema.base_schema.equals(
            expected, check_metadata=True
        )
    finally:
        pa.unregister_extension_type(custom.extension_name)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
