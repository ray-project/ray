"""Public-API checks for distributed full-schema reads."""

import os
from collections import Counter
from typing import Any, Dict, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.actor import ActorHandle
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data.context import DataContext
from ray.data.tests.schema_planning_utils import (
    read_schema_records as _records,
    recording_filesystem as _filesystem,
)
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

_FORMATS = [
    ("parquet", pq.write_table, ray.data.read_parquet),
    ("orc", orc.write_table, ray.data.read_orc),
]


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
    for key, value in dict(
        min_files=0, max_tasks=2, num_cpus=1, batch_size=2, io_concurrency=2
    ).items():
        monkeypatch.setattr(inference, f"_FULL_SCHEMA_INFERENCE_{key.upper()}", value)
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
        filesystem=_filesystem(trace, require_env="RAY_SCHEMA_TEST"),
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


@pytest.mark.parametrize(
    "name,write,read,placement",
    [
        ("parquet", pq.write_table, ray.data.read_parquet, "pg"),
        ("parquet", pq.write_table, ray.data.read_parquet, "zero"),
        ("parquet", pq.write_table, ray.data.read_parquet, "none"),
        ("orc", orc.write_table, ray.data.read_orc, "none"),
    ],
    ids=["parquet-pg", "parquet-zero", "parquet-none", "orc-none"],
)
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
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    paths = [tmp_path / f"f{i}.{name}" for i in range(3)]
    for i, path in enumerate(paths):
        write(
            (
                pa.table({"id": [i]})
                if i < 2
                else pa.table({"id": [i], "late": ["value"]})
            ),
            path,
        )
    pg = None
    options: Dict[str, Any]
    if placement == "pg":
        pg = placement_group([{"CPU": 0.1}])
        ray.get(pg.ready(), timeout=20)
        # CPU admission alone must not mask the PG fallback.
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_NUM_CPUS", 0.05)
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


def test_driver_schema_fallback_when_workers_are_busy(
    monkeypatch, tmp_path, ray_start_2_cpus
):
    class HoldResources:
        def ready(self):
            return True

    holder = cast(
        ActorHandle[Any], ray.remote(HoldResources).options(num_cpus=2).remote()
    )
    ray.get(holder.ready.remote())
    try:
        ctx = DataContext.get_current()
        monkeypatch.setattr(ctx, "use_datasource_v2", True)
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 2)
        monkeypatch.setattr(
            inference, "_FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S", 0.2
        )
        paths = [tmp_path / f"busy{i}.parquet" for i in range(2)]
        for i, path in enumerate(paths):
            pq.write_table(pa.table({"id": [i]}), path)
        trace = tmp_path / "busy-reads.jsonl"
        schema = ray.data.read_parquet(
            [str(path) for path in paths],
            merge_schema=True,
            filesystem=_filesystem(trace),
        ).schema(fetch_if_missing=False)
        assert schema.base_schema == pa.schema([("id", pa.int64())])
        records = _records(trace)
        assert Counter(record["path"] for record in records) == {
            str(path): 1 for path in paths
        }
        assert all(record["pid"] == os.getpid() for record in records)
    finally:
        ray.kill(holder)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
