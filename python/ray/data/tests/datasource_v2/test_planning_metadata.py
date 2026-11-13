import json
import os
from typing import Any, Dict, cast

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.cluster_utils import Cluster
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.read_operator import ReadFiles
from ray.data._internal.logical.rules.pushdown_count_files import PushdownCountFiles
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


@pytest.mark.parametrize("mode", ["distributed", "pg", "none"])
def test_metadata_count_preserves_storage_access(
    monkeypatch, tmp_path, ray_start_cluster, mode
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
    ctx = DataContext.get_current()
    config: Dict[str, Any] = dict(
        min_files=0,
        batch_size=2,
        max_tasks=2,
        num_cpus=1,
        io_concurrency=2,
        metadata_budget_bytes=8192,
        task_metadata_budget_bytes=4096,
    )
    monkeypatch.setattr(ctx, "use_datasource_v2", True)
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
    monkeypatch.setattr(
        ctx.execution_options,
        "label_selector",
        {"pool": "yes"} if mode == "distributed" else None,
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


def test_count_without_merge_schema_preserves_runtime_env(
    monkeypatch, tmp_path, ray_start_2_cpus
):
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), path)
    trace = tmp_path / "reads.jsonl"
    monkeypatch.setenv("RAY_METADATA_ACCESS", "enabled")
    dataset = ray.data.read_parquet(
        str(path),
        merge_schema=False,
        filesystem=_filesystem(trace, require_env="RAY_METADATA_ACCESS"),
        runtime_env={"env_vars": {"RAY_METADATA_ACCESS": "enabled"}},
    )
    before = _records(trace)
    count = (
        PushdownCountFiles()
        .apply(
            LogicalPlan(
                Count(input_dependencies=[dataset._logical_plan.dag]), dataset.context
            )
        )
        .dag
    )
    assert isinstance(count, MapBatches)
    assert (
        count.ray_remote_args["runtime_env"]["env_vars"]["RAY_METADATA_ACCESS"]
        == "enabled"
    )
    monkeypatch.delenv("RAY_METADATA_ACCESS")
    assert dataset.count() == 3
    reads = _records(trace)[len(before) :]
    assert reads and all(
        r["path"] == str(path) and r["pid"] != os.getpid() for r in reads
    )


@pytest.mark.parametrize("name,write,read", _FORMATS)
def test_full_schema_candidates_survive_new_cluster(
    monkeypatch, tmp_path, ray_start_cluster, name, write, read
):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
    ray.init(address=cluster.address)
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "use_datasource_v2", True)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 1)
    for i in range(3):
        write(pa.table({"id": [i]}), tmp_path / f"f{i}.{name}")
    trace = tmp_path / "opens.jsonl"
    dataset = read(str(tmp_path), merge_schema=True, filesystem=_filesystem(trace))
    serialized = dataset.serialize_lineage()
    assert len(_records(trace)) == 3
    ray.shutdown()
    cluster.shutdown()
    write(pa.table({"id": [99]}), tmp_path / f"appended.{name}")
    other = Cluster()
    try:
        other.add_node(num_cpus=2, object_store_memory=100 * 1024 * 1024)
        ray.init(address=other.address)
        restored = ray.data.Dataset.deserialize_lineage(serialized)
        if name == "parquet":
            assert restored.count() == 3
            assert len(_records(trace)) == 3
        else:
            assert sorted(row["id"] for row in restored.take_all()) == [0, 1, 2]
        assert {r["path"] for r in _records(trace)} == {
            str(tmp_path / f"f{i}.{name}") for i in range(3)
        }
    finally:
        ray.shutdown()
        other.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
