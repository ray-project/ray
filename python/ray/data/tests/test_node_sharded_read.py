import os

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.datasource.parquet_datasource import (
    _NodeShardedParquetDatasource,
)
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def _alive_cpu_node_ids():
    return [
        node["NodeID"]
        for node in ray.nodes()
        if node.get("Alive") and node.get("Resources", {}).get("CPU", 0) > 0
    ]


class _PerNodePinnedDatasource(Datasource):
    """Emits one read task per node, each pinned to its node. Each block records
    both the node it was *asked* to run on and the node it *actually* ran on, so
    a test can assert the per-task affinity was honored."""

    def __init__(self, node_ids):
        super().__init__()
        self._node_ids = node_ids

    def estimate_inmemory_data_size(self):
        return None

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        read_tasks = []
        for node_id in self._node_ids:
            scheduling_strategy = NodeAffinitySchedulingStrategy(node_id, soft=False)

            def read_fn(node_id=node_id):
                actual = ray.get_runtime_context().get_node_id()
                return [pa.table({"assigned_node": [node_id], "actual_node": [actual]})]

            metadata = BlockMetadata(
                num_rows=1, size_bytes=None, input_files=None, exec_stats=None
            )
            read_tasks.append(
                ReadTask(
                    read_fn,
                    metadata,
                    ray_remote_args={"scheduling_strategy": scheduling_strategy},
                )
            )
        return read_tasks


def test_per_read_task_node_affinity(ray_start_cluster):
    """Layer 1: a read task carrying a per-task NodeAffinity scheduling strategy
    actually runs on the node it was pinned to."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)

    ray.shutdown()
    ray.init(cluster.address)

    node_ids = _alive_cpu_node_ids()
    assert len(node_ids) >= 2

    rows = ray.data.read_datasource(_PerNodePinnedDatasource(node_ids)).take_all()

    # Each task ran on exactly the node it was pinned to...
    for row in rows:
        assert row["actual_node"] == row["assigned_node"]
    # ...and every node got exactly its own pinned task.
    assert {row["assigned_node"] for row in rows} == set(node_ids)


def _write_parquet_shard(data_path, shard_idx, num_rows=10):
    os.makedirs(data_path, exist_ok=True)
    start = shard_idx * num_rows
    df = pd.DataFrame({"id": list(range(start, start + num_rows))})
    df.to_parquet(os.path.join(data_path, f"shard_{shard_idx}.parquet"))


def test_read_parquet_shard_by_node(ray_start_cluster, tmp_path):
    """``read_parquet(shard_by_node=True)`` emits one read task per node, pinned
    to that node, and reads that node's local view of the path.

    NOTE: On a single-machine test cluster all "nodes" share one filesystem, so
    every node sees the same files (the shards aren't physically distinct). We
    therefore assert one-task-per-node distribution (via stats) and read success,
    not a disjoint row split (which requires truly separate disks per node).
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)

    ray.shutdown()
    ray.init(cluster.address)

    data_path = os.path.join(str(tmp_path), "dataset")
    for shard_idx in range(4):
        _write_parquet_shard(data_path, shard_idx)

    node_ids = _alive_cpu_node_ids()
    num_nodes = len(node_ids)
    assert num_nodes >= 2

    ds = ray.data.read_parquet(data_path, shard_by_node=True).materialize()

    # Exactly one read task per node => the read ran across all `num_nodes` nodes.
    stats = ds.stats()
    assert f"; {num_nodes} n" in stats, stats
    # Reading succeeded and returned data (shared FS => each node read all files).
    assert ds.count() > 0


def test_read_parquet_shard_by_node_honors_file_extensions(ray_start_cluster, tmp_path):
    """file_extensions is threaded through to the on-node listing (not ignored):
    a non-matching extension yields no files, hence an empty dataset."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)

    ray.shutdown()
    ray.init(cluster.address)

    data_path = os.path.join(str(tmp_path), "dataset")
    for shard_idx in range(2):
        _write_parquet_shard(data_path, shard_idx)

    # The shards are *.parquet; restricting to a bogus extension must read nothing,
    # proving the parameter is honored rather than silently replaced with the
    # default ["parquet"].
    ds = ray.data.read_parquet(
        data_path, shard_by_node=True, file_extensions=["doesnotexist"]
    )
    assert ds.count() == 0


@pytest.mark.parametrize("remote_path", ["s3://bucket/dataset", "gs://bucket/dataset"])
def test_read_parquet_shard_by_node_rejects_remote_storage(
    ray_start_regular_shared, remote_path
):
    # Raises on the driver from the path scheme, before any remote I/O.
    with pytest.raises(ValueError, match="node-local"):
        ray.data.read_parquet(remote_path, shard_by_node=True)


def test_read_parquet_shard_by_node_explicit_nodes(ray_start_cluster, tmp_path):
    """An explicit `nodes=` list restricts sharding to those nodes only."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)

    ray.shutdown()
    ray.init(cluster.address)

    data_path = os.path.join(str(tmp_path), "dataset")
    for shard_idx in range(3):
        _write_parquet_shard(data_path, shard_idx)

    target_nodes = _alive_cpu_node_ids()[:2]

    ds = ray.data.read_parquet(
        data_path, shard_by_node=True, nodes=target_nodes
    ).materialize()

    # Read ran on exactly the 2 requested nodes, not all 3.
    stats = ds.stats()
    assert "; 2 n" in stats, stats


@pytest.mark.parametrize(
    "kwargs,match",
    [
        (
            {"ray_remote_args": {"scheduling_strategy": "SPREAD"}},
            "scheduling_strategy",
        ),
        ({"shuffle": "files"}, "shuffle"),
        ({"include_row_hash": True}, "include_row_hash"),
        ({"tensor_column_schema": {"x": (None, (1,))}}, "tensor_column_schema"),
    ],
)
def test_shard_by_node_rejects_unsupported_options(
    ray_start_regular_shared, tmp_path, kwargs, match
):
    data_path = os.path.join(str(tmp_path), "dataset")
    _write_parquet_shard(data_path, 0)

    with pytest.raises(ValueError, match=match):
        ray.data.read_parquet(data_path, shard_by_node=True, **kwargs)


def test_write_parquet_shard_by_node_roundtrip(ray_start_cluster, tmp_path):
    """``write_parquet(shard_by_node=True)`` shards the dataset across nodes (one
    node-pinned write per node, no rows dropped). On a single-machine cluster the
    shards land in the same directory, so a plain read-back reconstructs the full,
    disjoint dataset."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)

    ray.shutdown()
    ray.init(cluster.address)

    num_rows = 100
    ds = ray.data.range(num_rows)

    out_path = os.path.join(str(tmp_path), "sharded")
    ds.write_parquet(out_path, shard_by_node=True)

    read_back = ray.data.read_parquet(out_path)
    ids = sorted(row["id"] for row in read_back.take_all())
    # Every row written exactly once, none dropped or duplicated.
    assert ids == list(range(num_rows))


def test_write_parquet_shard_by_node_rejects_scheduling_strategy(
    ray_start_regular_shared, tmp_path
):
    out_path = os.path.join(str(tmp_path), "sharded")
    with pytest.raises(ValueError, match="scheduling_strategy"):
        ray.data.range(10).write_parquet(
            out_path,
            shard_by_node=True,
            ray_remote_args={"scheduling_strategy": "SPREAD"},
        )


@pytest.mark.parametrize("remote_path", ["s3://bucket/out", "gs://bucket/out"])
def test_write_parquet_shard_by_node_rejects_remote_storage(
    ray_start_regular_shared, remote_path
):
    # Raises on the driver from the path scheme, before materialize/remote I/O.
    with pytest.raises(ValueError, match="node-local"):
        ray.data.range(10).write_parquet(remote_path, shard_by_node=True)


def test_node_sharded_datasource_resolves_all_nodes(ray_start_regular_shared, tmp_path):
    data_path = os.path.join(str(tmp_path), "dataset")
    _write_parquet_shard(data_path, 0)

    datasource = _NodeShardedParquetDatasource(data_path)
    read_tasks = datasource.get_read_tasks(parallelism=1)

    # One task per alive CPU node, each pinned to a node.
    assert len(read_tasks) == len(_alive_cpu_node_ids())
    for read_task in read_tasks:
        strategy = read_task.ray_remote_args["scheduling_strategy"]
        assert isinstance(strategy, NodeAffinitySchedulingStrategy)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
