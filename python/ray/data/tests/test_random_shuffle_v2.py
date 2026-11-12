from collections import Counter

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.random_shuffle import (
    RandomShufflePartitioner,
    make_random_shuffle_reduce_fn,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_map_operator import (  # noqa: E501
    DiskHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
)
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


@pytest.fixture
def disk_shuffle_v2(restore_data_context):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = True
    return ctx


def _ids(partitions):
    return sorted(i for t in partitions.values() for i in t["id"].to_pylist())


@pytest.mark.parametrize("seed", [0, None])
def test_random_partitioner_covers_every_row_once(seed):
    block = pa.table({"id": list(range(1000))})
    partitions = RandomShufflePartitioner(10, seed=seed).bind(0)(block)

    assert _ids(partitions) == list(range(1000))
    assert sorted(partitions) == list(range(10))
    sizes = [t.num_rows for t in partitions.values()]
    assert max(sizes) - min(sizes) <= 1
    # Rows are permuted before slicing, so a slice is not a contiguous range.
    assert any(
        t["id"].to_pylist() != sorted(t["id"].to_pylist()) for t in partitions.values()
    )


def test_random_partitioner_is_seeded_per_task_and_block():
    block = pa.table({"id": list(range(100))})
    partitioner = RandomShufflePartitioner(4, seed=7)

    same = [partitioner.bind(0)(block) for _ in range(2)]
    assert same[0][0].equals(same[1][0])

    other_task = partitioner.bind(1)(block)
    assert not same[0][0].equals(other_task[0])

    bound = partitioner.bind(0)
    first, second = bound(block), bound(block)
    assert not first[0].equals(second[0])


def test_random_partitioner_mixes_when_rows_equal_partitions():
    """Row and target permutations must not cancel out for num_rows == num_partitions."""
    block = pa.table({"id": list(range(8))})
    partitioner = RandomShufflePartitioner(8, seed=5)
    placements = {
        tuple(partitions[p]["id"][0].as_py() for p in range(8))
        for partitions in (partitioner.bind(task)(block) for task in range(4))
    }
    assert tuple(range(8)) not in placements
    assert len(placements) > 1


def test_random_partitioner_spreads_small_blocks():
    """Three rows into ten partitions must not always fill partitions 0..2."""
    block = pa.table({"id": [1, 2, 3]})
    partitioner = RandomShufflePartitioner(10, seed=3)
    targets = {frozenset(partitioner.bind(i)(block)) for i in range(8)}
    assert all(len(t) == 3 for t in targets)
    assert len(targets) > 1


def test_random_shuffle_reduce_permutes_and_is_seeded_per_partition():
    shards = [pa.table({"id": list(range(i * 10, (i + 1) * 10))}) for i in range(5)]
    reduce_fn = make_random_shuffle_reduce_fn(seed=11)

    (out,) = reduce_fn(0, [shards])
    assert sorted(out["id"].to_pylist()) == list(range(50))
    assert out["id"].to_pylist() != list(range(50))

    (again,) = reduce_fn(0, [shards])
    assert out.equals(again)
    (other_partition,) = reduce_fn(1, [shards])
    assert not out.equals(other_partition)

    assert list(reduce_fn(0, [[]])) == []


def test_random_shuffle_planner_routes_to_disk_shuffle_v2(
    ray_start_regular_shared_2_cpus, disk_shuffle_v2
):
    ds = ray.data.range(10, override_num_blocks=2).random_shuffle(
        seed=0, resources={"bar": 1}
    )

    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, DiskHashShuffleReduceOp)
    assert dag.name == "DiskRandomShuffleReduce(partitions=2)"
    assert dag._reduce_ray_remote_args == {"resources": {"bar": 1}}
    map_op = dag.input_dependencies[0]
    assert isinstance(map_op, DiskHashShuffleMapOp)
    assert map_op.name == "DiskRandomShuffleMap(partitions=2)"
    assert isinstance(map_op._partition_fn, RandomShufflePartitioner)
    assert map_op._map_ray_remote_args == {"resources": {"bar": 1}}
    assert not dag._preserve_partition_order

    # Ordering must survive fusion of a downstream map into the reduce.
    disk_shuffle_v2.execution_options.preserve_order = True
    ds_ordered = (
        ray.data.range(10, override_num_blocks=2)
        .random_shuffle()
        .map_batches(lambda b: b)
    )
    ordered_dag = get_execution_plan(ds_ordered._logical_plan)[0].dag
    assert isinstance(ordered_dag, DiskHashShuffleReduceOp)
    assert "->" in ordered_dag.name
    assert ordered_dag._preserve_partition_order

    # Without the disk transport, SHUFFLE_V2 keeps the legacy all-to-all op.
    disk_shuffle_v2.use_disk_based_hash_shuffle = False
    ds_legacy = ray.data.range(10, override_num_blocks=2).random_shuffle()
    assert isinstance(
        get_execution_plan(ds_legacy._logical_plan)[0].dag, AllToAllOperator
    )


def test_random_shuffle_v2_end_to_end(ray_start_regular_shared_2_cpus, disk_shuffle_v2):
    # One map task per input block, so reducers merge shards from several mappers.
    disk_shuffle_v2.shuffle_input_batch_bytes = 0
    # Block order is only reproducible when partitions are emitted in order.
    disk_shuffle_v2.execution_options.preserve_order = True

    ds = ray.data.range(1000, override_num_blocks=10)
    seeded = [row["id"] for row in ds.random_shuffle(seed=7).take_all()]
    assert sorted(seeded) == list(range(1000))
    assert seeded != list(range(1000))

    assert [row["id"] for row in ds.random_shuffle(seed=7).take_all()] == seeded
    assert [row["id"] for row in ds.random_shuffle(seed=8).take_all()] != seeded
    unseeded = [row["id"] for row in ds.random_shuffle().take_all()]
    assert sorted(unseeded) == list(range(1000))
    assert unseeded != seeded

    # Every output block mixes rows from every input block.
    shuffled = ds.random_shuffle(seed=7).materialize()
    for block in shuffled.iter_batches(batch_size=None, batch_format="pyarrow"):
        assert len({i // 100 for i in block["id"].to_pylist()}) == 10

    assert ray.data.from_items([]).random_shuffle().take_all() == []


def test_random_shuffle_v2_promotes_compatible_block_schemas(
    ray_start_regular_shared_2_cpus, disk_shuffle_v2
):
    # Default batching sends both blocks to one map task, so a partition's shards
    # carry a null and an int64 version of the same column.
    null_block = pa.table({"v": pa.array([None, None], type=pa.null())})
    int_block = pa.table({"v": pa.array([1, 2], type=pa.int64())})
    rows = (
        ray.data.from_arrow([null_block, int_block]).random_shuffle(seed=0).take_all()
    )
    assert Counter(r["v"] for r in rows) == {1: 1, 2: 1, None: 2}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
