import pyarrow as pa
import pytest

from ray.data._internal.execution.operators.random_shuffle import (
    RandomShufflePartitioner,
    make_random_shuffle_reduce_fn,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    resolve_partition_fn,
)


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


def test_random_shuffle_reduce_promotes_shard_schemas():
    null_shard = pa.table({"v": pa.array([None], pa.null())})
    int_shard = pa.table({"v": pa.array([1, 2], pa.int64())})
    (out,) = make_random_shuffle_reduce_fn(seed=0)(0, [[null_shard, int_shard]])
    assert out.schema.field("v").type == pa.int64()
    assert sorted(out["v"].to_pylist(), key=lambda v: (v is None, v)) == [1, 2, None]


def test_resolve_partition_fn_passes_plain_callables_through():
    def plain(block):
        return {0: block}

    assert resolve_partition_fn(plain, 3) is plain
    bound = resolve_partition_fn(RandomShufflePartitioner(2, seed=0), 3)
    assert bound is not plain and callable(bound)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
