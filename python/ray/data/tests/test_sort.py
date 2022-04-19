import random

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray

from ray.tests.conftest import *  # noqa
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_simple(ray_start_regular, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()
    ctx.use_push_based_shuffle = use_push_based_shuffle

    num_items = 100
    parallelism = 4
    xs = list(range(num_items))
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    assert ds.sort().take(num_items) == list(range(num_items))
    # Make sure we have rows in each block.
    assert len([n for n in ds.sort()._block_num_rows() if n > 0]) == parallelism
    assert ds.sort(descending=True).take(num_items) == list(reversed(range(num_items)))
    assert ds.sort(key=lambda x: -x).take(num_items) == list(reversed(range(num_items)))

    # Test empty dataset.
    ds = ray.data.from_items([])
    s1 = ds.sort()
    assert s1.count() == 0
    assert s1.take() == ds.take()
    ds = ray.data.range(10).filter(lambda r: r > 10).sort()
    assert ds.count() == 0


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_partition_same_key_to_same_block(
    ray_start_regular, use_push_based_shuffle
):
    ctx = ray.data.context.DatasetContext.get_current()
    ctx.use_push_based_shuffle = use_push_based_shuffle

    num_items = 100
    xs = [1] * num_items
    ds = ray.data.from_items(xs)
    sorted_ds = ds.repartition(num_items).sort()

    # We still have 100 blocks
    assert len(sorted_ds._block_num_rows()) == num_items
    # Only one of them is non-empty
    count = sum(1 for x in sorted_ds._block_num_rows() if x > 0)
    assert count == 1
    # That non-empty block contains all rows
    total = sum(x for x in sorted_ds._block_num_rows() if x > 0)
    assert total == num_items


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_arrow(ray_start_regular, num_items, parallelism, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()
    ctx.use_push_based_shuffle = use_push_based_shuffle

    a = list(reversed(range(num_items)))
    b = [f"{x:03}" for x in range(num_items)]
    shard = int(np.ceil(num_items / parallelism))
    offset = 0
    dfs = []
    while offset < num_items:
        dfs.append(
            pd.DataFrame(
                {"a": a[offset : offset + shard], "b": b[offset : offset + shard]}
            )
        )
        offset += shard
    if offset < num_items:
        dfs.append(pd.DataFrame({"a": a[offset:], "b": b[offset:]}))
    ds = ray.data.from_pandas(dfs)

    def assert_sorted(sorted_ds, expected_rows):
        assert [tuple(row.values()) for row in sorted_ds.iter_rows()] == list(
            expected_rows
        )

    assert_sorted(ds.sort(key="a"), zip(reversed(a), reversed(b)))
    # Make sure we have rows in each block.
    assert len([n for n in ds.sort(key="a")._block_num_rows() if n > 0]) == parallelism
    assert_sorted(ds.sort(key="b"), zip(a, b))
    assert_sorted(ds.sort(key="a", descending=True), zip(a, b))


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_arrow_with_empty_blocks(ray_start_regular, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()
    ctx.use_push_based_shuffle = use_push_based_shuffle

    assert (
        BlockAccessor.for_block(pa.Table.from_pydict({})).sample(10, "A").num_rows == 0
    )

    partitions = BlockAccessor.for_block(pa.Table.from_pydict({})).sort_and_partition(
        [1, 5, 10], "A", descending=False
    )
    assert len(partitions) == 4
    for partition in partitions:
        assert partition.num_rows == 0

    assert (
        BlockAccessor.for_block(pa.Table.from_pydict({}))
        .merge_sorted_blocks([pa.Table.from_pydict({})], "A", False)[0]
        .num_rows
        == 0
    )

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in range(3)], parallelism=3)
    ds = ds.filter(lambda r: r["A"] == 0)
    assert [row.as_pydict() for row in ds.sort("A").iter_rows()] == [{"A": 0, "B": 0}]

    # Test empty dataset.
    ds = ray.data.range_arrow(10).filter(lambda r: r["value"] > 10)
    assert (
        len(
            ray.data.impl.sort.sample_boundaries(
                ds._plan.execute().get_blocks(), "value", 3
            )
        )
        == 2
    )
    assert ds.sort("value").count() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
