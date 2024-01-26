import pytest

import ray
from ray.data import Dataset
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import (
    assert_blocks_expected_in_plasma,
    get_initial_core_execution_metrics_snapshot,
)
from ray.tests.conftest import *  # noqa


def test_map(shutdown_only, restore_data_context):
    ray.init(
        _system_config={
            "max_direct_call_object_size": 10_000,
        },
        num_cpus=2,
        object_store_memory=int(100e6),
    )

    ctx = DataContext.get_current()
    ctx.target_min_block_size = 10_000 * 8
    ctx.target_max_block_size = 10_000 * 8
    num_blocks_expected = 10
    last_snapshot = get_initial_core_execution_metrics_snapshot()

    # Test read.
    ds = ray.data.range(100_000, parallelism=1).materialize()
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected + 1
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected,
        block_size_expected=ctx.target_max_block_size,
    )

    # Test read -> map.
    # NOTE(swang): For some reason BlockBuilder's estimated memory usage when a
    # map fn is used is 2x the actual memory usage.
    ds = ray.data.range(100_000, parallelism=1).map(lambda row: row).materialize()
    assert num_blocks_expected * 2 <= ds.num_blocks() <= num_blocks_expected * 2 + 1
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected * 2,
        block_size_expected=ctx.target_max_block_size // 2,
    )

    # Test adjusted block size.
    ctx.target_max_block_size *= 2
    num_blocks_expected //= 2

    # Test read.
    ds = ray.data.range(100_000, parallelism=1).materialize()
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected + 1
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected,
        block_size_expected=ctx.target_max_block_size,
    )

    # Test read -> map.
    ds = ray.data.range(100_000, parallelism=1).map(lambda row: row).materialize()
    assert num_blocks_expected * 2 <= ds.num_blocks() <= num_blocks_expected * 2 + 1
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected * 2,
        block_size_expected=ctx.target_max_block_size // 2,
    )

    # Setting the shuffle block size doesn't do anything for
    # map-only Datasets.
    ctx.target_shuffle_max_block_size = ctx.target_max_block_size / 2

    # Test read.
    ds = ray.data.range(100_000, parallelism=1).materialize()
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected + 1
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected,
        block_size_expected=ctx.target_max_block_size,
    )

    # Test read -> map.
    ds = ray.data.range(100_000, parallelism=1).map(lambda row: row).materialize()
    assert num_blocks_expected * 2 <= ds.num_blocks() <= num_blocks_expected * 2 + 1
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected * 2,
        block_size_expected=ctx.target_max_block_size // 2,
    )


# TODO: Test that map stage output blocks are the correct size for groupby and
# repartition. Currently we only have access to the reduce stage output block
# size.
SHUFFLE_ALL_TO_ALL_OPS = [
    (Dataset.random_shuffle, {}, True),
    (Dataset.sort, {"key": "id"}, False),
]


@pytest.mark.parametrize(
    "shuffle_op",
    SHUFFLE_ALL_TO_ALL_OPS,
)
def test_shuffle(shutdown_only, restore_data_context, shuffle_op):
    ray.init(
        _system_config={
            "max_direct_call_object_size": 250,
        },
        num_cpus=2,
        object_store_memory=int(100e6),
    )

    # Test AllToAll and Map -> AllToAll Datasets. Check that Map inherits
    # AllToAll's target block size.
    ctx = DataContext.get_current()
    ctx.min_parallelism = 1
    ctx.target_min_block_size = 1
    mem_size = 800_000
    shuffle_fn, kwargs, fusion_supported = shuffle_op

    ctx.target_shuffle_max_block_size = 10_000 * 8
    num_blocks_expected = mem_size // ctx.target_shuffle_max_block_size
    block_size_expected = ctx.target_shuffle_max_block_size
    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds = shuffle_fn(ray.data.range(100_000), **kwargs).materialize()
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected * 1.5
    # map * reduce intermediate blocks + 1 metadata ref per map/reduce task.
    # If fusion is not supported, the un-fused map stage produces 1 data and 1
    # metadata per task.
    num_intermediate_blocks = num_blocks_expected**2 + num_blocks_expected * (
        2 if fusion_supported else 4
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        # Dataset.sort produces some empty intermediate blocks because the
        # input range is already partially sorted.
        num_intermediate_blocks,
        # Data is written out once before map phase if fusion is disabled, once
        # during map phase, once during reduce phase.
        total_bytes_expected=mem_size * 2 + (0 if fusion_supported else mem_size),
    )

    ds = shuffle_fn(ray.data.range(100_000).map(lambda x: x), **kwargs).materialize()
    if not fusion_supported:
        # TODO(swang): For some reason BlockBuilder's estimated
        # memory usage for range(1000)->map is 2x the actual memory usage.
        # Remove once https://github.com/ray-project/ray/issues/40246 is fixed.
        num_blocks_expected = int(num_blocks_expected * 2.2)
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected * 1.5
    num_intermediate_blocks = num_blocks_expected**2 + num_blocks_expected * (
        2 if fusion_supported else 4
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        # Dataset.sort produces some empty intermediate blocks because the
        # input range is already partially sorted.
        num_intermediate_blocks,
        # Data is written out once before map phase if fusion is disabled, once
        # during map phase, once during reduce phase.
        total_bytes_expected=mem_size * 2 + (0 if fusion_supported else mem_size),
    )

    ctx.target_shuffle_max_block_size //= 2
    num_blocks_expected = mem_size // ctx.target_shuffle_max_block_size
    block_size_expected = ctx.target_shuffle_max_block_size

    ds = shuffle_fn(ray.data.range(100_000), **kwargs).materialize()
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected * 1.5
    num_intermediate_blocks = num_blocks_expected**2 + num_blocks_expected * (
        2 if fusion_supported else 4
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_intermediate_blocks,
        total_bytes_expected=mem_size * 2 + (0 if fusion_supported else mem_size),
    )

    ds = shuffle_fn(ray.data.range(100_000).map(lambda x: x), **kwargs).materialize()
    if not fusion_supported:
        num_blocks_expected = int(num_blocks_expected * 2.2)
        block_size_expected //= 2.2
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected * 1.5
    num_intermediate_blocks = num_blocks_expected**2 + num_blocks_expected * (
        2 if fusion_supported else 4
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_intermediate_blocks,
        total_bytes_expected=mem_size * 2 + (0 if fusion_supported else mem_size),
    )

    # Setting target max block size does not affect map ops when there is a
    # shuffle downstream.
    ctx.target_max_block_size = ctx.target_shuffle_max_block_size * 2
    ds = shuffle_fn(ray.data.range(100_000).map(lambda x: x), **kwargs).materialize()
    assert num_blocks_expected <= ds.num_blocks() <= num_blocks_expected * 1.5
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_intermediate_blocks,
        total_bytes_expected=mem_size * 2 + (0 if fusion_supported else mem_size),
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
