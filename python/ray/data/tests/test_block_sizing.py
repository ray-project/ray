import pytest

import ray
from ray.data import Dataset
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_map(ray_start_regular_shared, restore_data_context):
    ctx = DataContext.get_current()
    ctx.target_min_block_size = 100 * 8

    # NOTE(swang): For some reason BlockBuilder's estimated
    # memory usage is 2x the actual memory usage.
    num_blocks_expected = 20
    ctx.target_max_block_size = 100 * 8

    ds = ray.data.range(1000).map(lambda row: row)
    assert ds.materialize().num_blocks() == num_blocks_expected
    ds = ray.data.range(1000).map(lambda row: row).map(lambda row: row)
    assert ds.materialize().num_blocks() == num_blocks_expected

    ctx.target_max_block_size *= 2
    ds = ray.data.range(1000).map(lambda row: row)
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected
    ds = ray.data.range(1000).map(lambda row: row).map(lambda row: row)
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected

    # Setting the shuffle block size doesn't do anything for
    # map-only Datasets.
    ctx.target_shuffle_max_block_size = 100 * 8
    ds = ray.data.range(1000).map(lambda row: row)
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected
    ds = ray.data.range(1000).map(lambda row: row).map(lambda row: row)
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected


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
def test_shuffle(ray_start_regular_shared, restore_data_context, shuffle_op):
    # Test AllToAll and Map -> AllToAll Datasets. Check that Map inherits
    # AllToAll's target block size.
    ctx = DataContext.get_current()
    ctx.target_min_block_size = 200 * 8
    mem_size = 8000
    min_blocks_expected = mem_size // ctx.target_min_block_size
    shuffle_fn, kwargs, fusion_supported = shuffle_op

    ctx.target_shuffle_max_block_size = 100 * 8
    if fusion_supported:
        num_blocks_expected = mem_size // ctx.target_shuffle_max_block_size
    else:
        num_blocks_expected = min_blocks_expected
    ds = shuffle_fn(ray.data.range(1000), **kwargs)
    assert ds.materialize().num_blocks() == num_blocks_expected
    ds = shuffle_fn(ray.data.range(1000).map(lambda x: x), **kwargs)
    assert ds.materialize().num_blocks() == num_blocks_expected

    ctx.target_shuffle_max_block_size /= 2
    if fusion_supported:
        num_blocks_expected = mem_size // ctx.target_shuffle_max_block_size
    else:
        num_blocks_expected = min_blocks_expected
    ds = shuffle_fn(ray.data.range(1000), **kwargs)
    assert ds.materialize().num_blocks() == num_blocks_expected
    ds = shuffle_fn(ray.data.range(1000).map(lambda x: x), **kwargs)
    assert ds.materialize().num_blocks() == num_blocks_expected

    ctx.target_max_block_size = 200 * 8
    if fusion_supported:
        num_blocks_expected = mem_size // ctx.target_shuffle_max_block_size
    else:
        num_blocks_expected = mem_size // ctx.target_max_block_size
    ds = shuffle_fn(ray.data.range(1000), **kwargs)
    assert ds.materialize().num_blocks() == num_blocks_expected

    if fusion_supported:
        num_blocks_expected = mem_size // ctx.target_shuffle_max_block_size
    else:
        # NOTE(swang): For some reason BlockBuilder's estimated
        # memory usage for range(1000)->map is 2x the actual memory usage.
        num_blocks_expected = mem_size // ctx.target_max_block_size * 2
    ds = shuffle_fn(ray.data.range(1000).map(lambda x: x), **kwargs)
    assert ds.materialize().num_blocks() == num_blocks_expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
