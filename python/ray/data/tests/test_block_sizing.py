import pytest

import ray
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.data import Dataset


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


#SHUFFLE_ALL_TO_ALL_OPS = [
#        (Dataset.random_shuffle, {}),
#        (Dataset.sort, {}),
#        (Dataset.sum, {"on": "id"}),
#        (Dataset.repartition, {"num_blocks": 20, "shuffle": True}),
#        ]
#NON_SHUFFLE_ALL_TO_ALL_OPS = [
#        (Dataset.repartition, {"num_blocks": 20, "shuffle": False}),
#            (Dataset.randomize_block_order, {}),
#            ]


def test_shuffle(ray_start_regular_shared, restore_data_context):
    ctx = DataContext.get_current()
    ctx.target_min_block_size = 200 * 8

    num_blocks_expected = 10
    ctx.target_shuffle_max_block_size = 100 * 8
    ds = ray.data.range(1000).random_shuffle()
    assert ds.materialize().num_blocks() == num_blocks_expected
    ds = ray.data.range(1000).map(lambda x: x).random_shuffle()
    assert ds.materialize().num_blocks() == num_blocks_expected

    ctx.target_shuffle_max_block_size *= 2
    ds = ray.data.range(1000).random_shuffle()
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected
    ds = ray.data.range(1000).map(lambda x: x).random_shuffle()
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected

    # Setting the normal block size doesn't do anything for shuffle Datasets.
    ctx.target_max_block_size = 100 * 8
    ds = ray.data.range(1000).random_shuffle()
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected
    ds = ray.data.range(1000).map(lambda x: x).random_shuffle()
    assert ds.materialize().num_blocks() * 2 == num_blocks_expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
