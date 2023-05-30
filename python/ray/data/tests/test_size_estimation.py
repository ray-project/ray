import pytest
import os
import uuid

import ray
from ray.tests.conftest import *  # noqa
from ray.data._internal.simple_block import SimpleBlockBuilder
from ray.data._internal.arrow_block import ArrowBlockBuilder

SMALL_VALUE = "a" * 100
LARGE_VALUE = "a" * 10000
ARROW_SMALL_VALUE = {"value": "a" * 100}
ARROW_LARGE_VALUE = {"value": "a" * 10000}


def assert_close(actual, expected, tolerance=0.3):
    print("assert_close", actual, expected)
    assert abs(actual - expected) / expected < tolerance, (actual, expected)


def test_py_size(ray_start_regular_shared):
    b = SimpleBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 111)
    b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 222)
    for _ in range(8):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 1110)
    for _ in range(90):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 11100)
    b.add_block([SMALL_VALUE] * 900)
    assert_close(b.get_estimated_memory_usage(), 111000)
    assert len(b.build()) == 1000


def test_py_size_diff_values(ray_start_regular_shared):
    b = SimpleBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    for _ in range(10):
        b.add(LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 100120)
    for _ in range(100):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 121120)
    for _ in range(100):
        b.add(LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 1166875)
    for _ in range(100):
        b.add(LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 2182927)
    b.add_block([SMALL_VALUE] * 1000)
    assert_close(b.get_estimated_memory_usage(), 2240613)
    assert len(b.build()) == 1310


def test_arrow_size(ray_start_regular_shared):
    b = ArrowBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 118)
    b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 236)
    for _ in range(8):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 1180)
    for _ in range(90):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 11800)
    for _ in range(900):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 118000)
    assert b.build().num_rows == 1000


def test_arrow_size_diff_values(ray_start_regular_shared):
    b = ArrowBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    b.add(ARROW_LARGE_VALUE)
    assert b._num_compactions == 0
    assert_close(b.get_estimated_memory_usage(), 10019)
    b.add(ARROW_LARGE_VALUE)
    assert b._num_compactions == 0
    assert_close(b.get_estimated_memory_usage(), 20038)
    for _ in range(10):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 25178)
    for _ in range(100):
        b.add(ARROW_SMALL_VALUE)
    assert b._num_compactions == 0
    assert_close(b.get_estimated_memory_usage(), 35394)
    for _ in range(13000):
        b.add(ARROW_LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 130131680)
    assert b._num_compactions == 2
    for _ in range(4000):
        b.add(ARROW_LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 170129189)
    assert b._num_compactions == 3
    assert b.build().num_rows == 17112


def test_arrow_size_add_block(ray_start_regular_shared):
    b = ArrowBlockBuilder()
    for _ in range(2000):
        b.add(ARROW_LARGE_VALUE)
    block = b.build()
    b2 = ArrowBlockBuilder()
    for _ in range(5):
        b2.add_block(block)
    assert b2._num_compactions == 0
    assert_close(b2.get_estimated_memory_usage(), 100040020)
    assert b2.build().num_rows == 10000


def test_split_read_csv(ray_start_regular_shared, tmp_path):
    ctx = ray.data.context.DataContext.get_current()
    ctx.block_splitting_enabled = True

    def gen(name):
        path = os.path.join(tmp_path, name)
        ray.data.range(1000, parallelism=1).map(
            lambda _: {"out": LARGE_VALUE}
        ).write_csv(path)
        return ray.data.read_csv(path)

    # 20MiB
    ctx.target_max_block_size = 20_000_000
    ds1 = gen("out1")
    assert ds1._block_num_rows() == [1000]

    # 3MiB
    ctx.target_max_block_size = 3_000_000
    ds2 = gen("out2")
    nrow = ds2._block_num_rows()
    assert 3 < len(nrow) < 5, nrow
    for x in nrow[:-1]:
        assert 200 < x < 400, (x, nrow)

    # 1MiB
    ctx.target_max_block_size = 1_000_000
    ds3 = gen("out3")
    nrow = ds3._block_num_rows()
    assert 8 < len(nrow) < 12, nrow
    for x in nrow[:-1]:
        assert 80 < x < 120, (x, nrow)

    # Disabled.
    # Setting infinite block size effectively disables block splitting.
    ctx.target_max_block_size = float("inf")
    ds4 = gen("out4")
    assert ds4._block_num_rows() == [1000]


def test_split_read_parquet(ray_start_regular_shared, tmp_path):
    ctx = ray.data.context.DataContext.get_current()
    ctx.block_splitting_enabled = True

    def gen(name):
        path = os.path.join(tmp_path, name)
        ds = (
            ray.data.range(200000, parallelism=1)
            .map(lambda _: {"out": uuid.uuid4().hex})
            .materialize()
        )
        # Fully execute the operations prior to write, because with
        # parallelism=1, there is only one task; so the write operator
        # will only write to one file, even though there are multiple
        # blocks created by block splitting.
        ds.write_parquet(path)
        return ray.data.read_parquet(path, parallelism=200)

    # 20MiB
    ctx.target_max_block_size = 20_000_000
    ds1 = gen("out1")
    assert ds1._block_num_rows() == [200000]

    # 3MiB
    ctx.target_max_block_size = 3_000_000
    ds2 = gen("out2")
    nrow = ds2._block_num_rows()
    assert 3 < len(nrow) < 5, nrow
    for x in nrow[:-1]:
        assert 50000 < x < 75000, (x, nrow)

    # 1MiB
    ctx.target_max_block_size = 1_000_000
    ds3 = gen("out3")
    nrow = ds3._block_num_rows()
    assert 8 < len(nrow) < 12, nrow
    for x in nrow[:-1]:
        assert 20000 < x < 25000, (x, nrow)


@pytest.mark.parametrize("use_actors", [False, True])
def test_split_map(shutdown_only, use_actors):
    ray.shutdown()
    ray.init(num_cpus=2)
    kwargs = {}
    if use_actors:
        kwargs = {"compute": ray.data.ActorPoolStrategy()}

    # Arrow block
    ctx = ray.data.context.DataContext.get_current()
    ctx.target_max_block_size = 20_000_000
    ctx.block_splitting_enabled = True
    ctx.target_max_block_size = 20_000_000
    ds2 = ray.data.range(1000, parallelism=1).map(lambda _: ARROW_LARGE_VALUE, **kwargs)
    nblocks = len(ds2.map(lambda x: x, **kwargs).get_internal_block_refs())
    assert nblocks == 1, nblocks
    ctx.target_max_block_size = 2_000_000
    nblocks = len(ds2.map(lambda x: x, **kwargs).get_internal_block_refs())
    assert 4 < nblocks < 7 or use_actors, nblocks

    # Disabled.
    # Setting infinite block size effectively disables block splitting.
    ctx.target_max_block_size = float("inf")
    ds3 = ray.data.range(1000, parallelism=1).map(lambda _: ARROW_LARGE_VALUE, **kwargs)
    nblocks = len(ds3.map(lambda x: x, **kwargs).get_internal_block_refs())
    assert nblocks == 1, nblocks


def test_split_flat_map(ray_start_regular_shared):
    ctx = ray.data.context.DataContext.get_current()
    ctx.target_max_block_size = 20_000_000
    ctx.block_splitting_enabled = True
    # Arrow block
    ctx.target_max_block_size = 20_000_000
    ds2 = ray.data.range(1000, parallelism=1).map(lambda _: ARROW_LARGE_VALUE)
    nblocks = len(ds2.flat_map(lambda x: [x]).get_internal_block_refs())
    assert nblocks == 1, nblocks
    ctx.target_max_block_size = 2_000_000
    nblocks = len(ds2.flat_map(lambda x: [x]).get_internal_block_refs())
    assert 4 < nblocks < 7, nblocks


def test_split_map_batches(ray_start_regular_shared):
    ctx = ray.data.context.DataContext.get_current()
    ctx.target_max_block_size = 20_000_000
    ctx.block_splitting_enabled = True
    # Arrow block
    ctx.target_max_block_size = 20_000_000
    ds2 = ray.data.range(1000, parallelism=1).map(lambda _: ARROW_LARGE_VALUE)
    nblocks = len(ds2.map_batches(lambda x: x, batch_size=1).get_internal_block_refs())
    assert nblocks == 1, nblocks
    ctx.target_max_block_size = 2_000_000
    nblocks = len(ds2.map_batches(lambda x: x, batch_size=16).get_internal_block_refs())
    assert 4 < nblocks < 7, nblocks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
