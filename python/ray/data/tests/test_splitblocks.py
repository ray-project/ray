import numpy as np
import pytest

import ray
from ray.data._internal.execution.operators.map_transformer import _splitrange
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_splitrange():
    def f(n, k):
        assert _splitrange(n, k) == [len(a) for a in np.array_split(range(n), k)]

    f(0, 1)
    f(5, 1)
    f(5, 3)
    f(5, 5)
    f(5, 10)
    f(50, 1)
    f(50, 2)
    f(50, 3)
    f(50, 4)
    f(50, 5)


def test_small_file_split(ray_start_10_cpus_shared, restore_data_context):
    ds = ray.data.read_csv("example://iris.csv", parallelism=1)
    assert ds.num_blocks() == 1
    assert ds.materialize().num_blocks() == 1
    assert ds.map_batches(lambda x: x).materialize().num_blocks() == 1

    ds = ds.map_batches(lambda x: x).materialize()
    stats = ds.stats()
    assert "Stage 1 ReadCSV->MapBatches" in stats, stats

    ds = ray.data.read_csv("example://iris.csv", parallelism=10)
    assert ds.num_blocks() == 1
    assert ds.map_batches(lambda x: x).materialize().num_blocks() == 10
    assert ds.materialize().num_blocks() == 10

    ds = ray.data.read_csv("example://iris.csv", parallelism=100)
    assert ds.num_blocks() == 1
    assert ds.map_batches(lambda x: x).materialize().num_blocks() == 100
    assert ds.materialize().num_blocks() == 100

    ds = ds.map_batches(lambda x: x).materialize()
    stats = ds.stats()
    assert "Stage 1 ReadCSV->SplitBlocks(100)" in stats, stats
    assert "Stage 2 MapBatches" in stats, stats

    ctx = ray.data.context.DataContext.get_current()
    # Smaller than a single row.
    ctx.target_max_block_size = 1
    ds = ds.map_batches(lambda x: x).materialize()
    # 150 rows.
    assert ds.num_blocks() == 150
    print(ds.stats())


def test_large_file_additional_split(ray_start_10_cpus_shared, tmp_path):
    ctx = ray.data.context.DataContext.get_current()
    ctx.target_max_block_size = 10 * 1024 * 1024

    # ~100MiB of tensor data
    ds = ray.data.range_tensor(1000, shape=(10000,))
    ds.repartition(1).write_parquet(tmp_path)

    ds = ray.data.read_parquet(tmp_path, parallelism=1)
    assert ds.num_blocks() == 1
    print(ds.materialize().stats())
    assert 5 < ds.materialize().num_blocks() < 20  # Size-based block split

    ds = ray.data.read_parquet(tmp_path, parallelism=10)
    assert ds.num_blocks() == 1
    assert 5 < ds.materialize().num_blocks() < 20

    ds = ray.data.read_parquet(tmp_path, parallelism=100)
    assert ds.num_blocks() == 1
    assert 50 < ds.materialize().num_blocks() < 200

    ds = ray.data.read_parquet(tmp_path, parallelism=1000)
    assert ds.num_blocks() == 1
    assert 500 < ds.materialize().num_blocks() < 2000


def test_map_batches_split(ray_start_10_cpus_shared, restore_data_context):
    ds = ray.data.range(1000, parallelism=1).map_batches(lambda x: x, batch_size=1000)
    assert ds.materialize().num_blocks() == 1

    ctx = ray.data.context.DataContext.get_current()
    # 100 integer rows per block.
    ctx.target_max_block_size = 800

    ds = ray.data.range(1000, parallelism=1).map_batches(lambda x: x, batch_size=1000)
    assert ds.materialize().num_blocks() == 10

    # A single row is already larger than the target block
    # size.
    ctx.target_max_block_size = 4
    assert ds.materialize().num_blocks() == 1000


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
