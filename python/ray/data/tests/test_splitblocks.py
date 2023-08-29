import numpy as np
import pytest

import ray
from ray.data._internal.planner.plan_read_op import _splitrange
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


def test_small_file_split(ray_start_10_cpus_shared):
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


def test_large_file_additional_split(ray_start_10_cpus_shared, tmp_path):
    ctx = ray.data.context.DataContext.get_current()
    ctx.target_max_block_size = 10 * 1024 * 1024

    # ~100MiB of tensor data
    ds = ray.data.range_tensor(1000, shape=(10000,))
    ds.repartition(1).write_parquet(tmp_path)

    ds = ray.data.read_parquet(tmp_path, parallelism=1)
    assert ds.num_blocks() == 1
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
