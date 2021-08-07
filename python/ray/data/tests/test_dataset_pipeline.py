import os
import time

import pytest
import pandas as pd

import ray
from ray.data.dataset_pipeline import DatasetPipeline

from ray.tests.conftest import *  # noqa


def test_pipeline_actors(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)
    pipe = ray.data.range(3) \
        .repeat(10) \
        .map(lambda x: x + 1) \
        .map(lambda x: x + 1, compute="actors", num_gpus=1)

    assert sorted(pipe.take(999)) == sorted([2, 3, 4] * 10)


def test_incremental_take(shutdown_only):
    ray.init(num_cpus=2)

    # Can read incrementally even if future results are delayed.
    def block_on_ones(x: int) -> int:
        if x == 1:
            time.sleep(999999)
        return x

    pipe = ray.data.range(2).pipeline(parallelism=1)
    pipe = pipe.map(block_on_ones)
    assert pipe.take(1) == [0]


def test_basic_pipeline(ray_start_regular_shared):
    ds = ray.data.range(10)

    pipe = ds.pipeline(parallelism=1)
    assert str(pipe) == "DatasetPipeline(length=10, num_stages=1)"
    for _ in range(2):
        assert pipe.count() == 10

    pipe = ds.pipeline(parallelism=1).map(lambda x: x).map(lambda x: x)
    assert str(pipe) == "DatasetPipeline(length=10, num_stages=3)"
    assert pipe.take() == list(range(10))

    pipe = ds.pipeline(parallelism=999)
    assert str(pipe) == "DatasetPipeline(length=1, num_stages=1)"
    assert pipe.count() == 10

    pipe = ds.repeat(10)
    assert str(pipe) == "DatasetPipeline(length=10, num_stages=1)"
    for _ in range(2):
        assert pipe.count() == 100
    assert pipe.sum() == 450


def test_from_iterable(ray_start_regular_shared):
    pipe = DatasetPipeline.from_iterable(
        [lambda: ray.data.range(3), lambda: ray.data.range(2)])
    assert pipe.take() == [0, 1, 2, 0, 1]


def test_repeat_forever(ray_start_regular_shared):
    ds = ray.data.range(10)
    pipe = ds.repeat()
    assert str(pipe) == "DatasetPipeline(length=None, num_stages=1)"
    for i, v in enumerate(pipe.iter_rows()):
        assert v == i % 10, (v, i, i % 10)
        if i > 1000:
            break


def test_repartition(ray_start_regular_shared):
    pipe = ray.data.range(10).repeat(10)
    assert pipe.repartition(1).sum() == 450
    assert pipe.repartition(10).sum() == 450
    assert pipe.repartition(100).sum() == 450


def test_iter_batches(ray_start_regular_shared):
    pipe = ray.data.range(10).pipeline(parallelism=2)
    batches = list(pipe.iter_batches())
    assert len(batches) == 10
    assert all(len(e) == 1 for e in batches)


def test_iter_datasets(ray_start_regular_shared):
    pipe = ray.data.range(10).pipeline(parallelism=2)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 5

    pipe = ray.data.range(10).pipeline(parallelism=5)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 2


def test_foreach_dataset(ray_start_regular_shared):
    pipe = ray.data.range(5).pipeline(parallelism=2)
    pipe = pipe.foreach_dataset(lambda ds: ds.map(lambda x: x * 2))
    assert pipe.take() == [0, 2, 4, 6, 8]


def test_schema(ray_start_regular_shared):
    pipe = ray.data.range(5).pipeline(parallelism=2)
    assert pipe.schema() == int


def test_split(ray_start_regular_shared):
    pipe = ray.data.range(3) \
        .map(lambda x: x + 1) \
        .repeat(10)

    @ray.remote
    def consume(shard, i):
        total = 0
        for row in shard.iter_rows():
            total += 1
            assert row == i + 1, row
        assert total == 10, total

    shards = pipe.split(3)
    refs = [consume.remote(s, i) for i, s in enumerate(shards)]
    ray.get(refs)


def test_parquet_write(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([ray.put(df1), ray.put(df2)])
    ds = ds.pipeline(parallelism=1)
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds._set_uuid("data")
    ds.write_parquet(path)
    path1 = os.path.join(path, "data_000000_000000.parquet")
    path2 = os.path.join(path, "data_000001_000000.parquet")
    dfds = pd.concat([pd.read_parquet(path1), pd.read_parquet(path2)])
    assert df.equals(dfds)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
