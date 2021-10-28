import os
import time

import pytest
import pandas as pd
import numpy as np

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

    pipe = ray.data.range(2).window(blocks_per_window=1)
    pipe = pipe.map(block_on_ones)
    assert pipe.take(1) == [0]


def test_epoch(ray_start_regular_shared):
    # Test dataset repeat.
    pipe = ray.data.range(5).map(lambda x: x * 2).repeat(3).map(
        lambda x: x * 2)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 4, 8, 12, 16], [0, 4, 8, 12, 16], [0, 4, 8, 12, 16]]

    # Test dataset pipeline repeat.
    pipe = ray.data.range(3).window(blocks_per_window=2).repeat(3)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2], [0, 1, 2], [0, 1, 2]]

    # Test nested repeat.
    pipe = ray.data.range(5).repeat(2).repeat(2)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2, 3, 4, 0, 1, 2, 3, 4],
                       [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]]

    # Test preserve_epoch=True.
    pipe = ray.data.range(5).repeat(2).rewindow(blocks_per_window=2)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]]

    # Test preserve_epoch=False.
    pipe = ray.data.range(5).repeat(2).rewindow(
        blocks_per_window=2, preserve_epoch=False)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2, 3], [4, 0, 1, 2, 3, 4]]


def test_cannot_read_twice(ray_start_regular_shared):
    ds = ray.data.range(10)
    pipe = ds.window(blocks_per_window=1)
    assert pipe.count() == 10
    with pytest.raises(RuntimeError):
        pipe.count()
    with pytest.raises(RuntimeError):
        next(pipe.iter_batches())
    with pytest.raises(RuntimeError):
        pipe.map(lambda x: x).count()
    with pytest.raises(RuntimeError):
        pipe.split(2)


def test_basic_pipeline(ray_start_regular_shared):
    ds = ray.data.range(10)

    pipe = ds.window(blocks_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=1)"
    assert pipe.count() == 10

    pipe = ds.window(blocks_per_window=1).map(lambda x: x).map(lambda x: x)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=3)"
    assert pipe.take() == list(range(10))

    pipe = ds.window(blocks_per_window=999)
    assert str(pipe) == "DatasetPipeline(num_windows=1, num_stages=1)"
    assert pipe.count() == 10

    pipe = ds.repeat(10)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=1)"
    assert pipe.count() == 100
    pipe = ds.repeat(10)
    assert pipe.sum() == 450


def test_window(ray_start_regular_shared):
    ds = ray.data.range(10)
    pipe = ds.window(blocks_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=1)"
    pipe = pipe.rewindow(blocks_per_window=3)
    assert str(pipe) == "DatasetPipeline(num_windows=None, num_stages=1)"
    datasets = list(pipe.iter_datasets())
    assert len(datasets) == 4
    assert datasets[0].take() == [0, 1, 2]
    assert datasets[1].take() == [3, 4, 5]
    assert datasets[2].take() == [6, 7, 8]
    assert datasets[3].take() == [9]

    ds = ray.data.range(10)
    pipe = ds.window(blocks_per_window=5)
    assert str(pipe) == "DatasetPipeline(num_windows=2, num_stages=1)"
    pipe = pipe.rewindow(blocks_per_window=3)
    assert str(pipe) == "DatasetPipeline(num_windows=None, num_stages=1)"
    datasets = list(pipe.iter_datasets())
    assert len(datasets) == 4
    assert datasets[0].take() == [0, 1, 2]
    assert datasets[1].take() == [3, 4, 5]
    assert datasets[2].take() == [6, 7, 8]
    assert datasets[3].take() == [9]


def test_repeat(ray_start_regular_shared):
    ds = ray.data.range(5)
    pipe = ds.window(blocks_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=5, num_stages=1)"
    pipe = pipe.repeat(2)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=1)"
    assert pipe.take() == (list(range(5)) + list(range(5)))

    ds = ray.data.range(5)
    pipe = ds.window(blocks_per_window=1)
    pipe = pipe.repeat()
    assert str(pipe) == "DatasetPipeline(num_windows=inf, num_stages=1)"
    assert len(pipe.take(99)) == 99

    pipe = ray.data.range(5).repeat()
    with pytest.raises(ValueError):
        pipe.repeat()


def test_from_iterable(ray_start_regular_shared):
    pipe = DatasetPipeline.from_iterable(
        [lambda: ray.data.range(3), lambda: ray.data.range(2)])
    assert pipe.take() == [0, 1, 2, 0, 1]


def test_repeat_forever(ray_start_regular_shared):
    ds = ray.data.range(10)
    pipe = ds.repeat()
    assert str(pipe) == "DatasetPipeline(num_windows=inf, num_stages=1)"
    for i, v in enumerate(pipe.iter_rows()):
        assert v == i % 10, (v, i, i % 10)
        if i > 1000:
            break


def test_repartition(ray_start_regular_shared):
    pipe = ray.data.range(10).repeat(10)
    assert pipe.repartition_each_window(1).sum() == 450
    pipe = ray.data.range(10).repeat(10)
    assert pipe.repartition_each_window(10).sum() == 450
    pipe = ray.data.range(10).repeat(10)
    assert pipe.repartition_each_window(100).sum() == 450


def test_iter_batches(ray_start_regular_shared):
    pipe = ray.data.range(10).window(blocks_per_window=2)
    batches = list(pipe.iter_batches())
    assert len(batches) == 10
    assert all(len(e) == 1 for e in batches)


def test_iter_datasets(ray_start_regular_shared):
    pipe = ray.data.range(10).window(blocks_per_window=2)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 5

    pipe = ray.data.range(10).window(blocks_per_window=5)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 2


def test_foreach_window(ray_start_regular_shared):
    pipe = ray.data.range(5).window(blocks_per_window=2)
    pipe = pipe.foreach_window(lambda ds: ds.map(lambda x: x * 2))
    assert pipe.take() == [0, 2, 4, 6, 8]


def test_schema(ray_start_regular_shared):
    pipe = ray.data.range(5).window(blocks_per_window=2)
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


def test_split_at_indices(ray_start_regular_shared):
    indices = [2, 5]
    n = 8
    pipe = ray.data.range(n) \
        .map(lambda x: x + 1) \
        .repeat(2)

    @ray.remote
    def consume(shard, i):
        total = 0
        out = []
        for row in shard.iter_rows():
            total += 1
            out.append(row)
        if i == 0:
            assert total == 2 * indices[i]
        elif i < len(indices):
            assert total == 2 * (indices[i] - indices[i - 1])
        else:
            assert total == 2 * (n - indices[i - 1])
        return out

    shards = pipe.split_at_indices(indices)
    refs = [consume.remote(s, i) for i, s in enumerate(shards)]
    outs = ray.get(refs)
    np.testing.assert_equal(
        np.array(outs, dtype=np.object),
        np.array(
            [[1, 2, 1, 2], [3, 4, 5, 3, 4, 5], [6, 7, 8, 6, 7, 8]],
            dtype=np.object))


def test_parquet_write(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = ds.window(blocks_per_window=1)
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds._set_uuid("data")
    ds.write_parquet(path)
    path1 = os.path.join(path, "data_000000_000000.parquet")
    path2 = os.path.join(path, "data_000001_000000.parquet")
    dfds = pd.concat([pd.read_parquet(path1), pd.read_parquet(path2)])
    assert df.equals(dfds)


def test_infinity_of_pipeline(ray_start_regular_shared):
    ds = ray.data.range(3)
    pipe = ds.repeat()
    assert float("inf") == pipe._length
    pipe = ds.window(blocks_per_window=2)
    assert float("inf") != pipe._length
    pipe = ds.repeat(3)
    assert float("inf") != pipe._length
    assert float("inf") == pipe.repeat()._length

    # ensure infinite length is transitive
    pipe = ds.repeat().rewindow(blocks_per_window=2)
    assert float("inf") == pipe._length
    pipe = ds.repeat().split(2)[0]
    assert float("inf") == pipe._length
    pipe = ds.repeat().foreach_window(lambda x: x)
    assert float("inf") == pipe._length


def test_count_sum_on_infinite_pipeline(ray_start_regular_shared):
    ds = ray.data.range(3)

    pipe = ds.repeat()
    assert float("inf") == pipe._length
    with pytest.raises(ValueError):
        pipe.count()

    pipe = ds.repeat()
    assert float("inf") == pipe._length
    with pytest.raises(ValueError):
        pipe.sum()

    pipe = ds.repeat(3)
    assert 9 == pipe.count()

    pipe = ds.repeat(3)
    assert 9 == pipe.sum()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
