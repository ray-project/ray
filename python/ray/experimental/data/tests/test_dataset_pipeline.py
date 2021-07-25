import pytest

import ray

from ray.tests.conftest import *  # noqa


def test_pipeline_actors(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)
    pipe = ray.experimental.data.range(3) \
        .map(lambda x: x + 1) \
        .map(lambda x: x + 1, compute="actors", num_gpus=1) \
        .repeat(3)

    assert sorted(pipe.take()) == sorted([2, 3, 4, 2, 3, 4, 2, 3, 4])


def test_basic_pipeline(ray_start_regular_shared):
    ds = ray.experimental.data.range(10)

    pipe = ds.pipeline(1)
    for _ in range(2):
        assert pipe.count() == 10

    pipe = ds.pipeline(1)
    assert pipe.take() == list(range(10))

    pipe = ds.pipeline(999)
    assert pipe.count() == 10

    pipe = ds.repeat(10)
    for _ in range(2):
        assert pipe.count() == 100

    pipe = ds.repeat(10)
    assert pipe.sum() == 450


def test_iter_batches(ray_start_regular_shared):
    pipe = ray.experimental.data.range(10).pipeline(2)
    batches = list(pipe.iter_batches())
    assert len(batches) == 10
    assert all(len(e) == 1 for e in batches)


def test_iter_datasets(ray_start_regular_shared):
    pipe = ray.experimental.data.range(10).pipeline(2)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 5

    pipe = ray.experimental.data.range(10).pipeline(5)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 2


def test_foreach_dataset(ray_start_regular_shared):
    pipe = ray.experimental.data.range(5).pipeline(2)
    pipe = pipe.foreach_dataset(lambda ds: ds.map(lambda x: x * 2))
    assert pipe.take() == [0, 2, 4, 6, 8]


def test_schema(ray_start_regular_shared):
    pipe = ray.experimental.data.range(5).pipeline(2)
    assert pipe.schema() == int


def test_split(ray_start_regular_shared):
    pipe = ray.experimental.data.range(3) \
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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
