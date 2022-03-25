import pytest
import numpy as np
import pandas as pd
import os

import ray
from ray.data.context import DatasetContext
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.internal.internal_api import memory_summary

from ray.tests.conftest import *  # noqa


def expect_stages(pipe, num_stages_expected, stage_names):
    stats = pipe.stats()
    for name in stage_names:
        name = " " + name + ":"
        assert name in stats, (name, stats)
    assert len(pipe._optimized_stages) == num_stages_expected, pipe._optimized_stages


def test_memory_sanity(shutdown_only):
    info = ray.init(num_cpus=1, object_store_memory=500e6)
    ds = ray.data.range(10)
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    meminfo = memory_summary(info.address_info["address"], stats_only=True)

    # Sanity check spilling is happening as expected.
    assert "Spilled" in meminfo, meminfo


def test_memory_release_eager(shutdown_only):
    info = ray.init(num_cpus=1, object_store_memory=1500e6)
    ds = ray.data.range(10)

    # Round 1.
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    meminfo = memory_summary(info.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo

    # Round 2.
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    meminfo = memory_summary(info["address"], stats_only=True)
    # TODO(ekl) we can add this back once we clear inputs on eager exec as well.
    # assert "Spilled" not in meminfo, meminfo


def test_memory_release_lazy(shutdown_only):
    info = ray.init(num_cpus=1, object_store_memory=1500e6)
    ds = ray.data.range(10)

    # Should get fused into single stage.
    ds = ds._experimental_lazy()
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds.fully_executed()
    meminfo = memory_summary(info.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_memory_release_lazy_shuffle(shutdown_only):
    # TODO(ekl) why is this flaky? Due to eviction delay?
    error = None
    for trial in range(3):
        print("Try", trial)
        try:
            info = ray.init(num_cpus=1, object_store_memory=1800e6)
            ds = ray.data.range(10)

            # Should get fused into single stage.
            ds = ds._experimental_lazy()
            ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
            ds.random_shuffle().fully_executed()
            meminfo = memory_summary(info.address_info["address"], stats_only=True)
            assert "Spilled" not in meminfo, meminfo
            return
        except Exception as e:
            error = e
            print("Failed", e)
        finally:
            ray.shutdown()
    raise error


def test_spread_hint_inherit(ray_start_regular_shared):
    ds = ray.data.range(10)._experimental_lazy()
    ds = ds.map(lambda x: x + 1)
    ds = ds.random_shuffle()
    for s in ds._plan._stages:
        assert s.ray_remote_args == {}, s.ray_remote_args
    ds._plan._optimize()
    assert len(ds._plan._stages) == 1, ds._plan._stages
    assert ds._plan._stages[0].ray_remote_args == {"scheduling_strategy": "SPREAD"}


def test_optimize_fuse(ray_start_regular_shared):
    context = DatasetContext.get_current()

    def build_pipe():
        pipe = ray.data.range(3).window(blocks_per_window=1).repeat(2)
        pipe = pipe.map_batches(lambda x: x)
        pipe = pipe.map_batches(lambda x: x)
        pipe = pipe.random_shuffle_each_window()
        results = [sorted(p.take()) for p in pipe.iter_epochs()]
        assert results == [[0, 1, 2], [0, 1, 2]], results
        return pipe

    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_fuse_shuffle_stages = True
    expect_stages(
        build_pipe(),
        1,
        ["read->map_batches->map_batches->random_shuffle_map", "random_shuffle_reduce"],
    )

    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = False
    context.optimize_fuse_shuffle_stages = True
    expect_stages(
        build_pipe(),
        1,
        [
            "read",
            "map_batches->map_batches->random_shuffle_map",
            "random_shuffle_reduce",
        ],
    )

    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = False
    context.optimize_fuse_shuffle_stages = False
    expect_stages(
        build_pipe(),
        2,
        [
            "read",
            "map_batches->map_batches",
            "random_shuffle_map",
            "random_shuffle_reduce",
        ],
    )

    context.optimize_fuse_stages = False
    context.optimize_fuse_read_stages = False
    context.optimize_fuse_shuffle_stages = False
    expect_stages(
        build_pipe(),
        3,
        [
            "read",
            "map_batches",
            "map_batches",
            "random_shuffle_map",
            "random_shuffle_reduce",
        ],
    )


def test_optimize_incompatible_stages(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_fuse_shuffle_stages = True

    pipe = ray.data.range(3).repeat(2)
    pipe = pipe.map_batches(lambda x: x, compute="actors")
    pipe = pipe.map_batches(lambda x: x, compute="tasks")
    pipe = pipe.random_shuffle_each_window()
    pipe.take()
    expect_stages(
        pipe,
        3,
        [
            "read",
            "map_batches",
            "map_batches->random_shuffle_map",
            "random_shuffle_reduce",
        ],
    )

    pipe = ray.data.range(3).repeat(2)
    pipe = pipe.map_batches(lambda x: x, compute="tasks")
    pipe = pipe.map_batches(lambda x: x, num_cpus=0.75)
    pipe = pipe.random_shuffle_each_window()
    pipe.take()
    expect_stages(
        pipe,
        3,
        [
            "read->map_batches",
            "map_batches",
            "random_shuffle_map",
            "random_shuffle_reduce",
        ],
    )


@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def get(self):
        return self.value

    def reset(self):
        self.value = 0


class MySource(CSVDatasource):
    def __init__(self, counter):
        self.counter = counter

    def _read_stream(self, f, path: str, **reader_args):
        count = self.counter.increment.remote()
        ray.get(count)
        for block in CSVDatasource._read_stream(self, f, path, **reader_args):
            yield block


def test_optimize_reread_base_data(ray_start_regular_shared, local_path):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_fuse_shuffle_stages = True

    # Re-read on.
    N = 4
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(local_path, "test1.csv")
    df1.to_csv(path1, index=False, storage_options={})
    counter = Counter.remote()
    source = MySource(counter)
    ds1 = ray.data.read_datasource(source, parallelism=1, paths=path1)
    pipe = ds1.repeat(N)
    pipe.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == N + 1, num_reads

    # Re-read off.
    context.optimize_fuse_read_stages = False
    ray.get(counter.reset.remote())
    ds1 = ray.data.read_datasource(source, parallelism=1, paths=path1)
    pipe = ds1.repeat(N)
    pipe.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == 1, num_reads


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
