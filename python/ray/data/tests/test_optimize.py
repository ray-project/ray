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
    ds = ds.experimental_lazy()
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
            ds = ds.experimental_lazy()
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
    ds = ray.data.range(10).experimental_lazy()
    ds = ds.map(lambda x: x + 1)
    ds = ds.random_shuffle()
    for s in ds._plan._stages_before_snapshot:
        assert s.ray_remote_args == {}, s.ray_remote_args
    for s in ds._plan._stages_after_snapshot:
        assert s.ray_remote_args == {}, s.ray_remote_args
    _, _, optimized_stages = ds._plan._optimize()
    assert len(optimized_stages) == 1, optimized_stages
    assert optimized_stages[0].ray_remote_args == {"scheduling_strategy": "SPREAD"}


def test_execution_preserves_original(ray_start_regular_shared):
    ds = ray.data.range(10).map(lambda x: x + 1).experimental_lazy()
    ds1 = ds.map(lambda x: x + 1)
    assert ds1._plan._snapshot_blocks is not None
    assert len(ds1._plan._stages_after_snapshot) == 1
    ds2 = ds1.fully_executed()
    # Confirm that snapshot blocks and stages on original lazy dataset have not changed.
    assert ds1._plan._snapshot_blocks is not None
    assert len(ds1._plan._stages_after_snapshot) == 1
    # Confirm that UUID on executed dataset is properly set.
    assert ds2._get_uuid() == ds1._get_uuid()
    # Check content.
    assert ds2.take() == list(range(2, 12))
    # Check original lazy dataset content.
    assert ds1.take() == list(range(2, 12))
    # Check source lazy dataset content.
    # TODO(Clark): Uncomment this when we add new block clearing semantics.
    # assert ds.take() == list(range(1, 11))


def _assert_has_stages(stages, stage_names):
    assert len(stages) == len(stage_names)
    for stage, name in zip(stages, stage_names):
        assert stage.name == name


def test_stage_linking(ray_start_regular_shared):
    # NOTE: This tests the internals of `ExecutionPlan`, which is bad practice. Remove
    # this test once we have proper unit testing of `ExecutionPlan`.
    # Test eager dataset.
    ds = ray.data.range(10)
    assert len(ds._plan._stages_before_snapshot) == 0
    assert len(ds._plan._stages_after_snapshot) == 0
    assert len(ds._plan._last_optimized_stages) == 0
    ds = ds.map(lambda x: x + 1)
    _assert_has_stages(ds._plan._stages_before_snapshot, ["map"])
    assert len(ds._plan._stages_after_snapshot) == 0
    _assert_has_stages(ds._plan._last_optimized_stages, ["read->map"])

    # Test lazy dataset.
    ds = ray.data.range(10).experimental_lazy()
    assert len(ds._plan._stages_before_snapshot) == 0
    assert len(ds._plan._stages_after_snapshot) == 0
    assert len(ds._plan._last_optimized_stages) == 0
    ds = ds.map(lambda x: x + 1)
    assert len(ds._plan._stages_before_snapshot) == 0
    _assert_has_stages(ds._plan._stages_after_snapshot, ["map"])
    assert ds._plan._last_optimized_stages is None
    ds = ds.fully_executed()
    _assert_has_stages(ds._plan._stages_before_snapshot, ["map"])
    assert len(ds._plan._stages_after_snapshot) == 0
    _assert_has_stages(ds._plan._last_optimized_stages, ["read->map"])


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
        for block in super()._read_stream(f, path, **reader_args):
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


@pytest.mark.skip(reason="reusing base data not enabled")
@pytest.mark.parametrize("with_shuffle", [True, False])
@pytest.mark.parametrize("enable_dynamic_splitting", [True, False])
def test_optimize_lazy_reuse_base_data(
    ray_start_regular_shared, local_path, enable_dynamic_splitting, with_shuffle
):
    context = DatasetContext.get_current()
    context.block_splitting_enabled = enable_dynamic_splitting

    num_blocks = 4
    dfs = [pd.DataFrame({"one": list(range(i, i + 4))}) for i in range(num_blocks)]
    paths = [os.path.join(local_path, f"test{i}.csv") for i in range(num_blocks)]
    for df, path in zip(dfs, paths):
        df.to_csv(path, index=False)
    counter = Counter.remote()
    source = MySource(counter)
    ds = ray.data.read_datasource(source, parallelism=4, paths=paths)
    num_reads = ray.get(counter.get.remote())
    assert num_reads == 1, num_reads
    ds = ds.experimental_lazy()
    ds = ds.map(lambda x: x)
    if with_shuffle:
        ds = ds.random_shuffle()
    ds.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == num_blocks, num_reads


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
