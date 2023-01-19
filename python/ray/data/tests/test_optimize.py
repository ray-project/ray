import os
from typing import List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data import Dataset
from ray.data.block import BlockMetadata
from ray.data.context import DatasetContext
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.tests.conftest import *  # noqa


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


def expect_stages(pipe, num_stages_expected, stage_names):
    stats = pipe.stats()
    for name in stage_names:
        name = " " + name + ":"
        assert name in stats, (name, stats)
    if isinstance(pipe, Dataset):
        assert (
            len(pipe._plan._stages_before_snapshot) == num_stages_expected
        ), pipe._plan._stages_before_snapshot
    else:
        assert (
            len(pipe._optimized_stages) == num_stages_expected
        ), pipe._optimized_stages


def test_memory_sanity(shutdown_only):
    info = ray.init(num_cpus=1, object_store_memory=500e6)
    ds = ray.data.range(10)
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds.fully_executed()
    meminfo = memory_summary(info.address_info["address"], stats_only=True)

    # Sanity check spilling is happening as expected.
    assert "Spilled" in meminfo, meminfo


class OnesSource(Datasource):
    def prepare_read(
        self,
        parallelism: int,
        n_per_block: int,
    ) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        meta = BlockMetadata(
            num_rows=1,
            size_bytes=n_per_block,
            schema=None,
            input_files=None,
            exec_stats=None,
        )

        for _ in range(parallelism):
            read_tasks.append(
                ReadTask(lambda: [[np.ones(n_per_block, dtype=np.uint8)]], meta)
            )
        return read_tasks


@pytest.mark.skip(reason="Flaky, see https://github.com/ray-project/ray/issues/24757")
@pytest.mark.parametrize("lazy_input", [True, False])
def test_memory_release_pipeline(shutdown_only, lazy_input):
    context = DatasetContext.get_current()
    # Disable stage fusion so we can keep reads and maps from being fused together,
    # since we're trying to test multi-stage memory releasing here.
    context.optimize_fuse_stages = False
    # This object store allocation can hold at most 1 copy of the transformed dataset.
    if lazy_input:
        object_store_memory = 3000e6
    else:
        object_store_memory = 3000e6

    n = 10
    info = ray.init(num_cpus=n, object_store_memory=object_store_memory)
    if lazy_input:
        ds = ray.data.read_datasource(
            OnesSource(),
            parallelism=n,
            n_per_block=100 * 1024 * 1024,
        )
    else:
        ds = ray.data.from_items(list(range(n)), parallelism=n)

    # Create a single-window pipeline.
    pipe = ds.window(blocks_per_window=n)

    # Round 1.
    def gen(x):
        import time

        # TODO(Clark): Remove this sleep once we have fixed memory pressure handling.
        time.sleep(2)
        if isinstance(x, np.ndarray):
            return x
        else:
            return np.ones(100 * 1024 * 1024, dtype=np.uint8)

    pipe = pipe.map(gen)

    def inc(x):
        import time

        # TODO(Clark): Remove this sleep once we have fixed memory pressure handling.
        time.sleep(2)
        return x + 1

    num_rounds = 10
    for _ in range(num_rounds):
        pipe = pipe.map(inc)

    for block in pipe.iter_batches(batch_size=None):
        for arr in block:
            np.testing.assert_equal(
                arr,
                np.ones(100 * 1024 * 1024, dtype=np.uint8) + num_rounds,
            )
    meminfo = memory_summary(info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_memory_release_lazy(shutdown_only):
    context = DatasetContext.get_current()
    # Ensure that stage fusion is enabled.
    context.optimize_fuse_stages = True
    info = ray.init(num_cpus=1, object_store_memory=1500e6)
    ds = ray.data.range(10)

    # Should get fused into single stage.
    ds = ds.lazy()
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds = ds.map(lambda x: np.ones(100 * 1024 * 1024, dtype=np.uint8))
    ds.fully_executed()
    meminfo = memory_summary(info.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


@pytest.mark.skip(reason="Flaky, see https://github.com/ray-project/ray/issues/24757")
def test_memory_release_lazy_shuffle(shutdown_only):
    # TODO(ekl) why is this flaky? Due to eviction delay?
    error = None
    for trial in range(3):
        print("Try", trial)
        try:
            info = ray.init(num_cpus=1, object_store_memory=1800e6)
            ds = ray.data.range(10)

            # Should get fused into single stage.
            ds = ds.lazy()
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


def test_lazy_fanout(shutdown_only, local_path):
    ray.init(num_cpus=1)
    map_counter = Counter.remote()

    def inc(row):
        map_counter.increment.remote()
        row = row.as_pydict()
        row["one"] += 1
        return row

    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path = os.path.join(local_path, "test.csv")
    df.to_csv(path, index=False, storage_options={})
    read_counter = Counter.remote()
    source = MySource(read_counter)

    # Test that fan-out of a lazy dataset results in re-execution up to the datasource,
    # due to block move semantics.
    ds = ray.data.read_datasource(source, parallelism=1, paths=path)
    ds = ds.lazy()
    ds1 = ds.map(inc)
    ds2 = ds1.map(inc)
    ds3 = ds1.map(inc)
    # Test content.
    assert ds2.fully_executed().take() == [
        {"one": 3, "two": "a"},
        {"one": 4, "two": "b"},
        {"one": 5, "two": "c"},
    ]
    assert ds3.fully_executed().take() == [
        {"one": 3, "two": "a"},
        {"one": 4, "two": "b"},
        {"one": 5, "two": "c"},
    ]
    # Test that data is read twice.
    assert ray.get(read_counter.get.remote()) == 2
    # Test that first map is executed twice.
    assert ray.get(map_counter.get.remote()) == 2 * 3 + 3 + 3

    # Test that fan-out of a lazy dataset with a non-lazy datasource results in
    # re-execution up to the datasource, due to block move semantics.
    ray.get(map_counter.reset.remote())

    def inc(x):
        map_counter.increment.remote()
        return x + 1

    # The source data shouldn't be cleared since it's non-lazy.
    ds = ray.data.from_items(list(range(10)))
    ds = ds.lazy()
    ds1 = ds.map(inc)
    ds2 = ds1.map(inc)
    ds3 = ds1.map(inc)
    # Test content.
    assert ds2.fully_executed().take() == list(range(2, 12))
    assert ds3.fully_executed().take() == list(range(2, 12))
    # Test that first map is executed twice.
    assert ray.get(map_counter.get.remote()) == 2 * 10 + 10 + 10

    ray.get(map_counter.reset.remote())
    # The source data shouldn't be cleared since it's non-lazy.
    ds = ray.data.from_items(list(range(10)))
    # Add extra transformation after being lazy.
    ds = ds.lazy()
    ds = ds.map(inc)
    ds1 = ds.map(inc)
    ds2 = ds.map(inc)
    # Test content.
    assert ds1.fully_executed().take() == list(range(2, 12))
    assert ds2.fully_executed().take() == list(range(2, 12))
    # Test that first map is executed twice, because ds1.fully_executed()
    # clears up the previous snapshot blocks, and ds2.fully_executed()
    # has to re-execute ds.map(inc) again.
    assert ray.get(map_counter.get.remote()) == 2 * 10 + 10 + 10


def test_spread_hint_inherit(ray_start_regular_shared):
    ds = ray.data.range(10).lazy()
    ds = ds.map(lambda x: x + 1)
    ds = ds.random_shuffle()
    for s in ds._plan._stages_before_snapshot:
        assert s.ray_remote_args == {}, s.ray_remote_args
    for s in ds._plan._stages_after_snapshot:
        assert s.ray_remote_args == {}, s.ray_remote_args
    _, _, optimized_stages = ds._plan._optimize()
    assert len(optimized_stages) == 1, optimized_stages
    assert optimized_stages[0].ray_remote_args == {"scheduling_strategy": "SPREAD"}


def _assert_has_stages(stages, stage_names):
    assert len(stages) == len(stage_names)
    for stage, name in zip(stages, stage_names):
        assert stage.name == name


def test_stage_linking(ray_start_regular_shared):
    # Test lazy dataset.
    ds = ray.data.range(10).lazy()
    assert len(ds._plan._stages_before_snapshot) == 0
    assert len(ds._plan._stages_after_snapshot) == 0
    assert ds._plan._last_optimized_stages is None
    ds = ds.map(lambda x: x + 1)
    assert len(ds._plan._stages_before_snapshot) == 0
    _assert_has_stages(ds._plan._stages_after_snapshot, ["map"])
    assert ds._plan._last_optimized_stages is None
    ds = ds.fully_executed()
    _assert_has_stages(ds._plan._stages_before_snapshot, ["map"])
    assert len(ds._plan._stages_after_snapshot) == 0
    _assert_has_stages(ds._plan._last_optimized_stages, ["read->map"])


def test_optimize_reorder(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_reorder_stages = True

    ds = ray.data.range(10).randomize_block_order().map_batches(lambda x: x)
    expect_stages(
        ds,
        2,
        ["read->map_batches", "randomize_block_order"],
    )

    ds2 = (
        ray.data.range(10)
        .randomize_block_order()
        .repartition(10)
        .map_batches(lambda x: x)
    )
    expect_stages(
        ds2,
        3,
        ["read->randomize_block_order", "repartition", "map_batches"],
    )


def test_window_randomize_fusion(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_reorder_stages = True

    pipe = ray.data.range(100).randomize_block_order().window().map_batches(lambda x: x)
    pipe.take()
    stats = pipe.stats()
    assert "read->randomize_block_order->map_batches" in stats, stats


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


def test_optimize_equivalent_remote_args(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_fuse_shuffle_stages = True

    equivalent_kwargs = [
        {},
        {"resources": {"blah": 0}},
        {"resources": {"blah": None}},
        {"num_cpus": None},
        {"num_cpus": 1},
        {"num_cpus": 1, "num_gpus": 0},
        {"num_cpus": 1, "num_gpus": None},
    ]

    for kwa in equivalent_kwargs:
        for kwb in equivalent_kwargs:
            print("CHECKING", kwa, kwb)
            pipe = ray.data.range(3).repeat(2)
            pipe = pipe.map_batches(lambda x: x, compute="tasks", **kwa)
            pipe = pipe.map_batches(lambda x: x, compute="tasks", **kwb)
            pipe.take()
            expect_stages(
                pipe,
                1,
                [
                    "read->map_batches->map_batches",
                ],
            )

    for kwa in equivalent_kwargs:
        for kwb in equivalent_kwargs:
            print("CHECKING", kwa, kwb)
            pipe = ray.data.range(3).repeat(2)
            pipe = pipe.map_batches(lambda x: x, compute="tasks", **kwa)
            pipe = pipe.random_shuffle_each_window(**kwb)
            pipe.take()
            expect_stages(
                pipe,
                1,
                [
                    "read->map_batches->random_shuffle_map",
                    "random_shuffle_reduce",
                ],
            )


def test_optimize_incompatible_stages(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_fuse_shuffle_stages = True

    pipe = ray.data.range(3).repeat(2)
    # Should get fused as long as their resource types are compatible.
    pipe = pipe.map_batches(lambda x: x, compute="actors")
    # Cannot fuse actors->tasks.
    pipe = pipe.map_batches(lambda x: x, compute="tasks")
    pipe = pipe.random_shuffle_each_window()
    pipe.take()
    expect_stages(
        pipe,
        2,
        [
            "read->map_batches",
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


def test_optimize_callable_classes(ray_start_regular_shared, tmp_path):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    context.optimize_fuse_read_stages = True
    context.optimize_fuse_shuffle_stages = True

    # Set up.
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))

    class CallableFn:
        def __init__(self, a, b=None):
            assert a == 1
            assert b == 2
            self.a = a
            self.b = b

        def __call__(self, x):
            return self.b * x + self.a

    # Test callable chain.
    fn_constructor_args = (ray.put(1),)
    fn_constructor_kwargs = {"b": ray.put(2)}
    pipe = (
        ray.data.read_parquet(str(tmp_path))
        .repeat(2)
        .map_batches(
            CallableFn,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
        .map_batches(
            CallableFn,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
    )
    ds_list = pipe.take()
    values = [s["one"] for s in ds_list]
    assert values == [7, 11, 15, 7, 11, 15]
    values = [s["two"] for s in ds_list]
    assert values == [11, 15, 19, 11, 15, 19]
    expect_stages(
        pipe,
        1,
        [
            "read->map_batches->map_batches",
        ],
    )

    # Test function + callable chain.
    fn_constructor_args = (ray.put(1),)
    fn_constructor_kwargs = {"b": ray.put(2)}
    pipe = (
        ray.data.read_parquet(str(tmp_path))
        .repeat(2)
        .map_batches(
            lambda df, a, b=None: b * df + a,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_args=(ray.put(1),),
            fn_kwargs={"b": ray.put(2)},
        )
        .map_batches(
            CallableFn,
            batch_size=1,
            batch_format="pandas",
            compute="actors",
            fn_constructor_args=fn_constructor_args,
            fn_constructor_kwargs=fn_constructor_kwargs,
        )
    )
    ds_list = pipe.take()
    values = [s["one"] for s in ds_list]
    assert values == [7, 11, 15, 7, 11, 15]
    values = [s["two"] for s in ds_list]
    assert values == [11, 15, 19, 11, 15, 19]
    expect_stages(
        pipe,
        1,
        [
            "read->map_batches->map_batches",
        ],
    )


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
    assert num_reads == N, num_reads

    ray.get(counter.reset.remote())
    ds1 = ray.data.read_datasource(source, parallelism=1, paths=path1)
    pipe = ds1.window(blocks_per_window=1).repeat(N)
    pipe.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == N, num_reads

    # Re-read off.
    context.optimize_fuse_read_stages = False
    ray.get(counter.reset.remote())
    ds1 = ray.data.read_datasource(source, parallelism=1, paths=path1)
    pipe = ds1.repeat(N)
    pipe.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == 1, num_reads

    ray.get(counter.reset.remote())
    ds1 = ray.data.read_datasource(source, parallelism=1, paths=path1)
    pipe = ds1.window(blocks_per_window=1).repeat(N)
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
    ds = ds.lazy()
    ds = ds.map(lambda x: x)
    if with_shuffle:
        ds = ds.random_shuffle()
    ds.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == num_blocks, num_reads


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
