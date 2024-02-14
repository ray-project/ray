import os
from typing import List

import numpy as np
import pandas as pd
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data import Dataset
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.data.tests.util import column_udf, extract_values
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
    def __init__(self, paths, counter):
        super().__init__(paths)

        self.counter = counter

    def _read_stream(self, f, path: str):
        count = self.counter.increment.remote()
        ray.get(count)
        for block in super()._read_stream(f, path):
            yield block


def expect_stages(pipe, num_stages_expected, stage_names):
    stats = pipe.stats()
    for name in stage_names:
        name = " " + name + ":"
        assert name in stats, (name, stats)
    if isinstance(pipe, Dataset):
        pass
    else:
        assert (
            len(pipe._optimized_stages) == num_stages_expected
        ), pipe._optimized_stages


def dummy_map(x):
    """Dummy function used in calls to map_batches in these tests."""
    return x


def test_memory_sanity(shutdown_only):
    info = ray.init(num_cpus=1, object_store_memory=500e6)
    ds = ray.data.range(10)
    ds = ds.map(lambda x: {"data": np.ones(100 * 1024 * 1024, dtype=np.uint8)})
    ds.materialize()
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


def test_memory_release(shutdown_only):
    info = ray.init(num_cpus=1, object_store_memory=1500e6)
    ds = ray.data.range(10)

    # Should get fused into single operator.
    ds = ds.map(lambda x: {"data": np.ones(100 * 1024 * 1024, dtype=np.uint8)})
    ds = ds.map(lambda x: {"data": np.ones(100 * 1024 * 1024, dtype=np.uint8)})
    ds = ds.map(lambda x: {"data": np.ones(100 * 1024 * 1024, dtype=np.uint8)})
    ds.materialize()
    meminfo = memory_summary(info.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


@pytest.mark.skip(reason="Flaky, see https://github.com/ray-project/ray/issues/24757")
def test_memory_release_shuffle(shutdown_only):
    # TODO(ekl) why is this flaky? Due to eviction delay?
    error = None
    for trial in range(3):
        print("Try", trial)
        try:
            info = ray.init(num_cpus=1, object_store_memory=1800e6)
            ds = ray.data.range(10)

            # Should get fused into single stage.
            ds = ds.map(lambda x: {"data": np.ones(100 * 1024 * 1024, dtype=np.uint8)})
            ds.random_shuffle().materialize()
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
        row["one"] += 1
        return row

    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path = os.path.join(local_path, "test.csv")
    df.to_csv(path, index=False, storage_options={})
    read_counter = Counter.remote()
    source = MySource(path, read_counter)

    # Test that fan-out of a lazy dataset results in re-execution up to the datasource,
    # due to block move semantics.
    ds = ray.data.read_datasource(source, parallelism=1)
    ds1 = ds.map(inc)
    ds2 = ds1.map(inc)
    ds3 = ds1.map(inc)
    # Test content.
    assert ds2.materialize().take() == [
        {"one": 3, "two": "a"},
        {"one": 4, "two": "b"},
        {"one": 5, "two": "c"},
    ]
    assert ds3.materialize().take() == [
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
        return {"item": x["item"] + 1}

    # The source data shouldn't be cleared since it's non-lazy.
    ds = ray.data.from_items(list(range(10)))
    ds1 = ds.map(inc)
    ds2 = ds1.map(inc)
    ds3 = ds1.map(inc)
    # Test content.
    assert extract_values("item", ds2.materialize().take()) == list(range(2, 12))
    assert extract_values("item", ds3.materialize().take()) == list(range(2, 12))
    # Test that first map is executed twice.
    assert ray.get(map_counter.get.remote()) == 2 * 10 + 10 + 10

    ray.get(map_counter.reset.remote())
    # The source data shouldn't be cleared since it's non-lazy.
    ds = ray.data.from_items(list(range(10)))
    # Add extra transformation after being lazy.
    ds = ds.map(inc)
    ds1 = ds.map(inc)
    ds2 = ds.map(inc)
    # Test content.
    assert extract_values("item", ds1.materialize().take()) == list(range(2, 12))
    assert extract_values("item", ds2.materialize().take()) == list(range(2, 12))
    # Test that first map is executed twice, because ds1.materialize()
    # clears up the previous snapshot blocks, and ds2.materialize()
    # has to re-execute ds.map(inc) again.
    assert ray.get(map_counter.get.remote()) == 2 * 10 + 10 + 10


def test_spread_hint_inherit(ray_start_regular_shared):
    ds = ray.data.range(10)
    ds = ds.map(column_udf("id", lambda x: x + 1))
    ds = ds.random_shuffle()

    shuffle_op = ds._plan._logical_plan.dag
    read_op = shuffle_op.input_dependencies[0].input_dependencies[0]
    assert read_op._ray_remote_args == {"scheduling_strategy": "SPREAD"}


def test_optimize_reorder(ray_start_regular_shared):
    ds = ray.data.range(10).randomize_block_order().map_batches(dummy_map).materialize()
    print("Stats", ds.stats())
    expect_stages(
        ds,
        2,
        ["ReadRange->MapBatches(dummy_map)", "RandomizeBlockOrder"],
    )

    ds2 = (
        ray.data.range(10)
        .randomize_block_order()
        .repartition(10)
        .map_batches(dummy_map)
        .materialize()
    )
    expect_stages(
        ds2,
        3,
        ["ReadRange", "RandomizeBlockOrder", "Repartition", "MapBatches(dummy_map)"],
    )


def test_write_fusion(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "out")
    ds = ray.data.range(100).map_batches(lambda x: x)
    ds.write_csv(path)
    stats = ds._write_ds.stats()
    assert "ReadRange->MapBatches(<lambda>)->Write" in stats, stats

    ds = (
        ray.data.range(100)
        .map_batches(lambda x: x)
        .random_shuffle()
        .map_batches(lambda x: x)
    )
    ds.write_csv(path)
    stats = ds._write_ds.stats()
    assert "ReadRange->MapBatches(<lambda>)" in stats, stats
    assert "RandomShuffle" in stats, stats
    assert "MapBatches(<lambda>)->Write" in stats, stats


def test_write_doesnt_reorder_randomize_block(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "out")
    ds = ray.data.range(100).randomize_block_order().map_batches(lambda x: x)
    ds.write_csv(path)
    stats = ds._write_ds.stats()

    # The randomize_block_order will switch order with the following map_batches,
    # but not the tailing write operator.
    assert "ReadRange->MapBatches(<lambda>)" in stats, stats
    assert "RandomizeBlockOrder" in stats, stats
    assert "Write" in stats, stats


@pytest.mark.skip(reason="reusing base data not enabled")
@pytest.mark.parametrize("with_shuffle", [True, False])
def test_optimize_lazy_reuse_base_data(
    ray_start_regular_shared, local_path, enable_dynamic_splitting, with_shuffle
):
    num_blocks = 4
    dfs = [pd.DataFrame({"one": list(range(i, i + 4))}) for i in range(num_blocks)]
    paths = [os.path.join(local_path, f"test{i}.csv") for i in range(num_blocks)]
    for df, path in zip(dfs, paths):
        df.to_csv(path, index=False)
    counter = Counter.remote()
    source = MySource(paths, counter)
    ds = ray.data.read_datasource(source, parallelism=4)
    num_reads = ray.get(counter.get.remote())
    assert num_reads == 1, num_reads
    ds = ds.map(column_udf("id", lambda x: x))
    if with_shuffle:
        ds = ds.random_shuffle()
    ds.take()
    num_reads = ray.get(counter.get.remote())
    assert num_reads == num_blocks, num_reads


def test_require_preserve_order(ray_start_regular_shared):
    ds = ray.data.range(100).map_batches(lambda x: x).sort()
    assert ds._plan.require_preserve_order()
    ds2 = ray.data.range(100).map_batches(lambda x: x).zip(ds)
    assert ds2._plan.require_preserve_order()
    ds3 = ray.data.range(100).map_batches(lambda x: x).repartition(10)
    assert not ds3._plan.require_preserve_order()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
