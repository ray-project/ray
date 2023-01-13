import os
import time
from typing import Tuple

import pytest
import pandas as pd
import numpy as np

import ray
from ray.data import dataset
from ray.data._internal.arrow_block import ArrowRow
from ray.data.context import DatasetContext, WARN_PREFIX, OK_PREFIX
from ray.data.dataset import Dataset
from ray.data.dataset_pipeline import DatasetPipeline

from ray.tests.conftest import *  # noqa


class MockLogger:
    def __init__(self):
        self.warnings = []
        self.infos = []

    def warning(self, msg):
        self.warnings.append(msg)
        print("warning:", msg)

    def info(self, msg):
        self.infos.append(msg)
        print("info:", msg)

    def debug(self, msg):
        print("debug:", msg)


def test_warnings(shutdown_only):
    ray.init(num_cpus=2)

    # Test parallelism warning.
    dataset.logger = MockLogger()
    ray.data.range(10, parallelism=10).window(blocks_per_window=1)
    print(dataset.logger.warnings)
    print(dataset.logger.infos)
    assert dataset.logger.warnings == [
        f"{WARN_PREFIX} This pipeline's parallelism is limited by its blocks per "
        "window to "
        "~1 concurrent tasks per window. To maximize "
        "performance, increase the blocks per window to at least 2. This "
        "may require increasing the base dataset's parallelism and/or "
        "adjusting the windowing parameters."
    ]
    assert dataset.logger.infos == [
        "Created DatasetPipeline with 10 windows: 8b min, 8b max, 8b mean",
        "Blocks per window: 1 min, 1 max, 1 mean",
        f"{OK_PREFIX} This pipeline's windows likely fit in object store memory "
        "without spilling.",
    ]

    try:
        res_dict = ray.cluster_resources()
        res_dict["object_store_memory"] = 1000
        old = ray.cluster_resources
        ray.cluster_resources = lambda: res_dict

        # Test window memory warning.
        dataset.logger = MockLogger()
        ray.data.range(100000, parallelism=100).window(blocks_per_window=10)
        print(dataset.logger.warnings)
        print(dataset.logger.infos)
        assert dataset.logger.warnings == [
            f"{WARN_PREFIX} This pipeline's windows are ~0.08MiB in size each and "
            "may not fit in "
            "object store memory without spilling. To improve performance, "
            "consider reducing the size of each window to 250b or less."
        ]
        assert dataset.logger.infos == [
            "Created DatasetPipeline with 10 windows: 0.08MiB min, 0.08MiB max, "
            "0.08MiB mean",
            "Blocks per window: 10 min, 10 max, 10 mean",
            f"{OK_PREFIX} This pipeline's per-window parallelism is high enough "
            "to fully "
            "utilize the cluster.",
        ]

        # Test warning on both.
        dataset.logger = MockLogger()
        ray.data.range(100000, parallelism=1).window(bytes_per_window=100000)
        print(dataset.logger.warnings)
        print(dataset.logger.infos)
        assert dataset.logger.warnings == [
            f"{WARN_PREFIX} This pipeline's parallelism is limited by its blocks "
            "per window "
            "to ~1 concurrent tasks per window. To maximize performance, increase "
            "the blocks per window to at least 2. This may require increasing the "
            "base dataset's parallelism and/or adjusting the windowing parameters.",
            f"{WARN_PREFIX} This pipeline's windows are ~0.76MiB in size each and may "
            "not fit "
            "in object store memory without spilling. To improve performance, "
            "consider reducing the size of each window to 250b or less.",
        ]
        assert dataset.logger.infos == [
            "Created DatasetPipeline with 1 windows: 0.76MiB min, 0.76MiB max, "
            "0.76MiB mean",
            "Blocks per window: 1 min, 1 max, 1 mean",
        ]
    finally:
        ray.cluster_resources = old

    # Test no warning.
    dataset.logger = MockLogger()
    ray.data.range(10, parallelism=10).window(blocks_per_window=10)
    print(dataset.logger.warnings)
    print(dataset.logger.infos)
    assert dataset.logger.warnings == []
    assert dataset.logger.infos == [
        "Created DatasetPipeline with 1 windows: 80b min, 80b max, 80b mean",
        "Blocks per window: 10 min, 10 max, 10 mean",
        f"{OK_PREFIX} This pipeline's per-window parallelism is high enough to fully "
        "utilize the cluster.",
        f"{OK_PREFIX} This pipeline's windows likely fit in object store memory "
        "without spilling.",
    ]


def test_pipeline_actors(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)
    pipe = (
        ray.data.range(3)
        .repeat(10)
        .map(lambda x: x + 1)
        .map(lambda x: x + 1, compute="actors", num_gpus=1)
    )

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


def test_pipeline_is_parallel(shutdown_only):
    ray.init(num_cpus=4)
    ds = ray.data.range(10)

    @ray.remote(num_cpus=0)
    class ParallelismTracker:
        def __init__(self):
            self.in_progress = 0
            self.max_in_progress = 0

        def inc(self):
            self.in_progress += 1
            if self.in_progress > self.max_in_progress:
                self.max_in_progress = self.in_progress

        def dec(self):
            self.in_progress = 0

        def get_max(self):
            return self.max_in_progress

    tracker = ParallelismTracker.remote()

    def sleep(x):
        ray.get(tracker.inc.remote())
        time.sleep(0.1)
        ray.get(tracker.dec.remote())
        return x

    pipe = ds.window(blocks_per_window=1)
    # Shuffle in between to prevent fusion.
    pipe = pipe.map(sleep).random_shuffle_each_window().map(sleep)
    for i, v in enumerate(pipe.iter_rows()):
        print(i, v)
    assert ray.get(tracker.get_max.remote()) > 1


def test_window_by_bytes(ray_start_regular_shared):
    with pytest.raises(ValueError):
        ray.data.range_table(10).window(blocks_per_window=2, bytes_per_window=2)

    pipe = ray.data.range_table(10000000, parallelism=100).window(blocks_per_window=2)
    assert str(pipe) == "DatasetPipeline(num_windows=50, num_stages=2)"

    pipe = ray.data.range_table(10000000, parallelism=100).window(
        bytes_per_window=10 * 1024 * 1024
    )
    assert str(pipe) == "DatasetPipeline(num_windows=8, num_stages=2)"
    dss = list(pipe.iter_datasets())
    assert len(dss) == 8, dss
    for ds in dss[:-1]:
        assert ds.num_blocks() in [12, 13]

    pipe = ray.data.range_table(10000000, parallelism=100).window(bytes_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=100, num_stages=2)"
    for ds in pipe.iter_datasets():
        assert ds.num_blocks() == 1

    pipe = ray.data.range_table(10000000, parallelism=100).window(bytes_per_window=1e9)
    assert str(pipe) == "DatasetPipeline(num_windows=1, num_stages=2)"
    for ds in pipe.iter_datasets():
        assert ds.num_blocks() == 100

    # Test creating from non-lazy BlockList.
    pipe = (
        ray.data.range_table(10000000, parallelism=100)
        .map_batches(lambda x: x)
        .window(bytes_per_window=10 * 1024 * 1024)
    )
    assert str(pipe) == "DatasetPipeline(num_windows=8, num_stages=1)"

    context = DatasetContext.get_current()
    old = context.optimize_fuse_read_stages
    try:
        context.optimize_fuse_read_stages = False
        dataset = ray.data.range(10).window(bytes_per_window=1)
        assert dataset.take(10) == list(range(10))
    finally:
        context.optimize_fuse_read_stages = old


def test_epoch(ray_start_regular_shared):
    # Test dataset repeat.
    pipe = ray.data.range(5).map(lambda x: x * 2).repeat(3).map(lambda x: x * 2)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 4, 8, 12, 16], [0, 4, 8, 12, 16], [0, 4, 8, 12, 16]]

    # Test dataset pipeline repeat.
    pipe = ray.data.range(3).window(blocks_per_window=2).repeat(3)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2], [0, 1, 2], [0, 1, 2]]

    # Test max epochs.
    pipe = ray.data.range(3).window(blocks_per_window=2).repeat(3)
    results = [p.take() for p in pipe.iter_epochs(2)]
    assert results == [[0, 1, 2], [0, 1, 2]]

    # Test nested repeat.
    pipe = ray.data.range(5).repeat(2).repeat(2)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2, 3, 4, 0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]]

    # Test preserve_epoch=True.
    pipe = ray.data.range(5).repeat(2).rewindow(blocks_per_window=2)
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]]

    # Test preserve_epoch=False.
    pipe = (
        ray.data.range(5).repeat(2).rewindow(blocks_per_window=2, preserve_epoch=False)
    )
    results = [p.take() for p in pipe.iter_epochs()]
    assert results == [[0, 1, 2, 3], [4, 0, 1, 2, 3, 4]]


# https://github.com/ray-project/ray/issues/20394
def test_unused_epoch(ray_start_regular_shared):
    pipe = ray.data.from_items([0, 1, 2, 3, 4]).repeat(3).random_shuffle_each_window()

    for i, epoch in enumerate(pipe.iter_epochs()):
        print("Epoch", i)


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
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(10, parallelism=10)

    pipe = ds.window(blocks_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=2)"
    assert pipe.count() == 10
    pipe = ds.window(blocks_per_window=1)
    pipe.show()

    pipe = ds.window(blocks_per_window=1).map(lambda x: x).map(lambda x: x)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=4)"
    assert pipe.take() == list(range(10))

    pipe = (
        ds.window(blocks_per_window=1).map(lambda x: x).flat_map(lambda x: [x, x + 1])
    )
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=4)"
    assert pipe.count() == 20

    pipe = ds.window(blocks_per_window=1).filter(lambda x: x % 2 == 0)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=3)"
    assert pipe.count() == 5

    pipe = ds.window(blocks_per_window=999)
    assert str(pipe) == "DatasetPipeline(num_windows=1, num_stages=2)"
    assert pipe.count() == 10

    pipe = ds.repeat(10)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=2)"
    assert pipe.count() == 100
    pipe = ds.repeat(10)
    assert pipe.sum() == 450
    pipe = ds.repeat(10)
    assert len(pipe.take_all()) == 100


def test_window(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(10, parallelism=10)
    pipe = ds.window(blocks_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=2)"
    pipe = pipe.rewindow(blocks_per_window=3)
    assert str(pipe) == "DatasetPipeline(num_windows=None, num_stages=1)"
    datasets = list(pipe.iter_datasets())
    assert len(datasets) == 4
    assert datasets[0].take() == [0, 1, 2]
    assert datasets[1].take() == [3, 4, 5]
    assert datasets[2].take() == [6, 7, 8]
    assert datasets[3].take() == [9]

    ds = ray.data.range(10, parallelism=10)
    pipe = ds.window(blocks_per_window=5)
    assert str(pipe) == "DatasetPipeline(num_windows=2, num_stages=2)"
    pipe = pipe.rewindow(blocks_per_window=3)
    assert str(pipe) == "DatasetPipeline(num_windows=None, num_stages=1)"
    datasets = list(pipe.iter_datasets())
    assert len(datasets) == 4
    assert datasets[0].take() == [0, 1, 2]
    assert datasets[1].take() == [3, 4, 5]
    assert datasets[2].take() == [6, 7, 8]
    assert datasets[3].take() == [9]


def test_repeat(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(5, parallelism=5)
    pipe = ds.window(blocks_per_window=1)
    assert str(pipe) == "DatasetPipeline(num_windows=5, num_stages=2)"
    pipe = pipe.repeat(2)
    assert str(pipe) == "DatasetPipeline(num_windows=10, num_stages=2)"
    assert pipe.take() == (list(range(5)) + list(range(5)))

    ds = ray.data.range(5)
    pipe = ds.window(blocks_per_window=1)
    pipe = pipe.repeat()
    assert str(pipe) == "DatasetPipeline(num_windows=inf, num_stages=2)"
    assert len(pipe.take(99)) == 99

    pipe = ray.data.range(5).repeat()
    with pytest.raises(ValueError):
        pipe.repeat()


def test_from_iterable(ray_start_regular_shared):
    pipe = DatasetPipeline.from_iterable(
        [lambda: ray.data.range(3), lambda: ray.data.range(2)]
    )
    assert pipe.take() == [0, 1, 2, 0, 1]


def test_repeat_forever(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(10)
    pipe = ds.repeat()
    assert str(pipe) == "DatasetPipeline(num_windows=inf, num_stages=2)"
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


def test_iter_batches_basic(ray_start_regular_shared):
    pipe = ray.data.range(10, parallelism=10).window(blocks_per_window=2)
    batches = list(pipe.iter_batches(batch_size=None))
    assert len(batches) == 10
    assert all(len(e) == 1 for e in batches)


def test_iter_batches_batch_across_windows(ray_start_regular_shared):
    # 3 windows, each containing 3 blocks, each containing 3 rows.
    pipe = ray.data.range(27, parallelism=9).window(blocks_per_window=3)
    # 4-row batches, with batches spanning both blocks and windows.
    batches = list(pipe.iter_batches(batch_size=4))
    assert len(batches) == 7, batches
    assert all(len(e) == 4 for e in batches[:-1])
    assert len(batches[-1]) == 3


def test_iter_datasets(ray_start_regular_shared):
    pipe = ray.data.range(10, parallelism=10).window(blocks_per_window=2)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 5

    pipe = ray.data.range(10, parallelism=10).window(blocks_per_window=5)
    ds = list(pipe.iter_datasets())
    assert len(ds) == 2


def test_foreach_window(ray_start_regular_shared):
    pipe = ray.data.range(5).window(blocks_per_window=2)
    pipe = pipe.foreach_window(lambda ds: ds.map(lambda x: x * 2))
    assert pipe.take() == [0, 2, 4, 6, 8]


def test_schema(ray_start_regular_shared):
    pipe = ray.data.range(5).window(blocks_per_window=2)
    assert pipe.schema() == int


def test_schema_peek(ray_start_regular_shared):
    # Multiple datasets
    pipe = ray.data.range(6, parallelism=6).window(blocks_per_window=2)
    assert pipe.schema() == int
    assert pipe._first_dataset is not None
    dss = list(pipe.iter_datasets())
    assert len(dss) == 3, dss
    assert pipe._first_dataset is None
    assert pipe.schema() == int

    # Only 1 dataset
    pipe = ray.data.range(1).window(blocks_per_window=2)
    assert pipe.schema() == int
    assert pipe._first_dataset is not None
    dss = list(pipe.iter_datasets())
    assert len(dss) == 1, dss
    assert pipe._first_dataset is None
    assert pipe.schema() == int

    # Empty datasets
    pipe = (
        ray.data.range(6, parallelism=6)
        .filter(lambda x: x < 0)
        .window(blocks_per_window=2)
    )
    assert pipe.schema() is None
    assert pipe._first_dataset is not None
    dss = list(pipe.iter_datasets())
    assert len(dss) == 3, dss
    assert pipe._first_dataset is None
    assert pipe.schema() is None


def test_split(ray_start_regular_shared):
    pipe = ray.data.range(3).map(lambda x: x + 1).repeat(10)

    @ray.remote(num_cpus=0)
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
    pipe = ray.data.range(n).map(lambda x: x + 1).repeat(2)

    @ray.remote(num_cpus=0)
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
        np.array(outs, dtype=object),
        np.array([[1, 2, 1, 2], [3, 4, 5, 3, 4, 5], [6, 7, 8, 6, 7, 8]], dtype=object),
    )


def _prepare_dataset_to_write(tmp_dir: str) -> Tuple[Dataset[ArrowRow], pd.DataFrame]:
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = ds.window(blocks_per_window=1)
    os.mkdir(tmp_dir)
    ds._set_uuid("data")
    return (ds, df)


def test_json_write(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_json_dir")
    ds, df = _prepare_dataset_to_write(path)
    ds.write_json(path)
    path1 = os.path.join(path, "data_000000_000000.json")
    path2 = os.path.join(path, "data_000001_000000.json")
    dfds = pd.concat([pd.read_json(path1, lines=True), pd.read_json(path2, lines=True)])
    assert df.equals(dfds)


def test_csv_write(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_csv_dir")
    ds, df = _prepare_dataset_to_write(path)
    ds.write_csv(path)
    path1 = os.path.join(path, "data_000000_000000.csv")
    path2 = os.path.join(path, "data_000001_000000.csv")
    dfds = pd.concat([pd.read_csv(path1), pd.read_csv(path2)])
    assert df.equals(dfds)


def test_parquet_write(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_parquet_dir")
    ds, df = _prepare_dataset_to_write(path)
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


def test_sort_each_window(ray_start_regular_shared):
    pipe = (
        ray.data.range(12, parallelism=12)
        .window(blocks_per_window=3)
        .sort_each_window()
    )
    assert pipe.take() == list(range(12))

    pipe = (
        ray.data.range(12, parallelism=12)
        .window(blocks_per_window=3)
        .sort_each_window(descending=True)
    )
    assert pipe.take() == [2, 1, 0, 5, 4, 3, 8, 7, 6, 11, 10, 9]

    pipe = (
        ray.data.range(12, parallelism=12)
        .window(blocks_per_window=3)
        .sort_each_window(key=lambda x: -x, descending=True)
    )
    assert pipe.take() == list(range(12))


def test_randomize_block_order_each_window(ray_start_regular_shared):
    pipe = ray.data.range(12).repartition(6).window(blocks_per_window=3)
    pipe = pipe.randomize_block_order_each_window(seed=0)
    assert pipe.take() == [0, 1, 4, 5, 2, 3, 6, 7, 10, 11, 8, 9]


def test_add_column(ray_start_regular_shared):
    df = pd.DataFrame({"col1": [1, 2, 3]})
    ds = ray.data.from_pandas(df)
    pipe = ds.repeat()
    assert pipe.add_column("col2", lambda x: x["col1"] + 1).take(1) == [
        {"col1": 1, "col2": 2}
    ]


def test_select_columns(ray_start_regular_shared):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds = ray.data.from_pandas(df)
    pipe = ds.repeat()
    assert pipe.select_columns(["col2", "col3"]).take(1) == [{"col2": 2, "col3": 3}]


def test_drop_columns(ray_start_regular_shared):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds = ray.data.from_pandas(df)
    pipe = ds.repeat()
    assert pipe.drop_columns(["col2"]).take(1) == [{"col1": 1, "col3": 3}]


def test_random_shuffle_each_window_with_custom_resource(ray_start_cluster):
    ray.shutdown()
    cluster = ray_start_cluster
    # Create two nodes which have different custom resources.
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    # Run pipeline in "bar" nodes.
    pipe = ray.data.read_datasource(
        ray.data.datasource.RangeDatasource(),
        parallelism=10,
        n=1000,
        block_format="list",
        ray_remote_args={"resources": {"bar": 1}},
    ).repeat(3)
    pipe = pipe.random_shuffle_each_window(resources={"bar": 1})
    for batch in pipe.iter_batches():
        pass
    assert "1 nodes used" in pipe.stats()
    assert "2 nodes used" not in pipe.stats()


def test_in_place_transformation_doesnt_clear_objects(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3, 4, 5, 6])

    def verify_integrity(p):
        # The pipeline's output blocks are from original dataset (i.e. not created
        # by the pipeline itself), so those blocks must not be cleared -- verified
        # below by re-reading the dataset.
        for b in p.iter_batches():
            pass
        # Verify the integrity of the blocks of original dataset.
        assert ds.take_all() == [1, 2, 3, 4, 5, 6]

    verify_integrity(ds.repeat(10).randomize_block_order_each_window())
    verify_integrity(
        ds.repeat(10)
        .randomize_block_order_each_window()
        .randomize_block_order_each_window()
    )
    # Mix in-place and non-in place transforms.
    verify_integrity(
        ds.repeat(10).map_batches(lambda x: x).randomize_block_order_each_window()
    )
    verify_integrity(
        ds.repeat(10).randomize_block_order_each_window().map_batches(lambda x: x)
    )


def test_in_place_transformation_split_doesnt_clear_objects(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3, 4, 5, 6], parallelism=3)

    @ray.remote
    def consume(p):
        for batch in p.iter_batches():
            pass

    def verify_integrity(p):
        # Divide 3 blocks ([1, 2], [3, 4] and [5, 6]) into 2 splits equally must
        # have one block got splitted. Since the blocks are not created by the
        # pipeline (randomize_block_order_each_window() didn't create new
        # blocks since it's in-place), so the block splitting will not clear
        # the input block -- verified below by re-reading the dataset.
        splits = p.split(2, equal=True)
        ray.get([consume.remote(p) for p in splits])
        # Verify the integrity of the blocks of original dataset
        assert ds.take_all() == [1, 2, 3, 4, 5, 6]

    verify_integrity(ds.repeat(10).randomize_block_order_each_window())
    verify_integrity(
        ds.repeat(10)
        .randomize_block_order_each_window()
        .randomize_block_order_each_window()
    )
    verify_integrity(
        ds.repeat(10).randomize_block_order_each_window().rewindow(blocks_per_window=1)
    )
    # Mix in-place and non-in place transforms.
    verify_integrity(
        ds.repeat(10)
        .randomize_block_order_each_window()
        .randomize_block_order_each_window()
        .map_batches(lambda x: x)
    )
    verify_integrity(
        ds.repeat(10)
        .map_batches(lambda x: x)
        .randomize_block_order_each_window()
        .randomize_block_order_each_window()
    )


def test_pipeline_executor_cannot_serialize_once_started(ray_start_regular_shared):
    class Iterable:
        def __init__(self, iter):
            self._iter = iter

        def __next__(self):
            ds = next(self._iter)
            return lambda: ds

    p1 = ray.data.range(10).repeat()
    p2 = DatasetPipeline.from_iterable(Iterable(p1.iter_datasets()))
    with pytest.raises(RuntimeError) as error:
        p2.split(2)
    assert "PipelineExecutor is not serializable once it has started" in str(error)


def test_if_blocks_owned_by_consumer(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3, 4, 5, 6], parallelism=3)
    assert not ds._plan.execute()._owned_by_consumer
    assert not ds.randomize_block_order()._plan.execute()._owned_by_consumer
    assert ds.map_batches(lambda x: x)._plan.execute()._owned_by_consumer
    assert (
        not ds.map_batches(lambda x: x)
        ._plan.execute(cache_output_blocks=True)
        ._owned_by_consumer
    )

    def verify_blocks(pipe, owned_by_consumer):
        for ds in pipe.iter_datasets():
            assert ds._plan.execute()._owned_by_consumer == owned_by_consumer

    verify_blocks(ds.repeat(1), False)
    verify_blocks(ds.repeat(1).randomize_block_order_each_window(), False)
    verify_blocks(ds.repeat(1).randomize_block_order_each_window().repeat(2), False)
    verify_blocks(
        ds.repeat(1).randomize_block_order_each_window().map_batches(lambda x: x), True
    )
    verify_blocks(ds.repeat(1).map_batches(lambda x: x), True)
    verify_blocks(ds.repeat(1).map(lambda x: x), True)
    verify_blocks(ds.repeat(1).filter(lambda x: x > 3), True)
    verify_blocks(ds.repeat(1).sort_each_window(), True)
    verify_blocks(ds.repeat(1).random_shuffle_each_window(), True)
    verify_blocks(ds.repeat(1).repartition_each_window(2), True)
    verify_blocks(ds.repeat(1).rewindow(blocks_per_window=1), False)
    verify_blocks(ds.repeat(1).rewindow(blocks_per_window=1).repeat(2), False)
    verify_blocks(
        ds.repeat(1).map_batches(lambda x: x).rewindow(blocks_per_window=1), True
    )
    verify_blocks(
        ds.repeat(1).rewindow(blocks_per_window=1).map_batches(lambda x: x), True
    )

    @ray.remote
    def consume(pipe, owned_by_consumer):
        verify_blocks(pipe, owned_by_consumer)

    splits = ds.repeat(1).split(2)
    ray.get([consume.remote(splits[0], False), consume.remote(splits[1], False)])

    splits = ds.repeat(1).randomize_block_order_each_window().split(2)
    ray.get([consume.remote(splits[0], False), consume.remote(splits[1], False)])

    splits = ds.repeat(1).map_batches(lambda x: x).split(2)
    ray.get([consume.remote(splits[0], True), consume.remote(splits[1], True)])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
