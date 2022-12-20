import random
from typing import Optional

import pytest

import ray
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig
from ray.data import Dataset, DatasetPipeline
from ray.data.preprocessors import BatchMapper
from ray.train.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


class TestBasic(DataParallelTrainer):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True),
        "test": DatasetConfig(),
        "baz": DatasetConfig(split=True),
    }

    def __init__(
        self, num_workers: int, expect_ds: bool, expect_sizes: Optional[dict], **kwargs
    ):
        def train_loop_per_worker():
            data_shard = session.get_dataset_shard("train")
            if expect_ds:
                assert isinstance(data_shard, Dataset), data_shard
            else:
                assert isinstance(data_shard, DatasetPipeline), data_shard
            for k, v in expect_sizes.items():
                shard = session.get_dataset_shard(k)
                if v == -1:
                    assert shard is None, shard
                else:
                    if isinstance(shard, DatasetPipeline):
                        assert next(shard.iter_epochs()).count() == v, shard
                    else:
                        assert shard.count() == v, shard

        kwargs.pop("scaling_config", None)
        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=num_workers),
            **kwargs,
        )


class TestWildcard(TestBasic):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True),
        "*": DatasetConfig(split=True),
    }


def test_basic(ray_start_4_cpus):
    ds = ray.data.range_table(10)

    # Single worker basic case.
    test = TestBasic(
        1,
        True,
        {"train": 10, "test": 10},
        dataset_config={},
        datasets={"train": ds, "test": ds},
    )
    test.fit()

    # Single worker, no test ds.
    test = TestBasic(
        1, True, {"train": 10, "test": -1}, dataset_config={}, datasets={"train": ds}
    )
    test.fit()

    # Two workers, train split.
    test = TestBasic(
        2, True, {"train": 5, "test": 10}, datasets={"train": ds, "test": ds}
    )
    test.fit()

    # Two workers, wild split.
    test = TestWildcard(
        2, True, {"train": 5, "wild": 5}, datasets={"train": ds, "wild": ds}
    )
    test.fit()

    # Two workers, both split.
    test = TestBasic(
        2,
        True,
        {"train": 5, "test": 5},
        dataset_config={"test": DatasetConfig(split=True)},
        datasets={"train": ds, "test": ds},
    )
    # Test get config.
    assert test.get_dataset_config()["train"].split
    assert test.get_dataset_config()["test"].split
    test.fit()


def test_error(ray_start_4_cpus):
    ds = ray.data.range_table(10)

    # Missing required dataset.
    with pytest.raises(ValueError):
        TestBasic(
            1, True, {"train": 10, "test": 10}, dataset_config={}, datasets={"test": ds}
        )

    # Missing optional dataset is OK.
    test = TestBasic(
        1,
        True,
        {"train": 10},
        dataset_config={},
        datasets={"train": ds},
        preprocessor=BatchMapper(lambda x: x, batch_format="pandas"),
    )
    test.fit()

    # Extra dataset.
    with pytest.raises(ValueError):
        TestBasic(
            1,
            True,
            {"train": 10, "test": 10},
            dataset_config={},
            datasets={"train": ds, "blah": ds},
        )


def test_use_stream_api_config(ray_start_4_cpus):
    ds = ray.data.range_table(10)

    # Single worker basic case.
    test = TestBasic(
        1,
        False,
        {"train": 10, "test": 10},
        dataset_config={"train": DatasetConfig(use_stream_api=True)},
        datasets={"train": ds, "test": ds},
    )
    test.fit()

    # Two worker split pipeline.
    test = TestBasic(
        2,
        False,
        {"train": 5, "test": 10},
        dataset_config={"train": DatasetConfig(use_stream_api=True)},
        datasets={"train": ds, "test": ds},
    )
    test.fit()


def test_fit_transform_config(ray_start_4_cpus):
    ds = ray.data.range_table(10)

    def drop_odd_pandas(batch):
        return batch[batch["value"] % 2 == 0]

    def drop_odd_numpy(batch):
        arr = batch["value"]
        return arr[arr % 2 == 0]

    prep_pandas = BatchMapper(drop_odd_pandas, batch_format="pandas")
    prep_numpy = BatchMapper(drop_odd_numpy, batch_format="numpy")

    # Single worker basic case.
    test = TestBasic(
        1,
        True,
        {"train": 5, "test": 5},
        dataset_config={},
        datasets={"train": ds, "test": ds},
        preprocessor=prep_pandas,
    )
    test.fit()

    # Single worker basic case.
    test = TestBasic(
        1,
        True,
        {"train": 5, "test": 5},
        dataset_config={},
        datasets={"train": ds, "test": ds},
        preprocessor=prep_numpy,
    )
    test.fit()

    # No transform for test.
    test = TestBasic(
        1,
        True,
        {"train": 5, "test": 10},
        dataset_config={"test": DatasetConfig(transform=False)},
        datasets={"train": ds, "test": ds},
        preprocessor=prep_pandas,
    )
    test.fit()


class TestStream(DataParallelTrainer):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True, use_stream_api=True),
    }

    def __init__(self, check_results_fn, **kwargs):
        def train_loop_per_worker():
            data_shard = session.get_dataset_shard("train")
            assert isinstance(data_shard, DatasetPipeline), data_shard
            results = []
            for epoch in data_shard.iter_epochs(2):
                results.append(epoch.take())
            check_results_fn(data_shard, results)

        kwargs.pop("scaling_config", None)
        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
            **kwargs,
        )


class TestBatch(DataParallelTrainer):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True, use_stream_api=False),
    }

    def __init__(self, check_results_fn, **kwargs):
        def train_loop_per_worker():
            data_shard = session.get_dataset_shard("train")
            assert isinstance(data_shard, Dataset), data_shard
            results = data_shard.take()
            check_results_fn(data_shard, results)

        kwargs.pop("scaling_config", None)
        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
            **kwargs,
        )


def test_stream_inf_window_cache_prep(ray_start_4_cpus):
    def checker(shard, results):
        results = [sorted(r) for r in results]
        assert len(results[0]) == 5, results
        assert results[0] == results[1], results
        stats = shard.stats()
        assert str(shard) == "DatasetPipeline(num_windows=inf, num_stages=1)", shard
        assert "Stage 1 read->map_batches: 1/1 blocks executed " in stats, stats

    def rand(x):
        return [random.random() for _ in range(len(x))]

    prep = BatchMapper(rand, batch_format="pandas")
    ds = ray.data.range_table(5, parallelism=1)
    test = TestStream(
        checker,
        preprocessor=prep,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(stream_window_size=-1)},
    )
    test.fit()


def test_stream_finite_window_nocache_prep(ray_start_4_cpus):
    def rand(x):
        return [random.random() for _ in range(len(x))]

    prep = BatchMapper(rand, batch_format="pandas")
    ds = ray.data.range_table(5, parallelism=1)

    # Test the default 1GiB window size.
    def checker(shard, results):
        results = [sorted(r) for r in results]
        assert int(results[0][0]) != results[0][0]
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert str(shard) == "DatasetPipeline(num_windows=inf, num_stages=1)", shard
        assert (
            "Stage 1 read->randomize_block_order->map_batches: 1/1 blocks executed "
            in stats
        ), stats

    test = TestStream(
        checker,
        preprocessor=prep,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig()},
    )
    test.fit()

    # Test a smaller window size.
    def checker(shard, results):
        results = [sorted(r) for r in results]
        assert int(results[0][0]) != results[0][0]
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert str(shard) == "DatasetPipeline(num_windows=inf, num_stages=1)", shard
        assert (
            "Stage 1 read->randomize_block_order->map_batches: 1/1 blocks executed "
            in stats
        ), stats

    test = TestStream(
        checker,
        preprocessor=prep,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(stream_window_size=10)},
    )
    test.fit()


def test_global_shuffle(ray_start_4_cpus):
    def checker(shard, results):
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert str(shard) == "DatasetPipeline(num_windows=inf, num_stages=1)", shard
        assert "Stage 1 read->randomize_block_order->random_shuffle" in stats, stats

    ds = ray.data.range_table(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(global_shuffle=True)},
    )
    test.fit()

    def checker(shard, results):
        assert len(results) == 5, results
        stats = shard.stats()
        assert "Stage 1 read->random_shuffle" in stats, stats

    ds = ray.data.range_table(5)
    test = TestBatch(
        checker,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(global_shuffle=True)},
    )
    test.fit()


def test_randomize_block_order(ray_start_4_cpus):
    def checker(shard, results):
        stats = shard.stats()
        assert "randomize_block_order: 5/5 blocks executed in 0s" in stats, stats

    ds = ray.data.range_table(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
    )
    test.fit()

    def checker(shard, results):
        stats = shard.stats()
        assert "randomize_block_order" not in stats, stats

    ds = ray.data.range_table(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(randomize_block_order=False)},
    )
    test.fit()

    def checker(shard, results):
        assert len(results) == 5, results
        stats = shard.stats()
        assert "randomize_block_order: 5/5 blocks executed" in stats, stats

    ds = ray.data.range_table(5)
    test = TestBatch(
        checker,
        datasets={"train": ds},
    )
    test.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
