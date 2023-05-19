import random
from typing import Optional

import numpy as np
import pytest

import ray
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig
from ray.air.util.check_ingest import make_local_dataset_iterator
from ray.data import DataIterator
from ray.data.preprocessor import Preprocessor
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
            assert isinstance(data_shard, DataIterator), data_shard
            for k, v in expect_sizes.items():
                shard = session.get_dataset_shard(k)
                if v == -1:
                    assert shard is None, shard
                else:
                    count = 0
                    for batch in shard.iter_batches():
                        count += len(batch)
                    assert count == v, shard

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
    ds = ray.data.range(10)

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
    ds = ray.data.range(10)

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
    ds = ray.data.range(10)

    # Single worker basic case.
    test = TestBasic(
        1,
        False,
        {"train": 10, "test": 10},
        dataset_config={"train": DatasetConfig(max_object_store_memory_fraction=1)},
        datasets={"train": ds, "test": ds},
    )
    test.fit()

    # Two worker split pipeline.
    test = TestBasic(
        2,
        False,
        {"train": 5, "test": 10},
        dataset_config={"train": DatasetConfig(max_object_store_memory_fraction=1)},
        datasets={"train": ds, "test": ds},
    )
    test.fit()


def test_fit_transform_config(ray_start_4_cpus):
    ds = ray.data.range(10)

    def drop_odd_pandas(batch):
        return batch[batch["id"] % 2 == 0]

    def drop_odd_numpy(batch):
        arr = batch["id"]
        return {"id": arr[arr % 2 == 0]}

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
        "train": DatasetConfig(
            split=True,
            required=True,
            # Use 30% of object store memory.
            max_object_store_memory_fraction=0.3,
        ),
    }

    def __init__(self, check_results_fn, **kwargs):
        kwargs.pop("scaling_config", None)
        super().__init__(
            train_loop_per_worker=lambda: self.train_loop_per_worker(
                session.get_dataset_shard("train"), check_results_fn
            ),
            scaling_config=ScalingConfig(num_workers=1),
            **kwargs,
        )

    @staticmethod
    def train_loop_per_worker(data_shard, check_results_fn):
        results = []
        for _ in range(2):
            result = []
            for batch in data_shard.iter_batches():
                for row in batch["id"]:
                    result.append(row)
            results.append(result)
        check_results_fn(data_shard, results)


class TestBatch(TestStream):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True),
    }


def test_stream_inf_window_cache_prep(ray_start_4_cpus):
    def checker(shard, results):
        results = [sorted(r) for r in results]
        assert len(results[0]) == 5, results
        # TODO(swang): Should modify the check to make sure that we are
        # applying the preprocessor on each epoch.
        assert results[0] == results[1], results
        stats = shard.stats()
        assert "Stage 1 ReadRange->BatchMapper: 1/1 blocks executed " in stats, stats

    def rand(x):
        x["id"] = x["id"].multiply(x["id"])
        return x

    prep = BatchMapper(rand, batch_format="pandas")
    ds = ray.data.range(5, parallelism=1)
    test = TestStream(
        checker,
        preprocessor=prep,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(max_object_store_memory_fraction=-1)},
    )
    test.fit()


def test_stream_finite_window_nocache_prep(ray_start_4_cpus):
    def rand(x):
        x["id"] = [random.random() for _ in range(len(x))]
        return x

    prep = BatchMapper(rand, batch_format="pandas")
    ds = ray.data.range(5, parallelism=1)

    # Test 50% object store memory..
    def checker(shard, results):
        results = [sorted(r) for r in results]
        assert int(results[0][0]) != results[0][0]
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert (
            "Stage 1 ReadRange->RandomizeBlockOrder->"
            "BatchMapper: 1/1 blocks executed " in stats
        ), stats

    test = TestStream(
        checker,
        preprocessor=prep,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(max_object_store_memory_fraction=0.5)},
    )
    test.fit()


def test_stream_transform_config(ray_start_4_cpus):
    """Tests that the preprocessor's transform config is
    respected when using the stream API."""
    batch_size = 2

    def check_batch(batch):
        assert isinstance(batch, dict)
        assert isinstance(batch["id"], np.ndarray)
        assert len(batch["id"]) == batch_size
        return batch

    prep = BatchMapper(check_batch, batch_format="numpy", batch_size=2)
    ds = ray.data.range(6, parallelism=1)

    test = TestStream(
        lambda *args: None,
        preprocessor=prep,
        datasets={"train": ds},
    )
    test.fit()


def test_global_shuffle(ray_start_4_cpus):
    def checker(shard, results):
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert "RandomizeBlockOrder->RandomShuffle" in stats, stats

    ds = ray.data.range(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(global_shuffle=True)},
    )
    test.fit()

    def checker(shard, results):
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert "Stage 1 ReadRange->RandomShuffle" in stats, stats

    ds = ray.data.range(5)
    test = TestBatch(
        checker,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(global_shuffle=True)},
    )
    test.fit()


def test_randomize_block_order(ray_start_4_cpus):
    def checker(shard, results):
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert "RandomizeBlockOrder: 5/5 blocks executed in" in stats, stats

    ds = ray.data.range(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
    )
    test.fit()

    def checker(shard, results):
        stats = shard.stats()
        assert "RandomizeBlockOrder" not in stats, stats

    ds = ray.data.range(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
        dataset_config={"train": DatasetConfig(randomize_block_order=False)},
    )
    test.fit()

    def checker(shard, results):
        assert len(results[0]) == 5, results
        # In streaming executor, the randomization in each epoch can be different, so
        # we eliminate the ordering in comparison.
        assert set(results[0]) == set(results[1]), results
        stats = shard.stats()
        assert "RandomizeBlockOrder: 5/5 blocks executed" in stats, stats

    ds = ray.data.range(5)
    test = TestBatch(
        checker,
        datasets={"train": ds},
    )
    test.fit()


def test_make_local_dataset_iterator(ray_start_4_cpus):
    def checker(shard, results):
        assert len(results[0]) == 5, results
        assert results[0] != results[1], results
        stats = shard.stats()
        assert "RandomizeBlockOrder: 5/5 blocks executed in" in stats, stats

    ds = ray.data.range(5)
    test = TestStream(
        checker,
        datasets={"train": ds},
    )
    test.fit()

    it = make_local_dataset_iterator(ds, None, TestStream._dataset_config["train"])
    TestStream.train_loop_per_worker(it, checker)

    # Check that make_local_dataset_iterator throws an error when called by a
    # worker.
    def check_error(shard, results):
        with pytest.raises(RuntimeError):
            make_local_dataset_iterator(ds, None, TestStream._dataset_config["train"])

    test = TestStream(
        check_error,
        datasets={"train": ds},
    )
    test.fit()


@pytest.mark.parametrize("max_object_store_memory_fraction", [None, 1, 0.3])
def test_deterministic_per_epoch_preprocessor(
    ray_start_4_cpus, max_object_store_memory_fraction
):
    ds = ray.data.range(5)

    def multiply(x):
        return x * 2

    it = make_local_dataset_iterator(
        ds,
        # Add some random noise to each integer.
        preprocessor=BatchMapper(
            lambda x: x + 0.1 * random.random(), batch_format="pandas"
        ),
        dataset_config=DatasetConfig(
            randomize_block_order=False,
            max_object_store_memory_fraction=max_object_store_memory_fraction,
            per_epoch_preprocessor=BatchMapper(multiply, batch_format="pandas"),
        ),
    )

    def checker(shard, results):
        assert len(results[0]) == 5, (max_object_store_memory_fraction, results)
        if max_object_store_memory_fraction is None:
            assert results[0] == results[1], (
                max_object_store_memory_fraction,
                results,
            )
        else:
            # Windowed pipelined ingest also reapplies the base
            # preprocessor on every epoch, so we get a random dataset each
            # time.
            assert results[0] != results[1], (
                max_object_store_memory_fraction,
                results,
            )
        # Per-epoch preprocessor was applied at least once.
        assert all(int(x) % 2 == 0 for x in results[0]), (
            max_object_store_memory_fraction,
            results,
        )
        # Per-epoch preprocessor was applied no more than once.
        assert any(int(x) % 4 != 0 for x in results[0]), (
            max_object_store_memory_fraction,
            results,
        )

    TestStream.train_loop_per_worker(it, checker)


@pytest.mark.parametrize("max_object_store_memory_fraction", [None, 1, 0.3])
def test_nondeterministic_per_epoch_preprocessor(
    ray_start_4_cpus, max_object_store_memory_fraction
):
    ds = ray.data.range(5)

    # Use randomized per-epoch preprocessor to check that it gets applied once
    # per epoch.
    def rand(x):
        return x * random.random()

    it = make_local_dataset_iterator(
        ds,
        preprocessor=None,
        dataset_config=DatasetConfig(
            randomize_block_order=False,
            max_object_store_memory_fraction=max_object_store_memory_fraction,
            per_epoch_preprocessor=BatchMapper(rand, batch_format="pandas"),
        ),
    )

    def checker(shard, results):
        assert len(results[0]) == 5, (max_object_store_memory_fraction, results)
        # Per-epoch preprocessor is randomized, so we should get a random
        # dataset on each epoch.
        assert results[0] != results[1], (max_object_store_memory_fraction, results)

    TestStream.train_loop_per_worker(it, checker)


def test_validate_per_epoch_preprocessor(ray_start_4_cpus):
    ds = ray.data.range(5)

    def multiply(x):
        return x * 2

    dataset_config = DatasetConfig(
        per_epoch_preprocessor=BatchMapper(multiply, batch_format="pandas")
    )
    DatasetConfig.validated(
        {
            "train": dataset_config,
        },
        {"train": ds},
    )

    with pytest.raises(ValueError):
        # Must specify a ray.data.Preprocessor.
        dataset_config = DatasetConfig(per_epoch_preprocessor=multiply)
        DatasetConfig.validated(
            {
                "train": dataset_config,
            },
            {"train": ds},
        )

    with pytest.raises(ValueError):
        # Must specify a non-fittable ray.data.Preprocessor.
        dataset_config = DatasetConfig(per_epoch_preprocessor=Preprocessor())
        DatasetConfig.validated(
            {
                "train": dataset_config,
            },
            {"train": ds},
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
