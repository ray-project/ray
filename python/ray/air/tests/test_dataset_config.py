import pytest
from typing import Optional

import ray
from ray.data import Dataset, DatasetPipeline
from ray.air.config import DatasetConfig
from ray import train

from ray.air.train.data_parallel_trainer import DataParallelTrainer
from ray.air.preprocessors import BatchMapper


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


class TestBasic(DataParallelTrainer):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True),
        "test": DatasetConfig(),
        "baz": DatasetConfig(split=True, _noncustomizable_fields=["split"]),
    }

    def __init__(
        self, num_workers: int, expect_ds: bool, expect_sizes: Optional[dict], **kwargs
    ):
        def train_loop_per_worker():
            data_shard = train.get_dataset_shard("train")
            if expect_ds:
                assert isinstance(data_shard, Dataset), data_shard
            else:
                assert isinstance(data_shard, DatasetPipeline), data_shard
            for k, v in expect_sizes.items():
                shard = train.get_dataset_shard(k)
                if v == -1:
                    assert shard is None, shard
                else:
                    if isinstance(shard, DatasetPipeline):
                        assert next(shard.iter_epochs()).count() == v, shard
                    else:
                        assert shard.count() == v, shard

        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config={"num_workers": num_workers},
            **kwargs,
        )


class TestWildcard(TestBasic):
    _dataset_config = {
        "train": DatasetConfig(split=True, required=True),
        "*": DatasetConfig(split=True, _noncustomizable_fields=["split"]),
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
        preprocessor=BatchMapper(lambda x: x),
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

    # Noncustomizable field.
    with pytest.raises(ValueError):
        TestBasic(
            1,
            True,
            {"train": 10, "test": 10},
            dataset_config={"baz": DatasetConfig(split=False)},
            datasets={"train": ds, "baz": ds},
        )
    with pytest.raises(ValueError):
        TestWildcard(
            1,
            True,
            {"train": 10, "test": 10},
            dataset_config={"baz": DatasetConfig(split=False)},
            datasets={"train": ds, "baz": ds},
        )


def test_streamable_config(ray_start_4_cpus):
    ds = ray.data.range(10)

    # Single worker basic case.
    test = TestBasic(
        1,
        False,
        {"train": 10, "test": 10},
        dataset_config={"train": DatasetConfig(streamable=True)},
        datasets={"train": ds, "test": ds},
    )
    test.fit()

    # Two worker split pipeline.
    test = TestBasic(
        2,
        False,
        {"train": 5, "test": 10},
        dataset_config={"train": DatasetConfig(streamable=True)},
        datasets={"train": ds, "test": ds},
    )
    test.fit()


def test_fit_transform_config(ray_start_4_cpus):
    ds = ray.data.range(10)

    def drop_odd(rows):
        key = list(rows)[0]
        return rows[(rows[key] % 2 == 0)]

    prep = BatchMapper(drop_odd)

    # Single worker basic case.
    test = TestBasic(
        1,
        True,
        {"train": 5, "test": 5},
        dataset_config={},
        datasets={"train": ds, "test": ds},
        preprocessor=prep,
    )
    test.fit()

    # No transform for test.
    test = TestBasic(
        1,
        True,
        {"train": 5, "test": 10},
        dataset_config={"test": DatasetConfig(transform=False)},
        datasets={"train": ds, "test": ds},
        preprocessor=prep,
    )
    test.fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
