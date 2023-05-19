from typing import Optional

import pytest

import ray
from ray.air import session
from ray.air.config import ScalingConfig
from ray.data import DataIterator
from ray.train.data_config import DataConfig
from ray.train.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


class TestBasic(DataParallelTrainer):
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


def test_basic(ray_start_4_cpus):
    ds = ray.data.range(10)

    # Single worker basic case.
    test = TestBasic(
        1,
        True,
        {"train": 10, "test": 10},
        datasets={"train": ds, "test": ds},
    )
    test.fit()

    # Single worker, no test ds.
    test = TestBasic(1, True, {"train": 10, "test": -1}, datasets={"train": ds})
    test.fit()

    # Two workers, train split.
    test = TestBasic(
        2, True, {"train": 5, "test": 10}, datasets={"train": ds, "test": ds}
    )
    test.fit()

    # Two workers, both split.
    test = TestBasic(
        2,
        True,
        {"train": 5, "test": 5},
        dataset_config=DataConfig(datasets_to_split=["train", "test"]),
        datasets={"train": ds, "test": ds},
    )
    # Test get config.
    assert isinstance(test.get_dataset_config(), DataConfig)
    test.fit()


def test_configure_execution_options(ray_start_4_cpus):
    ds = ray.data.range(10)
    # Resource limit is too low and will trigger an error.
    options = DataConfig.default_ingest_options()
    options.resource_limits.cpu = 0
    test = TestBasic(
        1,
        True,
        {"train": 10, "test": 10},
        datasets={"train": ds, "test": ds},
        dataset_config=DataConfig(execution_options=options),
    )
    with pytest.raises(ray.train.base_trainer.TrainingFailedError):
        test.fit()


def test_custom_config_subclass(ray_start_4_cpus):
    # TODO implement split in subclass
    pass


def test_per_epoch_preprocessing(ray_start_4_cpus):
    # TODO check read order is randomized each time with randomize block order
    pass


def test_materialized_preprocessing(ray_start_4_cpus):
    # TODO check side effect doesn't happen twice
    pass


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
