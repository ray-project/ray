from typing import Optional

import random
import pytest

import ray
from ray import train
from ray.train import DataConfig, ScalingConfig
from ray.data import DataIterator
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
            data_shard = train.get_dataset_shard("train")
            assert isinstance(data_shard, DataIterator), data_shard
            for k, v in expect_sizes.items():
                shard = train.get_dataset_shard(k)
                if v == -1:
                    assert shard is None, shard
                else:
                    count = 0
                    for batch in shard.iter_batches():
                        for arr in batch.values():
                            count += arr.size
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


@pytest.mark.skip(
    reason="Incomplete implementation of _validate_dag causes other errors, so we "
    "remove DAG validation for now; see https://github.com/ray-project/ray/pull/37829"
)
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


def test_configure_execution_options_carryover_context(ray_start_4_cpus):
    """Tests that execution options in DataContext are carried over to DatConfig
    automatically."""

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True
    ctx.execution_options.verbose_progress = True

    data_config = DataConfig()

    ingest_options = data_config.default_ingest_options()
    assert ingest_options.preserve_order is True
    assert ingest_options.verbose_progress is True


class CustomConfig(DataConfig):
    def __init__(self):
        pass

    def configure(self, *args, **kwargs):
        ds = ray.data.range(10)
        return [
            {"train": ds.iterator()},
            {"train": ds.iterator()},
        ]


def test_custom_config_subclass(ray_start_4_cpus):
    test = TestBasic(
        1,
        True,
        {"train": 10},
        dataset_config=CustomConfig(),
    )
    test.fit()


class TestRandom(DataParallelTrainer):
    def __init__(self, num_workers: int, expect_random: bool, **kwargs):
        def train_loop_per_worker():
            data_shard = train.get_dataset_shard("train")
            assert isinstance(data_shard, DataIterator), data_shard
            epoch1 = list(data_shard.iter_rows())
            epoch2 = list(data_shard.iter_rows())
            print("Epochs", epoch1, "\n", epoch2)
            if expect_random:
                assert epoch1 != epoch2
            else:
                assert epoch1 == epoch2

        kwargs.pop("scaling_config", None)
        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=num_workers),
            **kwargs,
        )


def test_per_epoch_preprocessing(ray_start_4_cpus):
    ds = ray.data.range(100, parallelism=100).randomize_block_order()
    test = TestRandom(2, True, datasets={"train": ds})
    test.fit()

    ds = ray.data.range(100, parallelism=100).random_shuffle()
    test = TestRandom(2, True, datasets={"train": ds})
    test.fit()

    ds = ray.data.range(100, parallelism=100).map(
        lambda x: {"id": x["id"] * random.random()}
    )
    test = TestRandom(2, True, datasets={"train": ds})
    test.fit()


def test_materialized_preprocessing(ray_start_4_cpus):
    # TODO(ekl) we should test all these configs with splitting enabled, but this
    # requires implementing deterministic streaming split.
    ds = ray.data.range(100, parallelism=100).randomize_block_order()
    ds = ds.materialize()
    test = TestRandom(
        2,
        False,
        datasets={"train": ds},
        dataset_config=DataConfig(datasets_to_split=[]),
    )
    test.fit()

    ds = ray.data.range(100, parallelism=100).random_shuffle()
    ds = ds.materialize()
    test = TestRandom(
        2,
        False,
        datasets={"train": ds},
        dataset_config=DataConfig(datasets_to_split=[]),
    )
    test.fit()

    ds = ray.data.range(100, parallelism=100).map(
        lambda x: {"id": x["id"] * random.random()}
    )
    ds = ds.materialize()
    test = TestRandom(
        2,
        False,
        datasets={"train": ds},
        dataset_config=DataConfig(datasets_to_split=[]),
    )
    test.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
