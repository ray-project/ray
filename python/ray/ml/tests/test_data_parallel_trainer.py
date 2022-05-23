import pytest

import ray
from ray import train, tune
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import PREPROCESSOR_KEY

from ray.ml.train.data_parallel_trainer import DataParallelTrainer
from ray.ml.preprocessor import Preprocessor
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = {"num_workers": 2}


def test_fit_train(ray_start_4_cpus):
    def train_func():
        train.report(loss=1)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    assert trainer.fit().metrics["loss"] == 1


def test_scaling_config(ray_start_4_cpus):
    def train_func():
        assert ray.available_resources()["CPU"] == 1
        train.report(loss=1)

    assert ray.available_resources()["CPU"] == 4
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config={"num_workers": 2}
    )
    trainer.fit()


def test_fit_train_config(ray_start_4_cpus):
    def train_func(config):
        train.report(loss=config["x"])

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        train_loop_config={"x": 100},
    )
    assert trainer.fit().metrics["loss"] == 100


def test_datasets(ray_start_4_cpus):
    num_train_data = 10
    num_val_data = 6

    train_dataset = ray.data.range(num_train_data)
    val_dataset = ray.data.range(num_val_data)

    def get_dataset():
        # Train dataset should be sharded.
        train_dataset = train.get_dataset_shard("train")
        assert train_dataset.count() == num_train_data / scale_config["num_workers"]
        # All other datasets should not be sharded.
        val_dataset = train.get_dataset_shard("val")
        assert val_dataset.count() == num_val_data

    trainer = DataParallelTrainer(
        train_loop_per_worker=get_dataset,
        scaling_config=scale_config,
        datasets={"train": train_dataset, "val": val_dataset},
    )
    trainer.fit()


def test_checkpoint(ray_start_4_cpus):
    def train_func():
        for i in range(3):
            train.save_checkpoint(model=i)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["model"] == 2


def test_preprocessor_in_checkpoint(ray_start_4_cpus):
    class DummyPreprocessor(Preprocessor):
        def __init__(self):
            super().__init__()
            self.is_same = True

    def train_func():
        for i in range(3):
            train.save_checkpoint(model=i)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["model"] == 2
    assert result.checkpoint.to_dict()[PREPROCESSOR_KEY].is_same


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    def train_func():
        checkpoint = train.load_checkpoint()
        if checkpoint:
            epoch = checkpoint["epoch"]
        else:
            epoch = 0
        for i in range(epoch, epoch + 2):
            train.save_checkpoint(epoch=i)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["epoch"] == 1

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        resume_from_checkpoint=resume_from,
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["epoch"] == 2


def test_invalid_train_loop(ray_start_4_cpus):
    def train_loop(config, extra_arg):
        pass

    with pytest.raises(ValueError):
        DataParallelTrainer(train_loop_per_worker=train_loop)


def test_tune(ray_start_4_cpus):
    def train_func(config):
        train.report(loss=config["x"])

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"x": 100},
        scaling_config=scale_config,
    )

    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"x": tune.choice([200, 300])}},
        tune_config=TuneConfig(num_samples=2),
    )
    result_grid = tuner.fit()
    assert result_grid[0].metrics["loss"] in [200, 300]

    # Make sure original Trainer is not affected.
    assert trainer.train_loop_config["x"] == 100


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
