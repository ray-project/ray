"""This is a very minimal set of windows tests for Train/Tune."""

import os

import pytest

import ray
from ray import train, tune
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.tests.util import create_dict_checkpoint


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def chdir_tmpdir(tmp_path):
    original_path = os.getcwd()
    os.chdir(tmp_path)
    yield
    os.chdir(original_path)


def test_storage_path(ray_start_4_cpus, chdir_tmpdir):
    """Tests that Train/Tune with a local storage path works on Windows."""

    def train_fn(config):
        for i in range(5):
            if train.get_context().get_world_rank() == 0:
                with create_dict_checkpoint({"dummy": "data"}) as checkpoint:
                    train.report({"loss": i}, checkpoint=checkpoint)
            else:
                train.report({"loss": i})

    tuner = tune.Tuner(train_fn, run_config=train.RunConfig(storage_path=os.getcwd()))
    results = tuner.fit()
    assert not results.errors

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(storage_path=os.getcwd()),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
