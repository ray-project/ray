import pytest
import ray
from ray.air.config import ScalingConfig
from ray.tune.result import TRAINING_ITERATION
from ray.train.examples.alpa_mnist_example import (
    train_func as alpa_mnist_train_func,
)
from ray.air import session
from ray.train.alpa import AlpaTrainer

import jax
import numpy as np


from ray import tune
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
import alpa


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_1_cpu_1_gpu():
    address_info = ray.init(num_cpus=1, num_gpus=1)
    yield address_info
    ray.shutdown()


def test_jax_get_device(ray_start_4_cpus_2_gpus):
    def train_fn():
        """Creates a barrier across all hosts/devices."""
        session.report(
            dict(devices=alpa.device_mesh.global_cluster.num_devices)
        )

    num_gpus_per_worker = 2
    trainer = AlpaTrainer(
        train_fn,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=True,
            resources_per_worker={"CPU": 1, "GPU": num_gpus_per_worker},
        ),
    )

    results = trainer.fit()
    devices = results.metrics["devices"]
    assert devices = num_gpus_per_worker


def test_jax_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 1
    num_epochs = 2
    num_gpus_per_worker = 2
    trainer = AlpaTrainer(
        train_loop_per_worker=alpa_mnist_train_func,
        train_loop_config={
            "num_epochs": num_epochs,
            "learning_rate": 1e-3,
            "momentum": 0.9,
            "batch_size": 8192,
        },
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=True,
            resources_per_worker={"CPU": 1, "GPU": num_gpus_per_worker},
        ),
    )

    results = trainer.fit()
    result = results.metrics
    assert result[TRAINING_ITERATION] == num_epochs


def tune_jax_mnist(num_workers, use_gpu, num_samples, num_gpus_per_worker=0):
    trainer = AlpaTrainer(
        alpa_mnist_train_func,
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker={"CPU": 1, "GPU": num_gpus_per_worker} if use_gpu else None,
        ),
    )
    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.loguniform(1e-4, 1e-1),
                "batch_size": tune.choice([32, 64, 128]),
                "epochs": 2,
            }
        },
        tune_config=TuneConfig(
            num_samples=num_samples,
        ),
    )
    analysis = tuner.fit()._experiment_analysis

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


# def test_tune_jax_mnist(ray_start_8_cpus):
#     tune_jax_mnist(num_workers=2, use_gpu=False, num_samples=2)


def test_tune_jax_mnist_gpu(ray_start_4_cpus_2_gpus):
    tune_jax_mnist(num_workers=1, use_gpu=True, num_samples=1, num_gpus_per_worker=2)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))