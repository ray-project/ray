import pytest
import os
import ray
from ray.air import session
from ray.air.config import ScalingConfig
from ray.train.constants import TRAINING_ITERATION
from ray.train.examples.jax_mnist_example import (
    train_func as jax_mnist_train_func,
)
from ray.train.jax import JaxTrainer

from ray import tune
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_tpu(request):
    tpu_dev = request.param
    runtime_env = {"env_vars": {"RAY_TPU_DEV": tpu_dev}}
    address_info = ray.init(resources={"TPU": 1}, runtime_env=runtime_env)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_jax_mnist_gpu(ray_start_8_cpus):
    num_workers = 1
    num_epochs = 2
    trainer = JaxTrainer(
        train_loop_per_worker=jax_mnist_train_func,
        train_loop_config={
            "num_epochs": num_epochs,
            "learning_rate": 1e-3,
            "momentum": 0.9,
            "batch_size": 8192,
        },
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=False,
        ),
    )

    results = trainer.fit()
    result = results.metrics
    assert result[TRAINING_ITERATION] == num_epochs


def tune_jax_mnist(num_workers, use_gpu, num_samples, num_gpus_per_worker=0):
    trainer = JaxTrainer(
        jax_mnist_train_func,
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker={"GPU": num_gpus_per_worker} if use_gpu else None,
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


def test_tune_jax_mnist(ray_start_8_cpus):
    tune_jax_mnist(num_workers=2, use_gpu=False, num_samples=2)


@pytest.mark.parametrize(
    "ray_start_tpu,expected", [("1", False), ("0", True)], indirect=["ray_start_tpu"]
)
def test_tpu_lockfile(ray_start_tpu, expected):
    lock_file_path = "/tmp/libtpu_lockfile"
    open(lock_file_path, "w").close()
    assert os.path.exists(lock_file_path)

    def dummy_train():
        return 1

    trainer = JaxTrainer(
        train_loop_per_worker=dummy_train,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=False,
            resources_per_worker={"TPU": 1},
        ),
    )

    trainer.fit()

    assert os.path.exists(lock_file_path) == expected


class AssertingJaxTrainer(JaxTrainer):
    def training_loop(self) -> None:
        scaling_config = self._validate_scaling_config(self.scaling_config)
        pgf = scaling_config.as_placement_group_factory()
        tr = session.get_trial_resources()
        assert pgf == tr, pgf.strategy == "SPREAD"
        return super().training_loop()


def test_scaling_config(ray_start_8_cpus):
    def dummy_train():
        return 1

    trainer = AssertingJaxTrainer(
        train_loop_per_worker=dummy_train,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=False,
        ),
    )

    trainer.fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
