import os

import pytest

import torch
import tensorflow as tf

import ray
from ray import tune
from ray.tune import TuneError

import ray.util.sgd.v2 as sgd
from ray.util.sgd.v2 import Trainer
from ray.util.sgd.v2.constants import TUNE_CHECKPOINT_FILE_NAME
from ray.util.sgd.v2.backends.backend import BackendInterface, BackendConfig
from ray.util.sgd.v2.examples.tensorflow_mnist_example import train_func as \
    tensorflow_mnist_train_func
from ray.util.sgd.v2.examples.train_fashion_mnist import train_func as \
    fashion_mnist_train_func
from ray.util.sgd.v2.worker_group import WorkerGroup


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_4_gpus():
    address_info = ray.init(num_cpus=2, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(BackendInterface):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: TestConfig):
        pass


def torch_fashion_mnist(num_workers, use_gpu, num_samples):
    epochs = 2

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=use_gpu)
    MnistTrainable = trainer.to_tune_trainable(fashion_mnist_train_func)

    analysis = tune.run(
        MnistTrainable,
        num_samples=num_samples,
        config={
            "lr": tune.loguniform(1e-4, 1e-1),
            "batch_size": tune.choice([32, 64, 128]),
            "epochs": epochs
        })

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


def test_tune_torch_fashion_mnist(ray_start_8_cpus):
    torch_fashion_mnist(num_workers=2, use_gpu=False, num_samples=2)


@pytest.mark.skipif(
    torch.cuda.device_count() < 2,
    reason="Only run if multiple GPUs are available.")
def test_tune_fashion_mnist_gpu(ray_start_4_cpus_4_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def tune_tensorflow_mnist(num_workers, use_gpu, num_samples):
    epochs = 2
    trainer = Trainer("tensorflow", num_workers=num_workers, use_gpu=use_gpu)
    MnistTrainable = trainer.to_tune_trainable(tensorflow_mnist_train_func)

    analysis = tune.run(
        MnistTrainable,
        num_samples=num_samples,
        config={
            "lr": tune.loguniform(1e-4, 1e-1),
            "batch_size": tune.choice([32, 64, 128]),
            "epochs": epochs
        })

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


def test_tune_tensorflow_mnist(ray_start_8_cpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=False, num_samples=2)


@pytest.mark.skipif(
    len(tf.config.list_physical_devices("GPU")) < 2,
    reason="Only run if multiple GPUs are available.")
def test_tune_tensorflow_mnist_gpu(ray_start_4_cpus_4_gpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_tune_error(ray_start_2_cpus):
    def train_func(config):
        raise RuntimeError("Error in training function!")

    trainer = Trainer(TestConfig())
    TestTrainable = trainer.to_tune_trainable(train_func)

    with pytest.raises(TuneError):
        tune.run(TestTrainable)

def test_tune_checkpoint(ray_start_2_cpus):
    def train_func():
        for i in range(10):
            sgd.report(test=i)
        sgd.save_checkpoint(hello="world")

    trainer = Trainer(TestConfig())
    TestTrainable = trainer.to_tune_trainable(train_func)

    [trial] = tune.run(TestTrainable).trials
    assert os.path.exists(os.path.join(trial.checkpoint.value,
                                       TUNE_CHECKPOINT_FILE_NAME))

def test_tune_checkpoint_frequent(ray_start_2_cpus):
    def train_func():
        pass

def test_tune_checkpoint_infrequent(ray_start_2_cpus):
    def train_func():
        pass










if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
