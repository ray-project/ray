import sys

import pytest

from ray.train import ScalingConfig
from ray.train.examples.pytorch.torch_fashion_mnist_example import (
    train_func_per_worker as fashion_mnist_train_func,
)
from ray.train.tests.test_tune import torch_fashion_mnist
from ray.train.torch import TorchTrainer


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Tensorflow is not installed in CI for Python 3.12",
)
def test_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    from ray.train.examples.tf.tensorflow_mnist_example import (
        train_func as tensorflow_mnist_train_func,
    )
    from ray.train.tensorflow import TensorflowTrainer

    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TensorflowTrainer(
        tensorflow_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    trainer.fit()


def test_torch_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size_per_worker": 32, "epochs": epochs}
    trainer = TorchTrainer(
        fashion_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    trainer.fit()


def test_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_concurrent_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=1, use_gpu=True, num_samples=2)


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Tensorflow is not installed in CI for Python 3.12",
)
def test_tune_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    from ray.train.tests.test_tune import tune_tensorflow_mnist

    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_train_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.train.examples.pytorch.torch_regression_example import train_regression

    train_regression(num_workers=2, use_gpu=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
