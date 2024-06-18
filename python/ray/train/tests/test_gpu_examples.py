import os
from tempfile import TemporaryDirectory

import pytest
import torch

from ray import train
from ray.air.constants import TRAINING_ITERATION
from ray.train import Checkpoint, ScalingConfig
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
)
from ray.train.examples.pytorch.torch_fashion_mnist_example import (
    train_func_per_worker as fashion_mnist_train_func,
)
from ray.train.examples.tf.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.horovod.horovod_trainer import HorovodTrainer
from ray.train.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray.train.tests.test_tune import torch_fashion_mnist, tune_tensorflow_mnist
from ray.train.torch.torch_trainer import TorchTrainer


def test_tensorflow_mnist_acc(ray_start_4_cpus_2_accs):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TensorflowTrainer(
        tensorflow_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_acc=True),
    )
    results = trainer.fit()

    result = results.metrics

    assert result[TRAINING_ITERATION] == epochs


def test_torch_fashion_mnist_acc(ray_start_4_cpus_2_accs):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size_per_worker": 32, "epochs": epochs}
    trainer = TorchTrainer(
        fashion_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_acc=True),
    )
    results = trainer.fit()

    result = results.metrics

    assert result[TRAINING_ITERATION] == epochs


def test_horovod_torch_mnist_acc(ray_start_4_cpus_2_accs):
    num_workers = 2
    num_epochs = 2
    trainer = HorovodTrainer(
        horovod_torch_train_func,
        train_loop_config={"num_epochs": num_epochs, "lr": 1e-3},
        scaling_config=ScalingConfig(num_workers=num_workers, use_acc=True),
    )
    results = trainer.fit()
    result = results.metrics
    assert result[TRAINING_ITERATION] == num_workers


def test_horovod_torch_mnist_acc_checkpoint(ray_start_4_cpus_2_accs):
    def checkpointing_func(config):
        net = torch.nn.Linear(in_features=8, out_features=16)
        net.to("cuda")

        with TemporaryDirectory() as tmpdir:
            torch.save(net.state_dict(), os.path.join(tmpdir, "checkpoint.pt"))
            train.report({"metric": 1}, checkpoint=Checkpoint.from_directory(tmpdir))

    num_workers = 2
    trainer = HorovodTrainer(
        checkpointing_func,
        scaling_config=ScalingConfig(num_workers=num_workers, use_acc=True),
    )
    trainer.fit()


def test_tune_fashion_mnist_acc(ray_start_4_cpus_2_accs):
    torch_fashion_mnist(num_workers=2, use_acc=True, num_samples=1)


def test_concurrent_tune_fashion_mnist_acc(ray_start_4_cpus_2_accs):
    torch_fashion_mnist(num_workers=1, use_acc=True, num_samples=2)


def test_tune_tensorflow_mnist_acc(ray_start_4_cpus_2_accs):
    tune_tensorflow_mnist(num_workers=2, use_acc=True, num_samples=1)


def test_train_linear_dataset_acc(ray_start_4_cpus_2_accs):
    from ray.train.examples.pytorch.torch_regression_example import train_regression

    assert train_regression(num_workers=2, use_acc=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
