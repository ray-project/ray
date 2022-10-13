import pytest
import torch

from ray.air import Checkpoint, session

from ray.air.config import ScalingConfig
from ray.train.constants import TRAINING_ITERATION
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
)
from ray.train.examples.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.torch_fashion_mnist_example import (
    train_func as fashion_mnist_train_func,
)
from ray.train.horovod.horovod_trainer import HorovodTrainer
from ray.train.tests.test_tune import (
    torch_fashion_mnist,
    tune_tensorflow_mnist,
)
from ray.train.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray.train.torch.torch_trainer import TorchTrainer


def test_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TensorflowTrainer(
        tensorflow_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    results = trainer.fit()

    result = results.metrics

    assert result[TRAINING_ITERATION] == epochs


def test_torch_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TorchTrainer(
        fashion_mnist_train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    results = trainer.fit()

    result = results.metrics

    assert result[TRAINING_ITERATION] == epochs


def test_horovod_torch_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    num_epochs = 2
    trainer = HorovodTrainer(
        horovod_torch_train_func,
        train_loop_config={"num_epochs": num_epochs, "lr": 1e-3},
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    results = trainer.fit()
    result = results.metrics
    assert result[TRAINING_ITERATION] == num_workers


def test_horovod_torch_mnist_gpu_checkpoint(ray_start_4_cpus_2_gpus):
    def checkpointing_func(config):
        net = torch.nn.Linear(in_features=8, out_features=16)
        net.to("cuda")

        checkpoint = Checkpoint.from_dict({"model": net.state_dict()})
        session.report({"metric": 1}, checkpoint=checkpoint)

    num_workers = 2
    trainer = HorovodTrainer(
        checkpointing_func,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )
    trainer.fit()


def test_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_concurrent_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=1, use_gpu=True, num_samples=2)


def test_tune_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_train_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.air.examples.pytorch.torch_regression_example import train_regression

    assert train_regression(num_workers=2, use_gpu=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
