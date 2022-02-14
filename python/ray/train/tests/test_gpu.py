import os
import pytest

import torch
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler

import ray
import ray.train as train
from ray.train import Trainer, TrainingCallback
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
)
from ray.train.examples.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.train_fashion_mnist_example import (
    train_func as fashion_mnist_train_func,
)
from ray.train.examples.train_linear_example import LinearDataset
from test_tune import torch_fashion_mnist, tune_tensorflow_mnist


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_torch_prepare_model(ray_start_4_cpus_2_gpus):
    """Tests if ``prepare_model`` correctly wraps in DDP."""

    def train_fn():
        model = torch.nn.Linear(1, 1)

        # Wrap in DDP.
        model = train.torch.prepare_model(model)

        # Make sure model is wrapped in DDP.
        assert isinstance(model, DistributedDataParallel)

        # Make sure model is on cuda.
        assert next(model.parameters()).is_cuda

    trainer = Trainer("torch", num_workers=2, use_gpu=True)
    trainer.start()
    trainer.run(train_fn)
    trainer.shutdown()


def test_torch_prepare_dataloader(ray_start_4_cpus_2_gpus):
    data_loader = DataLoader(LinearDataset(a=1, b=2, size=10))

    def train_fn():
        wrapped_data_loader = train.torch.prepare_data_loader(data_loader)

        # Check that DistributedSampler has been added to the data loader.
        assert isinstance(wrapped_data_loader.sampler, DistributedSampler)

        # Make sure you can properly iterate through the DataLoader.
        for batch in wrapped_data_loader:
            X = batch[0]
            y = batch[1]

            # Make sure the data is on the correct device.
            assert X.is_cuda and y.is_cuda

    trainer = Trainer("torch", num_workers=2, use_gpu=True)
    trainer.start()
    trainer.run(train_fn)
    trainer.shutdown()


def test_torch_auto_gpu_to_cpu(ray_start_4_cpus_2_gpus):
    """Tests if GPU tensors are auto converted to CPU on driver."""

    # Disable GPU on the driver.
    os.environ["CUDA_VISIBLE_DEVICES"] = ""

    num_workers = 2

    class ValidateCPUCallback(TrainingCallback):
        def handle_result(self, results, **info):
            for result in results:
                model = result["model"]
                assert not next(model.parameters()).is_cuda

    def train_func():
        model = torch.nn.Linear(1, 1)

        # Move to GPU device.
        model = ray.train.torch.prepare_model(model)

        assert next(model.parameters()).is_cuda

        ray.train.save_checkpoint(model=model)
        ray.train.report(model=model)

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=True)
    trainer.start()
    trainer.run(train_func, callbacks=[ValidateCPUCallback()])
    model = trainer.latest_checkpoint["model"]
    assert not next(model.parameters()).is_cuda
    trainer.shutdown()

    # Test the same thing for state dict.

    class ValidateCPUStateDictCallback(TrainingCallback):
        def handle_result(self, results, **info):
            for result in results:
                state_dict = result["state_dict"]
                for tensor in state_dict.values():
                    assert not tensor.is_cuda

    def train_func():
        model = torch.nn.Linear(1, 1)

        # Move to GPU device.
        model = ray.train.torch.prepare_model(model)

        assert next(model.parameters()).is_cuda

        state_dict = model.state_dict()

        for tensor in state_dict.values():
            assert tensor.is_cuda

        ray.train.save_checkpoint(state_dict=state_dict)
        ray.train.report(state_dict=state_dict)

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=True)
    trainer.start()
    trainer.run(train_func, callbacks=[ValidateCPUStateDictCallback()])

    state_dict = trainer.latest_checkpoint["state_dict"]
    for tensor in state_dict.values():
        assert not tensor.is_cuda
    trainer.shutdown()

    # Reset the env var.
    os.environ.pop("CUDA_VISIBLE_DEVICES")


def test_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("tensorflow", num_workers=num_workers, use_gpu=True)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(tensorflow_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers
    result = results[0]

    loss = result["loss"]
    assert len(loss) == epochs
    assert loss[-1] < loss[0]

    accuracy = result["accuracy"]
    assert len(accuracy) == epochs
    assert accuracy[-1] > accuracy[0]


def test_torch_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=True)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(fashion_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers

    for result in results:
        assert len(result) == epochs
        assert result[-1] < result[0]


def test_horovod_torch_mnist_gpu(ray_start_4_cpus_2_gpus):
    num_workers = 2
    num_epochs = 2
    trainer = Trainer("horovod", num_workers, use_gpu=True)
    trainer.start()
    results = trainer.run(
        horovod_torch_train_func, config={"num_epochs": num_epochs, "lr": 1e-3}
    )
    trainer.shutdown()

    assert len(results) == num_workers
    for worker_result in results:
        assert len(worker_result) == num_epochs
        assert worker_result[num_epochs - 1] < worker_result[0]


def test_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_tune_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_train_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.train.examples.train_linear_dataset_example import train_linear

    results = train_linear(num_workers=2, use_gpu=True)
    for result in results:
        assert result[-1]["loss"] < result[0]["loss"]


def test_tensorflow_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.train.examples.tensorflow_linear_dataset_example import (
        train_tensorflow_linear,
    )

    results = train_tensorflow_linear(num_workers=2, use_gpu=True)
    for result in results:
        assert result[-1]["loss"] < result[0]["loss"]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
