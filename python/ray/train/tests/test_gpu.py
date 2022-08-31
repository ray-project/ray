import os
from timeit import default_timer as timer
from collections import Counter

from unittest.mock import patch
import pytest
import torch
import torchvision
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler

import ray
from ray.cluster_utils import Cluster

import ray.train as train
from ray.train import Trainer, TrainingCallback
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
from ray.train.examples.torch_linear_example import LinearDataset
from ray.train.horovod.horovod_trainer import HorovodTrainer
from ray.train.tests.test_tune import (
    torch_fashion_mnist,
    tune_tensorflow_mnist,
)
from ray.train.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray.train.torch import TorchConfig
from ray.train.torch.torch_trainer import TorchTrainer


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


@pytest.fixture
def ray_2_node_2_gpu():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=4, num_gpus=2)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


class LinearDatasetDict(LinearDataset):
    """Modifies the LinearDataset to return a Dict instead of a Tuple."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": self.y[index, None]}


class NonTensorDataset(LinearDataset):
    """Modifies the LinearDataset to also return non-tensor objects."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": 2}


# TODO: Refactor as a backend test.
@pytest.mark.parametrize("num_gpus_per_worker", [0.5, 1])
def test_torch_get_device(ray_start_4_cpus_2_gpus, num_gpus_per_worker):
    def train_fn():
        return train.torch.get_device().index

    trainer = Trainer(
        "torch",
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"GPU": num_gpus_per_worker},
    )
    trainer.start()
    devices = trainer.run(train_fn)
    trainer.shutdown()

    if num_gpus_per_worker == 0.5:
        assert devices == [0, 0]
    elif num_gpus_per_worker == 1:
        assert devices == [0, 1]
    else:
        raise RuntimeError(
            "New parameter for this test has been added without checking that the "
            "correct devices have been returned."
        )


# TODO: Refactor as a backend test.
@pytest.mark.parametrize("num_gpus_per_worker", [0.5, 1, 2])
def test_torch_get_device_dist(ray_2_node_2_gpu, num_gpus_per_worker):
    @patch("torch.cuda.is_available", lambda: True)
    def train_fn():
        return train.torch.get_device().index

    trainer = Trainer(
        TorchConfig(backend="gloo"),  # use gloo instead of nccl,
        # since nccl is not supported on this virtual gpu ray environment
        num_workers=int(4 / num_gpus_per_worker),
        use_gpu=True,
        resources_per_worker={"GPU": num_gpus_per_worker},
    )
    trainer.start()
    devices = trainer.run(train_fn)
    trainer.shutdown()

    count = Counter(devices)
    # cluster setups: 2 nodes, 2 gpus per node
    # `CUDA_VISIBLE_DEVICES` is set to "0,1" on node 1 and node 2
    if num_gpus_per_worker == 0.5:
        # worker gpu topology:
        # 4 workers on node 1, 4 workers on node 2
        # `ray.get_gpu_ids()` returns [0], [0], [1], [1] on node 1
        # and [0], [0], [1], [1] on node 2
        for i in range(2):
            assert count[i] == 4
    elif num_gpus_per_worker == 1:
        # worker gpu topology:
        # 2 workers on node 1, 2 workers on node 2
        # `ray.get_gpu_ids()` returns [0], [1] on node 1 and [0], [1] on node 2
        for i in range(2):
            assert count[i] == 2
    elif num_gpus_per_worker == 2:
        # worker gpu topology:
        # 1 workers on node 1, 1 workers on node 2
        # `ray.get_gpu_ids()` returns [0, 1] on node 1 and [0, 1] on node 2
        # and `device_id` returns the first index
        assert count[0] == 2
    else:
        raise RuntimeError(
            "New parameter for this test has been added without checking that the "
            "correct devices have been returned."
        )


# TODO: Refactor as a backend test.
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


# TODO: Refactor as a backend test.
@pytest.mark.parametrize(
    "dataset", (LinearDataset, LinearDatasetDict, NonTensorDataset)
)
def test_torch_prepare_dataloader(ray_start_4_cpus_2_gpus, dataset):
    data_loader = DataLoader(dataset(a=1, b=2, size=10))

    def train_fn():
        wrapped_data_loader = train.torch.prepare_data_loader(data_loader)

        # Check that DistributedSampler has been added to the data loader.
        assert isinstance(wrapped_data_loader.sampler, DistributedSampler)

        # Make sure you can properly iterate through the DataLoader.
        # Case where the dataset returns a tuple or list from __getitem__.
        if isinstance(dataset, LinearDataset):
            for batch in wrapped_data_loader:
                x = batch[0]
                y = batch[1]

                # Make sure the data is on the correct device.
                assert x.is_cuda and y.is_cuda
        # Case where the dataset returns a dict from __getitem__.
        elif isinstance(dataset, LinearDatasetDict):
            for batch in wrapped_data_loader:
                for x, y in zip(batch["x"], batch["y"]):
                    # Make sure the data is on the correct device.
                    assert x.is_cuda and y.is_cuda

        elif isinstance(dataset, NonTensorDataset):
            for batch in wrapped_data_loader:
                for x, y in zip(batch["x"], batch["y"]):
                    # Make sure the data is on the correct device.
                    assert x.is_cuda and y == 2

    trainer = Trainer("torch", num_workers=2, use_gpu=True)
    trainer.start()
    trainer.run(train_fn)
    trainer.shutdown()


# TODO: Refactor as a backend test.
@pytest.mark.parametrize("use_gpu", (False, True))
def test_enable_reproducibility(ray_start_4_cpus_2_gpus, use_gpu):
    # NOTE: Reproducible results aren't guaranteed between seeded executions, even with
    # identical hardware and software dependencies. This test should be okay given that
    # it only runs for two epochs on a small dataset.
    # NOTE: I've chosen to use a ResNet model over a more simple model, because
    # `enable_reproducibility` disables CUDA convolution benchmarking, and a simpler
    # model (e.g., linear) might not test this feature.
    def train_func():
        train.torch.enable_reproducibility()

        model = torchvision.models.resnet18()
        model = train.torch.prepare_model(model)

        dataset_length = 128
        dataset = torch.utils.data.TensorDataset(
            torch.randn(dataset_length, 3, 32, 32),
            torch.randint(low=0, high=1000, size=(dataset_length,)),
        )
        dataloader = torch.utils.data.DataLoader(dataset, batch_size=64)
        dataloader = train.torch.prepare_data_loader(dataloader)

        optimizer = torch.optim.SGD(model.parameters(), lr=0.001)

        model.train()
        for epoch in range(2):
            for images, targets in dataloader:
                optimizer.zero_grad()

                outputs = model(images)
                loss = torch.nn.functional.cross_entropy(outputs, targets)

                loss.backward()
                optimizer.step()

        return loss.item()

    trainer = Trainer("torch", num_workers=2, use_gpu=use_gpu)
    trainer.start()
    result1 = trainer.run(train_func)
    result2 = trainer.run(train_func)
    trainer.shutdown()

    assert result1 == result2


# TODO: Refactor as a backend test.
def test_torch_amp_performance(ray_start_4_cpus_2_gpus):
    def train_func(config):
        train.torch.accelerate(amp=config["amp"])

        model = torchvision.models.resnet101()
        model = train.torch.prepare_model(model)

        dataset_length = 1000
        dataset = torch.utils.data.TensorDataset(
            torch.randn(dataset_length, 3, 224, 224),
            torch.randint(low=0, high=1000, size=(dataset_length,)),
        )
        dataloader = torch.utils.data.DataLoader(dataset, batch_size=64)
        dataloader = train.torch.prepare_data_loader(dataloader)

        optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
        optimizer = train.torch.prepare_optimizer(optimizer)

        model.train()
        for epoch in range(1):
            for images, targets in dataloader:
                optimizer.zero_grad()

                outputs = model(images)
                loss = torch.nn.functional.cross_entropy(outputs, targets)

                train.torch.backward(loss)
                optimizer.step()

    def latency(amp: bool) -> float:
        trainer = Trainer("torch", num_workers=2, use_gpu=True)
        trainer.start()
        start_time = timer()
        trainer.run(train_func, {"amp": amp})
        end_time = timer()
        trainer.shutdown()
        return end_time - start_time

    # Training should be at least 5% faster with AMP.
    assert 1.05 * latency(amp=True) < latency(amp=False)


# TODO: Refactor as a backend test.
def test_checkpoint_torch_model_with_amp(ray_start_4_cpus_2_gpus):
    """Test that model with AMP is serializable."""

    def train_func():
        train.torch.accelerate(amp=True)

        model = torchvision.models.resnet101()
        model = train.torch.prepare_model(model)

        train.save_checkpoint(model=model)

    trainer = Trainer("torch", num_workers=1, use_gpu=True)
    trainer.start()
    trainer.run(train_func)
    trainer.shutdown()


# TODO: Refactor as a backend test.
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


def test_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_concurrent_tune_fashion_mnist_gpu(ray_start_4_cpus_2_gpus):
    torch_fashion_mnist(num_workers=1, use_gpu=True, num_samples=2)


def test_tune_tensorflow_mnist_gpu(ray_start_4_cpus_2_gpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_train_linear_dataset_gpu(ray_start_4_cpus_2_gpus):
    from ray.air.examples.pytorch.torch_regression_example import train_regression

    assert train_regression(num_workers=2, use_gpu=True)


# TODO: Refactor as a backend test.
@pytest.mark.parametrize(
    ("device_choice", "auto_transfer"),
    [
        ("cpu", True),
        ("cpu", False),
        ("cuda", True),
        ("cuda", False),
    ],
)
def test_auto_transfer_data_from_host_to_device(
    ray_start_1_cpu_1_gpu, device_choice, auto_transfer
):
    import numpy as np
    import torch

    def compute_average_runtime(func):
        device = torch.device(device_choice)
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        runtime = []
        for _ in range(10):
            torch.cuda.synchronize()
            start.record()
            func(device)
            end.record()
            torch.cuda.synchronize()
        runtime.append(start.elapsed_time(end))
        return np.mean(runtime)

    small_dataloader = [
        (torch.randn((1024 * 4, 1024 * 4), device="cpu"),) for _ in range(10)
    ]

    def host_to_device(device):
        for (x,) in small_dataloader:
            x = x.to(device)
            torch.matmul(x, x)

    def host_to_device_auto_pipeline(device):
        wrapped_dataloader = ray.train.torch.train_loop_utils._WrappedDataLoader(
            small_dataloader, device, auto_transfer
        )
        for (x,) in wrapped_dataloader:
            torch.matmul(x, x)

    # test if all four configurations are okay
    with_auto_transfer = compute_average_runtime(host_to_device_auto_pipeline)

    if device_choice == "cuda" and auto_transfer:
        assert compute_average_runtime(host_to_device) >= with_auto_transfer


def test_auto_transfer_correct_device(ray_start_4_cpus_2_gpus):
    """Tests that auto_transfer uses the right device for the cuda stream."""
    import nvidia_smi

    nvidia_smi.nvmlInit()

    def get_gpu_used_mem(i):
        handle = nvidia_smi.nvmlDeviceGetHandleByIndex(i)
        info = nvidia_smi.nvmlDeviceGetMemoryInfo(handle)
        return info.used

    start_gpu_memory = get_gpu_used_mem(1)

    device = torch.device("cuda:1")
    small_dataloader = [(torch.randn((1024 * 4, 1024 * 4)),) for _ in range(10)]
    wrapped_dataloader = (  # noqa: F841
        ray.train.torch.train_loop_utils._WrappedDataLoader(
            small_dataloader, device, True
        )
    )

    end_gpu_memory = get_gpu_used_mem(1)

    # Verify GPU memory usage increases on the right cuda device
    assert end_gpu_memory > start_gpu_memory


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
