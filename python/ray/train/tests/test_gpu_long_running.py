import os
from timeit import default_timer as timer

from unittest.mock import patch
import pytest
from ray.air.constants import MODEL_KEY
from ray.train.torch.torch_checkpoint import TorchCheckpoint
import torch
import torchvision

import ray
from ray.air import session

import ray.train as train
from ray.air.config import ScalingConfig
from ray.train.torch.torch_trainer import TorchTrainer


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


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

        session.report(dict(loss=loss.item()))

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    result1 = trainer.fit()

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    result2 = trainer.fit()

    assert result1.metrics["loss"] == result2.metrics["loss"]


def test_torch_amp_performance(ray_start_4_cpus_2_gpus):
    def train_func(config):
        train.torch.accelerate(amp=config["amp"])

        start_time = timer()
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
        end_time = timer()
        session.report({"latency": end_time - start_time})

    def latency(amp: bool) -> float:
        trainer = TorchTrainer(
            train_func,
            train_loop_config={"amp": amp},
            scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
        )
        results = trainer.fit()
        return results.metrics["latency"]

    # Training should be at least 5% faster with AMP.
    assert 1.05 * latency(amp=True) < latency(amp=False)


def test_checkpoint_torch_model_with_amp(ray_start_4_cpus_2_gpus):
    """Test that model with AMP is serializable."""

    def train_func():
        train.torch.accelerate(amp=True)

        model = torchvision.models.resnet101()
        model = train.torch.prepare_model(model)

        session.report({"model": model}, checkpoint=TorchCheckpoint.from_model(model))

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    results = trainer.fit()
    assert results.checkpoint
    assert results.checkpoint.get_model()


@patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": ""})
def test_torch_auto_gpu_to_cpu(ray_start_4_cpus_2_gpus):
    """Tests if GPU tensors are auto converted to CPU on driver."""
    num_workers = 2
    assert os.environ["CUDA_VISIBLE_DEVICES"] == ""

    def train_func():
        model = torch.nn.Linear(1, 1)

        # Move to GPU device.
        model = ray.train.torch.prepare_model(model)

        assert next(model.parameters()).is_cuda

        session.report({"model": model}, checkpoint=TorchCheckpoint.from_model(model))

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True)
    )
    results = trainer.fit()

    model_checkpoint = results.checkpoint.get_model()
    model_report = results.metrics["model"]
    assert not next(model_checkpoint.parameters()).is_cuda
    assert not next(model_report.parameters()).is_cuda

    # Test the same thing for state dict.

    def train_func():
        model = torch.nn.Linear(1, 1)

        # Move to GPU device.
        model = ray.train.torch.prepare_model(model)

        assert next(model.parameters()).is_cuda

        state_dict = model.state_dict()

        for tensor in state_dict.values():
            assert tensor.is_cuda

        session.report(
            {"state_dict": state_dict},
            checkpoint=TorchCheckpoint.from_state_dict(state_dict),
        )

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True)
    )
    results = trainer.fit()

    state_dict_checkpoint = results.checkpoint.to_dict()[MODEL_KEY]
    state_dict_report = results.metrics["state_dict"]

    for tensor in state_dict_report.values():
        assert not tensor.is_cuda

    for tensor in state_dict_checkpoint.values():
        assert not tensor.is_cuda


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
