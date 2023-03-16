from timeit import default_timer as timer

from ray.train.torch.torch_checkpoint import TorchCheckpoint
import torch
import torchvision

from ray.air import session

import ray.train as train
from ray.air.config import ScalingConfig
from ray.train.torch.torch_trainer import TorchTrainer


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

        session.report({}, checkpoint=TorchCheckpoint.from_model(model))

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    results = trainer.fit()
    assert results.checkpoint
    assert results.checkpoint.get_model()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
