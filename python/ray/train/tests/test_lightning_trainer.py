import pytorch_lightning as pl
import torch.nn as nn
import numpy as np
from ray.train.lightning import LightningCheckpoint, LightningTrainer
from torch.utils.data import DataLoader
import torch
import ray
from ray.train.tests.dummy_preprocessor import DummyPreprocessor


class LinearModule(pl.LightningModule):
    def __init__(self, input_dim, output_dim) -> None:
        super().__init__()
        self.linear = nn.Linear(input_dim, output_dim)

    def forward(self, input):
        return self.linear(input)

    def training_step(self, batch):
        output = self.forward(batch)
        loss = torch.sum(output)
        self.log("loss", loss)
        return loss

    def predict_step(self, batch, batch_idx):
        return self.forward(batch)

    def configure_optimizers(self):
        return torch.optim.SGD(self.parameters(), lr=0.1)


def test_trainer_with_native_dataloader():
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    module_init_config = {
        "input_dim": 32,
        "output_dim": 4
    }

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    dataloader = DataLoader(dataset, batch_size=batch_size)


    lightning_trainer_fit_params = {
        "train_dataloaders": dataloader
    }

    lightning_trainer_config = {
        "max_epochs": num_epochs, 
        "accelerator": "cpu",
        "strategy": "ddp"
    }

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=False, resources_per_worker={"CPU": 1})
    
    trainer = LightningTrainer(
        lightning_module=LinearModule, 
        lightning_module_config=module_init_config,
        lightning_trainer_config=lightning_trainer_config,
        lightning_trainer_fit_params=lightning_trainer_fit_params,
        scaling_config=scaling_config
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    assert "loss" in results.metrics


def test_trainer_with_ray_data():
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    module_init_config = {
        "input_dim": 32,
        "output_dim": 4
    }

    lightning_trainer_config = {
        "max_epochs": 4, 
        "accelerator": "cpu",
        "strategy": "ddp"
    }

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    train_dataset = ray.data.from_numpy(dataset)

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=False, resources_per_worker={"CPU": 1})
    
    trainer = LightningTrainer(
        lightning_module=LinearModule, 
        lightning_module_config=module_init_config,
        lightning_trainer_config=lightning_trainer_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset},
        datasets_iter_config={"batch_size": batch_size},
        preprocessor=DummyPreprocessor()
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    assert "loss" in results.metrics
    # assert results.checkpoint
    # assert isinstance(results.checkpoint, LightningCheckpoint)

if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
    