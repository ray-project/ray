import pytorch_lightning as pl
import torch.nn as nn
import numpy as np
from ray.train.lightning import LightningCheckpoint, LightningTrainer
from torch.utils.data import DataLoader
import torch
import ray


class LinearModule(pl.LightningModule):
    def __init__(self, input_dim, output_dim) -> None:
        super().__init__()
        self.linear = nn.Linear(input_dim, output_dim)

    def forward(self, input):
        return self.linear(input)

    def training_step(self, batch):
        output = self.forward(batch)
        return torch.sum(output)

    def predict_step(self, batch, batch_idx):
        return self.forward(batch)

    def configure_optimizers(self):
        return torch.optim.SGD(self.parameters(), lr=0.1)


def test_trainer_native_dataloader():
    module_init_config = {
        "input_dim": 8,
        "output_dim": 4
    }

    dataset = np.random.rand(10, 8).astype(np.float32)
    dataloader = DataLoader(dataset, batch_size=2)


    lightning_trainer_fit_params = {
        "train_dataloaders": dataloader
    }

    lightning_trainer_config = {
        "max_epochs": 4, 
        "accelerator": "cpu",
        "strategy": "ddp"
    }

    scaling_config = ray.air.ScalingConfig(num_workers=2, use_gpu=False, resources_per_worker={"CPU": 1})
    
    trainer = LightningTrainer(
        lightning_module=LinearModule, 
        lightning_module_config=module_init_config,
        lightning_trainer_config=lightning_trainer_config,
        lightning_trainer_fit_params=lightning_trainer_fit_params,
        scaling_config=scaling_config
    )

    trainer.fit()

if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
    