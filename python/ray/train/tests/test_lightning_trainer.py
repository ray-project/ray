import pytorch_lightning as pl
from pytorch_lightning.core import datamodule
from ray.train import lightning
import torch.nn as nn
import numpy as np
from ray.train.lightning import LightningConfig, LightningTrainer
from torch.utils.data import DataLoader
import torch
import ray
from ray.train.tests.dummy_preprocessor import DummyPreprocessor
import pytest


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
    
    def validation_step(self, val_batch, batch_idx):
        loss = self.forward(val_batch)
        return {"val_loss": loss}
    
    def validation_epoch_end(self, outputs) -> None:
        avg_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
        self.log("val_loss", avg_loss)

    def predict_step(self, batch, batch_idx):
        return self.forward(batch)

    def configure_optimizers(self):
        return torch.optim.SGD(self.parameters(), lr=0.1)


class DummyDataModule(pl.LightningDataModule):
    def __init__(self, batch_size: int = 8, dataset_size: int = 256) -> None:
        super().__init__()
        self.batch_size = batch_size
        self.train_data = np.random.rand(dataset_size, 32).astype(np.float32)
        self.val_data = np.random.rand(dataset_size, 32).astype(np.float32)
    
    def train_dataloader(self):
        return DataLoader(self.train_data, batch_size=self.batch_size)
    
    def val_dataloader(self):
        return DataLoader(self.val_data, batch_size=self.batch_size)


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(accelerator, datasource):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    lightning_config = LightningConfig()
    lightning_config.set_module_class(LinearModule)
    lightning_config.set_module_init_config(input_dim=32, output_dim=4)
    lightning_config.set_trainer_init_config(max_epochs=num_epochs, accelerator=accelerator)

    datamodule = DummyDataModule(batch_size, dataset_size)
    train_loader = datamodule.train_dataloader()
    val_loader = datamodule.val_dataloader()

    if datasource == "dataloader":
        lightning_config.set_trainer_fit_params(train_dataloaders=train_loader, val_dataloaders=val_loader)
    if datasource == "datamodule":
        lightning_config.set_trainer_fit_params(datamodule=datamodule)

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=(accelerator=="gpu"))
    
    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config
    )

    results = trainer.fit()
    # TODO(yunxuanx): add asserts after supporting logging and checkpointing
    # assert results.metrics["epoch"] == num_epochs - 1
    # assert results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    # assert "loss" in results.metrics


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_ray_data(accelerator):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    train_dataset = ray.data.from_numpy(dataset)
    val_dataset = ray.data.from_numpy(dataset)

    lightning_config = LightningConfig()
    lightning_config.set_module_class(LinearModule)
    lightning_config.set_module_init_config(input_dim=32, output_dim=4)
    lightning_config.set_trainer_init_config(max_epochs=num_epochs, accelerator=accelerator)

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=(accelerator=="gpu"))
    
    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "val": val_dataset},
        dataset_iter_config={"batch_size": batch_size}
    )

    results = trainer.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
    