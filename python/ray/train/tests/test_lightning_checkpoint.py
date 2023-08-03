import os
import pytorch_lightning as pl
import torch
import torch.nn as nn
import tempfile

import ray
from ray.air.constants import MODEL_KEY
from torch.utils.data import DataLoader
from ray.train.tests.lightning_test_utils import LinearModule, DummyDataModule
from ray.train.lightning import (
    LightningCheckpoint,
    LightningConfigBuilder,
    LightningTrainer,
)


class Net(pl.LightningModule):
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


def test_load_from_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        model = Net(input_dim=3, output_dim=3)

        # Define a simple dataset
        data = torch.randn(10, 3)
        dataloader = DataLoader(data, batch_size=2)

        # Train one epoch and save a checkpoint
        trainer = pl.Trainer(
            max_epochs=1,
            accelerator="cpu",
            enable_progress_bar=False,
            enable_checkpointing=False,
        )
        trainer.fit(model=model, train_dataloaders=dataloader)
        ckpt_path = f"{tmpdir}/random_checkpoint_name.ckpt"
        trainer.save_checkpoint(ckpt_path)

        # Load the checkpoint from directory
        lightning_checkpoint = LightningCheckpoint.from_path(ckpt_path)
        checkpoint_model = lightning_checkpoint.get_model(
            Net, input_dim=3, output_dim=3
        )
        assert torch.equal(checkpoint_model.linear.weight, model.linear.weight)

        # Ensure the model checkpoint was copied into a tmp dir
        cached_checkpoint_dir = lightning_checkpoint._cache_dir
        cached_checkpoint_path = f"{cached_checkpoint_dir}/{MODEL_KEY}"
        assert os.path.exists(cached_checkpoint_dir)
        assert os.path.exists(cached_checkpoint_path)

        # Check the model outputs
        for i, batch in enumerate(dataloader):
            output = model.predict_step(batch, i)
            checkpoint_output = checkpoint_model.predict_step(batch, i)
            assert torch.equal(output, checkpoint_output)


def test_from_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        print("tmpdir", str(tmpdir))
        model = Net(input_dim=3, output_dim=3)

        # Define a simple dataset
        data = torch.randn(10, 3)
        dataloader = DataLoader(data, batch_size=2)

        # Train one epoch and save a checkpoint
        trainer = pl.Trainer(
            max_epochs=1,
            accelerator="cpu",
            enable_progress_bar=False,
            enable_checkpointing=False,
        )
        trainer.fit(model=model, train_dataloaders=dataloader)
        trainer.save_checkpoint(f"{tmpdir}/{MODEL_KEY}")

        # Load the checkpoint from directory
        lightning_checkpoint = LightningCheckpoint.from_directory(tmpdir)
        checkpoint_model = lightning_checkpoint.get_model(
            Net, input_dim=3, output_dim=3
        )
        assert torch.equal(checkpoint_model.linear.weight, model.linear.weight)

        # Check the model outputs
        for i, batch in enumerate(dataloader):
            output = model.predict_step(batch, i)
            checkpoint_output = checkpoint_model.predict_step(batch, i)
            assert torch.equal(output, checkpoint_output)


def test_fsdp_checkpoint():
    num_epochs = 1
    batch_size = 8
    input_dim = 32
    output_dim = 4
    dataset_size = 256

    datamodule = DummyDataModule(batch_size, dataset_size)

    config_builder = (
        LightningConfigBuilder()
        .module(
            LinearModule, input_dim=input_dim, output_dim=output_dim, strategy="fsdp"
        )
        .trainer(max_epochs=num_epochs, accelerator="gpu")
        .strategy("fsdp")
        .checkpointing(save_last=True)
        .fit_params(datamodule=datamodule)
    )

    scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)

    trainer = LightningTrainer(
        lightning_config=config_builder.build(), scaling_config=scaling_config
    )

    results = trainer.fit()

    with results.checkpoint.as_directory() as checkpoint_dir:
        checkpoint = torch.load(f"{checkpoint_dir}/{MODEL_KEY}")
        model = LinearModule(input_dim=input_dim, output_dim=output_dim)

        for key in model.state_dict().keys():
            assert key in checkpoint["state_dict"]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
