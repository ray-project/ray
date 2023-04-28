import os
import pytorch_lightning as pl
import torch
import torch.nn as nn
import tempfile

from ray.train.lightning import LightningCheckpoint
from ray.air.constants import MODEL_KEY
from torch.utils.data import DataLoader


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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
