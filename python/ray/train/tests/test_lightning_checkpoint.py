import pytorch_lightning as pl
import torch.nn as nn
import numpy as np
from ray.train.lightning import LightningCheckpoint
from torch.utils.data import DataLoader
import torch
from ray.air.constants import MODEL_KEY


class Net(pl.LightningModule):
    def __init__(self) -> None:
        super().__init__()
        self.linear = nn.Linear(3, 3, dtype=torch.float64)

    def forward(self, input):
        return self.linear(input)

    def training_step(self, batch):
        output = self.forward(batch)
        return torch.sum(output)

    def predict_step(self, batch, batch_idx):
        return self.forward(batch)

    def configure_optimizers(self):
        return torch.optim.SGD(self.parameters(), lr=0.1)


def test_from_path():
    CHECKPOINT_TMP_DIR = "./tmp"
    model = Net()

    # Define a simple dataset
    data = np.random.rand(10, 3)
    dataloader = DataLoader(data, batch_size=2)

    # Train one epoch and save a checkpoint
    trainer = pl.Trainer(max_epochs=1, enable_progress_bar=False,
                         enable_checkpointing=False)
    trainer.fit(model=model, train_dataloaders=dataloader)
    ckpt_path = f"{CHECKPOINT_TMP_DIR}/{MODEL_KEY}"
    trainer.save_checkpoint(ckpt_path)

    # Load the checkpoint from directory
    lightning_checkpoint = LightningCheckpoint.from_path(ckpt_path)
    checkpoint_model = lightning_checkpoint.get_model(Net)
    assert torch.equal(checkpoint_model.linear.weight, model.linear.weight)

    # Check the model outputs
    for i, batch in enumerate(dataloader):
        output = model.predict_step(batch, i)
        checkpoint_output = checkpoint_model.predict_step(batch, i)
        assert torch.equal(output, checkpoint_output)


def test_from_directory():
    CHECKPOINT_TMP_DIR = "./tmp"
    model = Net()

    # Define a simple dataset
    data = np.random.rand(10, 3)
    dataloader = DataLoader(data, batch_size=2)

    # Train one epoch and save a checkpoint
    trainer = pl.Trainer(max_epochs=1, enable_progress_bar=False,
                         enable_checkpointing=False)
    trainer.fit(model=model, train_dataloaders=dataloader)
    trainer.save_checkpoint(f"{CHECKPOINT_TMP_DIR}/{MODEL_KEY}")

    # Load the checkpoint from directory
    lightning_checkpoint = LightningCheckpoint.from_directory(CHECKPOINT_TMP_DIR)
    checkpoint_model = lightning_checkpoint.get_model(Net)
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
