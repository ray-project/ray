import pytorch_lightning as pl
import torch.nn as nn
import torch
from torch.utils.data import DataLoader


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


class DoubleLinearModule(pl.LightningModule):
    def __init__(self, input_dim_1, input_dim_2, output_dim) -> None:
        super().__init__()
        self.linear_1 = nn.Linear(input_dim_1, output_dim)
        self.linear_2 = nn.Linear(input_dim_2, output_dim)

    def forward(self, batch):
        input_1 = batch["input_1"]
        input_2 = batch["input_2"]
        return self.linear_1(input_1) + self.linear_2(input_2)

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
        self.train_data = torch.randn(dataset_size, 32)
        self.val_data = torch.randn(dataset_size, 32)

    def train_dataloader(self):
        return DataLoader(self.train_data, batch_size=self.batch_size)

    def val_dataloader(self):
        return DataLoader(self.val_data, batch_size=self.batch_size)
