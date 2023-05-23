import torch
import torch.nn as nn
import torch.nn.functional as F
import pytorch_lightning as pl
from torch.utils.data import DataLoader
from torchmetrics import Accuracy


class LinearModule(pl.LightningModule):
    def __init__(self, input_dim, output_dim, strategy="ddp") -> None:
        super().__init__()
        self.linear = nn.Linear(input_dim, output_dim)
        self.loss = []
        self.strategy = strategy

    def forward(self, input):
        # Backwards compat for Ray data strict mode.
        if isinstance(input, dict) and len(input) == 1:
            input = list(input.values())[0]
        return self.linear(input)

    def training_step(self, batch):
        output = self.forward(batch)
        loss = torch.sum(output)
        self.log("loss", loss)
        return loss

    def validation_step(self, val_batch, batch_idx):
        loss = self.forward(val_batch)
        self.loss.append(loss)
        return {"val_loss": loss}

    def on_validation_epoch_end(self) -> None:
        avg_loss = torch.stack(self.loss).mean()
        self.log("val_loss", avg_loss)
        self.loss.clear()

    def predict_step(self, batch, batch_idx):
        return self.forward(batch)

    def configure_optimizers(self):
        if self.strategy == "fsdp":
            # Feed FSDP wrapped model parameters to optimizer
            return torch.optim.SGD(self.trainer.model.parameters(), lr=0.1)
        else:
            return torch.optim.SGD(self.parameters(), lr=0.1)


class DoubleLinearModule(pl.LightningModule):
    def __init__(self, input_dim_1, input_dim_2, output_dim) -> None:
        super().__init__()
        self.linear_1 = nn.Linear(input_dim_1, output_dim)
        self.linear_2 = nn.Linear(input_dim_2, output_dim)
        self.loss = []

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
        self.loss.append(loss)
        return {"val_loss": loss}

    def on_validation_epoch_end(self) -> None:
        print("Validation Epoch:", self.current_epoch)
        avg_loss = torch.stack(self.loss).mean()
        self.log("val_loss", avg_loss)
        self.loss.clear()

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


class LightningMNISTClassifier(pl.LightningModule):
    def __init__(self, lr: float, layer_1: int, layer_2: int):
        super(LightningMNISTClassifier, self).__init__()

        self.lr = lr
        # mnist images are (1, 28, 28) (channels, width, height)
        self.layer_1 = torch.nn.Linear(28 * 28, layer_1)
        self.layer_2 = torch.nn.Linear(layer_1, layer_2)
        self.layer_3 = torch.nn.Linear(layer_2, 10)
        self.accuracy = Accuracy(task="multiclass", num_classes=10)
        self.val_acc_list = []
        self.val_loss_list = []

    def forward(self, x):
        batch_size, channels, width, height = x.size()
        x = x.view(batch_size, -1)
        x = self.layer_1(x)
        x = torch.relu(x)
        x = self.layer_2(x)
        x = torch.relu(x)
        x = self.layer_3(x)
        x = torch.log_softmax(x, dim=1)
        return x

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.lr)

    def training_step(self, train_batch, batch_idx):
        x, y = train_batch
        logits = self.forward(x)
        loss = F.nll_loss(logits, y)
        acc = self.accuracy(logits, y)
        self.log("ptl/train_loss", loss)
        self.log("ptl/train_accuracy", acc)
        return loss

    def validation_step(self, val_batch, batch_idx):
        x, y = val_batch
        logits = self.forward(x)
        loss = F.nll_loss(logits, y)
        acc = self.accuracy(logits, y)
        self.val_acc_list.append(acc)
        self.val_loss_list.append(loss)
        return {"val_loss": loss, "val_accuracy": acc}

    def on_validation_epoch_end(self):
        avg_loss = torch.stack(self.val_loss_list).mean()
        avg_acc = torch.stack(self.val_acc_list).mean()
        self.log("ptl/val_loss", avg_loss)
        self.log("ptl/val_accuracy", avg_acc)
        self.val_acc_list.clear()
        self.val_loss_list.clear()

    def predict_step(self, batch, batch_idx, dataloader_idx=None):
        x = batch
        logits = self.forward(x)
        return torch.argmax(logits, dim=-1)
