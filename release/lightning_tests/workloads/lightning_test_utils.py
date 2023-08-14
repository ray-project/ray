import os
import torch
import pytorch_lightning as pl
import torch.nn.functional as F
from torchmetrics import Accuracy
from torch.utils.data import DataLoader, random_split
from torchvision.datasets import MNIST
from torchvision import transforms


class MNISTClassifier(pl.LightningModule):
    def __init__(self, lr, feature_dim):
        super(MNISTClassifier, self).__init__()
        self.fc1 = torch.nn.Linear(28 * 28, feature_dim)
        self.fc2 = torch.nn.Linear(feature_dim, 10)
        self.lr = lr
        self.accuracy = Accuracy(task="multiclass", num_classes=10, top_k=1)
        self.validation_step_outputs = []

    def forward(self, x):
        x = x.view(-1, 28 * 28)
        x = torch.relu(self.fc1(x))
        x = self.fc2(x)
        return x

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("train_loss", loss, on_step=True)
        return loss

    def validation_step(self, val_batch, batch_idx):
        x, y = val_batch
        logits = self.forward(x)
        loss = F.nll_loss(logits, y)
        acc = self.accuracy(logits, y)
        result = {"val_loss": loss, "val_accuracy": acc}
        self.validation_step_outputs.append(result)
        return

    def on_validation_epoch_end(self):
        avg_loss = torch.stack(
            [x["val_loss"] for x in self.validation_step_outputs]
        ).mean()
        avg_acc = torch.stack(
            [x["val_accuracy"] for x in self.validation_step_outputs]
        ).mean()
        self.validation_step_outputs.clear()

        # TODO(yunxuanx): change this back to ptl/val_loss after
        # we resolved the metric unpacking issue
        self.log("val_loss", avg_loss, sync_dist=True)
        self.log("val_accuracy", avg_acc, sync_dist=True)

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
        return optimizer


class MNISTDataModule(pl.LightningDataModule):
    def __init__(self, batch_size=100):
        super().__init__()
        self.data_dir = os.getcwd()
        self.batch_size = batch_size
        self.transform = transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        )

    def setup(self, stage=None):
        # split data into train and val sets
        if stage == "fit" or stage is None:
            mnist = MNIST(
                self.data_dir, train=True, download=True, transform=self.transform
            )
            self.mnist_train, self.mnist_val = random_split(mnist, [55000, 5000])

        # assign test set for use in dataloader(s)
        if stage == "test" or stage is None:
            self.mnist_test = MNIST(
                self.data_dir, train=False, download=True, transform=self.transform
            )

    def train_dataloader(self):
        return DataLoader(self.mnist_train, batch_size=self.batch_size, num_workers=4)

    def val_dataloader(self):
        return DataLoader(self.mnist_val, batch_size=self.batch_size, num_workers=4)

    def test_dataloader(self):
        return DataLoader(self.mnist_test, batch_size=self.batch_size, num_workers=4)
