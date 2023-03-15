import os
import time
import json

import pytorch_lightning as pl

import torch
import torch.nn.functional as F
from torchmetrics import Accuracy
from torch.utils.data import DataLoader, random_split
from torchvision.datasets import MNIST
from torchvision import transforms
from pytorch_lightning.loggers.csv_logs import CSVLogger
from ray.air.config import ScalingConfig
from ray.train.lightning import LightningTrainer, LightningConfigBuilder


class MNISTClassifier(pl.LightningModule):
    def __init__(self, lr, feature_dim):
        super(MNISTClassifier, self).__init__()
        self.fc1 = torch.nn.Linear(28 * 28, feature_dim)
        self.fc2 = torch.nn.Linear(feature_dim, 10)
        self.lr = lr
        self.accuracy = Accuracy()

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
        return {"val_loss": loss, "val_accuracy": acc}

    def validation_epoch_end(self, outputs):
        avg_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
        avg_acc = torch.stack([x["val_accuracy"] for x in outputs]).mean()
        self.log("ptl/val_loss", avg_loss, sync_dist=True)
        self.log("ptl/val_accuracy", avg_acc, sync_dist=True)

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


def get_mnist_dataloaders():
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )
    mnist_train = MNIST("/tmp/data", train=True, download=True, transform=transform)
    mnist_val = MNIST("/tmp/data", train=False, download=True, transform=transform)
    train_loader = DataLoader(mnist_train, batch_size=32, shuffle=True)
    val_loader = DataLoader(mnist_val, batch_size=32, shuffle=False)
    return train_loader, val_loader


if __name__ == "__main__":
    start = time.time()

    lightning_config = (
        LightningConfigBuilder()
        .module(MNISTClassifier, feature_dim=128, lr=0.001)
        .trainer(
            max_epochs=3,
            accelerator="gpu",
            logger=CSVLogger("logs", name="my_exp_name"),
        )
        .fit_params(datamodule=MNISTDataModule(batch_size=128))
        .checkpointing(monitor="ptl/val_accuracy", mode="max", save_last=True)
        .build()
    )

    scaling_config = ScalingConfig(
        num_workers=6, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
    )

    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
    )

    trainer.fit()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/ray_lightning_user_test.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
