import os
import time
import json
import pytorch_lightning as pl
from pytorch_lightning.loggers.csv_logs import CSVLogger

import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader, random_split
from torchmetrics import Accuracy
from torchvision.datasets import MNIST
from torchvision import transforms

import ray
from ray.air.config import CheckpointConfig, ScalingConfig
from ray.train.lightning import LightningTrainer, LightningConfigBuilder
import ray.tune as tune


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
        self.log("ptl/val_loss", avg_loss.item(), sync_dist=True)
        self.log("ptl/val_accuracy", avg_acc.item(), sync_dist=True)

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


if __name__ == "__main__":
    start = time.time()

    lightning_config = (
        LightningConfigBuilder()
        .module(
            MNISTClassifier,
            feature_dim=tune.choice([64, 128]),
            lr=tune.grid_search([0.01, 0.001]),
        )
        .trainer(
            max_epochs=5,
            accelerator="gpu",
            logger=CSVLogger("logs", name="my_exp_name"),
        )
        .fit_params(datamodule=MNISTDataModule(batch_size=200))
        .checkpointing(monitor="ptl/val_accuracy", mode="max")
        .build()
    )

    scaling_config = ScalingConfig(
        num_workers=3, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
    )

    lightning_trainer = LightningTrainer(
        scaling_config=scaling_config,
    )

    tuner = tune.Tuner(
        lightning_trainer,
        param_space={"lightning_config": lightning_config},
        run_config=ray.air.RunConfig(
            name="release-tuner-test",
            verbose=2,
            checkpoint_config=CheckpointConfig(
                num_to_keep=1,
                checkpoint_score_attribute="ptl/val_accuracy",
                checkpoint_score_order="max",
            ),
        ),
        tune_config=tune.TuneConfig(
            metric="ptl/val_accuracy", mode="max", num_samples=2
        ),
    )
    results = tuner.fit()
    print(results.get_best_result(metric="ptl/val_accuracy", mode="max"))
    print(results.__dict__)

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
