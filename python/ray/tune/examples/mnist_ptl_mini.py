import math

import torch
from filelock import FileLock
from torch.nn import functional as F
import pytorch_lightning as pl
from pl_bolts.datamodules.mnist_datamodule import MNISTDataModule
import os
from ray.tune.integration.pytorch_lightning import TuneReportCallback

from ray import tune


class LightningMNISTClassifier(pl.LightningModule):
    def __init__(self, config, data_dir=None):
        super(LightningMNISTClassifier, self).__init__()

        self.data_dir = data_dir or os.getcwd()
        self.lr = config["lr"]
        layer_1, layer_2 = config["layer_1"], config["layer_2"]
        self.batch_size = config["batch_size"]

        # mnist images are (1, 28, 28) (channels, width, height)
        self.layer_1 = torch.nn.Linear(28 * 28, layer_1)
        self.layer_2 = torch.nn.Linear(layer_1, layer_2)
        self.layer_3 = torch.nn.Linear(layer_2, 10)
        self.accuracy = pl.metrics.Accuracy()

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
        return {"val_loss": loss, "val_accuracy": acc}

    def validation_epoch_end(self, outputs):
        avg_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
        avg_acc = torch.stack([x["val_accuracy"] for x in outputs]).mean()
        self.log("ptl/val_loss", avg_loss)
        self.log("ptl/val_accuracy", avg_acc)


def train_mnist_tune(config, num_epochs=10, num_gpus=0):
    data_dir = os.path.abspath("./data")
    model = LightningMNISTClassifier(config, data_dir)
    with FileLock(os.path.expanduser("~/.data.lock")):
        dm = MNISTDataModule(
            data_dir=data_dir, num_workers=1, batch_size=config["batch_size"]
        )
    metrics = {"loss": "ptl/val_loss", "acc": "ptl/val_accuracy"}
    trainer = pl.Trainer(
        max_epochs=num_epochs,
        # If fractional GPUs passed in, convert to int.
        gpus=math.ceil(num_gpus),
        progress_bar_refresh_rate=0,
        callbacks=[TuneReportCallback(metrics, on="validation_end")],
    )
    trainer.fit(model, dm)


def tune_mnist(num_samples=10, num_epochs=10, gpus_per_trial=0):
    config = {
        "layer_1": tune.choice([32, 64, 128]),
        "layer_2": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
    }

    trainable = tune.with_parameters(
        train_mnist_tune, num_epochs=num_epochs, num_gpus=gpus_per_trial
    )
    analysis = tune.run(
        trainable,
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        metric="loss",
        mode="min",
        config=config,
        num_samples=num_samples,
        name="tune_mnist",
    )

    print("Best hyperparameters found were: ", analysis.best_config)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using " "Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_mnist(num_samples=1, num_epochs=1, gpus_per_trial=0)
    else:
        if args.server_address:
            import ray

            ray.init(f"ray://{args.server_address}")

        tune_mnist(num_samples=10, num_epochs=10, gpus_per_trial=0)
