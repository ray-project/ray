import os
from ray.air import Checkpoint
import torch
import torch.nn as nn
import torch.nn.functional as F
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
from torchvision import transforms
from torchvision.datasets import MNIST
from torch.utils.data import DataLoader, random_split
from torchmetrics import Accuracy

import ray
from ray.train.lightning import RayDDPStrategy, RayEnvironment, RayModelCheckpoint
from ray.train.torch import TorchTrainer
from ray.air.config import RunConfig, CheckpointConfig, ScalingConfig
from ray.train.torch.train_loop_utils import RayIterableDataset

transform = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
)
mnist = MNIST("/tmp", train=True, download=True, transform=transform)
mnist_train, mnist_val = random_split(mnist, [55000, 5000])


class MNISTClassifier(pl.LightningModule):
    def __init__(self, lr=1e-3, feature_dim=128):
        torch.manual_seed(421)
        super(MNISTClassifier, self).__init__()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, feature_dim),
            nn.ReLU(),
            nn.Linear(feature_dim, 10),
            nn.ReLU(),
        )
        self.lr = lr
        self.accuracy = Accuracy(task="multiclass", num_classes=10)
        self.eval_loss = []
        self.eval_accuracy = []

    def forward(self, x):
        x = x.view(-1, 28 * 28)
        x = self.linear_relu_stack(x)
        return x

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("train_loss", loss)
        return loss

    def validation_step(self, val_batch, batch_idx):
        loss, acc = self._shared_eval(val_batch)
        self.log("val_accuracy", acc)
        self.eval_loss.append(loss)
        self.eval_accuracy.append(acc)
        return {"val_loss": loss, "val_accuracy": acc}

    def _shared_eval(self, batch):
        x, y = batch
        logits = self.forward(x)
        loss = F.nll_loss(logits, y)
        acc = self.accuracy(logits, y)
        return loss, acc

    def on_validation_epoch_end(self):
        avg_loss = torch.stack(self.eval_loss).mean()
        avg_acc = torch.stack(self.eval_accuracy).mean()
        self.log("val_loss", avg_loss, sync_dist=True)
        self.log("val_accuracy", avg_acc, sync_dist=True)
        self.eval_loss.clear()
        self.eval_accuracy.clear()

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
        return optimizer


from ray.air import session


def train_loop_per_worker(config):
    parallel_devices = ray.train.lightning.setup()

    model = MNISTClassifier(lr=config["lr"], feature_dim=config["feature_dim"])

    train_ds = RayIterableDataset(session.get_dataset_shard("train"))
    val_ds = RayIterableDataset(session.get_dataset_shard("val"))

    train_dl = DataLoader(train_ds, batch_size=config["batch_size"])
    val_dl = DataLoader(val_ds, batch_size=config["batch_size"])

    logger = WandbLogger(
        name="demo_run", save_dir="./wandb_logs", offline=True, id="unique_id"
    )

    # Create Strategy, Environment and ModelCheckpoint provided by Ray AIR
    strategy = RayDDPStrategy()
    environment = RayEnvironment()
    checkpoint_callback = RayModelCheckpoint(
        monitor="val_accuracy", mode="max", save_top_k=3, save_last=True
    )

    trainer = pl.Trainer(
        max_epochs=5,
        accelerator="gpu",
        devices=parallel_devices,
        strategy=strategy,
        plugins=[environment],
        callbacks=[checkpoint_callback],
        logger=logger,
    )

    ray.train.lightning.prepare_trainer(trainer)

    trainer.fit(model, train_dataloaders=train_dl, val_dataloaders=val_dl)


if __name__ == "__main__":
    train_loop_config = {"batch_size": 128, "lr": 1e-3, "feature_dim": 128}

    air_trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
        run_config=RunConfig(
            name="demo_run",
            storage_path="/mnt/cluster_storage/ray_results",
            checkpoint_config=CheckpointConfig(
                num_to_keep=3,
                checkpoint_score_order="max",
                checkpoint_score_attribute="val_accuracy",
            ),
        ),
        datasets={
            "train": ray.data.from_torch(mnist_train),
            "val": ray.data.from_torch(mnist_val),
        },
    )

    air_trainer.fit()
