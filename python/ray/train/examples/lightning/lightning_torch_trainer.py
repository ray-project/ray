import os
import ray
import pytorch_lightning as pl
from ray.air import CheckpointConfig, RunConfig
from ray.air.config import ScalingConfig
from ray.train.lightning import (
    get_devices,
    prepare_trainer,
    RayDDPStrategy,
    RayDeepSpeedStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
    RayModelCheckpoint,
)

from ray.train.torch import TorchTrainer

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
        self.val_loss = []
        self.val_accuracy = []

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
        self.val_loss.append(loss)
        self.val_accuracy.append(acc)
        return {"val_loss": loss, "val_accuracy": acc}

    def on_validation_epoch_end(self):
        avg_loss = torch.stack(self.val_loss).mean()
        avg_acc = torch.stack(self.val_accuracy).mean()
        self.log("ptl/val_loss", avg_loss, sync_dist=True)
        self.log("ptl/val_accuracy", avg_acc, sync_dist=True)
        self.val_loss.clear()
        self.val_accuracy.clear()

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.trainer.model.parameters(), lr=self.lr)
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


import ray
from tempfile import TemporaryDirectory
from pytorch_lightning.callbacks import Callback
from pytorch_lightning import Trainer, LightningModule
from ray.train import Checkpoint


class RayTrainReportCallback(Callback):
    def on_train_epoch_end(self, trainer: Trainer, pl_module: LightningModule) -> None:
        with TemporaryDirectory() as tmpdir:
            metrics = trainer.callback_metrics
            metrics = {k: v.item() for k, v in metrics.items()}
            ckpt_path = os.path.join(tmpdir, f"ckpt_epoch_{trainer.current_epoch}.pth")
            trainer.save_checkpoint(ckpt_path, weights_only=False)
            checkpoint = Checkpoint.from_directory(tmpdir)
            ray.train.report(metrics=metrics, checkpoint=checkpoint)


def train_loop_per_worker():
    datamodule = MNISTDataModule(batch_size=128)
    model = MNISTClassifier(feature_dim=128, lr=0.001)

    trainer = pl.Trainer(
        accelerator="gpu",
        max_epochs=4,
        devices=get_devices(),
        # strategy=RayDDPStrategy(),
        strategy=RayFSDPStrategy(),
        plugins=[RayLightningEnvironment()],
        # callbacks=[RayModelCheckpoint(save_top_k=2, monitor="ptl/val_accuracy", save_last=True)]
        callbacks=[RayTrainReportCallback()],
    )

    trainer = prepare_trainer(trainer)

    trainer.fit(model, datamodule=datamodule)


if __name__ == "__main__":
    scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        scaling_config=scaling_config,
        run_config=RunConfig(
            name="exp",
            checkpoint_config=CheckpointConfig(
                num_to_keep=2,
                checkpoint_score_attribute="ptl/val_accuracy",
                checkpoint_score_order="max",
            ),
        ),
    )

    result = trainer.fit()
    print(result)
