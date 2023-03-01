import os
os.environ['RAY_ML_DEV'] = "1"

import pytorch_lightning as pl
from ray.train.lightning import LightningTrainer
from torchvision.datasets import MNIST
from torchvision import transforms
from torch.utils.data import DataLoader, random_split
import torch
from torch import nn
import torch.nn.functional as F

from ray.tune.syncer import SyncConfig
from ray.air.config import CheckpointConfig, ScalingConfig, RunConfig
import ray.train as train
from ptl_tests.utils import LitAutoEncoder, LightningMNISTClassifier, MNISTDataModule
from torchmetrics import Accuracy

LightningMNISTModelConfig = {
    "config": {
        "layer_1": 32,
        "layer_2": 64,
        "lr": 1e-4,
    }
}

# class LightningMNISTClassifier(pl.LightningModule):
#     def __init__(self, config, data_dir=None):
#         super(LightningMNISTClassifier, self).__init__()

#         self.data_dir = data_dir or os.getcwd()
#         self.lr = config["lr"]
#         layer_1, layer_2 = config["layer_1"], config["layer_2"]

#         # mnist images are (1, 28, 28) (channels, width, height)
#         self.layer_1 = torch.nn.Linear(28 * 28, layer_1)
#         self.layer_2 = torch.nn.Linear(layer_1, layer_2)
#         self.layer_3 = torch.nn.Linear(layer_2, 10)
#         self.accuracy = Accuracy()

#     def forward(self, x):
#         batch_size, channels, width, height = x.size()
#         x = x.view(batch_size, -1)
#         x = self.layer_1(x)
#         x = torch.relu(x)
#         x = self.layer_2(x)
#         x = torch.relu(x)
#         x = self.layer_3(x)
#         x = torch.log_softmax(x, dim=1)
#         return x

#     def configure_optimizers(self):
#         return torch.optim.Adam(self.parameters(), lr=self.lr)

#     def training_step(self, train_batch, batch_idx):
#         x, y = train_batch
#         logits = self.forward(x)
#         loss = F.nll_loss(logits, y)
#         acc = self.accuracy(logits, y)
#         self.log("ptl/train_loss", loss)
#         self.log("ptl/train_accuracy", acc)
#         return loss

#     def validation_step(self, val_batch, batch_idx):
#         x, y = val_batch
#         logits = self.forward(x)
#         loss = F.nll_loss(logits, y)
#         acc = self.accuracy(logits, y)
#         return {"val_loss": loss, "val_accuracy": acc}

#     def validation_epoch_end(self, outputs):
#         avg_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
#         avg_acc = torch.stack([x["val_accuracy"] for x in outputs]).mean()
#         self.log("ptl/val_loss", avg_loss)
#         self.log("ptl/val_accuracy", avg_acc)


# class MNISTDataModule(pl.LightningDataModule):
#     def __init__(self, batch_size=100):
#         super().__init__()
#         self.data_dir = os.getcwd()
#         self.batch_size = batch_size
#         self.transform = transforms.Compose([
#             transforms.ToTensor(),
#             transforms.Normalize((0.1307,), (0.3081,))
#         ])

#     def prepare_data(self):
#         # download data
#         MNIST(self.data_dir, train=True, download=True)
#         MNIST(self.data_dir, train=False, download=True)

#     def setup(self, stage=None):
#         # split data into train and val sets
#         if stage == 'fit' or stage is None:
#             mnist = MNIST(self.data_dir, train=True, transform=self.transform)
#             self.mnist_train, self.mnist_val = random_split(mnist, [55000, 5000])

#         # assign test set for use in dataloader(s)
#         if stage == 'test' or stage is None:
#             self.mnist_test = MNIST(self.data_dir, train=False, transform=self.transform)

#     def train_dataloader(self):
#         return DataLoader(self.mnist_train, batch_size=self.batch_size, num_workers=4)

#     def val_dataloader(self):
#         return DataLoader(self.mnist_val, batch_size=self.batch_size, num_workers=4)

#     def test_dataloader(self):
#         return DataLoader(self.mnist_test, batch_size=self.batch_size, num_workers=4)


lightning_trainer_config = {
    "max_epochs": 10, 
    "accelerator": "gpu",
    "strategy": "ddp"
}

model_checkpoint_config = {
    "monitor": "ptl/val_accuracy",
    "save_top_k": 3,
    "mode": "max"
}

scaling_config = ScalingConfig(num_workers=8, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1})

air_checkpoint_config = CheckpointConfig(num_to_keep=3, checkpoint_score_attribute="ptl/val_accuracy", checkpoint_score_order="max")

run_config = RunConfig(
    name="ptl-e2e-classifier",
    local_dir="/mnt/cluster_storage/ray_lightning_results",
    sync_config=SyncConfig(syncer=None),
    checkpoint_config=air_checkpoint_config
)

trainer = LightningTrainer(
    lightning_module=LightningMNISTClassifier,
    lightning_module_config=LightningMNISTModelConfig,
    lightning_trainer_config=lightning_trainer_config,
    ddp_strategy_config={},
    model_checkpoint_config=model_checkpoint_config,
    scaling_config=scaling_config,
    run_config=run_config,
    datamodule=MNISTDataModule()
)

trainer.fit()