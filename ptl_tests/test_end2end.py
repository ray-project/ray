import os
import pytorch_lightning as pl
from ray.train.lightning import LightningTrainer
from torchvision.datasets import MNIST
from torchvision import transforms
from torch.utils.data import DataLoader, random_split
import torch
from torch import nn
import torch.nn.functional as F

from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig
import ray.train as train

class MNISTDataModule(pl.LightningDataModule):
    def __init__(self, batch_size=100):
        super().__init__()
        self.data_dir = os.getcwd()
        self.batch_size = batch_size
        self.transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307,), (0.3081,))
        ])

    def prepare_data(self):
        # download data
        MNIST(self.data_dir, train=True, download=True)
        MNIST(self.data_dir, train=False, download=True)

    def setup(self, stage=None):
        # split data into train and val sets
        if stage == 'fit' or stage is None:
            mnist = MNIST(self.data_dir, train=True, transform=self.transform)
            self.mnist_train, self.mnist_val = random_split(mnist, [55000, 5000])

        # assign test set for use in dataloader(s)
        if stage == 'test' or stage is None:
            self.mnist_test = MNIST(self.data_dir, train=False, transform=self.transform)

    def train_dataloader(self):
        # TODO(): figure out how does lightning create distributed samplers
        dataloader = DataLoader(self.mnist_train, batch_size=self.batch_size, num_workers=4)
        return train.torch.prepare_data_loader(dataloader)

    def val_dataloader(self):
        dataloader = DataLoader(self.mnist_val, batch_size=self.batch_size, num_workers=4)
        return train.torch.prepare_data_loader(dataloader)

    def test_dataloader(self):
        dataloader = DataLoader(self.mnist_test, batch_size=self.batch_size, num_workers=4)
        return train.torch.prepare_data_loader(dataloader)

# Define Lightning Models
class Encoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = nn.Sequential(nn.Linear(28 * 28, 64), nn.ReLU(), nn.Linear(64, 3))

    def forward(self, x):
        return self.l1(x)

class Decoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = nn.Sequential(nn.Linear(3, 64), nn.ReLU(), nn.Linear(64, 28 * 28))

    def forward(self, x):
        return self.l1(x)

class LitAutoEncoder(pl.LightningModule):
    def __init__(self):
        super().__init__()
        self.encoder = Encoder()
        self.decoder = Decoder()
        self.global_steps = 0

    def training_step(self, batch, batch_idx):
        # training_step defines the train loop.
        x, y = batch
        x = x.view(x.size(0), -1)
        z = self.encoder(x)
        x_hat = self.decoder(z)
        loss = F.mse_loss(x_hat, x)

        self.global_steps += 1
        if self.global_steps % 10 == 0:
            loss_value = loss.item()
            self.log_dict({"loss": loss_value, "steps": self.global_steps})
        if self.global_step % 3 == 0:
            self.log("metric_step_3", self.global_steps)
        if self.global_step % 5 == 0:
            self.log("metric_step_5", self.global_steps)
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
        return optimizer

    def training_epoch_end(self, outputs) -> None:
        loss = sum(output["loss"] for output in outputs) / len(outputs)
        self.log_dict({"epoch_end_metric": 123})
    
    def validation_step(self, batch, batch_idx):
        x, y = batch
        x = x.view(x.size(0), -1)
        z = self.encoder(x)
        x_hat = self.decoder(z)
        val_loss = F.mse_loss(x_hat, x)
        return {"val_loss": val_loss}
    
    def validation_epoch_end(self, outputs):
        val_loss_mean = torch.stack([x['val_loss'] for x in outputs]).mean()
        self.log('val_loss', val_loss_mean.item(), prog_bar=False)


lightning_trainer_config = {
    "max_epochs": 100, 
    "accelerator": "gpu",
    "strategy": "ddp"
}

model_checkpoint_config = {
    "monitor": "loss",
    "save_top_k": 3,
    "mode": "min"
}

scaling_config = ScalingConfig(num_workers=8, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1})

run_config = RunConfig(
    name="ptl-e2e-test",
    local_dir="/mnt/cluster_storage/ray_lightning_results",
    sync_config=SyncConfig(syncer=None),
)

trainer = LightningTrainer(
    lightning_module=LitAutoEncoder,
    lightning_module_config={},
    lightning_trainer_config=lightning_trainer_config,
    ddp_strategy_config={},
    model_checkpoint_config=model_checkpoint_config,
    scaling_config=scaling_config,
    run_config=run_config,
    datamodule=MNISTDataModule()
)

trainer.fit()