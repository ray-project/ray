from ray import train
from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer, TorchCheckpoint
import ray
import socket
import pytorch_lightning as pl
from torch.utils.data import DataLoader
from torchvision.datasets import MNIST
from torchvision import transforms
import torch.nn.functional as F
from torch import nn
import torch
import os


from pytorch_lightning import strategies
os.environ['RAY_ML_DEV'] = "1"


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
    def __init__(self, encoder, decoder):
        super().__init__()
        self.encoder = encoder
        self.decoder = decoder
        self.global_steps = 0

    def training_step(self, batch, batch_idx):
        # training_step defines the train loop.
        x, y = batch
        x = x.view(x.size(0), -1)
        z = self.encoder(x)
        x_hat = self.decoder(z)
        loss = F.mse_loss(x_hat, x)

        self.global_steps += 1
        if self.global_steps % 100 == 0:
            loss_value = loss.item()
            self.log_dict({"loss": loss_value, "steps": self.global_steps})
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
        return optimizer

    def training_epoch_end(self, outputs) -> None:
        loss = sum(output["loss"] for output in outputs) / len(outputs)
        print("Epoch loss = ", loss)
        # print("weight = ", self.encoder.l1[0].weight)

# model
encoder = Encoder()
decoder = Decoder()
autoencoder = LitAutoEncoder(encoder, decoder)

dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
train_loader = DataLoader(dataset, batch_size=100)


# train model
# trainer = pl.Trainer(max_epochs=100, accelerator="gpu", strategy="ddp", enable_checkpointing=False, devices=[device.index])
trainer = pl.Trainer(max_epochs=10, accelerator="gpu", strategy="ddp", enable_checkpointing=True)

trainer.fit(model=autoencoder, train_dataloaders=train_loader)

