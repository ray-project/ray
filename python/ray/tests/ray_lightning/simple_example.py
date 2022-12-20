# This file is duplicated in release/ml_user_tests/ray-lightning
# flake8: noqa

from importlib_metadata import version
from packaging.version import parse as v_parse

if v_parse(version("ray_lightning")) < v_parse("0.3.0"):  # Older Ray Lightning Version.
    import ray_lightning

    ray_lightning.RayStrategy = ray_lightning.RayPlugin

# __pl_module_init__
import torch
from torch import nn
import torch.nn.functional as F

import pytorch_lightning as pl


class LitAutoEncoder(pl.LightningModule):
    def __init__(self, lr=1e-1):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(28 * 28, 128), nn.ReLU(), nn.Linear(128, 3)
        )
        self.decoder = nn.Sequential(
            nn.Linear(3, 128), nn.ReLU(), nn.Linear(128, 28 * 28)
        )
        self.lr = lr

    def forward(self, x):
        # in lightning, forward defines the prediction/inference actions
        embedding = self.encoder(x)
        return embedding

    def training_step(self, batch, batch_idx):
        # training_step defines the train loop. It is independent of forward
        x, y = batch
        x = x.view(x.size(0), -1)
        z = self.encoder(x)
        x_hat = self.decoder(z)
        loss = F.mse_loss(x_hat, x)
        self.log("train_loss", loss)
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
        return optimizer


# __pl_module_end__


def main():
    # __train_begin__
    import os
    from torch.utils.data import DataLoader, random_split
    from torchvision.datasets import MNIST
    from torchvision import transforms

    from ray_lightning import RayStrategy

    num_workers = 2
    use_gpu = False
    max_steps = 10

    dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
    train, val = random_split(dataset, [55000, 5000])

    autoencoder = LitAutoEncoder()
    trainer = pl.Trainer(
        strategy=RayStrategy(num_workers=num_workers, use_gpu=use_gpu),
        max_steps=max_steps,
    )
    trainer.fit(autoencoder, DataLoader(train), DataLoader(val))
    # __train_end__


if __name__ == "__main__":
    main()
