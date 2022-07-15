import os
import torch
from torch import nn
import torch.nn.functional as F
from torchvision.datasets import MNIST
from torch.utils.data import DataLoader, random_split
from torchvision import transforms
import pytorch_lightning as pl

from ray.util.ray_lightning import RayPlugin
from ray.util.ray_lightning.tune import TuneReportCallback, get_tune_resources

num_cpus_per_actor = 1
num_workers = 1


class LitAutoEncoder(pl.LightningModule):
    def __init__(self, lr):
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


def train(config):
    dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
    train, val = random_split(dataset, [55000, 5000])

    metrics = {"loss": "train_loss"}
    autoencoder = LitAutoEncoder(lr=config["lr"])
    trainer = pl.Trainer(
        callbacks=[TuneReportCallback(metrics, on="batch_end")],
        plugins=[RayPlugin(num_workers=num_workers)],
        max_steps=10,
    )
    trainer.fit(autoencoder, DataLoader(train), DataLoader(val))


def main():
    from ray import tune

    config = {"lr": tune.loguniform(1e-4, 1e-1)}

    analysis = tune.run(
        train,
        config=config,
        num_samples=1,
        metric="loss",
        mode="min",
        resources_per_trial=get_tune_resources(
            num_workers=num_workers, cpus_per_worker=num_cpus_per_actor
        ),
    )

    print("Best hyperparameters: ", analysis.best_config)


if __name__ == "__main__":
    main()
