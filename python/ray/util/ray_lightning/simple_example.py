import argparse
import os
import torch
from torch import nn
import torch.nn.functional as F
from torchvision.datasets import MNIST
from torch.utils.data import DataLoader, random_split
from torchvision import transforms
import pytorch_lightning as pl

from ray.util.ray_lightning import RayPlugin


class LitAutoEncoder(pl.LightningModule):
    def __init__(self):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(28 * 28, 128), nn.ReLU(), nn.Linear(128, 3)
        )
        self.decoder = nn.Sequential(
            nn.Linear(3, 128), nn.ReLU(), nn.Linear(128, 28 * 28)
        )

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
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
        return optimizer


def main(num_workers: int = 2, use_gpu: bool = False, max_steps: int = 10):
    dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
    train, val = random_split(dataset, [55000, 5000])

    autoencoder = LitAutoEncoder()
    trainer = pl.Trainer(
        plugins=[RayPlugin(num_workers=num_workers, use_gpu=use_gpu)],
        max_steps=max_steps,
    )
    trainer.fit(autoencoder, DataLoader(train), DataLoader(val))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ray Lightning Example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=2,
        help="Number of workers to use for training.",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=10,
        help="Maximum number of steps to run for training.",
    )
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Whether to enable GPU training.",
    )

    args = parser.parse_args()

    main(num_workers=args.num_workers, max_steps=args.max_steps, use_gpu=args.use_gpu)
