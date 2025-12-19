# ruff: noqa
# fmt: off
# isort: off

# __start__
from filelock import FileLock
import os

import torch
import wandb

from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from torchvision.models import resnet18

import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

# Run the following script with the WANDB_API_KEY env var set.
assert os.environ.get("WANDB_API_KEY", None), "Please set WANDB_API_KEY env var."

# This makes sure that all workers have this env var set.
ray.init(
    runtime_env={"env_vars": {"WANDB_API_KEY": os.environ["WANDB_API_KEY"]}}
)


def train_func(config):
    if ray.train.get_context().get_world_rank() == 0:
        wandb.init()

    # Model, Loss, Optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model = ray.train.torch.prepare_model(model)
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.module.parameters(), lr=0.001)

    # Data
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.28604,), (0.32025,))]
    )
    with FileLock("./data.lock"):
        train_data = datasets.FashionMNIST(
            root="./data", train=True, download=True, transform=transform
        )
    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(1):
        if ray.train.get_context().get_world_size() > 1:
            train_loader.sampler.set_epoch(epoch)

        for images, labels in train_loader:
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if ray.train.get_context().get_world_rank() == 0:
                wandb.log({"loss": loss, "epoch": epoch})

    if ray.train.get_context().get_world_rank() == 0:
        wandb.finish()


trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=2),
)
trainer.fit()
