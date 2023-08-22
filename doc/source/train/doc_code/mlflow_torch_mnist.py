# flake8: noqa
# isort: skip_file

# __start__
# Run the following script with SAVE_DIR env var set, where you
# want to mlflow offline logs saved.

import mlflow
import os
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
import torch
from torchvision import datasets, transforms
from torchvision.models import resnet18
from torch.utils.data import DataLoader

assert os.environ.get("SAVE_DIR", None), "Please set SAVE_DIR env var."

# This function is assuming `save_dir` is set in `config`
def train_func(config):
    save_dir = config.get["save_dir"]
    mlflow.start_run(tracking_uri=f"file:{save_dir}")

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
        [transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))]
    )
    train_data = datasets.FashionMNIST(
        root="./data", train=True, download=True, transform=transform
    )
    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(10):
        for images, labels in train_loader:
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            mlflow.log_metrics({"loss": loss.item(), "epoch": epoch})


trainer = TorchTrainer(
    train_func,
    train_loop_config={"save_dir": os.environ["SAVE_DIR"]},
    scaling_config=ScalingConfig(num_workers=4),
)
trainer.fit()
