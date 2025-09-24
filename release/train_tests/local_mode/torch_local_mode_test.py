"""Ray train release test: local mode launched by torchrun
"""

import os
import tempfile

import logging

import torch
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torch.utils.data import DataLoader
from torchvision.models import resnet18
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose
from filelock import FileLock
import torch.distributed as dist

import ray
from ray.train import (
    Checkpoint,
    CheckpointConfig,
    RunConfig,
    ScalingConfig,
    get_context,
)
from ray.train.torch import TorchTrainer

logger = logging.getLogger(__name__)
DATA_ROOT = "/tmp/test_data"


def train_func():
    # Model, Loss, Optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    lock = FileLock(os.path.join(DATA_ROOT, "fashionmnist.lock"))
    # [1] Prepare model.
    model = ray.train.torch.prepare_model(model)

    # model.to("cuda")  # This is done by `prepare_model`
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Data
    transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
    local_rank = get_context().get_local_rank()
    if local_rank == 0:
        logger.info(f"Downloading FashionMNIST data to {DATA_ROOT}")
        with lock:
            _ = FashionMNIST(
                root=DATA_ROOT, train=True, download=True, transform=transform
            )
    dist.barrier()
    logger.info(f"Loading FashionMNIST data from {DATA_ROOT}")
    train_data = FashionMNIST(
        root=DATA_ROOT, train=True, download=False, transform=transform
    )

    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    # [2] Prepare dataloader.
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(10):
        if ray.train.get_context().get_world_size() > 1:
            train_loader.sampler.set_epoch(epoch)

        for images, labels in train_loader:
            # This is done by `prepare_data_loader`!
            # images, labels = images.to("cuda"), labels.to("cuda")
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # [3] Report metrics and checkpoint.
        metrics = {"loss": loss.item(), "epoch": epoch}
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            torch.save(
                model.state_dict(), os.path.join(temp_checkpoint_dir, "model.pt")
            )
            ray.train.report(
                metrics,
                checkpoint=Checkpoint.from_directory(temp_checkpoint_dir),
            )
        if ray.train.get_context().get_world_rank() == 0:
            logger.info(f"metrics: {metrics}")


def fit_func():
    # Define configurations.
    train_loop_config = {"num_epochs": 20, "lr": 0.01, "batch_size": 32}
    scaling_config = ScalingConfig(num_workers=0, use_gpu=True)
    run_config = RunConfig(checkpoint_config=CheckpointConfig(num_to_keep=1))

    # Initialize the Trainer.
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        run_config=run_config,
    )

    # Train the model.
    result = trainer.fit()

    # Inspect the results.
    final_loss = result.metrics["loss"]
    logger.info(f"final_loss: {final_loss}")


if __name__ == "__main__":
    fit_func()
