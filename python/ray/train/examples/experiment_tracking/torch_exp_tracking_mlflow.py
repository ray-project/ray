# flake8: noqa
# isort: skip_file
from filelock import FileLock
import os
import tempfile

tempdir = tempfile.TemporaryDirectory()
os.environ["SHARED_STORAGE_PATH"] = tempdir.name

# __start__
# Run the following script with the SHARED_STORAGE_PATH env var set.
# The MLflow offline logs are saved to SHARED_STORAGE_PATH/mlruns.

import mlflow
import os
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
import torch
from torchvision import datasets, transforms
from torchvision.models import resnet18
from torch.utils.data import DataLoader

assert os.environ.get(
    "SHARED_STORAGE_PATH", None
), "Please set SHARED_STORAGE_PATH env var."


# Assumes you are passing a `save_dir` in `config`
def train_func(config):
    save_dir = config["save_dir"]
    if ray.train.get_context().get_world_rank() == 0:
        mlflow.set_tracking_uri(f"file:{save_dir}")
        mlflow.set_experiment("my_experiment")
        mlflow.start_run()

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
                mlflow.log_metrics({"loss": loss.item(), "epoch": epoch})

    if ray.train.get_context().get_world_rank() == 0:
        mlflow.end_run()


trainer = TorchTrainer(
    train_func,
    train_loop_config={
        "save_dir": os.path.join(os.environ["SHARED_STORAGE_PATH"], "mlruns")
    },
    scaling_config=ScalingConfig(num_workers=2),
)
trainer.fit()
# __end__

tempdir.cleanup()
