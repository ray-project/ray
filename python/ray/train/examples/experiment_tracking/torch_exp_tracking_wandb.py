# flake8: noqa
# isort: skip_file

# __start__
# Run the following script with WANDB_API_KEY env var set.
import os
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
import torch
from torchvision import datasets, transforms
from torchvision.models import resnet18
from torch.utils.data import DataLoader
import wandb

assert os.environ.get("WANDB_API_KEY", None), "Please set WANDB_API_KEY env var."


# This function is assuming `wandb_api_key` is set in `config`
def train_func(config):
    if ray.train.get_context().get_world_rank() == 0:
        wandb.login(key=config.get("wandb_api_key", None))
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
        [transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))]
    )
    train_data = datasets.FashionMNIST(
        root="./data", train=True, download=True, transform=transform
    )
    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(2):
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
    train_loop_config={"wandb_api_key": os.environ["WANDB_API_KEY"]},
    scaling_config=ScalingConfig(num_workers=4),
)
trainer.fit()
