# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
import os
import argparse
from filelock import FileLock
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

import ray
from ray import air, tune
from ray.air import session
from ray.train.torch import TorchCheckpoint
from ray.tune.schedulers import AsyncHyperBandScheduler

# Change these values if you want the training to run quicker or slower.
EPOCH_SIZE = 512
TEST_SIZE = 256


class ConvNet(nn.Module):
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)


def train(model, optimizer, train_loader, device=None):
    device = device or torch.device("cpu")
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if batch_idx * len(data) > EPOCH_SIZE:
            return
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, data_loader, device=None):
    device = device or torch.device("cpu")
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(data_loader):
            if batch_idx * len(data) > TEST_SIZE:
                break
            data, target = data.to(device), target.to(device)
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct / total


def get_data_loaders():
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock(os.path.expanduser("~/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data", train=True, download=True, transform=mnist_transforms
            ),
            batch_size=64,
            shuffle=True,
        )
        test_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data", train=False, download=True, transform=mnist_transforms
            ),
            batch_size=64,
            shuffle=True,
        )
    return train_loader, test_loader


def train_mnist(config):
    should_checkpoint = config.get("should_checkpoint", False)
    use_cuda = torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    train_loader, test_loader = get_data_loaders()
    model = ConvNet().to(device)

    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"]
    )

    while True:
        train(model, optimizer, train_loader, device)
        acc = test(model, test_loader, device)
        checkpoint = None
        if should_checkpoint:
            checkpoint = TorchCheckpoint.from_state_dict(model.state_dict())
        # Report metrics (and possibly a checkpoint) to Tune
        session.report({"mean_accuracy": acc}, checkpoint=checkpoint)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyTorch MNIST Example")
    parser.add_argument(
        "--cuda", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--ray-address",
        help="Address of Ray cluster for seamless distributed execution.",
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.server_address:
        ray.init(f"ray://{args.server_address}")
    elif args.ray_address:
        ray.init(address=args.ray_address)
    else:
        ray.init(num_cpus=2 if args.smoke_test else None)

    # for early stopping
    sched = AsyncHyperBandScheduler()

    resources_per_trial = {"cpu": 2, "gpu": int(args.cuda)}  # set this for GPUs
    tuner = tune.Tuner(
        tune.with_resources(train_mnist, resources=resources_per_trial),
        tune_config=tune.TuneConfig(
            metric="mean_accuracy",
            mode="max",
            scheduler=sched,
            num_samples=1 if args.smoke_test else 50,
        ),
        run_config=air.RunConfig(
            name="exp",
            stop={
                "mean_accuracy": 0.98,
                "training_iteration": 5 if args.smoke_test else 100,
            },
        ),
        param_space={
            "lr": tune.loguniform(1e-4, 1e-2),
            "momentum": tune.uniform(0.1, 0.9),
        },
    )
    results = tuner.fit()

    print("Best config is:", results.get_best_result().config)
