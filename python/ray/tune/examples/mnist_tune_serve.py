import os
from functools import partial
from math import ceil

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from ray import tune
from ray.tune import CLIReporter
from ray.tune.schedulers import ASHAScheduler

from torch.utils.data import random_split, Subset
from torchvision.datasets import MNIST
from torchvision.transforms import transforms


class MNISTDataInterface(object):
    """Data interface. Simulates that new data arrives every day."""
    def __init__(self, data_dir, max_days=10):
        self.data_dir = data_dir
        self.max_days = max_days

        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307, ), (0.3081, ))
        ])
        self.dataset = MNIST(self.data_dir, train=True, download=True, transform=transform)

    def _get_day_slice(self, day=0):
        if day < 0:
            return 0
        n = len(self.dataset)
        # Start with 30% of the data, get more data each day
        return ceil(n * (0.3 + 0.7 * day / self.max_days))

    def get_data(self, day=0):
        """Get complete normalized train and validation data to date."""
        end = self._get_day_slice(day)

        available_data = Subset(self.dataset, list(range(end)))
        train_n = int(0.8 * end)  # 80% train data, 20% validation data

        return random_split(available_data, [train_n, end-train_n])

    def get_incremental_data(self, day=0):
        """Get next normalized train and validation data day slice."""
        start = self._get_day_slice(day-1)
        end = self._get_day_slice(day)

        available_data = Subset(self.dataset, list(range(start, end)))
        train_n = int(0.8 * (end-start))  # 80% train data, 20% validation data

        return random_split(available_data, [train_n, end-start-train_n])



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
            data, target = data.to(device), target.to(device)
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct / total


def train_from_scratch(config, checkpoint=None, num_epochs=10, data_interface=None, day=0):
    # Create model
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    model = ConvNet().to(device)

    # Create optimizer
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])

    if checkpoint:
        model_state, optimizer_state = torch.load(os.path.join(checkpoint, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    # Get full training datasets
    train_dataset, validation_dataset = data_interface.get_data(day=day)

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=config["batch_size"],
        shuffle=True)

    validation_loader = torch.utils.data.DataLoader(
        validation_dataset,
        batch_size=config["batch_size"],
        shuffle=True)

    for i in range(num_epochs):
        train(model, optimizer, train_loader, device)
        acc = test(model, validation_loader, device)
        tune.report(mean_accuracy=acc)


def train_from_existing(config, checkpoint=None, start_model=None, num_epochs=10, data_interface=None, day=0):
    # Create model
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    model = ConvNet().to(device)

    # Create optimizer
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])

    if checkpoint:
        model_state, optimizer_state = torch.load(os.path.join(checkpoint, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)
    elif start_model:
        model_state, optimizer_state = torch.load(os.path.join(start_model, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    # Get full training datasets
    train_dataset, validation_dataset = data_interface.get_data(day=day)

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=config["batch_size"],
        shuffle=True)

    validation_loader = torch.utils.data.DataLoader(
        validation_dataset,
        batch_size=config["batch_size"],
        shuffle=True)

    for i in range(num_epochs):
        train(model, optimizer, train_loader, device)
        acc = test(model, validation_loader, device)
        tune.report(mean_accuracy=acc)


def tune_from_scratch(num_samples=10, num_epochs=10, gpus_per_trial=0, day=0):
    data_interface = MNISTDataInterface("/tmp/mnist_data", max_days=10)

    config = {
        "batch_size": tune.choice([16, 32, 64]),
        "layer_1_size": tune.choice([32, 64, 128]),
        "layer_2_size": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "momentum": tune.uniform(0.1, 0.9),
    }

    scheduler = ASHAScheduler(
        metric="mean_accuracy",
        mode="max",
        max_t=num_epochs,
        grace_period=1,
        reduction_factor=2)

    reporter = CLIReporter(
        parameter_columns=["layer_1_size", "layer_2_size", "lr", "momentum", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"])

    analysis = tune.run(
        partial(
            train_from_scratch,
            data_interface=data_interface,
            num_epochs=num_epochs,
            day=day),
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        checkpoint_at_end=True,
        name="tune_serve_mnist_fromscratch")

    best_trial = analysis.get_best_trial("mean_accuract", "max", "last")
    best_accuracy = best_trial.metric_analysis["mean_accuracy"]["last"]
    best_trial_config = best_trial.config
    best_checkpoint = best_trial.checkpoint.value

    return best_accuracy, best_trial_config, best_checkpoint


def tune_from_existing(start_model, num_samples=10, num_epochs=10, gpus_per_trial=0, day=0):
    data_interface = MNISTDataInterface("/tmp/mnist_data", max_days=10)

    config = {
        "batch_size": tune.choice([16, 32, 64]),
        "layer_1_size": tune.choice([32, 64, 128]),
        "layer_2_size": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "momentum": tune.uniform(0.1, 0.9),
    }

    scheduler = ASHAScheduler(
        metric="mean_accuracy",
        mode="max",
        max_t=num_epochs,
        grace_period=1,
        reduction_factor=2)

    reporter = CLIReporter(
        parameter_columns=["layer_1_size", "layer_2_size", "lr", "momentum", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"])

    analysis = tune.run(
        partial(
            train_from_scratch,
            data_interface=data_interface,
            num_epochs=num_epochs,
            day=day),
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        checkpoint_at_end=True,
        name="tune_serve_mnist_fromscratch")

    best_trial = analysis.get_best_trial("mean_accuracy", "max", "last")
    best_accuracy = best_trial.metric_analysis["mean_accuracy"]["last"]
    best_trial_config = best_trial.config
    best_checkpoint = best_trial.checkpoint.value

    return best_accuracy, best_trial_config, best_checkpoint


if __name__ == "__main__":
    if True:  # train everyday from scratch
        for day in range(10):
            best_accuracy, best_trial_config, best_checkpoint = tune_from_scratch(10, 10, 0, day=day)
            print("Trained day {}. Best accuracy: {:.4f}. Best config: {}".format(day, best_accuracy, best_checkpoint))
    else:
        best_checkpoint = None

        for day in range(10):
            best_accuracy, best_trial_config, best_checkpoint = tune_from_existing(best_checkpoint, 10, 10, 0, day=day)
            print("Trained day {}. Best accuracy: {:.4f}. Best config: {}".format(day, best_accuracy, best_checkpoint))
