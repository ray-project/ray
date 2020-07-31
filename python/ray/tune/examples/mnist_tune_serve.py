import argparse
import os
import sys
from collections import namedtuple
from functools import partial
from math import ceil

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from ray import tune, serve
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
        self.dataset = MNIST(
            self.data_dir, train=True, download=True, transform=transform)

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

        return random_split(available_data, [train_n, end - train_n])

    def get_incremental_data(self, day=0):
        """Get next normalized train and validation data day slice."""
        start = self._get_day_slice(day - 1)
        end = self._get_day_slice(day)

        available_data = Subset(self.dataset, list(range(start, end)))
        train_n = int(
            0.8 * (end - start))  # 80% train data, 20% validation data

        return random_split(available_data, [train_n, end - start - train_n])


class ConvNet(nn.Module):
    def __init__(self, layer_size=192):
        super(ConvNet, self).__init__()
        self.layer_size = layer_size
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, self.layer_size)
        self.out = nn.Linear(self.layer_size, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        x = self.out(x)
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


# Define Ray Serve model,
class MNISTBackend:
    def __init__(self, checkpoint_dir, config, metrics):
        self.checkpoint_dir = checkpoint_dir
        self.config = config
        self.metrics = metrics

        # use_cuda = config.get("use_gpu") and torch.cuda.is_available()
        # self.device = torch.device("cuda" if use_cuda else "cpu")
        self.device = "cpu"
        model = ConvNet(layer_size=self.config["layer_size"]).to(self.device)

        model_state, optimizer_state = torch.load(
            os.path.join(self.checkpoint_dir, "checkpoint"))
        model.load_state_dict(model_state)

        self.model = model

    def __call__(self, flask_request):
        images = torch.tensor(flask_request.json["images"])
        images = images.to(self.device)
        outputs = self.model(images)
        predicted = torch.max(outputs.data, 1)[1]
        return {"result": predicted.numpy().tolist()}


def train_from_scratch(config,
                       checkpoint_dir=None,
                       num_epochs=10,
                       data_interface=None,
                       day=0):
    # Create model
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    model = ConvNet(layer_size=config["layer_size"]).to(device)

    # Create optimizer
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])

    if checkpoint_dir:
        model_state, optimizer_state = torch.load(
            os.path.join(checkpoint_dir, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    # Get full training datasets
    train_dataset, validation_dataset = data_interface.get_data(day=day)

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=config["batch_size"], shuffle=True)

    validation_loader = torch.utils.data.DataLoader(
        validation_dataset, batch_size=config["batch_size"], shuffle=True)

    for i in range(num_epochs):
        train(model, optimizer, train_loader, device)
        acc = test(model, validation_loader, device)
        if i == num_epochs - 1:
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                torch.save((model.state_dict(), optimizer.state_dict()),
                           os.path.join(checkpoint_dir, "checkpoint"))
            tune.report(mean_accuracy=acc, done=True)
        else:
            tune.report(mean_accuracy=acc)


def train_from_existing(config,
                        checkpoint_dir=None,
                        start_model=None,
                        num_epochs=10,
                        data_interface=None,
                        day=0):
    # Create model
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    model = ConvNet(layer_size=config["layer_size"]).to(device)

    # Create optimizer
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])

    if checkpoint_dir:
        model_state, optimizer_state = torch.load(
            os.path.join(checkpoint_dir, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)
    elif start_model:
        model_state, optimizer_state = torch.load(
            os.path.join(start_model, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    # Get full training datasets
    train_dataset, validation_dataset = data_interface.get_data(day=day)

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=config["batch_size"], shuffle=True)

    validation_loader = torch.utils.data.DataLoader(
        validation_dataset, batch_size=config["batch_size"], shuffle=True)

    for i in range(num_epochs):
        train(model, optimizer, train_loader, device)
        acc = test(model, validation_loader, device)
        if i == num_epochs - 1:
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                torch.save((model.state_dict(), optimizer.state_dict()),
                           os.path.join(checkpoint_dir, "checkpoint"))
            tune.report(mean_accuracy=acc, done=True)
        else:
            tune.report(mean_accuracy=acc)


def tune_from_scratch(num_samples=10, num_epochs=10, gpus_per_trial=0., day=0):
    data_interface = MNISTDataInterface("/tmp/mnist_data", max_days=10)
    N = data_interface._get_day_slice(day)

    config = {
        "batch_size": tune.choice([16, 32, 64]),
        "layer_size": tune.choice([32, 64, 128, 192]),
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
        parameter_columns=["layer_size", "lr", "momentum", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"])

    analysis = tune.run(
        partial(
            train_from_scratch,
            data_interface=data_interface,
            num_epochs=num_epochs,
            day=day),
        resources_per_trial={
            "cpu": 1,
            "gpu": gpus_per_trial
        },
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        checkpoint_at_end=True,
        verbose=0,
        name="tune_serve_mnist_fromscratch")

    best_trial = analysis.get_best_trial("mean_accuracy", "max", "last")
    best_accuracy = best_trial.metric_analysis["mean_accuracy"]["last"]
    best_trial_config = best_trial.config
    best_checkpoint = best_trial.checkpoint.value

    return best_accuracy, best_trial_config, best_checkpoint, N


def tune_from_existing(start_model,
                       num_samples=10,
                       num_epochs=10,
                       gpus_per_trial=0.,
                       day=0):
    data_interface = MNISTDataInterface("/tmp/mnist_data", max_days=10)
    N = data_interface._get_day_slice(day) - data_interface._get_day_slice(
        day - 1)

    config = {
        "batch_size": tune.choice([16, 32, 64]),
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
        parameter_columns=["lr", "momentum", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"])

    analysis = tune.run(
        partial(
            train_from_scratch,
            data_interface=data_interface,
            num_epochs=num_epochs,
            day=day),
        resources_per_trial={
            "cpu": 1,
            "gpu": gpus_per_trial
        },
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        checkpoint_at_end=True,
        verbose=0,
        name="tune_serve_mnist_fromscratch")

    best_trial = analysis.get_best_trial("mean_accuracy", "max", "last")
    best_accuracy = best_trial.metric_analysis["mean_accuracy"]["last"]
    best_trial_config = best_trial.config
    best_checkpoint = best_trial.checkpoint.value

    return best_accuracy, best_trial_config, best_checkpoint, N


if __name__ == "__main__":
    """
    WIP usage:

    Train with data available at day 0. Automatically serves best model:

    ``python mnist_tune_serve.py --from_scratch --day 0``

    Query with test data (example #5 from dataset):

    ``python mnist_tune_serve.py --query 5``

    Sample output:

    ```
    python mnist_tune_serve.py --query 28
    Querying our model with example #28. Label = 2, Response = 7, Correct = F
    python mnist_tune_serve.py --query 27
    Querying our model with example #27. Label = 3, Response = 3, Correct = T
    ```

    Load model from checkpoint Y and serve as day X backend
    (for debugging only!)

    ``python mnist_tune_serve.py --day X --checkpoint Y``

    Load model from checkpoint Y and query with test data without using Serve
    (for debugging only!)

    ``python mnist_tune_serve.py --day X --checkpoint_debug Y``
    """
    parser = argparse.ArgumentParser(description="MNIST Tune/Serve example")
    parser.add_argument("--checkpoint", type=str, default="")
    parser.add_argument("--checkpoint_debug", type=str, default="")

    parser.add_argument(
        "--from_scratch",
        action="store_true",
        help="Train and select best model from scratch",
        default=False)
    parser.add_argument("--day", help="Indicate the day", type=int, default=0)
    parser.add_argument(
        "--query", help="Query endpoint with example", type=int, default=-1)

    args = parser.parse_args()

    if args.checkpoint:
        serve.init()
        backend_name = "mnist:day_{}".format(args.day)
        serve.create_backend(backend_name, MNISTBackend, args.checkpoint,
                             {"layer_size": 192}, 0.8)
        serve.create_endpoint(
            "mnist", backend=backend_name, route="/mnist", methods=["POST"])
        sys.exit(0)

    if args.checkpoint_debug:
        backend_name = "mnist:day_{}".format(args.day)
        backend = MNISTBackend(args.checkpoint_debug, {"layer_size": 192}, 0.8)

        dataset = MNISTDataInterface("/tmp/mnist_data", max_days=0).dataset
        data = dataset[0]
        label = data[1]
        req = namedtuple("flask_request", ["json"])
        preds = backend(req({"images": [data[0].numpy().tolist()]}))
        print(preds)
        sys.exit(0)

    if args.query >= 0:
        import requests

        dataset = MNISTDataInterface("/tmp/mnist_data", max_days=0).dataset
        data = dataset[args.query]
        label = data[1]

        # Query our model
        response = requests.post(
            "http://localhost:8000/mnist",
            json={"images": [data[0].numpy().tolist()]})

        try:
            pred = response.json()["result"][0]
        except:  # noqa: E722
            pred = -1

        print("Querying our model with example #{}. "
              "Label = {}, Response = {}, Correct = {}".format(
                  args.query, label, pred, label == pred))
        sys.exit(0)

    if args.from_scratch:  # train everyday from scratch
        print("Start training job from scratch on day {}.".format(args.day))
        acc, config, best_checkpoint, N = tune_from_scratch(
            2, 4, 0.5, day=args.day)
        print("Trained day {} from scratch on {} samples. "
              "Best accuracy: {:.4f}. Best config: {}".format(
                  args.day, N, acc, config))
        print("Serving checkpoint: {}".format(best_checkpoint))
        serve.init()
        backend_name = "mnist:day_{}".format(args.day)
        serve.create_backend(backend_name, MNISTBackend, best_checkpoint,
                             config, acc)
        serve.create_endpoint(
            "mnist", backend=backend_name, route="/mnist", methods=["POST"])

    if args.incremental:
        pass  # load latest model here
