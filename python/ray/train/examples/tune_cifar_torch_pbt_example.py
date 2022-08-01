import argparse

import numpy as np
from ray.air import session
import torch
import torch.nn as nn
import torchvision.transforms as transforms
from torchvision.models import resnet18
from filelock import FileLock
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import CIFAR10

import ray
import ray.train as train
from ray import tune
from ray.air.config import FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


def train_epoch(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset) // session.get_world_size()
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def validate_epoch(dataloader, model, loss_fn):
    size = len(dataloader.dataset) // session.get_world_size()
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    print(
        f"Test Error: \n "
        f"Accuracy: {(100 * correct):>0.1f}%, "
        f"Avg loss: {test_loss:>8f} \n"
    )
    return {"loss": test_loss}


def train_func(config):
    epochs = config.pop("epochs", 3)
    model = resnet18()
    model = train.torch.prepare_model(model)

    # Create optimizer.
    optimizer = torch.optim.SGD(
        model.parameters(),
        lr=config.get("lr", 0.1),
        momentum=config.get("momentum", 0.9),
    )

    # Load in training and validation data.
    transform_train = transforms.Compose(
        [
            transforms.RandomCrop(32, padding=4),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
        ]
    )  # meanstd transformation

    transform_test = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
        ]
    )

    with FileLock(".ray.lock"):
        train_dataset = CIFAR10(
            root="~/data", train=True, download=True, transform=transform_train
        )
        validation_dataset = CIFAR10(
            root="~/data", train=False, download=False, transform=transform_test
        )

    if config.get("test_mode"):
        train_dataset = Subset(train_dataset, list(range(64)))
        validation_dataset = Subset(validation_dataset, list(range(64)))

    worker_batch_size = config["batch_size"] // session.get_world_size()

    train_loader = DataLoader(train_dataset, batch_size=worker_batch_size)
    validation_loader = DataLoader(validation_dataset, batch_size=worker_batch_size)

    train_loader = train.torch.prepare_data_loader(train_loader)
    validation_loader = train.torch.prepare_data_loader(validation_loader)

    # Create loss.
    criterion = nn.CrossEntropyLoss()

    results = []
    for _ in range(epochs):
        train_epoch(train_loader, model, criterion, optimizer)
        result = validate_epoch(validation_loader, model, criterion)
        session.report(result)
        results.append(result)

    # return required for backwards compatibility with the old API
    # TODO(team-ml) clean up and remove return
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Redis"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--num-epochs", type=int, default=5, help="Number of epochs to train."
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )

    args, _ = parser.parse_known_args()
    if args.smoke_test:
        ray.init(num_cpus=4)
    else:
        ray.init(address=args.address)

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(
            num_workers=args.num_workers, use_gpu=args.use_gpu
        ),
    )
    pbt_scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=1,
        hyperparam_mutations={
            "train_loop_config": {
                # distribution for resampling
                "lr": lambda: np.random.uniform(0.001, 1),
                # allow perturbations within this set of categorical values
                "momentum": [0.8, 0.9, 0.99],
            }
        },
    )

    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.choice([0.001, 0.01, 0.1]),
                "momentum": 0.8,
                "batch_size": 128 * args.num_workers,
                "epochs": args.num_epochs,
                "test_mode": args.smoke_test,  # whether to to subset the data
            }
        },
        tune_config=TuneConfig(
            num_samples=4, metric="loss", mode="min", scheduler=pbt_scheduler
        ),
        run_config=RunConfig(
            stop={"training_iteration": 2 if args.smoke_test else 100},
            failure_config=FailureConfig(max_failures=3),  # used for fault tolerance
        ),
    )

    results = tuner.fit()

    print(results.get_best_result(metric="loss", mode="min"))
