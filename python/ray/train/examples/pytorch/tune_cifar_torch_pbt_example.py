import argparse
import os
import tempfile

import torch
import torch.nn as nn
import torchvision.transforms as transforms
from filelock import FileLock
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import CIFAR10
from torchvision.models import resnet18

import ray
import ray.cloudpickle as cpickle
from ray import train, tune
from ray.train import Checkpoint, FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


def train_epoch(epoch, dataloader, model, loss_fn, optimizer):
    if ray.train.get_context().get_world_size() > 1:
        dataloader.sampler.set_epoch(epoch)

    size = len(dataloader.dataset) // train.get_context().get_world_size()
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
    size = len(dataloader.dataset) // train.get_context().get_world_size()
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


def update_optimizer_config(optimizer, config):
    for param_group in optimizer.param_groups:
        for param, val in config.items():
            param_group[param] = val


def train_func(config):
    epochs = config.get("epochs", 3)

    model = resnet18()

    # Note that `prepare_model` needs to be called before setting optimizer.
    if not train.get_checkpoint():  # fresh start
        model = train.torch.prepare_model(model)

    # Create optimizer.
    optimizer_config = {
        "lr": config.get("lr"),
        "momentum": config.get("momentum"),
    }
    optimizer = torch.optim.SGD(model.parameters(), **optimizer_config)

    starting_epoch = 0
    if train.get_checkpoint():
        with train.get_checkpoint().as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "data.ckpt"), "rb") as fp:
                checkpoint_dict = cpickle.load(fp)

        # Load in model
        model_state = checkpoint_dict["model"]
        model.load_state_dict(model_state)
        model = train.torch.prepare_model(model)

        # Load in optimizer
        optimizer_state = checkpoint_dict["optimizer_state_dict"]
        optimizer.load_state_dict(optimizer_state)

        # Optimizer configs (`lr`, `momentum`) are being mutated by PBT and passed in
        # through config, so we need to update the optimizer loaded from the checkpoint
        update_optimizer_config(optimizer, optimizer_config)

        # The current epoch increments the loaded epoch by 1
        checkpoint_epoch = checkpoint_dict["epoch"]
        starting_epoch = checkpoint_epoch + 1

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

    data_dir = config.get("data_dir", os.path.expanduser("~/data"))
    os.makedirs(data_dir, exist_ok=True)
    with FileLock(os.path.join(data_dir, ".ray.lock")):
        train_dataset = CIFAR10(
            root=data_dir, train=True, download=True, transform=transform_train
        )
        validation_dataset = CIFAR10(
            root=data_dir, train=False, download=False, transform=transform_test
        )

    if config.get("test_mode"):
        train_dataset = Subset(train_dataset, list(range(64)))
        validation_dataset = Subset(validation_dataset, list(range(64)))

    worker_batch_size = config["batch_size"] // train.get_context().get_world_size()

    train_loader = DataLoader(train_dataset, batch_size=worker_batch_size, shuffle=True)
    validation_loader = DataLoader(validation_dataset, batch_size=worker_batch_size)

    train_loader = train.torch.prepare_data_loader(train_loader)
    validation_loader = train.torch.prepare_data_loader(validation_loader)

    # Create loss.
    criterion = nn.CrossEntropyLoss()

    for epoch in range(starting_epoch, epochs):
        train_epoch(epoch, train_loader, model, criterion, optimizer)
        result = validate_epoch(validation_loader, model, criterion)

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "data.ckpt"), "wb") as fp:
                cpickle.dump(
                    {
                        "epoch": epoch,
                        "model": model.state_dict(),
                        "optimizer_state_dict": optimizer.state_dict(),
                    },
                    fp,
                )
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            train.report(result, checkpoint=checkpoint)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="The address to use for Redis."
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
        "--use-gpu", action="store_true", default=False, help="Enables GPU training."
    )
    parser.add_argument(
        "--data-dir",
        required=False,
        type=str,
        default="~/data",
        help="Root directory for storing downloaded dataset.",
    )
    parser.add_argument(
        "--synch", action="store_true", default=False, help="Use synchronous PBT."
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
                "lr": tune.loguniform(0.001, 0.1),
                # allow perturbations within this set of categorical values
                "momentum": [0.8, 0.9, 0.99],
            }
        },
        synch=args.synch,
    )

    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.grid_search([0.001, 0.01, 0.05, 0.1]),
                "momentum": 0.8,
                "batch_size": 128 * args.num_workers,
                "test_mode": args.smoke_test,  # whether to to subset the data
                "data_dir": args.data_dir,
                "epochs": args.num_epochs,
            }
        },
        tune_config=TuneConfig(
            num_samples=1, metric="loss", mode="min", scheduler=pbt_scheduler
        ),
        run_config=RunConfig(
            stop={"training_iteration": 3 if args.smoke_test else args.num_epochs},
            failure_config=FailureConfig(max_failures=3),  # used for fault tolerance
        ),
    )

    results = tuner.fit()

    print(results.get_best_result(metric="loss", mode="min"))
