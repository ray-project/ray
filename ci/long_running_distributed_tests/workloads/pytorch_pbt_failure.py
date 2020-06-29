import argparse
import numpy as np
import os
import random
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import CIFAR10
import torchvision.transforms as transforms

import ray
from ray import tune
from ray.autoscaler.commands import kill_node
from ray.tune import CLIReporter
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.utils.util import merge_dicts
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import BATCH_SIZE

parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test",
    action="store_true",
    default=False,
    help="Finish quickly for training.")
args = parser.parse_args()


class FailureInjectorExecutor(RayTrialExecutor):
    """Adds random failure injection to the TrialExecutor."""

    def on_step_begin(self, trial_runner):
        """Before step(), update available resources and inject failure."""
        self._update_avail_resources()
        # With 10% probability inject failure to a worker.
        if random.random() < 0.1 and not args.smoke_test:
            # With 10% probability fully terminate the node.
            should_terminate = random.random() < 0.1
            kill_node(
                "/home/ubuntu/ray_bootstrap_config.yaml",
                yes=True,
                hard=should_terminate,
                override_cluster_name=None)


def initialization_hook():
    # Need this for avoiding a connection restart issue on AWS.
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"

    # set the below if needed
    # print("NCCL DEBUG SET")
    # os.environ["NCCL_DEBUG"] = "INFO"


def cifar_creator(config):
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])  # meanstd transformation

    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])
    train_dataset = CIFAR10(
        root="~/data", train=True, download=True, transform=transform_train)
    validation_dataset = CIFAR10(
        root="~/data", train=False, download=False, transform=transform_test)

    if config.get("test_mode"):
        train_dataset = Subset(train_dataset, list(range(64)))
        validation_dataset = Subset(validation_dataset, list(range(64)))

    train_loader = DataLoader(
        train_dataset, batch_size=config[BATCH_SIZE], num_workers=2)
    validation_loader = DataLoader(
        validation_dataset, batch_size=config[BATCH_SIZE], num_workers=2)
    return train_loader, validation_loader


def optimizer_creator(model, config):
    """Returns optimizer"""
    return torch.optim.SGD(
        model.parameters(),
        lr=config.get("lr", 0.1),
        momentum=config.get("momentum", 0.9))


ray.init(address="auto" if not args.smoke_test else None, log_to_driver=True)
num_training_workers = 1 if args.smoke_test else 3

executor = FailureInjectorExecutor(queue_trials=True)

TorchTrainable = TorchTrainer.as_trainable(
    model_creator=ResNet18,
    data_creator=cifar_creator,
    optimizer_creator=optimizer_creator,
    loss_creator=nn.CrossEntropyLoss,
    initialization_hook=initialization_hook,
    num_workers=num_training_workers,
    config={
        "test_mode": args.smoke_test,
        BATCH_SIZE: 128 * num_training_workers,
    },
    use_gpu=not args.smoke_test)


class NoFaultToleranceTrainable(TorchTrainable):
    def _train(self):
        train_stats = self.trainer.train(max_retries=0, profile=True)
        validation_stats = self.trainer.validate(profile=True)
        stats = merge_dicts(train_stats, validation_stats)
        return stats


pbt_scheduler = PopulationBasedTraining(
    time_attr="training_iteration",
    metric="val_loss",
    mode="min",
    perturbation_interval=1,
    hyperparam_mutations={
        # distribution for resampling
        "lr": lambda: np.random.uniform(0.001, 1),
        # allow perturbations within this set of categorical values
        "momentum": [0.8, 0.9, 0.99],
    })

reporter = CLIReporter()
reporter.add_metric_column("val_loss", "loss")
reporter.add_metric_column("val_accuracy", "acc")

analysis = tune.run(
    NoFaultToleranceTrainable,
    num_samples=4,
    config={
        "lr": tune.choice([0.001, 0.01, 0.1]),
        "momentum": 0.8,
        "head_location": None,
        "worker_locations": None
    },
    max_failures=-1,  # used for fault tolerance
    checkpoint_freq=2,  # used for fault tolerance
    progress_reporter=reporter,
    scheduler=pbt_scheduler,
    trial_executor=executor,
    stop={"training_iteration": 1} if args.smoke_test else None)

print(analysis.get_best_config(metric="val_loss", mode="min"))
