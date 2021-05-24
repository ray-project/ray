import argparse
import json
import time

import numpy as np
import os
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import CIFAR10
import torchvision.transforms as transforms

import ray
from ray import tune
from ray.tune import CLIReporter, Callback
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.utils.mock import FailureInjectorCallback
from ray.util.sgd.torch import TorchTrainer, TrainingOperator
from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import BATCH_SIZE

parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test",
    action="store_true",
    default=False,
    help="Finish quickly for training.")
args = parser.parse_args()


class JsonCallback(Callback):
    """Tune Callback to periodically write results to output JSON file."""

    def __init__(self, output_json_file):
        self.output_file = output_json_file

    def on_step_end(self, iteration: int, trials, **info):
        """Write output every 6 tuning loop step."""
        if iteration % 3 == 0:
            current_time = time.time()
            trial_ids = [trial.trial_id for trial in trials]
            trial_names = [str(trial) for trial in trials]
            training_iterations = [
                trial.last_result.get("training_iteration", None)
                for trial in trials
            ]
            trial_statuses = [trial.status for trial in trials]

            output = {
                "last_update": current_time,
                "trial_ids": trial_ids,
                "trial_names": trial_names,
                "training_iterations": training_iterations,
                "trial_statuses": trial_statuses
            }
            with open(self.output_file, "at") as f:
                json.dump(output, f)


def initialization_hook():
    # Need this for avoiding a connection restart issue on AWS.
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"

    # set the below if needed
    print("NCCL DEBUG SET")
    os.environ["NCCL_DEBUG"] = "INFO"


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

CustomTrainingOperator = TrainingOperator.from_creators(
    model_creator=ResNet18,
    optimizer_creator=optimizer_creator,
    data_creator=cifar_creator,
    loss_creator=nn.CrossEntropyLoss)

TorchTrainable = TorchTrainer.as_trainable(
    training_operator_cls=CustomTrainingOperator,
    initialization_hook=initialization_hook,
    num_workers=num_training_workers,
    config={
        "test_mode": args.smoke_test,
        BATCH_SIZE: 128 * num_training_workers,
    },
    use_gpu=not args.smoke_test,
)

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

callbacks = [FailureInjectorCallback(time_between_checks=90)]
if not args.smoke_test:
    callbacks.append(
        JsonCallback(
            output_json_file=os.environ.get("TEST_OUTPUT_JSON",
                                            "/tmp/pytorch_pbt_failure.json")))

analysis = tune.run(
    TorchTrainable,
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
    callbacks=[callbacks],
    queue_trials=True,
    stop={"training_iteration": 1} if args.smoke_test else None)

print(analysis.get_best_config(metric="val_loss", mode="min"))
