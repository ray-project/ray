import argparse
import sys

import numpy as np

import ray

from ray import tune
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.utils.mock import FailureInjectorCallback
from ray.tune.utils.release_test_util import ProgressCallback

from ray.train import Trainer
from ray.train.examples.tune_cifar_pytorch_pbt_example import train_func
from ray.train.torch import TorchConfig

parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test",
    action="store_true",
    default=False,
    help="Finish quickly for training.",
)
args = parser.parse_args()

ray.init(address="auto" if not args.smoke_test else None, log_to_driver=True)
num_training_workers = 1 if args.smoke_test else 3

trainer = Trainer(
    num_workers=num_training_workers,
    use_gpu=not args.smoke_test,
    backend=TorchConfig(backend="gloo"),
)
TorchTrainable = trainer.to_tune_trainable(train_func=train_func)


pbt_scheduler = PopulationBasedTraining(
    time_attr="training_iteration",
    metric="loss",
    mode="min",
    perturbation_interval=1,
    hyperparam_mutations={
        # distribution for resampling
        "lr": lambda: np.random.uniform(0.001, 1),
        # allow perturbations within this set of categorical values
        "momentum": [0.8, 0.9, 0.99],
    },
)

analysis = tune.run(
    TorchTrainable,
    num_samples=4,
    config={
        "lr": tune.choice([0.001, 0.01, 0.1]),
        "momentum": 0.8,
        "head_location": None,
        "worker_locations": None,
        "test_mode": args.smoke_test,
        "batch_size": 128 * num_training_workers,
        # For the long running test, we want the training to run forever, and it will
        # be terminated by the release test infra.
        "epochs": 1 if args.smoke_test else sys.maxsize,
    },
    max_failures=-1,  # used for fault tolerance
    checkpoint_freq=2,  # used for fault tolerance
    scheduler=pbt_scheduler,
    callbacks=[FailureInjectorCallback(time_between_checks=90), ProgressCallback()],
    stop={"training_iteration": 1} if args.smoke_test else None,
)

print(analysis.get_best_config(metric="loss", mode="min"))
