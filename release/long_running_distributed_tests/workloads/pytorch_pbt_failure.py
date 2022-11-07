import argparse
import sys

import numpy as np

import ray
from ray import tune
from ray.air.config import RunConfig, ScalingConfig, FailureConfig
from ray.train.examples.pytorch.tune_cifar_torch_pbt_example import train_func
from ray.train.torch import TorchConfig, TorchTrainer
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.tune.utils.mock import FailureInjectorCallback
from ray.tune.utils.release_test_util import ProgressCallback

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

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=num_training_workers,
        use_gpu=not args.smoke_test,
    ),
    torch_config=TorchConfig(backend="gloo"),
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
            "head_location": None,
            "worker_locations": None,
            "test_mode": args.smoke_test,
            "batch_size": 128 * num_training_workers,
            # For the long running test, we want the training to run forever,
            # and it will be terminated by the release test infra.
            "epochs": 1 if args.smoke_test else sys.maxsize,
        }
    },
    tune_config=TuneConfig(
        num_samples=4, metric="loss", mode="min", scheduler=pbt_scheduler
    ),
    run_config=RunConfig(
        stop={"training_iteration": 1} if args.smoke_test else None,
        failure_config=FailureConfig(max_failures=-1),
        callbacks=[FailureInjectorCallback(time_between_checks=90), ProgressCallback()],
    ),
)

results = tuner.fit()

print(results.get_best_result(metric="loss", mode="min"))
