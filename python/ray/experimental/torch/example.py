#!/usr/bin/env python

import sys
import argparse
import numpy as np

import ray
import time
from ray import tune
from ray.tune.trial import Resources
from ray.tune.logger import pretty_print

from ray.experimental.torch.pytorch_trainable import PytorchSGD, DEFAULT_CONFIG
from ray.experimental.torch.pytorch_helpers import prefetch

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument(
    "--redis-address",
    default=None,
    type=str,
    help="The Redis address of the cluster.")
parser.add_argument(
    "--batch-size", default=1, type=int, help="Batch per device.")
parser.add_argument(
    "--num-workers", default=0, type=int, help="Number of workers.")
parser.add_argument(
    "--num-iters", default=1, type=int, help="Number of iterations.")
# There is a difference between this and refresh_freq.
parser.add_argument(
    "--grace",
    default=10,
    type=int,
    help="Number of iterations before resizing.")

parser.add_argument(
    "--prefetch", action='store_true', help="Prefetch data onto all nodes")
parser.add_argument("--gpu", action='store_true', help="Use GPUs")
parser.add_argument("--tune", action='store_true', help="Use Tune.")

args = parser.parse_args(sys.argv[1:])
ray.init(redis_address=args.redis_address)

config = DEFAULT_CONFIG.copy()


def name_creator(trial):
    return "{}_{}".format(trial.trainable_name,
                          trial.config.get("starting_lr"))


if __name__ == '__main__':
    config["num_workers"] = args.num_workers
    config["batch_per_device"] = args.batch_size
    config["gpu"] = args.gpu

    res = "extra_gpu" if args.gpu else "extra_cpu"

    if args.prefetch:
        # This launches data downloading for all the experiments.
        prefetch()

    if not args.tune:
        config["verbose"] = True
        sgd = PytorchSGD(
            config=config,
            resources=Resources(**{
                "cpu": 0,
                "gpu": 0,
                res: args.num_workers
            }))
        from ray.tune.logger import pretty_print
        for i in range(args.num_iters):
            print(pretty_print(sgd.train()))
    else:

        config["starting_lr"] = tune.grid_search([0.1, 0.01, 0.001])
        config["weight_decay"] = tune.grid_search([1e-3, 5e-4, 1e-4])
        # sched = None/
        tune.run(
            PytorchSGD,
            name="pytorch-sgd",
            config=config,
            trial_name_creator=tune.function(name_creator),
            stop={
                "mean_accuracy": 90,
                "training_iteration": args.num_iters
            },
            resources_per_trial={
                "cpu": 0,
                "gpu": 0,
                "extra_gpu": 2
            },
            local_dir="~/ray_results/{}/".format("cifar"),
            queue_trials=True)
