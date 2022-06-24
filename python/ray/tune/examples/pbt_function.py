#!/usr/bin/env python

import numpy as np
import argparse
import json
import os
import random

import ray
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining


def pbt_function(config, checkpoint_dir=None):
    """Toy PBT problem for benchmarking adaptive learning rate.

    The goal is to optimize this trainable's accuracy. The accuracy increases
    fastest at the optimal lr, which is a function of the current accuracy.

    The optimal lr schedule for this problem is the triangle wave as follows.
    Note that many lr schedules for real models also follow this shape:

     best lr
      ^
      |    /\
      |   /  \
      |  /    \
      | /      \
      ------------> accuracy

    In this problem, using PBT with a population of 2-4 is sufficient to
    roughly approximate this lr schedule. Higher population sizes will yield
    faster convergence. Training will not converge without PBT.
    """
    lr = config["lr"]
    accuracy = 0.0  # end = 1000
    start = 0
    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
            state = json.loads(f.read())
            accuracy = state["acc"]
            start = state["step"]

    midpoint = 100  # lr starts decreasing after acc > midpoint
    q_tolerance = 3  # penalize exceeding lr by more than this multiple
    noise_level = 2  # add gaussian noise to the acc increase
    # triangle wave:
    #  - start at 0.001 @ t=0,
    #  - peak at 0.01 @ t=midpoint,
    #  - end at 0.001 @ t=midpoint * 2,
    for step in range(start, 100):
        if accuracy < midpoint:
            optimal_lr = 0.01 * accuracy / midpoint
        else:
            optimal_lr = 0.01 - 0.01 * (accuracy - midpoint) / midpoint
        optimal_lr = min(0.01, max(0.001, optimal_lr))

        # compute accuracy increase
        q_err = max(lr, optimal_lr) / min(lr, optimal_lr)
        if q_err < q_tolerance:
            accuracy += (1.0 / q_err) * random.random()
        elif lr > optimal_lr:
            accuracy -= (q_err - q_tolerance) * random.random()
        accuracy += noise_level * np.random.normal()
        accuracy = max(0, accuracy)

        if step % 3 == 0:
            with tune.checkpoint_dir(step=step) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"acc": accuracy, "step": start}))

        tune.report(
            mean_accuracy=accuracy,
            cur_lr=lr,
            optimal_lr=optimal_lr,  # for debugging
            q_err=q_err,  # for debugging
            done=accuracy > midpoint * 2,  # this stops the training process
        )


def run_tune_pbt():
    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=4,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: random.uniform(0.0001, 0.02),
            # allow perturbations within this set of categorical values
            "some_other_factor": [1, 2],
        },
    )

    analysis = tune.run(
        pbt_function,
        name="pbt_test",
        scheduler=pbt,
        verbose=False,
        metric="mean_accuracy",
        mode="max",
        stop={
            "training_iteration": 30,
        },
        num_samples=8,
        fail_fast=True,
        config={
            "lr": 0.0001,
            # note: this parameter is perturbed but has no effect on
            # the model training in this example
            "some_other_factor": 1,
        },
    )

    print("Best hyperparameters found were: ", analysis.best_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()
    if args.smoke_test:
        ray.init(num_cpus=2)  # force pausing to happen for test
    else:
        if args.server_address is not None:
            ray.init(f"ray://{args.server_address}")
        else:
            ray.init()

    run_tune_pbt()
