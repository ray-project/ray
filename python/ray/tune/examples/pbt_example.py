#!/usr/bin/env python

import numpy as np
import argparse
import random

import ray
from ray import air, tune
from ray.tune.schedulers import PopulationBasedTraining


class PBTBenchmarkExample(tune.Trainable):
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

    def setup(self, config):
        self.lr = config["lr"]
        self.accuracy = 0.0  # end = 1000

    def step(self):
        midpoint = 100  # lr starts decreasing after acc > midpoint
        q_tolerance = 3  # penalize exceeding lr by more than this multiple
        noise_level = 2  # add gaussian noise to the acc increase
        # triangle wave:
        #  - start at 0.001 @ t=0,
        #  - peak at 0.01 @ t=midpoint,
        #  - end at 0.001 @ t=midpoint * 2,
        if self.accuracy < midpoint:
            optimal_lr = 0.01 * self.accuracy / midpoint
        else:
            optimal_lr = 0.01 - 0.01 * (self.accuracy - midpoint) / midpoint
        optimal_lr = min(0.01, max(0.001, optimal_lr))

        # compute accuracy increase
        q_err = max(self.lr, optimal_lr) / min(self.lr, optimal_lr)
        if q_err < q_tolerance:
            self.accuracy += (1.0 / q_err) * random.random()
        elif self.lr > optimal_lr:
            self.accuracy -= (q_err - q_tolerance) * random.random()
        self.accuracy += noise_level * np.random.normal()
        self.accuracy = max(0, self.accuracy)

        return {
            "mean_accuracy": self.accuracy,
            "cur_lr": self.lr,
            "optimal_lr": optimal_lr,  # for debugging
            "q_err": q_err,  # for debugging
            "done": self.accuracy > midpoint * 2,
        }

    def save_checkpoint(self, checkpoint_dir):
        return {
            "accuracy": self.accuracy,
            "lr": self.lr,
        }

    def load_checkpoint(self, checkpoint):
        self.accuracy = checkpoint["accuracy"]

    def reset_config(self, new_config):
        self.lr = new_config["lr"]
        self.config = new_config
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--cluster", action="store_true", help="Distribute tuning on a cluster"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.server_address:
        ray.init(f"ray://{args.server_address}")
    elif args.cluster:
        ray.init(address="auto")
    elif args.smoke_test:
        ray.init(num_cpus=2)  # force pausing to happen for test
    else:
        ray.init()

    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=20,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: random.uniform(0.0001, 0.02),
            # allow perturbations within this set of categorical values
            "some_other_factor": [1, 2],
        },
    )

    tuner = tune.Tuner(
        PBTBenchmarkExample,
        run_config=air.RunConfig(
            name="pbt_test",
            stop={
                "training_iteration": 200,
            },
            verbose=0,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=20,
            ),
        ),
        tune_config=tune.TuneConfig(
            scheduler=pbt,
            metric="mean_accuracy",
            mode="max",
            reuse_actors=True,
            num_samples=8,
        ),
        param_space={
            "lr": 0.0001,
            # note: this parameter is perturbed but has no effect on
            # the model training in this example
            "some_other_factor": 1,
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
