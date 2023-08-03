#!/usr/bin/env python

import argparse

import ray
from ray import train, tune
from ray.tune.schedulers.pb2 import PB2
from ray.tune.examples.pbt_function import pbt_function

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        ray.init(num_cpus=2)  # force pausing to happen for test

    perturbation_interval = 5
    pbt = PB2(
        time_attr="training_iteration",
        perturbation_interval=perturbation_interval,
        hyperparam_bounds={
            # hyperparameter bounds.
            "lr": [0.0001, 0.02],
        },
    )

    tuner = tune.Tuner(
        pbt_function,
        run_config=train.RunConfig(
            name="pbt_test",
            verbose=False,
            stop={
                "training_iteration": 30,
            },
            failure_config=train.FailureConfig(
                fail_fast=True,
            ),
        ),
        tune_config=tune.TuneConfig(
            scheduler=pbt,
            metric="mean_accuracy",
            mode="max",
            num_samples=8,
        ),
        param_space={
            "lr": 0.0001,
            # note: this parameter is perturbed but has no effect on
            # the model training in this example
            "some_other_factor": 1,
            # This parameter is not perturbed and is used to determine
            # checkpoint frequency. We set checkpoints and perturbations
            # to happen at the same frequency.
            "checkpoint_interval": perturbation_interval,
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
