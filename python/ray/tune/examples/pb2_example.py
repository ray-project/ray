#!/usr/bin/env python

import argparse

import ray
from ray import air, tune
from ray.tune.schedulers.pb2 import PB2
from ray.tune.examples.pbt_function import pbt_function

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
        if args.server_address:
            ray.init(f"ray://{args.server_address}")
        else:
            ray.init()

    pbt = PB2(
        perturbation_interval=20,
        hyperparam_bounds={
            # hyperparameter bounds.
            "lr": [0.0001, 0.02],
        },
    )

    tuner = tune.Tuner(
        pbt_function,
        run_config=air.RunConfig(
            name="pbt_test",
            verbose=False,
            stop={
                "training_iteration": 30,
            },
            failure_config=air.FailureConfig(
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
        },
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
