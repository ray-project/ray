#!/usr/bin/env python

import argparse

import ray
from ray import train, tune
from ray.tune.utils.mock_trainable import MyTrainableClass
from ray.tune.schedulers import HyperBandScheduler

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    ray.init(num_cpus=4 if args.smoke_test else None)

    # Hyperband early stopping, configured with `episode_reward_mean` as the
    # objective and `training_iteration` as the time unit,
    # which is automatically filled by Tune.
    hyperband = HyperBandScheduler(time_attr="training_iteration", max_t=200)

    tuner = tune.Tuner(
        MyTrainableClass,
        run_config=train.RunConfig(
            name="hyperband_test",
            stop={"training_iteration": 1 if args.smoke_test else 200},
            verbose=1,
            failure_config=train.FailureConfig(
                fail_fast=True,
            ),
        ),
        tune_config=tune.TuneConfig(
            num_samples=20 if args.smoke_test else 200,
            metric="episode_reward_mean",
            mode="max",
            scheduler=hyperband,
        ),
        param_space={"width": tune.randint(10, 90), "height": tune.randint(0, 100)},
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
