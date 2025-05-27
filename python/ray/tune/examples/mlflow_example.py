#!/usr/bin/env python
"""Examples using MLfowLoggerCallback and setup_mlflow.
"""
import os
import tempfile
import time

import mlflow

from ray import tune
from ray.air.integrations.mlflow import MLflowLoggerCallback, setup_mlflow


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def train_function(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config.get("steps", 100)):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back to Tune.
        tune.report({"iterations": step, "mean_loss": intermediate_score})
        time.sleep(0.1)


def tune_with_callback(mlflow_tracking_uri, finish_fast=False):

    tuner = tune.Tuner(
        train_function,
        run_config=tune.RunConfig(
            name="mlflow",
            callbacks=[
                MLflowLoggerCallback(
                    tracking_uri=mlflow_tracking_uri,
                    experiment_name="example",
                    save_artifact=True,
                )
            ],
        ),
        tune_config=tune.TuneConfig(
            num_samples=5,
        ),
        param_space={
            "width": tune.randint(10, 100),
            "height": tune.randint(0, 100),
            "steps": 5 if finish_fast else 100,
        },
    )
    tuner.fit()


def train_function_mlflow(config):
    setup_mlflow(config)

    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config.get("steps", 100)):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Log the metrics to mlflow
        mlflow.log_metrics(dict(mean_loss=intermediate_score), step=step)
        # Feed the score back to Tune.
        tune.report({"iterations": step, "mean_loss": intermediate_score})
        time.sleep(0.1)


def tune_with_setup(mlflow_tracking_uri, finish_fast=False):
    # Set the experiment, or create a new one if does not exist yet.
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name="mixin_example")
    tuner = tune.Tuner(
        train_function_mlflow,
        run_config=tune.RunConfig(
            name="mlflow",
        ),
        tune_config=tune.TuneConfig(
            num_samples=5,
        ),
        param_space={
            "width": tune.randint(10, 100),
            "height": tune.randint(0, 100),
            "steps": 5 if finish_fast else 100,
            "mlflow": {
                "experiment_name": "mixin_example",
                "tracking_uri": mlflow.get_tracking_uri(),
            },
        },
    )
    tuner.fit()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--tracking-uri",
        type=str,
        help="The tracking URI for the MLflow tracking server.",
    )
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        mlflow_tracking_uri = os.path.join(tempfile.gettempdir(), "mlruns")
    else:
        mlflow_tracking_uri = args.tracking_uri

    tune_with_callback(mlflow_tracking_uri, finish_fast=args.smoke_test)
    if not args.smoke_test:
        df = mlflow.search_runs(
            [mlflow.get_experiment_by_name("example").experiment_id]
        )
        print(df)

    tune_with_setup(mlflow_tracking_uri, finish_fast=args.smoke_test)
    if not args.smoke_test:
        df = mlflow.search_runs(
            [mlflow.get_experiment_by_name("mixin_example").experiment_id]
        )
        print(df)
