#!/usr/bin/env python
"""Simple MLFLow Logger example.

This uses a simple MLFlow logger. One limitation of this is that there is
no artifact support; to save artifacts with Tune and MLFlow, you will need to
start a MLFlow run inside the Trainable function/class.

"""
import mlflow
from mlflow.tracking import MlflowClient
import time

from ray import tune
from ray.tune.logger import MLFLowLogger, DEFAULT_LOGGERS


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config.get("steps", 100)):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


if __name__ == "__main__":
    client = MlflowClient()
    experiment_id = client.create_experiment("test")

    trials = tune.run(
        easy_objective,
        name="mlflow",
        num_samples=5,
        loggers=DEFAULT_LOGGERS + (MLFLowLogger, ),
        config={
            "logger_config": {
                "mlflow_experiment_id": experiment_id,
            },
            "width": tune.randint(10, 100),
            "height": tune.randint(0, 100),
        })

    df = mlflow.search_runs([experiment_id])
    print(df)
