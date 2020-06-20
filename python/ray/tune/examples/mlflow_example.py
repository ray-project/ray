#!/usr/bin/env python
"""Simple MLFLow Logger example.

This uses a simple MLFlow logger. One limitation of this is that there is
no artifact support; to save artifacts with Tune and MLFlow, you will need to
start a MLFlow run inside the Trainable function/class.

"""
import mlflow
from mlflow.tracking import MlflowClient
import time
import random

from ray import tune
from ray.tune.logger import MLFLowLogger, DEFAULT_LOGGERS


def easy_objective(config):
    for i in range(20):
        result = dict(
            timesteps_total=i,
            mean_loss=(config["height"] - 14)**2 - abs(config["width"] - 3))
        tune.report(**result)
        time.sleep(0.02)


if __name__ == "__main__":
    client = MlflowClient()
    experiment_id = client.create_experiment("test")

    trials = tune.run(
        easy_objective,
        name="mlflow",
        num_samples=5,
        loggers=DEFAULT_LOGGERS + (MLFLowLogger, ),
        config={
            "mlflow_experiment_id": experiment_id,
            "width": tune.sample_from(
                lambda spec: 10 + int(90 * random.random())),
            "height": tune.sample_from(lambda spec: int(100 * random.random()))
        })

    df = mlflow.search_runs([experiment_id])
    print(df)
