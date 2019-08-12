#!/usr/bin/env python
"""Simple MLFLow Logger example.

This uses a simple MLFlow logger. One limitation of this is that there is
no artifact support; to save artifacts with Tune and MLFlow, you will need to
start a MLFlow run inside the Trainable function/class.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import mlflow
from  mlflow.tracking import MlflowClient
import numpy as np
import time
import os
import random

from ray import tune
from ray.tune import Trainable, run


def trial_str_creator(trial):
    return "{}_{}_123".format(trial.trainable_name, trial.trial_id)


def easy_objective(config):
    for i in range(20):
        result = dict(
            timesteps_total=i,
            mean_loss=(config["height"] - 14)**2 - abs(config["width"] - 3)
        )
        tune.track.log(**result)
        time.sleep(0.02)
    tune.track.log(done=True)


if __name__ == "__main__":
    client = MlflowClient()
    experiment_id = client.create_experiment("test")

    trials = run(
        easy_objective,
        name="mlflow",
        num_samples=5,
        trial_name_creator=tune.function(trial_str_creator),
        loggers=[MLFLowLogger],
        config={
            "mlflow_experiment_id": experiment_id,
            "width": tune.sample_from(
                lambda spec: 10 + int(90 * random.random())),
            "height": tune.sample_from(lambda spec: int(100 * random.random()))
        })

    df = mlflow.search_runs([experiment_id])
    print(df)
