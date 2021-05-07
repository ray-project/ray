#!/usr/bin/env python
"""Examples logging Tune runs to comet.ml"""
import argparse
import os
import tempfile
import time

from ray import tune
from ray.tune.integration.comet import CometLoggerCallback

def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config.get("steps", 100)):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


def tune_function(api_key=None, project_name=None, finish_fast=False):
    analysis = tune.run(
        easy_objective,
        name="comet",
        num_samples=5,
        callbacks=[
            CometLoggerCallback(api_key=api_key, project_name=project_name,
                                tags=["comet_example"])
        ],
        config={
            "width": tune.randint(10, 100),
            "height": tune.randint(0, 100),
            "steps": 5 if finish_fast else 100,
        })
    return analysis.best_config

# def ptl_example(config):
#     # Use Comet autologging for Pytorch Lightning.

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-key", type=str, help="API Key for Comet access. If not passed in, COMET_API_KEY environment variable should be set."
    )
    parser.add_argument(
        "--project-name", type=str, help="Project to log experiment to. If "
                                         "not passed in, experiment will be "
                                         "logger under 'Uncategorized "
                                         "Experiments'"
    )
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish fast and use mock "
                                                  "API access.")
    args, _ = parser.parse_known_args()

    tune_function(args.api_key, args.project_name, finish_fast=True)

