#!/usr/bin/env python
"""Examples logging Tune runs to comet.ml"""
import argparse
import numpy as np
from unittest.mock import MagicMock

from ray import tune
from ray.tune.integration.comet import CometLoggerCallback


def train_function(config, checkpoint_dir=None):
    for i in range(30):
        loss = config["mean"] + config["sd"] * np.random.randn()
        tune.report(loss=loss)


def tune_function(api_key=None, project_name=None):
    analysis = tune.run(
        train_function,
        name="comet",
        metric="loss",
        mode="min",
        callbacks=[
            CometLoggerCallback(
                api_key=api_key, project_name=project_name, tags=["comet_example"]
            )
        ],
        config={"mean": tune.grid_search([1, 2, 3]), "sd": tune.uniform(0.2, 0.8)},
    )
    return analysis.best_config


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-key",
        type=str,
        help="API Key for Comet access. If not passed in, COMET_API_KEY"
        + "environment variable should be set.",
    )
    parser.add_argument(
        "--project-name",
        type=str,
        help="Project to log experiment to. If "
        "not passed in, experiment will be "
        "logger under 'Uncategorized "
        "Experiments'",
        default="comet-ray-example",
    )
    parser.add_argument(
        "--mock-api",
        action="store_true",
        help="Finish fast and use mock " "API access.",
    )
    args, _ = parser.parse_known_args()

    # Add mock api for testing
    if args.mock_api:
        CometLoggerCallback._logger_process_cls = MagicMock
        args.api_key = "abc"

    tune_function(args.api_key, args.project_name)
