#!/usr/bin/env python

import argparse
import time
from typing import Dict, Any

from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler


def evaluation_fn(step, width, height) -> float:
    # simulate model evaluation
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config: Dict[str, Any]) -> None:
    # Config contains the hyperparameters to tune
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be an arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report({"iterations": step, "mean_loss": intermediate_score})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AsyncHyperBand optimization example")
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    # AsyncHyperBand enables aggressive early stopping of poorly performing trials
    scheduler = AsyncHyperBandScheduler(
        grace_period=5,  # Minimum training iterations before stopping
        max_t=100,  # Maximum training iterations
    )

    tuner = tune.Tuner(
        tune.with_resources(easy_objective, {"cpu": 1, "gpu": 0}),
        run_config=tune.RunConfig(
            name="asynchyperband_test",
            stop={"training_iteration": 1 if args.smoke_test else 9999},
            verbose=1,
        ),
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            scheduler=scheduler,
            num_samples=20,  # Number of trials to run
        ),
        param_space={
            "steps": 100,
            "width": tune.uniform(10, 100),
            "height": tune.uniform(0, 100),
        },
    )

    # Run the hyperparameter optimization
    results = tuner.fit()
    print(f"Best hyperparameters found: {results.get_best_result().config}")
