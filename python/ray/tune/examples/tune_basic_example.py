"""This example demonstrates basic Ray Tune random search and grid search."""
import time
import json

import ray
from ray.air import session
from ray import tune

# "distribution" corresponds to a distribution in `ray.tune`, eg. `ray.tune.uniform`.
# "constant" is a special key signifying this is not a distribution but a constant.
config_json = """
[
    {
        "name": "width",
        "distribution": "uniform",
        "lower": 0,
        "upper": 20
    },
    {
        "name": "height",
        "distribution": "uniform",
        "lower": -100,
        "upper": 100
    },
    {
        "name": "activation",
        "distribution": "grid_search",
        "values": ["relu", "tanh"]
    },
    {
        "name": "steps",
        "distribution": "constant",
        "value": 5
    }
]
"""


def parse_config_entry(entry: dict) -> dict:
    if not isinstance(entry, dict):
        return entry
    # Deal with nested dicts
    entry = {k: parse_config_entry(v) for k, v in entry.items()}
    if entry["distribution"] == "constant":
        return {entry["name"]: entry["value"]}
    return {
        entry["name"]: getattr(tune, entry["distribution"])(
            **{k: v for k, v in entry.items() if k not in ("name", "distribution")}
        )
    }


def parse_config_json(json_string: str) -> dict:
    raw_config = json.loads(json_string)
    parsed_config = {}
    for entry in raw_config:
        parsed_config.update(parse_config_entry(entry))
    return parsed_config


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        session.report({"iterations": step, "mean_loss": intermediate_score})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    ray.init(configure_logging=False)

    # This will do a grid search over the `activation` parameter. This means
    # that each of the two values (`relu` and `tanh`) will be sampled once
    # for each sample (`num_samples`). We end up with 2 * 50 = 100 samples.
    # The `width` and `height` parameters are sampled randomly.
    # `steps` is a constant parameter.

    tuner = tune.Tuner(
        easy_objective,
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            num_samples=5 if args.smoke_test else 50,
        ),
        param_space=parse_config_json(config_json),
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
