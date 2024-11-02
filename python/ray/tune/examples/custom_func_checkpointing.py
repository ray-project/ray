# If want to use checkpointing with a custom training function (not a Ray
# integration like PyTorch or Tensorflow), your function can read/write
# checkpoint through the ``ray.train.report(metrics, checkpoint=...)`` API.
import argparse
import json
import os
import tempfile
import time

from ray import train, tune
from ray.train import Checkpoint


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def train_func(config):
    step = 0
    width, height = config["width"], config["height"]

    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "checkpoint.json")) as f:
                state = json.load(f)
            step = state["step"] + 1

    for current_step in range(step, 100):
        intermediate_score = evaluation_fn(current_step, width, height)

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            with open(os.path.join(temp_checkpoint_dir, "checkpoint.json"), "w") as f:
                json.dump({"step": current_step}, f)
            train.report(
                {"iterations": current_step, "mean_loss": intermediate_score},
                checkpoint=Checkpoint.from_directory(temp_checkpoint_dir),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    tuner = tune.Tuner(
        train_func,
        run_config=train.RunConfig(
            name="hyperband_test",
            stop={"training_iteration": 1 if args.smoke_test else 10},
        ),
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            num_samples=5,
        ),
        param_space={
            "steps": 10,
            "width": tune.randint(10, 100),
            "height": tune.loguniform(10, 100),
        },
    )
    results = tuner.fit()
    best_result = results.get_best_result()
    print("Best hyperparameters: ", best_result.config)
    best_checkpoint = best_result.checkpoint
    print("Best checkpoint: ", best_checkpoint)
