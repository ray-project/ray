# If want to use checkpointing with a custom training function (not a Ray
# integration like PyTorch or Tensorflow), you must expose a
# ``checkpoint_dir`` argument in the function signature, and call
# ``tune.checkpoint_dir``:
import os
import time
import json
import argparse

from ray import tune


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def train_func(config, checkpoint_dir=None):
    start = 0
    width, height = config["width"], config["height"]

    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
            state = json.loads(f.read())
            start = state["step"] + 1

    for step in range(start, 100):
        intermediate_score = evaluation_fn(step, width, height)

        # Obtain a checkpoint directory
        with tune.checkpoint_dir(step=step) as checkpoint_dir:
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"step": step}))

        tune.report(iterations=step, mean_loss=intermediate_score)


# You can restore a single trial checkpoint by using
# ``tune.run(restore=<checkpoint_dir>)`` By doing this, you can change
# whatever experiments' configuration such as the experiment's name.

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using " "Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.server_address:
        import ray

        ray.init(f"ray://{args.server_address}")

    analysis = tune.run(
        train_func,
        name="hyperband_test",
        metric="mean_loss",
        mode="min",
        num_samples=5,
        stop={"training_iteration": 1 if args.smoke_test else 10},
        config={
            "steps": 10,
            "width": tune.randint(10, 100),
            "height": tune.loguniform(10, 100),
        },
    )
    print("Best hyperparameters: ", analysis.best_config)
    print("Best checkpoint directory: ", analysis.best_checkpoint)
    with open(os.path.join(analysis.best_checkpoint, "checkpoint"), "r") as f:
        print("Best checkpoint: ", json.load(f))
