# If want to use checkpointing with a custom training function (not a Ray
# integration like PyTorch or Tensorflow), your function can read/write
# checkpoint through ``ray.air.session`` APIs.
import time
import argparse

from ray import tune
from ray.air import session
from ray.air.checkpoint import Checkpoint


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


def train_func(config):
    step = 0
    width, height = config["width"], config["height"]

    if session.get_checkpoint():
        loaded_checkpoint = session.get_checkpoint()
        step = loaded_checkpoint.to_dict()["step"] + 1

    for step in range(step, 100):
        intermediate_score = evaluation_fn(step, width, height)
        checkpoint = Checkpoint.from_dict({"step": step})
        session.report(
            {"iterations": step, "mean_loss": intermediate_score}, checkpoint=checkpoint
        )


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
        help="The address of server to connect to if using Ray Client.",
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
    best_checkpoint = analysis.best_checkpoint
    checkpoint_data = best_checkpoint.to_dict()
    print("Best checkpoint: ", checkpoint_data)
