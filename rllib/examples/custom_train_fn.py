"""Example of a custom training workflow. Run this for a demo.

This example shows:
  - using Tune trainable functions to implement custom training workflows

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
import argparse
import os

import ray
from ray import train, tune
from ray.rllib.algorithms.ppo import PPO, PPOConfig

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)


def my_train_fn(config):
    iterations = config.pop("train-iterations", 10)

    config = PPOConfig().update_from_dict(config).environment("CartPole-v1")

    # Train for n iterations with high LR.
    config.lr = 0.01
    agent1 = config.build()
    for _ in range(iterations):
        result = agent1.train()
        result["phase"] = 1
        train.report(result)
        phase1_time = result["timesteps_total"]
    state = agent1.save()
    agent1.stop()

    # Train for n iterations with low LR
    config.lr = 0.0001
    agent2 = config.build()
    agent2.restore(state)
    for _ in range(iterations):
        result = agent2.train()
        result["phase"] = 2
        result["timesteps_total"] += phase1_time  # keep time moving forward
        train.report(result)
    agent2.stop()


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    config = {
        # Special flag signalling `my_train_fn` how many iters to do.
        "train-iterations": 2,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 0,
        "framework": args.framework,
    }
    resources = PPO.default_resource_request(config)
    tuner = tune.Tuner(
        tune.with_resources(my_train_fn, resources=resources), param_space=config
    )
    tuner.fit()
