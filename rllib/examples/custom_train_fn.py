"""Example of a custom training workflow. Run this for a demo.

This example shows:
  - using Tune trainable functions to implement custom training workflows

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
import argparse
import os

import ray
from ray import tune
from ray.rllib.algorithms.ppo import PPO

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)


def my_train_fn(config, reporter):
    iterations = config.pop("train-iterations", 10)

    # Train for n iterations with high LR
    agent1 = PPO(env="CartPole-v1", config=config)
    for _ in range(iterations):
        result = agent1.train()
        result["phase"] = 1
        reporter(**result)
        phase1_time = result["timesteps_total"]
    state = agent1.save()
    agent1.stop()

    # Train for n iterations with low LR
    config["lr"] = 0.0001
    agent2 = PPO(env="CartPole-v1", config=config)
    agent2.restore(state)
    for _ in range(iterations):
        result = agent2.train()
        result["phase"] = 2
        result["timesteps_total"] += phase1_time  # keep time moving forward
        reporter(**result)
    agent2.stop()


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    config = {
        # Special flag signalling `my_train_fn` how many iters to do.
        "train-iterations": 2,
        "lr": 0.01,
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
