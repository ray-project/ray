"""Example of a custom training workflow. Run this for a demo.

This example shows:
  - using Tune trainable functions to implement custom training workflows

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
import argparse
import os

import ray
from ray import tune
from ray.rllib.agents.ppo import PPOTrainer

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")


def my_train_fn(config, reporter):
    # Train for 100 iterations with high LR
    agent1 = PPOTrainer(env="CartPole-v0", config=config)
    for _ in range(10):
        result = agent1.train()
        result["phase"] = 1
        reporter(**result)
        phase1_time = result["timesteps_total"]
    state = agent1.save()
    agent1.stop()

    # Train for 100 iterations with low LR
    config["lr"] = 0.0001
    agent2 = PPOTrainer(env="CartPole-v0", config=config)
    agent2.restore(state)
    for _ in range(10):
        result = agent2.train()
        result["phase"] = 2
        result["timesteps_total"] += phase1_time  # keep time moving forward
        reporter(**result)
    agent2.stop()


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    config = {
        "lr": 0.01,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 0,
        "framework": "torch" if args.torch else "tf",
    }
    resources = PPOTrainer.default_resource_request(config).to_json()
    tune.run(my_train_fn, resources_per_trial=resources, config=config)
