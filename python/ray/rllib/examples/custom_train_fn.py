"""Example of a custom training workflow. Run this for a demo.

This example shows:
  - using Tune trainable functions to implement custom training workflows

You can visualize experiment results in ~/ray_results using TensorBoard.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.agents.ppo import PPOAgent
from ray.tune import run_experiments


def my_train_fn(config, reporter):
    # Train for 100 iterations with high LR
    agent1 = PPOAgent(env="CartPole-v0", config=config)
    for _ in range(10):
        result = agent1.train()
        result["phase"] = 1
        reporter(**result)
        phase1_time = result["timesteps_total"]
    state = agent1.save()
    agent1.stop()

    # Train for 100 iterations with low LR
    config["lr"] = 0.0001
    agent2 = PPOAgent(env="CartPole-v0", config=config)
    agent2.restore(state)
    for _ in range(10):
        result = agent2.train()
        result["phase"] = 2
        result["timesteps_total"] += phase1_time  # keep time moving forward
        reporter(**result)
    agent2.stop()


if __name__ == "__main__":
    ray.init()
    run_experiments({
        "demo": {
            "run": my_train_fn,
            "resources_per_trial": {
                "cpu": 1,
            },
            "config": {
                "lr": 0.01,
                "num_workers": 0,
            },
        },
    })
