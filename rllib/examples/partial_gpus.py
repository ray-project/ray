"""Example of a custom gym environment and model. Run this for a demo.

This example shows:
  - using a custom environment
  - using a custom model
  - using Tune for grid search

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
import argparse
import gym
from gym.spaces import Discrete, Box
import numpy as np
import os
import random

import ray
from ray import tune
from ray.tune import grid_search
from ray.rllib.env.env_context import EnvContext
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFC
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--framework", choices=["tf", "tf2", "tfe", "torch"],
                    default="tf")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=50)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=180.0)


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    config = {
        "env": "CartPole-v0",

        # torch:
        # Works all fine. Tried with:
        # num_gpus=0.5 (2 trials at the same time)
        # num_gpus=0.3 (3 trials at the same time)
        # num_gpus=0.25 (4 trials at the same time)
        # num_gpus=0.2 + 0.1 per-worker (1 worker) -> 0.3 -> 3 trials at same time
        # num_gpus=0.2 + 0.1 per-worker (2 workers) -> 0.4 -> 2 trials at same time
        # num_gpus=0.4 + 0.1 per-worker (2 workers) -> 0.6 -> 1 trial at same time

        # tf:
        # Works all fine. Tried with:
        # num_gpus=0.5 (2 trials at the same time)
        # num_gpus=0.3 (3 trials at the same time)
        # num_gpus=0.25 (4 trials at the same time)
        # num_gpus=0.2 + 0.1 per-worker (1 worker) -> 0.3 -> 3 trials at same time
        # num_gpus=0.2 + 0.1 per-worker (2 workers) -> 0.4 -> 2 trials at same time

        # ???
        # num_gpus=0.4 + 0.1 per-worker (2 workers) -> 0.6 -> 1 trial at same time

        # - Get strange [tf tensor] -> passed into numpy call when doing tf.zeros(shape=([some tf tensor])) in random.py (Exploration)
        # Fix: install numpy 1.19.5 (this seems to be a numpy problem).

        "num_gpus": 0.25,
        "num_workers": 1,
        "num_envs_per_worker": 1,
        "lr": tune.grid_search([0.005, 0.003, 0.001, 0.0001]),
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
