"""Example of a custom gym environment. Run this for a demo."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import gym
from gym.spaces import Discrete, Box
from gym.envs.registration import EnvSpec

import ray
from ray.tune import run_experiments


if __name__ == "__main__":
    ray.init()
    run_experiments({
        "without-rnd-test": {
            "run": "IMPALA",
            "stop": {
                "episode_reward_mean": 1
            },
            "env": "maze-random-10x10-plus-v0",
            "config": {
                "env_config": {
                    "corridor_length": 1000,
                },
                "sample_batch_size": 50,
                "train_batch_size": 500,
                "num_workers": 1,
                "num_envs_per_worker": 10,
                "rnd": 1
            },
        },
    })

