"""Example of a custom gym environment. Run this for a demo."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import gym
import gym_maze

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env


if __name__ == "__main__":

    def create_env(config):
        import os
        import gym
        import gym_maze

        os.environ["SDL_VIDEODRIVER"] = "dummy"

        return gym.make("maze-random-10x10-plus-v0")

#    register_env("maze-random-10x10-plus-v0", lambda config: gym.make("maze-random-10x10-plus-v0"))
    register_env("maze-random-10x10-plus-v0", create_env)
    ray.init()
    run_experiments({
        "without-rnd-test": {
            "run": "IMPALA",
            "stop": {
                "episode_reward_mean": 1
            },
            "env": "maze-random-10x10-plus-v0",
            "config": {
                "sample_batch_size": 50,
                "train_batch_size": 500,
                "num_workers": 1,
                "num_envs_per_worker": 1,
                "rnd": 1
            },
        },
    })

