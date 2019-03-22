"""Example of a custom gym environment and model. Run this for a demo.

This example shows:
  - using a custom environment
  - using a custom model
  - using Tune for grid search

You can visualize experiment results in ~/ray_results using TensorBoard.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import gym
from ray.rllib.models import FullyConnectedNetwork, Model, ModelCatalog
from gym.spaces import Discrete, Box

import ray
from ray.tune import run_experiments, grid_search


class SimpleCorridor(gym.Env):
    """Example of a custom env in which you have to walk down a corridor.

    You can configure the length of the corridor via the env config."""

    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(
            0.0, self.end_pos, shape=(1, ), dtype=np.float32)

    def reset(self):
        self.cur_pos = 0
        return [self.cur_pos]

    def step(self, action):
        assert action in [0, 1], action
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        elif action == 1:
            self.cur_pos += 1
        done = self.cur_pos >= self.end_pos
        return [self.cur_pos], 1 if done else 0, done, {}


class CustomModel(Model):
    """Example of a custom model.

    This model just delegates to the built-in fcnet.
    """

    def _build_layers_v2(self, input_dict, num_outputs, options):
        self.obs_in = input_dict["obs"]
        self.fcnet = FullyConnectedNetwork(input_dict, self.obs_space,
                                           self.action_space, num_outputs,
                                           options)
        return self.fcnet.outputs, self.fcnet.last_layer


if __name__ == "__main__":
    # Can also register the env creator function explicitly with:
    # register_env("corridor", lambda config: SimpleCorridor(config))
    ray.init()
    ModelCatalog.register_custom_model("my_model", CustomModel)
    run_experiments({
        "demo": {
            "run": "PPO",
            "env": SimpleCorridor,  # or "corridor" if registered above
            "stop": {
                "timesteps_total": 10000,
            },
            "config": {
                "model": {
                    "custom_model": "my_model",
                },
                "lr": grid_search([1e-2, 1e-4, 1e-6]),  # try different lrs
                "num_workers": 1,  # parallelism
                "env_config": {
                    "corridor_length": 5,
                },
            },
        },
    })
