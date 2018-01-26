"""Example of a custom gym environment. Run this for a demo."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym.spaces import Discrete, Box
from gym.envs.registration import EnvSpec

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env


class SimpleCorridor(gym.Env):
    """Example of a custom env in which you have to walk down a corridor.

    You can configure the length of the corridor via the env config."""

    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, self.end_pos, shape=(1,))
        self._spec = EnvSpec("SimpleCorridor-{}-v0".format(self.end_pos))

    def _reset(self):
        self.cur_pos = 0
        return [self.cur_pos]

    def _step(self, action):
        assert action in [0, 1]
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        elif action == 1:
            self.cur_pos += 1
        done = self.cur_pos >= self.end_pos
        return [self.cur_pos], 1 if done else 0, done, {}


if __name__ == "__main__":
    env_creator_name = "corridor"
    register_env(env_creator_name, lambda config: SimpleCorridor(config))
    ray.init()
    run_experiments({
        "demo": {
            "run": "PPO",
            "env": "corridor",
            "config": {
                "env_config": {
                    "corridor_length": 5,
                },
            },
        },
    })
