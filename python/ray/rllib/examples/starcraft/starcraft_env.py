from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import random
import unittest

from ray.rllib.env.multi_agent_env import MultiAgentEnv



class BasicMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    def __init__(self, num):
        self._starcraft_env = pymarl.MultiAgentEnv()

    def reset(self):
        self._starcraft_env.reset()

    def step(self, action_dict):
        actions = process_action_dict(action_dict)
        rew, done, info = self._starcraft_env.step(actions)
        obs = self._starcraft_env.get_obs()
        return obs, rew, done, info

