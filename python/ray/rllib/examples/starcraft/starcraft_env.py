from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import os
import random
import sys
import yaml
from types import SimpleNamespace

from ray.rllib.env.multi_agent_env import MultiAgentEnv



class SC2MultiAgentEnv(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    def __init__(self):
        PYMARL_PATH = os.environ.get("PYMARL_PATH")
        sys.path.append(os.path.join(PYMARL_PATH, "src"))
        from envs.starcraft2 import StarCraft2Env
        with open(os.path.join(PYMARL_PATH, "src/config/envs/sc2.yaml")) as f:
            pymarl_args = yaml.load(f)
            # HACK
            pymarl_args["env_args"]["seed"] = 0
            
        self._starcraft_env = StarCraft2Env(**pymarl_args)

    def reset(self):
        self._starcraft_env.reset()

    def step(self, action_dict):
        actions = process_action_dict(action_dict)
        rew, done, info = self._starcraft_env.step(actions)
        obs = self._starcraft_env.get_obs()
        return obs, rew, done, info


if __name__ == "__main__":
    path_to_pymarl = "/data/rliaw/pymarl/"
    os.environ["PYMARL_PATH"] = path_to_pymarl
    os.environ["SC2PATH"] = os.path.join(path_to_pymarl, "3rdparty/StarCraftII")
    env = SC2MultiAgentEnv()
    env.reset()
