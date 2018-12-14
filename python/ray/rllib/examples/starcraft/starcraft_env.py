from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import os
import random
import sys
import yaml

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
        obs_list, state_list = self._starcraft_env.reset()
        return dict(enumerate(obs_list))

    def step(self, action_dict):
        # TODO(rliaw): Check to handle missing agents, if any
        actions = [action_dict[k] for k in sorted(action_dict)]
        rew, done, info = self._starcraft_env.step(actions)
        obs_list = self._starcraft_env.get_obs()
        obs = dict(enumerate(obs_list))
        rews = {i: rew for i in range(len(obs_list))}
        dones = {i: done for i in range(len(obs_list))}
        infos = {i: info for i in range(len(obs_list))}
        return obs, rews, dones, infos


if __name__ == "__main__":
    path_to_pymarl = "/data/rliaw/pymarl/"
    os.environ.setdefault("PYMARL_PATH", path_to_pymarl)
    os.environ["SC2PATH"] = os.path.join(path_to_pymarl,
                                         "3rdparty/StarCraftII")
    env = SC2MultiAgentEnv()
    x = env.reset()
    returns = env.step({i: 0 for i in range(len(x))})
