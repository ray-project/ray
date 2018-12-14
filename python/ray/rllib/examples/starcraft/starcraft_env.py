from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym.spaces import Discrete, Box, Dict
import os
import random
import sys
import yaml

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.tune.registry import register_env


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
        obs_size = self._starcraft_env.get_obs_size()
        num_actions = self._starcraft_env.get_total_actions()
        # self.obs_space = Dict({
        #     "action_mask": Box(0, 1, shape=(num_actions,)),
        #     "real_obs": Box(-1, 1, shape=(obs_size,))
        # })
        self.observation_space = Box(-1, 1, shape=(obs_size,))
        self.action_space = Discrete(self._starcraft_env.get_total_actions())

    def reset(self):
        obs_list, state_list = self._starcraft_env.reset()
        return dict(enumerate(obs_list))

    def step(self, action_dict):
        # TODO(rliaw): Check to handle missing agents, if any
        actions = [action_dict[k] for k in sorted(action_dict)]
        rew, done, info = self._starcraft_env.step(actions)
        obs_list = self._starcraft_env.get_obs()
        # return_obs = {}
        # for i, obs in enumerate(obs_list):
        #     return_obs[i] = {
        #         "action_mask": self._starcraft_env.get_avail_agent_actions(i),

        #     }
        obs = dict(enumerate(obs_list))
        rews = {i: rew for i in range(len(obs_list))}
        dones = {i: done for i in range(len(obs_list))}
        dones["__all__"] = done
        infos = {i: info for i in range(len(obs_list))}
        return obs, rews, dones, infos


if __name__ == "__main__":
    path_to_pymarl = "/data/rliaw/pymarl/"
    os.environ.setdefault("PYMARL_PATH", path_to_pymarl)
    os.environ["SC2PATH"] = os.path.join(path_to_pymarl,
                                         "3rdparty/StarCraftII")
    from ray.rllib.agents.pg import PGAgent
    ray.init()
    register_env("starcraft", lambda _: SC2MultiAgentEnv())
    agent = PGAgent(env="starcraft")
    for i in range(100):
        agent.train()

    # env = SC2MultiAgentEnv()
    # x = env.reset()
    # returns = env.step({i: 0 for i in range(len(x))})
