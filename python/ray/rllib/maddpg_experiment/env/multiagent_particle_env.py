from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete, Box, MultiDiscrete
from ray import rllib
from make_env import make_env

import numpy as np
import time


class RLlibMultiAgentParticleEnv(rllib.MultiAgentEnv):
    """Wraps OpenAI Multi-Agent Particle env to be compatible with RLLib multi-agent."""

    def __init__(self, **mpe_args):
        """Create a new Multi-Agent Particle env compatible with RLlib.

        Arguments:
            mpe_args (dict): Arguments to pass to the underlying
                make_env.make_env instance.

        Examples:
            >>> from rllib_env import RLlibMultiAgentParticleEnv
            >>> env = RLlibMultiAgentParticleEnv(scenario_name="simple_reference")
            >>> print(env.reset())
        """

        self._env = make_env(**mpe_args)
        self.num_agents = self._env.n
        self.agent_ids = list(range(self.num_agents))

        self.observation_space_dict = self._make_dict(self._env.observation_space)
        self.action_space_dict = self._make_dict(self._env.action_space)

    def reset(self):
        """Resets the env and returns observations from ready agents.

        Returns:
            obs_dict: New observations for each ready agent.
        """

        obs_dict = self._make_dict(self._env.reset())
        return obs_dict

    def step(self, action_dict):
        """Returns observations from ready agents.

        The returns are dicts mapping from agent_id strings to values. The
        number of agents in the env can vary over time.

        Returns:
            obs_dict:
                New observations for each ready agent.
            rew_dict:
                Reward values for each ready agent.
            done_dict:
                Done values for each ready agent.
                The special key "__all__" (required) is used to indicate env termination.
            info_dict:
                Optional info values for each agent id.
        """

        actions = list(action_dict.values())
        obs_list, rew_list, done_list, info_list = self._env.step(actions)

        obs_dict = self._make_dict(obs_list)
        rew_dict = self._make_dict(rew_list)
        done_dict = self._make_dict(done_list)
        done_dict["__all__"] = all(done_list)
        # FIXME: Currently, this is the best option to transfer agent-wise termination signal without touching RLlib code hugely.
        # FIXME: Hopefully, this will be solved in the future.
        info_dict = self._make_dict([{"done": done} for done in done_list])

        return obs_dict, rew_dict, done_dict, info_dict

    def render(self, mode='human'):
        time.sleep(0.05)
        self._env.render(mode=mode)

    def _make_dict(self, values):
        return dict(zip(self.agent_ids, values))


if __name__ == '__main__':
    for scenario_name in ["simple",
                          "simple_adversary",
                          "simple_crypto",
                          "simple_push",
                          "simple_reference",
                          "simple_speaker_listener",
                          "simple_spread",
                          "simple_tag",
                          "simple_world_comm"]:
        print("scenario_name: ", scenario_name)
        env = RLlibMultiAgentParticleEnv(scenario_name=scenario_name)
        print("obs: ", env.reset())
        print(env.observation_space_dict)
        print(env.action_space_dict)

        action_dict = {}
        for i, ac_space in env.action_space_dict.items():
            sample = ac_space.sample()
            if isinstance(ac_space, Discrete):
                action_dict[i] = np.zeros(ac_space.n)
                action_dict[i][sample] = 1.0
            elif isinstance(ac_space, Box):
                action_dict[i] = sample
            elif isinstance(ac_space, MultiDiscrete):
                print("sample: ", sample)
                print("ac_space: ", ac_space.nvec)
                action_dict[i] = np.zeros(sum(ac_space.nvec))
                start_ls = np.cumsum([0] + list(ac_space.nvec))[:-1]
                for l in list(start_ls + sample):
                    action_dict[i][l] = 1.0
            else:
                raise NotImplementedError

        print("action_dict: ", action_dict)

        for i in env.step(action_dict):
            print(i)
