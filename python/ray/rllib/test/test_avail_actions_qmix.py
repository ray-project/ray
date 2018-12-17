from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Tuple, Discrete

import ray
from ray.tune import register_env
from ray.rllib.env.constants import AVAIL_ACTIONS_KEY
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.agents.qmix import QMixAgent


class AvailActionsTestEnv(MultiAgentEnv):
    def __init__(self, env_config):
        self.state = None
        self.avail = env_config["avail_action"]
        self.avail_actions = [0] * 10
        self.avail_actions[env_config["avail_action"]] = 1
        self.action_space = Discrete(10)
        self.observation_space = Discrete(3)

    def reset(self):
        self.state = 0
        return {"agent_1": self.state}

    def step(self, action_dict):
        if self.state > 0:
            assert action_dict["agent_1"] == self.avail, \
                "Failed to obey available actions mask!"
        self.state += 1
        rewards = {"agent_1": 1}
        obs = {"agent_1": 0}
        dones = {"__all__": self.state > 20}
        infos = {
            "agent_1": {
                AVAIL_ACTIONS_KEY: self.avail_actions
            },
        }
        return obs, rewards, dones, infos


if __name__ == "__main__":
    grouping = {
        "group_1": ["agent_1"],  # trivial grouping for testing
    }
    obs_space = Tuple([Discrete(3)])
    act_space = Tuple([Discrete(10)])
    register_env(
        "avail_actions_test",
        lambda config: AvailActionsTestEnv(config).with_agent_groups(
            grouping, obs_space=obs_space, act_space=act_space))

    ray.init()
    agent = QMixAgent(
        env="avail_actions_test",
        config={
            "num_envs_per_worker": 5,  # test with vectorization on
            "env_config": {
                "avail_action": 3,
            },
        })
    for _ in range(5):
        agent.train()  # OK if it doesn't trip the action assertion error
    assert agent.train()["episode_reward_mean"] == 21.0
