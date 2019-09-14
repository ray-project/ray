from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from gym.spaces import Tuple, Discrete, Dict, Box

import ray
from ray.tune import register_env
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.agents.qmix import QMixTrainer


class AvailActionsTestEnv(MultiAgentEnv):
    action_space = Discrete(10)
    observation_space = Dict({
        "obs": Discrete(3),
        "action_mask": Box(0, 1, (10, )),
    })

    def __init__(self, env_config):
        self.state = None
        self.avail = env_config["avail_action"]
        self.action_mask = np.array([0] * 10)
        self.action_mask[env_config["avail_action"]] = 1

    def reset(self):
        self.state = 0
        return {
            "agent_1": {
                "obs": self.state,
                "action_mask": self.action_mask
            }
        }

    def step(self, action_dict):
        if self.state > 0:
            assert action_dict["agent_1"] == self.avail, \
                "Failed to obey available actions mask!"
        self.state += 1
        rewards = {"agent_1": 1}
        obs = {"agent_1": {"obs": 0, "action_mask": self.action_mask}}
        dones = {"__all__": self.state > 20}
        return obs, rewards, dones, {}


if __name__ == "__main__":
    grouping = {
        "group_1": ["agent_1"],  # trivial grouping for testing
    }
    obs_space = Tuple([AvailActionsTestEnv.observation_space])
    act_space = Tuple([AvailActionsTestEnv.action_space])
    register_env(
        "action_mask_test",
        lambda config: AvailActionsTestEnv(config).with_agent_groups(
            grouping, obs_space=obs_space, act_space=act_space))

    ray.init()
    agent = QMixTrainer(
        env="action_mask_test",
        config={
            "num_envs_per_worker": 5,  # test with vectorization on
            "env_config": {
                "avail_action": 3,
            },
        })
    for _ in range(5):
        agent.train()  # OK if it doesn't trip the action assertion error
    assert agent.train()["episode_reward_mean"] == 21.0
