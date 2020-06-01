from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import unittest

import ray
from ray.tune import register_env
from ray.rllib.agents.qmix import QMixTrainer
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class AvailActionsTestEnv(MultiAgentEnv):
    num_actions = 10
    action_space = Discrete(num_actions)
    observation_space = Dict({
        "obs": Dict({
            "test": Dict({
                "a": Discrete(2),
                "b": MultiDiscrete([2, 3, 4])
            }),
            "state": MultiDiscrete([2, 2, 2])
        }),
        "action_mask": Box(0, 1, (num_actions, )),
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
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask
            }
        }

    def step(self, action_dict):
        if self.state > 0:
            assert action_dict["agent_1"] == self.avail, \
                "Failed to obey available actions mask!"
        self.state += 1
        rewards = {"agent_1": 1}
        obs = {
            "agent_1": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask
            }
        }
        dones = {"__all__": self.state > 20}
        return obs, rewards, dones, {}


class TestAvailActionsQMix(unittest.TestCase):
    def test_avail_actions_qmix(self):
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
                "framework": "torch",
            })
        for _ in range(5):
            agent.train()  # OK if it doesn't trip the action assertion error
        assert agent.train()["episode_reward_mean"] == 21.0


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
