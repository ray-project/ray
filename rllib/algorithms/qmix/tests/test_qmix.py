from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import os
import unittest

import ray
from ray.tune import register_env
from ray.rllib.algorithms.qmix import QMixConfig
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class AvailActionsTestEnv(MultiAgentEnv):
    num_actions = 10
    action_space = Discrete(num_actions)
    observation_space = Dict(
        {
            "obs": Dict(
                {
                    "test": Dict({"a": Discrete(2), "b": MultiDiscrete([2, 3, 4])}),
                    "state": MultiDiscrete([2, 2, 2]),
                }
            ),
            "action_mask": Box(0, 1, (num_actions,)),
        }
    )

    def __init__(self, env_config):
        super().__init__()
        self.state = None
        self.avail = env_config.get("avail_actions", [3])
        self.action_mask = np.array([0] * 10)
        for a in self.avail:
            self.action_mask[a] = 1

    def reset(self, *, seed=None, options=None):
        self.state = 0
        return {
            "agent_1": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
            "agent_2": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
        }, {}

    def step(self, action_dict):
        if self.state > 0:
            assert (
                action_dict["agent_1"] in self.avail
                and action_dict["agent_2"] in self.avail
            ), "Failed to obey available actions mask!"
        self.state += 1
        rewards = {"agent_1": 1, "agent_2": 0.5}
        obs = {
            "agent_1": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
            "agent_2": {
                "obs": self.observation_space["obs"].sample(),
                "action_mask": self.action_mask,
            },
        }
        terminateds = {"__all__": False}
        truncateds = {"__all__": self.state >= 20}
        return obs, rewards, terminateds, truncateds, {}


class TestQMix(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_avail_actions_qmix(self):
        grouping = {
            "group_1": ["agent_1", "agent_2"],
        }
        obs_space = Tuple(
            [
                AvailActionsTestEnv.observation_space,
                AvailActionsTestEnv.observation_space,
            ]
        )
        act_space = Tuple(
            [AvailActionsTestEnv.action_space, AvailActionsTestEnv.action_space]
        )
        register_env(
            "action_mask_test",
            lambda config: AvailActionsTestEnv(config).with_agent_groups(
                grouping, obs_space=obs_space, act_space=act_space
            ),
        )

        config = (
            QMixConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .framework(framework="torch")
            .environment(
                env="action_mask_test",
                env_config={"avail_actions": [3, 4, 8]},
            )
            .rollouts(num_envs_per_worker=5)
        )  # Test with vectorization on.

        algo = config.build()

        for _ in range(4):
            algo.train()  # OK if it doesn't trip the action assertion error

        assert algo.train()["episode_reward_mean"] == 30.0
        algo.stop()
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
