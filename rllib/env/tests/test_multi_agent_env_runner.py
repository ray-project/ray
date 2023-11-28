import gymnasium as gym
import numpy as np
import unittest

import ray

from typing import Dict, Optional

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.tests.test_multi_agent_episode import MultiAgentTestEnv
from ray.rllib.policy.policy import PolicySpec


class MultiAgentTestEnvWithBox(MultiAgentTestEnv):
    def __init__(self, truncate: bool = False):
        super().__init__(truncate=truncate)
        self.observation_space = gym.spaces.Box(0, 201, shape=(1,), dtype=np.float32)

    def reset(self, *, seed: Optional[int] = None, options: Optional[Dict] = None):
        obs, info = super().reset(seed=seed, options=options)

        return {
            agent_id: np.array([agent_obs], dtype=np.float32)
            for agent_id, agent_obs in obs.items()
        }, info

    def step(self, action):
        obs, reward, is_terminated, is_truncated, info = super().step(action=action)

        return (
            {
                agent_id: np.array([agent_obs], dtype=np.float32)
                for agent_id, agent_obs in obs.items()
            },
            reward,
            is_terminated,
            is_truncated,
            info,
        )


class TestMultiAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(self) -> None:
        ray.shutdown()

    def test_init(self):
        # Build a multi agent config.
        config = self._build_config()
        # Create a `MultiAgentEnvRunner`.
        env_runner = MultiAgentEnvRunner(config=config)

        # Create an environment and compare the agent ids.
        env = MultiAgentTestEnvWithBox()
        self.assertEqual(env.get_agent_ids(), env_runner.agent_ids)

        # Next test that when using a single agent environment an
        # exception is thrown.
        config.environment("CartPole-v1")
        with self.assertRaises(AssertionError):
            env_runner = MultiAgentEnvRunner(config=config)

    def _build_config(self):
        # Create an environment to retrieve the agent ids.
        env = MultiAgentTestEnvWithBox()

        # Generate the `policy_dict` for the multi-agent setup and
        # use `PPO` for each agent.
        # TODO (sven, simon): Setup is still for `Policy`, change as soon
        # as we have switched fully to the new stack.
        multi_agent_policies = {}
        for agent_id in env.get_agent_ids():
            multi_agent_policies[agent_id] = PolicySpec(config=PPOConfig())

        # Build the configuration and use `PPO`.
        config = (
            PPOConfig()
            .environment(MultiAgentTestEnvWithBox)
            .framework(framework="torch")
            .experimental(_enable_new_api_stack=True)
            .multi_agent(policies=multi_agent_policies)
            .training(num_sgd_iter=10)
        )

        return config


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
