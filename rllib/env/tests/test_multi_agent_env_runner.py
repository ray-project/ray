import gymnasium as gym
import numpy as np
import tree
import unittest

import ray

from typing import Any, Dict, Optional

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.tests.test_multi_agent_episode import MultiAgentTestEnv
from ray.rllib.policy.policy import PolicySpec


class MultiAgentTestEnvWithBox(MultiAgentTestEnv):
    def __init__(self, env_config: Dict[str, Any]):
        # We generally want to truncate at timestep 200.
        super().__init__(truncate=env_config.get("truncate", True))

        # The preferred format for `MultiAgentEnv` spaces is a dictionary
        # mapping agent ids to their spaces. The alternative is to use
        # the same space for each agent and only set this single agent
        # space.
        with_preferred_format = env_config.get("with_preferred_format", False)

        # If we want the preferred format we need to set it here.
        if with_preferred_format:
            self.action_space = gym.spaces.Dict(
                {agent_id: gym.spaces.Discrete(200) for agent_id in self._agent_ids}
            )
            self.observation_space = gym.spaces.Dict(
                {
                    agent_id: gym.spaces.Box(0, 201, shape=(1,), dtype=np.float32)
                    for agent_id in self._agent_ids
                }
            )
            # Set this to `True` for the assertion in the
            # `MultiAgentEnvRunner.__init__()`.
            self._action_space_in_preferred_format = True
            self._obs_space_in_preferred_format = True
        # In the other case, set only the observation space to `gym.spaces.Box` as
        # `RLlib` has at this time no encoder for `gym.spaces.Discrete`.
        else:
            self.action_space = self.action_space["agent_0"]
            self.observation_space = gym.spaces.Box(
                0, 201, shape=(1,), dtype=np.float32
            )
            self._action_space_in_preferred_format = False
            self._obs_space_in_preferred_format = False

    def reset(self, *, seed: Optional[int] = None, options: Optional[Dict] = None):
        obs, info = super().reset(seed=seed, options=options)

        # `gym.spaces.Box` encoder expects `numpy.array` for observations.
        return tree.map_structure(lambda s: np.array([s]), obs), info

    def step(self, action):
        obs, reward, is_terminated, is_truncated, info = super().step(action)

        # `gym.spaces.Box` encoder expects `numpy.array` for observations.
        return (
            tree.map_structure(lambda s: np.array([s]), obs),
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
        env = MultiAgentTestEnv()
        self.assertEqual(env.get_agent_ids(), env_runner.agent_ids)

        # Next test that when using a single agent environment an
        # exception is thrown.
        config.environment("CartPole-v1")
        # Notge, due to the `deep_update` use in `AlgorithmConfig` we can
        # not reset the `env_config` in `ALgorithmConfig.environment()`.
        config.env_config = {}
        with self.assertRaises(AssertionError):
            env_runner = MultiAgentEnvRunner(config=config)

        # Now assert that initialization also works for `MulitAgentEnv`s
        # with preferred space formats, i.e. `gym.spaces.Dict`.
        config = self._build_config(with_preferred_format=True)
        # Create an `MultiAgentEnvRunner`.
        env_runner = MultiAgentEnvRunner(config=config)

    def test_sample_timesteps(self):
        # Build a multi agent config.
        config = self._build_config()
        # Create a `MultiAgentEnvRunner` instance.
        env_runner = MultiAgentEnvRunner(config=config)

        # Now sample 10 timesteps.
        episodes = env_runner.sample(num_timesteps=10)
        # Assert that we have 10 timesteps sampled.
        self.assertEqual(sum(len(episode) for episode in episodes), 10)

        # Now sample 200 timesteps.
        episodes = env_runner.sample(num_timesteps=200)
        # Ensure that two episodes are returned.
        # Note, after 200 timesteps the test environment truncates.
        self.assertEqual(len(episodes), 2)
        # Also ensure that the first episode was truncated.
        self.assertTrue(episodes[0].is_truncated)
        # Assert that indeed 200 timesteps were sampled.
        self.assertEqual(len(episodes[0]) + len(episodes[1]), 200)
        # Assert that the timesteps however in the episodes are 210.
        # Note, the first episode started at `t_started=10`.
        self.assertEqual(episodes[0].t + episodes[1].t, 210)
        # Assert that all agents extra model outputs are recorded.
        for agent_eps in episodes[0].agent_episodes.values():
            self.assertTrue("action_logp" in agent_eps.extra_model_outputs)
            self.assertEqual(
                len(agent_eps.actions),
                len(agent_eps.extra_model_outputs["action_logp"]),
            )
            self.assertEqual(
                len(agent_eps.actions),
                len(agent_eps.extra_model_outputs["action_dist_inputs"]),
            )

    def test_sample_episodes(self):
        # Build a multi agent config.
        config = self._build_config()
        # Create a `MultiAgentEnvRunner` instance.
        env_runner = MultiAgentEnvRunner(config=config)

        # Now sample 5 episodes.
        episodes = env_runner.sample(num_episodes=5)
        # Assert that we have 5 episodes sampled.
        self.assertEqual(len(episodes), 5)
        # Also assert that the episodes are indeed truncated.
        self.assertTrue(all(eps.is_truncated for eps in episodes))
        # Assert then that all episodes have 200 steps.
        for eps in episodes:
            self.assertEqual(eps.t, 200)
            # Assert that all agents have the extra model outputs.
            for agent_eps in eps.agent_episodes.values():
                self.assertTrue("action_logp" in agent_eps.extra_model_outputs)
                self.assertEqual(
                    len(agent_eps.actions),
                    len(agent_eps.extra_model_outputs["action_logp"]),
                )
                self.assertEqual(
                    len(agent_eps.actions),
                    len(agent_eps.extra_model_outputs["action_dist_inputs"]),
                )

        # Now sample 10 timesteps and then 1 episode.
        episodes = env_runner.sample(num_timesteps=10)
        episodes += env_runner.sample(num_episodes=1)
        # Ensure that the episodes both start at zero.
        for eps in episodes:
            self.assertEqual(eps.t_started, 0)

        # Now sample 1 episode and then 10 timesteps.
        episodes = env_runner.sample(num_episodes=1)
        episodes += env_runner.sample(num_timesteps=10)
        # Assert that in both cases we start at zero.
        for eps in episodes:
            self.assertEqual(eps.t_started, 0)

    def _build_config(self, with_preferred_format: bool = False):
        # Create an environment to retrieve the agent ids.
        env = MultiAgentTestEnvWithBox({"with_preferred_format": with_preferred_format})

        # Generate the `policy_dict` for the multi-agent setup and
        # use `PPO` for each agent.
        # TODO (sven, simon): Setup is still for `Policy`, change as soon
        #  as we have switched fully to the new stack.
        multi_agent_policies = {}
        for agent_id in env.get_agent_ids():
            multi_agent_policies[agent_id] = PolicySpec(config=PPOConfig())

        # Build the configuration and use `PPO`.
        config = (
            PPOConfig()
            .environment(
                MultiAgentTestEnvWithBox,
                # with_preferred_format=with_preferred_format,
                env_config={"with_preferred_format": with_preferred_format},
            )
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
