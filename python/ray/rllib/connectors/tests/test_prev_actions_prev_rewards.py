import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.env_to_module import (
    EnvToModulePipeline,
    FlattenObservations,
    PrevActionsPrevRewards,
)
from ray.rllib.env.single_agent_episode import SingleAgentEpisode


class TestPrevActionsPrevRewards(unittest.TestCase):
    def _run(self, action_space, n_prev_actions, num_steps=4):
        obs_space = gym.spaces.Box(-1.0, 1.0, (3,), np.float32)
        pipeline = EnvToModulePipeline(
            input_observation_space=obs_space,
            input_action_space=action_space,
            connectors=[
                PrevActionsPrevRewards(n_prev_actions=n_prev_actions),
                FlattenObservations(),
            ],
        )
        episode = SingleAgentEpisode(
            observations=[obs_space.sample()],
            observation_space=obs_space,
            action_space=action_space,
        )
        for _ in range(num_steps):
            episode.add_env_step(obs_space.sample(), action_space.sample(), 1.0)
        pipeline(rl_module=None, batch={}, episodes=[episode], explore=True)
        # The connectors write the augmented and flattened obs back to the episode.
        # Its layout is: original obs (3), previous actions, previous reward (1).
        obs = episode.get_observations(-1)
        self.assertEqual(obs.shape, pipeline.observation_space.shape)
        self.assertTrue(pipeline.observation_space.contains(obs))
        return obs, episode

    def test_discrete_actions(self):
        obs, _ = self._run(gym.spaces.Discrete(4), n_prev_actions=2)
        # 3 (orig obs) + 2 * 4 (one-hot prev actions) + 1 (prev reward).
        self.assertEqual(obs.shape, (12,))

    def test_multi_discrete_actions(self):
        obs, episode = self._run(gym.spaces.MultiDiscrete([3, 2]), n_prev_actions=2)
        # 3 (orig obs) + 2 * (3 + 2) (one-hot prev actions) + 1 (prev reward).
        self.assertEqual(obs.shape, (14,))
        # Each previous action is one-hot'd per component.
        expected = []
        for action in episode.get_actions(slice(-2, None)):
            expected += [np.eye(3)[action[0]], np.eye(2)[action[1]]]
        np.testing.assert_allclose(obs[3:13], np.concatenate(expected))

    def test_box_actions(self):
        obs, episode = self._run(
            gym.spaces.Box(-2.0, 2.0, (2,), np.float32), n_prev_actions=3
        )
        # 3 (orig obs) + 3 * 2 (prev actions) + 1 (prev reward).
        self.assertEqual(obs.shape, (10,))
        # The previous actions are the episode's last 3 actions, flattened.
        np.testing.assert_allclose(
            obs[3:9], np.concatenate(episode.get_actions(slice(-3, None)))
        )

    def test_box_actions_at_episode_start(self):
        # Fewer past actions than `n_prev_actions`: the rest are zero-filled.
        obs, episode = self._run(
            gym.spaces.Box(1.0, 2.0, (2,), np.float32), n_prev_actions=3, num_steps=1
        )
        self.assertEqual(obs.shape, (10,))
        np.testing.assert_allclose(obs[3:7], np.zeros(4))
        np.testing.assert_allclose(obs[7:9], episode.get_actions(-1))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
