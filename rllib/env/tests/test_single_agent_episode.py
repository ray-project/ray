import gymnasium as gym
import numpy as np
import unittest

from gymnasium.core import ActType, ObsType
from typing import Any, Dict, Optional, SupportsFloat, Tuple

import ray
from ray.rllib.env.single_agent_episode import SingleAgentEpisode

# TODO (simon): Add to the tests `info` and `extra_model_outputs`
# as soon as #39732 is merged.


class TestEnv(gym.Env):
    def __init__(self):
        self.observation_space = gym.spaces.Discrete(201)
        self.action_space = gym.spaces.Discrete(200)
        self.t = 0

    def reset(
        self, *, seed: Optional[int] = None, options=Optional[Dict[str, Any]]
    ) -> Tuple[ObsType, Dict[str, Any]]:
        self.t = 0
        return 0, {}

    def step(
        self, action: ActType
    ) -> Tuple[ObsType, SupportsFloat, bool, bool, Dict[str, Any]]:
        self.t += 1
        if self.t == 200:
            is_terminated = True
        else:
            is_terminated = False

        return self.t, self.t, is_terminated, False, {}


class TestSingelAgentEpisode(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_init(self):
        # Create empty episode.
        episode = SingleAgentEpisode()
        # Empty episode should have a start point and count of zero.
        self.assertTrue(episode.t_started == episode.t == 0)

        # Create an episode with a specific starting point.
        episode = SingleAgentEpisode(t_started=10)
        self.assertTrue(episode.t == episode.t_started == 10)

        # Sample 100 values and initialize episode with observations.
        env = gym.make("CartPole-v1")
        observations = rewards = is_terminateds = is_truncateds = []
        init_obs, _ = env.reset()
        observations.append(init_obs)
        for _ in range(100):
            action = env.action_space.sample()
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            observations.append(obs)
            rewards.append(reward)
            is_terminateds.append(is_terminated)
            is_truncateds.append(is_truncated)

        episode = SingleAgentEpisode(
            observations=observations,
            rewards=rewards,
            is_terminated=is_terminateds,
            is_truncated=is_truncated,
        )
        # The starting point and count should now be at `len(observations) - 1`.
        self.assertTrue(episode.t == episode.t_started == (len(observations) - 1))

    def test_add_initial_observation(self):
        # Create empty episode.
        episode = SingleAgentEpisode()
        # Create environment.
        env = gym.make("CartPole-v1")

        # Add initial observations.
        obs, _ = env.reset()
        episode.add_initial_observation(initial_observation=obs)

        # Assert that the observations are added to list.
        self.assertTrue(len(episode.observations) == 1)
        # Assert that the timesteps are still at zero as we have not stepped, yet.
        self.assertTrue(episode.t == episode.t_started == 0)

    def test_add_timestep(self):
        # Set the random seed (otherwise the episode will terminate at
        # different points in each test run).
        np.random.seed(0)
        # Create an empty episode and add initial observations.
        episode = SingleAgentEpisode()
        env = gym.make("CartPole-v1")
        obs, _ = env.reset()
        episode.add_initial_observation(initial_observation=obs)

        # Sample 100 timesteps and add them to the episode.
        for i in range(100):
            action = env.action_space.sample()
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
            )
            if is_terminated:
                break

        # Assert that the episode timestep is at 100.
        self.assertTrue(episode.t == len(episode.observations) - 1 == i + 1)
        # Assert that `t_started` stayed at zero.
        self.assertTrue(episode.t_started == 0)
        # Assert that all lists have the proper lengths.
        self.assertTrue(
            len(episode.actions)
            == len(episode.rewards)
            == len(episode.observations) - 1
            == i + 1
        )
        # Assert that the flags are set correctly.
        self.assertTrue(episode.is_terminated == is_terminated)
        self.assertTrue(episode.is_truncated == is_truncated)
        self.assertTrue(episode.is_done == is_terminated or is_truncated)

    def test_create_successor(self):
        # Create an empty episode.
        episode_1 = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, _ = env.reset()
        episode_1.add_initial_observation(initial_observation=init_obs)
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            episode_1.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
            )

        # Assert that the episode has indeed 100 timesteps.
        self.assertTrue(episode_1.t == 100)

        # Create a successor.
        episode_2 = episode_1.create_successor()
        # Assert that it has the same id.
        self.assertTrue(episode_1.id_ == episode_2.id_)
        # Assert that the timestep starts at the end of the last episode.
        self.assertTrue(episode_1.t == episode_2.t == episode_2.t_started)
        # Assert that the last observation of `episode_1` is the first of
        # `episode_2`.
        self.assertTrue(episode_1.observations[-1] == episode_2.observations[0])

    def test_concat_episode(self):
        # Create two episodes and fill them with 100 timesteps each.
        env = TestEnv()
        init_obs, _ = env.reset()
        episode_1 = SingleAgentEpisode()
        episode_1.add_initial_observation(initial_observation=init_obs)
        # Sample 100 timesteps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            episode_1.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
            )

        # Create an successor.
        episode_2 = episode_1.create_successor()

        # Now, sample 100 more timesteps.
        for i in range(100, 200):
            action = i
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            episode_2.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
            )

        # Assert that the second episode's `t_started` is at the first episode's
        # `t`.
        self.assertTrue(episode_1.t == episode_2.t_started)
        # Assert that the second episode's `t` is at 200.
        self.assertTrue(episode_2.t == 200)

        # Concate the episodes.
        episode_1.concat_episode(episode_2)
        # Assert that the concatenated episode start at `t_started=0`
        # and has 200 sampled steps, i.e. `t=200`.
        self.assertTrue(episode_1.t_started == 0)
        self.assertTrue(episode_1.t == 200)
        # Assert that all lists have appropriate length.
        self.assertTrue(
            len(episode_1.actions)
            == len(episode_1.rewards)
            == len(episode_1.observations) - 1
            == 200
        )

    def test_get_and_from_state(self):
        # Create an empty episode.
        episode = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, _ = env.reset()
        episode.add_initial_observation(initial_observation=init_obs)
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
            )

        # Get the state and reproduce it from state.
        state = episode.get_state()
        episode_reproduced = SingleAgentEpisode.from_state(state)

        # Assert that the data is complete.
        self.assertEqual(episode.id_, episode_reproduced.id_)
        self.assertEqual(episode.t, episode_reproduced.t)
        self.assertEqual(episode.t_started, episode_reproduced.t_started)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertListEqual(episode.observations, episode_reproduced.observations)
        self.assertListEqual(episode.actions, episode_reproduced.actions)
        self.assertListEqual(episode.rewards, episode_reproduced.rewards)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertEqual(episode.states, episode_reproduced.states)
        self.assertListEqual(episode.render_images, episode_reproduced.render_images)

    def test_to_and_from_sample_batch(self):

        # Create an empty episode.
        episode = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, _ = env.reset()
        episode.add_initial_observation(initial_observation=init_obs)
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, is_terminated, is_truncated, _ = env.step(action)
            episode.add_timestep(
                observation=obs,
                action=action,
                reward=reward,
                is_terminated=is_terminated,
                is_truncated=is_truncated,
            )

        # Create `SampleBatch`.
        batch = episode.to_sample_batch()
        # Reproduce form `SampleBatch`.
        episode_reproduced = SingleAgentEpisode.from_sample_batch(batch)
        # Assert that the data is complete.
        self.assertEqual(episode.id_, episode_reproduced.id_)
        self.assertEqual(episode.t, episode_reproduced.t)
        self.assertEqual(episode.t_started, episode_reproduced.t_started)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertListEqual(episode.observations, episode_reproduced.observations)
        self.assertListEqual(episode.actions, episode_reproduced.actions)
        self.assertListEqual(episode.rewards, episode_reproduced.rewards)
        self.assertEqual(episode.is_terminated, episode_reproduced.is_terminated)
        self.assertEqual(episode.is_truncated, episode_reproduced.is_truncated)
        self.assertEqual(episode.states, episode_reproduced.states)
        self.assertListEqual(episode.render_images, episode_reproduced.render_images)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
