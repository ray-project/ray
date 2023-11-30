from collections import defaultdict
from typing import Any, Dict, Optional, SupportsFloat, Tuple
import unittest

import gymnasium as gym
from gymnasium.core import ActType, ObsType
import numpy as np

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.test_utils import check

# TODO (simon): Add to the tests `info` and `extra_model_outputs`
#  as soon as #39732 is merged.


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
    def test_init(self):
        """Tests initialization of `SingleAgentEpisode`.

        Three cases are tested:
            1. Empty episode with default starting timestep.
            2. Empty episode starting at `t_started=10`. This is only interesting
                for ongoing episodes, where we do not want to carry on the stale
                entries from the last rollout.
            3. Initialization with pre-collected data.
        """
        # Create empty episode.
        episode = SingleAgentEpisode()
        # Empty episode should have a start point and count of zero.
        self.assertTrue(episode.t_started == episode.t == 0)

        # Create an episode with a specific starting point.
        episode = SingleAgentEpisode(t_started=10)
        self.assertTrue(episode.t == episode.t_started == 10)

        episode = self._create_episode(num_data=100)
        # The starting point and count should now be at `len(observations) - 1`.
        self.assertTrue(len(episode) == 100)
        self.assertTrue(episode.t == 100)
        self.assertTrue(episode.t_started == 0)

        # Build the same episode, but with a 10 ts lookback buffer.
        episode = self._create_episode(num_data=100, len_lookback_buffer=10)
        # The lookback buffer now takes 10 ts and the length of the episode is only 90.
        self.assertTrue(len(episode) == 90)
        # `t_started` is 0 by default.
        self.assertTrue(episode.t_started == 0)
        self.assertTrue(episode.t == 90)
        self.assertTrue(len(episode.rewards) == 90)
        self.assertTrue(len(episode.rewards.data) == 100)

        # Build the same episode, but with a 10 ts lookback buffer AND a specific
        # `t_started`.
        episode = self._create_episode(
            num_data=100, len_lookback_buffer=10, t_started=50
        )
        # The lookback buffer now takes 10 ts and the length of the episode is only 90.
        self.assertTrue(len(episode) == 90)
        self.assertTrue(episode.t_started == 50)
        self.assertTrue(episode.t == 140)
        self.assertTrue(len(episode.rewards) == 90)
        self.assertTrue(len(episode.rewards.data) == 100)

    def test_add_env_reset(self):
        """Tests adding initial observations and infos.

        This test ensures that when initial observation and info are provided
        the length of the lists are correct and the timestep is still at zero,
        as the agent has not stepped, yet.
        """
        # Create empty episode.
        episode = SingleAgentEpisode()
        # Create environment.
        env = gym.make("CartPole-v1")

        # Add initial observations.
        obs, info = env.reset()
        episode.add_env_reset(observation=obs, infos=info)

        # Assert that the observations are added to their list.
        self.assertTrue(len(episode.observations) == 1)
        # Assert that the infos are added to their list.
        self.assertTrue(len(episode.infos) == 1)
        # Assert that the timesteps are still at zero as we have not stepped, yet.
        self.assertTrue(episode.t == episode.t_started == 0)

    def test_add_env_step(self):
        """Tests if adding timestep data to a `SingleAgentEpisode` works.

        Adding timestep data is the central part of collecting episode
        dara. Here it is tested if adding to the internal data lists
        works as intended and the timestep is increased during each step.
        """
        # Create an empty episode and add initial observations.
        episode = SingleAgentEpisode()
        env = gym.make("CartPole-v1")
        # Set the random seed (otherwise the episode will terminate at
        # different points in each test run).
        obs, info = env.reset(seed=0)
        episode.add_env_reset(observation=obs, infos=info)

        # Sample 100 timesteps and add them to the episode.
        terminated = truncated = False
        for i in range(100):
            action = env.action_space.sample()
            obs, reward, terminated, truncated, info = env.step(action)
            episode.add_env_step(
                observation=obs,
                action=action,
                reward=reward,
                infos=info,
                terminated=terminated,
                truncated=truncated,
                extra_model_outputs={"extra": np.random.random(1)},
            )
            if terminated or truncated:
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
            == len(episode.infos) - 1
            == i + 1
        )
        # Assert that the flags are set correctly.
        self.assertTrue(episode.is_terminated == terminated)
        self.assertTrue(episode.is_truncated == truncated)
        self.assertTrue(episode.is_done == terminated or truncated)

    def test_cut(self):
        """Tests creation of a scucessor of a `SingleAgentEpisode`.

        This test makes sure that when creating a successor the successor's
        data is coherent with the episode that should be succeeded.
        Observation and info are available before each timestep; therefore
        these data is carried over to the successor.
        """

        # Create an empty episode.
        episode_1 = SingleAgentEpisode()
        # Create an environment.
        env = TestEnv()
        # Add initial observation.
        init_obs, init_info = env.reset()
        episode_1.add_env_reset(observation=init_obs, infos=init_info)
        # Sample 100 steps.
        for i in range(100):
            action = i
            obs, reward, terminated, truncated, info = env.step(action)
            episode_1.add_env_step(
                observation=obs,
                action=action,
                reward=reward,
                infos=info,
                terminated=terminated,
                truncated=truncated,
                extra_model_outputs={"extra": np.random.random(1)},
            )

        # Assert that the episode has indeed 100 timesteps.
        self.assertTrue(episode_1.t == 100)

        # Create a successor.
        episode_2 = episode_1.cut()
        # Assert that it has the same id.
        self.assertEqual(episode_1.id_, episode_2.id_)
        # Assert that the timestep starts at the end of the last episode.
        self.assertTrue(episode_1.t == episode_2.t == episode_2.t_started)
        # Assert that the last observation of `episode_1` is the first of
        # `episode_2`.
        self.assertTrue(episode_1.observations[-1] == episode_2.observations[0])
        # Assert that the last info of `episode_1` is the first of episode_2`.
        self.assertTrue(episode_1.infos[-1] == episode_2.infos[0])

        # Test immutability.
        action = 100
        obs, reward, terminated, truncated, info = env.step(action)
        episode_2.add_env_step(
            observation=obs,
            action=action,
            reward=reward,
            infos=info,
            terminated=terminated,
            truncated=truncated,
            extra_model_outputs={"extra": np.random.random(1)},
        )
        # Assert that this does not change also the predecessor's data.
        self.assertFalse(len(episode_1.observations) == len(episode_2.observations))

    def test_slices(self):
        # TEST #1: even split (50/50)
        episode = self._create_episode(100)
        self.assertTrue(episode.t == 100 and episode.t_started == 0)

        # Convert to numpy before splitting.
        episode.finalize()

        # Create two 50/50 episode chunks.
        e1 = episode[:50]
        self.assertTrue(e1.is_finalized)
        e2 = episode.slice(slice(50, None))
        self.assertTrue(e2.is_finalized)

        # Make sure, `e1` and `e2` make sense.
        self.assertTrue(len(e1) == 50)
        self.assertTrue(len(e2) == 50)
        self.assertTrue(e1.id_ == e2.id_)
        self.assertTrue(e1.t_started == 0)
        self.assertTrue(e1.t == 50)
        self.assertTrue(e2.t_started == 50)
        self.assertTrue(e2.t == 100)
        # Make sure the chunks are not identical, but last obs of `e1` matches
        # last obs of `e2`.
        check(e1.get_observations(-1), e2.get_observations(0))
        check(e1.observations[4], e2.observations[4], false=True)
        check(e1.observations[10], e2.observations[10], false=True)

        # TEST #2: Uneven split (33/66).
        episode = self._create_episode(99)
        self.assertTrue(episode.t == 99 and episode.t_started == 0)

        # Convert to numpy before splitting.
        episode.finalize()

        # Create two 50/50 episode chunks.
        e1 = episode.slice(slice(None, 33))
        self.assertTrue(e1.is_finalized)
        e2 = episode[33:]
        self.assertTrue(e2.is_finalized)

        # Make sure, `e1` and `e2` chunk make sense.
        self.assertTrue(len(e1) == 33)
        self.assertTrue(len(e2) == 66)
        self.assertTrue(e1.id_ == e2.id_)
        self.assertTrue(e1.t_started == 0)
        self.assertTrue(e1.t == 33)
        self.assertTrue(e2.t_started == 33)
        self.assertTrue(e2.t == 99)
        # Make sure the chunks are not identical, but last obs of `e1` matches
        # last obs of `e2`.
        check(e1.get_observations(-1), e2.get_observations(0))
        check(e1.observations[4], e2.observations[4], false=True)
        check(e1.observations[10], e2.observations[10], false=True)

        # TEST #3: Split with lookback buffer (buffer=10, split=20/30).
        episode = self._create_episode(
            num_data=60, t_started=15, len_lookback_buffer=10
        )
        self.assertTrue(episode.t == 65 and episode.t_started == 15)

        # Convert to numpy before splitting.
        episode.finalize()

        # Create two 20/30 episode chunks.
        e1 = episode.slice(slice(None, 20))
        self.assertTrue(e1.is_finalized)
        e2 = episode[20:]
        self.assertTrue(e2.is_finalized)

        # Make sure, `e1` and `e2` make sense.
        self.assertTrue(len(e1) == 20)
        self.assertTrue(len(e2) == 30)
        self.assertTrue(e1.id_ == e2.id_)
        self.assertTrue(e1.t_started == 15)
        self.assertTrue(e1.t == 35)
        self.assertTrue(e2.t_started == 35)
        self.assertTrue(e2.t == 65)
        # Make sure the chunks are not identical, but last obs of `e1` matches
        # last obs of `e2`.
        check(e1.get_observations(-1), e2.get_observations(0))
        check(e1.observations[5], e2.observations[5], false=True)
        check(e1.observations[11], e2.observations[11], false=True)
        # Make sure the lookback buffers of both chunks are still working.
        check(
            e1.get_observations(-1, neg_indices_left_of_zero=True),
            episode.observations.data[episode._len_lookback_buffer - 1],
        )
        check(
            e1.get_actions(-1, neg_indices_left_of_zero=True),
            episode.actions.data[episode._len_lookback_buffer - 1],
        )
        check(
            e2.get_observations([-5, -2], neg_indices_left_of_zero=True),
            [
                episode.observations.data[20 + episode._len_lookback_buffer - 5],
                episode.observations.data[20 + episode._len_lookback_buffer - 2],
            ],
        )
        check(
            e2.get_rewards([-5, -2], neg_indices_left_of_zero=True),
            [
                episode.rewards.data[20 + episode._len_lookback_buffer - 5],
                episode.rewards.data[20 + episode._len_lookback_buffer - 2],
            ],
        )

    def test_concat_episode(self):
        """Tests if concatenation of two `SingleAgentEpisode`s works.

        This test ensures that concatenation of two episodes work. Note that
        concatenation should only work for two chunks of the same episode, i.e.
        they have the same `id_` and one should be the successor of the other.
        It is also tested that concatenation fails, if timesteps do not match or
        the episode to which we want to concatenate is already terminated.
        """
        # Create two episodes and fill them with 100 timesteps each.
        env = TestEnv()
        init_obs, init_info = env.reset()
        episode_1 = SingleAgentEpisode()
        episode_1.add_env_reset(observation=init_obs, infos=init_info)
        # Sample 100 timesteps.
        for i in range(100):
            action = i
            obs, reward, terminated, truncated, info = env.step(action)
            episode_1.add_env_step(
                observation=obs,
                action=action,
                reward=reward,
                infos=info,
                terminated=terminated,
                truncated=truncated,
                extra_model_outputs={"extra": np.random.random(1)},
            )

        # Create a successor.
        episode_2 = episode_1.cut()

        # Now, sample 100 more timesteps.
        for i in range(100, 200):
            action = i
            obs, reward, terminated, truncated, info = env.step(action)
            episode_2.add_env_step(
                observation=obs,
                action=action,
                reward=reward,
                infos=info,
                terminated=terminated,
                truncated=truncated,
                extra_model_outputs={"extra": np.random.random(1)},
            )

        # Assert that the second episode's `t_started` is at the first episode's
        # `t`.
        self.assertTrue(episode_1.t == episode_2.t_started)
        # Assert that the second episode's `t` is at 200.
        self.assertTrue(episode_2.t == 200)

        # Manipulate the id of the second episode and make sure an error is
        # thrown during concatenation.
        episode_2.id_ = "wrong"
        with self.assertRaises(AssertionError):
            episode_1.concat_episode(episode_2)
        # Reset the id.
        episode_2.id_ = episode_1.id_
        # Assert that when timesteps do not match an error is thrown.
        episode_2.t_started += 1
        with self.assertRaises(AssertionError):
            episode_1.concat_episode(episode_2)
        # Reset the timestep.
        episode_2.t_started -= 1
        # Assert that when the first episode is already done no concatenation can take
        # place.
        episode_1.is_terminated = True
        with self.assertRaises(AssertionError):
            episode_1.concat_episode(episode_2)
        # Reset `is_terminated`.
        episode_1.is_terminated = False

        # Concatenate the episodes.

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
            == len(episode_1.infos) - 1
            == 200
        )
        # Assert that specific observations in the two episodes match.
        self.assertEqual(episode_2.observations[5], episode_1.observations[105])
        # Assert that they are not the same object.
        # TODO (sven): Do we really need a deepcopy here?
        # self.assertNotEqual(id(episode_2.observations[5]),
        # id(episode_1.observations[105]))

    def _create_episode(self, num_data, t_started=None, len_lookback_buffer=0):
        # Sample 100 values and initialize episode with observations and infos.
        env = gym.make("CartPole-v1")
        # Initialize containers.
        observations = []
        rewards = []
        actions = []
        infos = []
        extra_model_outputs = defaultdict(list)

        # Initialize observation and info.
        init_obs, init_info = env.reset()
        observations.append(init_obs)
        infos.append(init_info)
        # Run n samples.
        for _ in range(num_data):
            action = env.action_space.sample()
            obs, reward, _, _, info = env.step(action)
            observations.append(obs)
            actions.append(action)
            rewards.append(reward)
            infos.append(info)
            extra_model_outputs["extra_1"].append(np.random.random())
            extra_model_outputs["state_out"].append(np.random.random())

        return SingleAgentEpisode(
            observations=observations,
            infos=infos,
            actions=actions,
            rewards=rewards,
            extra_model_outputs=extra_model_outputs,
            t_started=t_started,
            len_lookback_buffer=len_lookback_buffer,
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
