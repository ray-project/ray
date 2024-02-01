import unittest

import numpy as np
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.replay_buffers.prioritized_episode_replay_buffer import (
    PrioritizedEpisodeReplayBuffer,
)


class TestPrioritizedEpisodeReplayBuffer(unittest.TestCase):
    @staticmethod
    def _get_episode(episode_len=None, id_=None):
        eps = SingleAgentEpisode(id_=id_, observations=[0.0], infos=[{}])
        ts = np.random.randint(1, 200) if episode_len is None else episode_len
        for t in range(ts):
            eps.add_env_step(
                observation=float(t + 1),
                action=int(t),
                reward=0.1 * (t + 1),
                infos={},
            )
        eps.is_terminated = np.random.random() > 0.5
        eps.is_truncated = False if eps.is_terminated else np.random.random() > 0.8
        return eps

    def test_add_and_eviction_logic(self):
        # Fill the buffer till capacity (100 ts).
        buffer = PrioritizedEpisodeReplayBuffer(capacity=100)

        episode = self._get_episode(id_="A", episode_len=50)
        buffer.add(episode)
        self.assertEqual(buffer.get_num_episodes(), 1)
        self.assertEqual(buffer.get_num_timesteps(), 50)

        episode = self._get_episode(id_="B", episode_len=25)
        buffer.add(episode)
        self.assertEqual(buffer.get_num_episodes(), 2)
        self.assertEqual(buffer.get_num_timesteps(), 75)

        # No eviction yet (but we are full).
        episode = self._get_episode(id_="C", episode_len=25)
        buffer.add(episode)
        self.assertEqual(buffer.get_num_episodes(), 3)
        self.assertEqual(buffer.get_num_timesteps(), 100)

        # Trigger eviction of first episode by adding a single timestep episode.
        episode = self._get_episode(id_="D", episode_len=1)
        buffer.add(episode)

        self.assertEqual(buffer.get_num_episodes(), 3)
        self.assertEqual(buffer.get_num_timesteps(), 51)
        self.assertEqual({eps.id_ for eps in buffer.episodes}, {"B", "C", "D"})

        # Add another big episode and trigger another eviction.
        episode = self._get_episode(id_="E", episode_len=200)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 1)
        self.assertTrue(buffer.get_num_timesteps() == 200)
        self.assertTrue({eps.id_ for eps in buffer.episodes} == {"E"})

        # Add another small episode and trigger another eviction.
        episode = self._get_episode(id_="F", episode_len=2)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 1)
        self.assertTrue(buffer.get_num_timesteps() == 2)
        self.assertTrue({eps.id_ for eps in buffer.episodes} == {"F"})

        # Add N small episodes.
        for i in range(10):
            episode = self._get_episode(id_=str(i), episode_len=10)
            buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 10)
        self.assertTrue(buffer.get_num_timesteps() == 100)

        # Add a 20-ts episode and expect to have evicted 3 episodes.
        episode = self._get_episode(id_="G", episode_len=21)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 8)
        self.assertTrue(buffer.get_num_timesteps() == 91)
        self.assertTrue(
            {eps.id_ for eps in buffer.episodes}
            == {"3", "4", "5", "6", "7", "8", "9", "G"}
        )

    def test_prioritized_buffer_sample_logic(self):
        buffer = PrioritizedEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for _ in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=1)
            obs, actions, rewards, next_obs, is_terminated, is_truncated, weights = (
                sample["obs"],
                sample["actions"],
                sample["rewards"],
                sample["new_obs"],
                sample["terminateds"],
                sample["truncateds"],
                sample["weights"],
            )

            # Make sure terminated and truncated are never both True.
            assert not np.any(np.logical_and(is_truncated, is_terminated))

            # All fields have same shape.
            assert (
                obs.shape[:2]
                == rewards.shape
                == actions.shape
                == next_obs.shape
                == is_truncated.shape
                == is_terminated.shape
            )

            # Note, floating point numbers cannot be compared directly.
            tolerance = 1e-8
            # Assert that actions correspond to the observations.
            self.assertTrue(np.all(actions - obs < tolerance))
            # Assert that next observations are correctly one step after
            # observations.
            self.assertTrue(np.all(next_obs - obs - 1 < tolerance))
            # Assert that the reward comes from the next observation.
            self.assertTrue(np.all(rewards * 10 - next_obs < tolerance))

            # Furthermore, assert that the improtance sampling weights are
            # one for `beta=0.0`.
            self.assertTrue(np.all(weights - 1.0 < tolerance))

        # Now test a 3-step sampling.
        for _ in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=3, beta=1.0)
            obs, actions, rewards, next_obs, is_terminated, is_truncated, weights = (
                sample["obs"],
                sample["actions"],
                sample["rewards"],
                sample["new_obs"],
                sample["terminateds"],
                sample["truncateds"],
                sample["weights"],
            )

            # Make sure terminated and truncated are never both True.
            assert not np.any(np.logical_and(is_truncated, is_terminated))

            # All fields have same shape.
            assert (
                obs.shape[:2]
                == rewards.shape
                == actions.shape
                == next_obs.shape
                == is_truncated.shape
                == is_terminated.shape
            )

            # Note, floating point numbers cannot be compared directly.
            tolerance = 1e-8
            # Assert that actions correspond to the observations.
            self.assertTrue(np.all(actions - obs < tolerance))
            # Assert that next observations are correctly one step after
            # observations.
            self.assertTrue(np.all(next_obs - obs - 1 - 2 < tolerance))
            # Assert that the reward is indeed the cumulated sum of rewards
            # collected between the observation and the next_observation.
            reward_sum = (
                next_obs * 0.99**2 + (next_obs - 1) * 0.99 + next_obs - 2
            ) * 0.1
            self.assertTrue(np.all(rewards - reward_sum < tolerance))

    def test_update_priorities(self):
        # Define replay buffer (alpha=1.0).
        buffer = PrioritizedEpisodeReplayBuffer(capacity=100)

        # Generate 200 episode of random length.
        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        # Now sample from the buffer and update priorities.

        sample = buffer.sample(batch_size_B=16, n_step=1)
        weights = sample["weights"]

        # Make sure the initial weights are 1.0.
        tolerance = 1e-5
        self.assertTrue(np.all(weights - 1 < tolerance))

        # Define some deltas.
        deltas = np.array([0.01] * 16)
        # Get the last sampled indices (in the segment trees).
        last_sampled_indices = buffer._last_sampled_indices
        # Update th epriorities of the last sampled transitions.
        buffer.update_priorities(priorities=deltas)

        # Assert that the new priorities are indeed the ones we passed in.
        new_priorities = [buffer._sum_segment[idx] for idx in last_sampled_indices]
        self.assertTrue(np.all(new_priorities - deltas < tolerance))

        # Sample several times.
        index_counts = []
        for _ in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=1)

            index_counts.append(
                any(
                    [
                        idx in last_sampled_indices
                        for idx in buffer._last_sampled_indices
                    ]
                )
            )

        self.assertGreater(0.15, sum(index_counts) / len(index_counts))

        # Define replay buffer (alpha=1.0).
        buffer = PrioritizedEpisodeReplayBuffer(capacity=10)
        episode = self._get_episode(10)
        buffer.add(episode)

        # Manipulate the priorities such that 1's priority is
        # way higher than the others and sample.
        buffer._last_sampled_indices = [1]
        randn = np.random.random() + 0.2
        buffer.update_priorities(np.array([randn]))
        buffer._last_sampled_indices = [2, 3, 4, 5, 6, 7, 8, 9]
        buffer.update_priorities(np.array([0.01] * 8))

        # Expect that around 90% of the samples are from index 1.
        for _ in range(10):
            sample = buffer.sample(1000)
            number_of_ones = np.sum(np.array(buffer._last_sampled_indices) == 1)
            self.assertTrue(number_of_ones / 1000 > 0.8)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
