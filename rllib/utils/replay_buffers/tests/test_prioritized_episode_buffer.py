import copy
import unittest

import numpy as np
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.replay_buffers.prioritized_episode_buffer import (
    PrioritizedEpisodeReplayBuffer,
)
from ray.rllib.utils.test_utils import check


class TestPrioritizedEpisodeReplayBuffer(unittest.TestCase):
    @staticmethod
    def _get_episode(episode_len=None, id_=None, with_extra_model_outs=False):
        eps = SingleAgentEpisode(id_=id_, observations=[0.0], infos=[{}])
        ts = np.random.randint(1, 200) if episode_len is None else episode_len
        for t in range(ts):
            eps.add_env_step(
                observation=float(t + 1),
                action=int(t),
                reward=0.1 * (t + 1),
                infos={},
                extra_model_outputs=(
                    {k: k for k in range(2)} if with_extra_model_outs else None
                ),
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

    def test_buffer_sample_logic(self):
        buffer = PrioritizedEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for i in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=1)
            check(buffer.get_sampled_timesteps(), 16 * (i + 1))
            for eps in sample:

                (
                    obs,
                    action,
                    reward,
                    next_obs,
                    is_terminated,
                    is_truncated,
                    weight,
                    n_step,
                ) = (
                    eps.get_observations(0),
                    eps.get_actions(-1),
                    eps.get_rewards(-1),
                    eps.get_observations(-1),
                    eps.is_terminated,
                    eps.is_truncated,
                    eps.get_extra_model_outputs("weights", -1),
                    eps.get_extra_model_outputs("n_step", -1),
                )

                # Make sure terminated and truncated are never both True.
                assert not (is_truncated and is_terminated)

                # Note, floating point numbers cannot be compared directly.
                tolerance = 1e-8
                # Assert that actions correspond to the observations.
                check(obs, action, atol=tolerance)
                # Assert that next observations are correctly one step after
                # observations.
                check(next_obs, obs + 1, atol=tolerance)
                # Assert that the reward comes from the next observation.
                check(reward * 10, next_obs, atol=tolerance)

                # Furthermore, assert that the importance sampling weights are
                # one for `beta=0.0`.
                check(weight, 1.0, atol=tolerance)

                # Assert that all n-steps are 1.0 as passed into `sample`.
                check(n_step, 1.0, atol=tolerance)

    def test_buffer_sample_logic_with_3_step(self):
        buffer = PrioritizedEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for i in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=3)
            check(buffer.get_sampled_timesteps(), 16 * (i + 1))
            for eps in sample:

                (
                    obs,
                    action,
                    reward,
                    next_obs,
                    is_terminated,
                    is_truncated,
                    weight,
                    n_step,
                ) = (
                    eps.get_observations(0),
                    eps.get_actions(-1),
                    eps.get_rewards(-1),
                    eps.get_observations(-1),
                    eps.is_terminated,
                    eps.is_truncated,
                    eps.get_extra_model_outputs("weights", -1),
                    eps.get_extra_model_outputs("n_step", -1),
                )

                # Make sure terminated and truncated are never both True.
                assert not (is_truncated and is_terminated)

                # Note, floating point numbers cannot be compared directly.
                tolerance = 1e-8
                # Assert that actions correspond to the observations.
                check(obs, action, atol=tolerance)
                # Assert that next observations are correctly one step after
                # observations.
                check(next_obs, obs + 3, atol=tolerance)
                # Assert that the reward comes from the next observation.
                # Assert that the reward is indeed the cumulated sum of rewards
                # collected between the observation and the next_observation.
                reward_sum = (
                    next_obs * 0.99**2 + (next_obs - 1) * 0.99 + next_obs - 2
                ) * 0.1
                check(reward, reward_sum, atol=tolerance)

                # Furthermore, assert that the importance sampling weights are
                # one for `beta=0.0`.
                check(weight, 1.0, atol=tolerance)

                # Assert that all n-steps are 1.0 as passed into `sample`.
                check(n_step, 3.0, atol=tolerance)

    def test_buffer_sample_logic_with_random_n_step(self):
        buffer = PrioritizedEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for i in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=(1, 5))
            check(buffer.get_sampled_timesteps(), 16 * (i + 1))
            n_steps = []
            for eps in sample:
                # Get the n-step that was used for sampling.
                n_step = eps.get_extra_model_outputs("n_step", -1)

                # Note, floating point numbers cannot be compared directly.
                tolerance = 1e-8
                # Ensure that n-steps are in between 1 and 5.
                self.assertTrue(n_step - 5.0 < tolerance)
                self.assertTrue(n_step - 1.0 > -tolerance)
                n_steps.append(n_step)
            # Ensure that there is variation in the n-steps.
            self.assertTrue(np.var(n_steps) > 0.0)

    def test_buffer_sample_logic_with_infos_and_extra_model_output(self):
        buffer = PrioritizedEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode(with_extra_model_outs=True)
            buffer.add(episode)

        for i in range(1000):
            sample = buffer.sample(
                batch_size_B=16,
                n_step=1,
                include_infos=True,
                include_extra_model_outputs=True,
            )
            check(buffer.get_sampled_timesteps(), 16 * (i + 1))
            for eps in sample:

                (infos, extra_model_output_0, extra_model_output_1,) = (
                    eps.get_infos(),
                    eps.get_extra_model_outputs(0),
                    eps.get_extra_model_outputs(1),
                )

                # Assert that we have infos from both steps.
                check(len(infos), 2)
                # Ensure both extra model outputs have both a length of 1.abs
                check(len(extra_model_output_0), 1)
                check(len(extra_model_output_1), 1)

    def test_update_priorities(self):
        # Define replay buffer (alpha=1.0).
        buffer = PrioritizedEpisodeReplayBuffer(capacity=100)

        # Generate 200 episode of random length.
        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        # Now sample from the buffer and update priorities.

        sample = buffer.sample(batch_size_B=16, n_step=1)
        weights = np.array(
            [eps.get_extra_model_outputs("weights", -1) for eps in sample]
        )

        # Make sure the initial weights are 1.0.
        tolerance = 1e-5
        self.assertTrue(np.all(weights - 1 < tolerance))

        # Define some deltas.
        deltas = np.array([0.01] * 16)
        # Get the last sampled indices (in the segment trees).
        last_sampled_indices = copy.deepcopy(buffer._last_sampled_indices)
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
        buffer._last_sampled_indices = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        buffer.update_priorities(np.array([0.01] * 9))

        # Expect that around 90% of the samples are from index 1.
        for _ in range(10):
            sample = buffer.sample(1000)
            number_of_ones = np.sum(np.array(buffer._last_sampled_indices) == 0)
            print(f"1s: {number_of_ones / 1000}")
            self.assertTrue(number_of_ones / 1000 > 0.8)

    def test_get_state_and_set_state(self):
        """Test the get_state and set_state methods.

        This test checks that the state of the buffer can be saved and restored
        correctly. It does so by filling the buffer with episodes and then
        saving the state. Then it resets the buffer and restores the state.
        Finally, it checks that the two buffers are the same by comparing their
        properties.
        """

        # Define replay buffer (alpha=1.0).
        buffer = PrioritizedEpisodeReplayBuffer(capacity=100)

        # Fill the buffer with episodes.
        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        # Now get the state of the buffer.
        state = buffer.get_state()

        # Now reset the buffer and set the state.
        buffer2 = PrioritizedEpisodeReplayBuffer(capacity=100)
        buffer2.set_state(state)

        # Check that the two buffers are the same.
        check(buffer.get_num_episodes(), buffer2.get_num_episodes())
        check(buffer.get_num_episodes_evicted(), buffer2.get_num_episodes_evicted())
        check(buffer.get_num_timesteps(), buffer2.get_num_timesteps())
        check(buffer.get_added_timesteps(), buffer2.get_added_timesteps())
        check(buffer.get_sampled_timesteps(), buffer2.get_sampled_timesteps())
        check(buffer._max_idx, buffer2._max_idx)
        check(buffer._max_priority, buffer2._max_priority)
        check(buffer._tree_idx_to_sample_idx, buffer2._tree_idx_to_sample_idx)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
