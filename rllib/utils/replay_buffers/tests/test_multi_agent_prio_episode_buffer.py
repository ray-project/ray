import numpy as np
import unittest

from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.replay_buffers import (
    MultiAgentPrioritizedEpisodeReplayBuffer,
)
from ray.rllib.utils.test_utils import check


class TestMultiAgentPrioritizedEpisodeReplayBuffer(unittest.TestCase):
    @staticmethod
    def _get_episode(episode_len=None, id_=None):
        initial_observation = [{"agent_1": 0.0, "agent_2": 0.0}]
        initial_infos = [{"agent_1": {}, "agent_2": {}}]
        eps = MultiAgentEpisode(
            id_=id_,
            observations=initial_observation,
            infos=initial_infos,
            agent_module_ids={"agent_1": "module_1", "agent_2": "module_2"},
        )
        ts = np.random.randint(1, 200) if episode_len is None else episode_len
        for t in range(ts):
            eps.add_env_step(
                observations={"agent_1": float(t + 1), "agent_2": float(t + 1)},
                actions={"agent_1": int(t), "agent_2": int(t)},
                rewards={"agent_1": 0.1 * (t + 1), "agent_2": 0.1 * (t + 1)},
                infos={"agent_1": {}, "agent_2": {}},
            )
        eps.is_terminated = np.random.random() > 0.5
        eps.is_truncated = (
            False if eps.is_terminated else (np.random.random() > 0.8 or ts >= 200)
        )
        return eps

    def test_add_and_eviction_logic(self):
        """Tests episodes getting properly added to buffer and cause proper eviction."""

        # Fill a buffer till capacity (100 ts).
        buffer = MultiAgentPrioritizedEpisodeReplayBuffer(capacity=100)

        episode = self._get_episode(id_="A", episode_len=50)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 1)
        self.assertTrue(buffer.get_num_timesteps() == 50)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 50)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 50)
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 1)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 50)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 50)

        episode = self._get_episode(id_="B", episode_len=25)
        buffer.add(episode)
        # self.assertTrue(buffer.get_num_episodes() == 2)
        self.assertTrue(buffer.get_num_timesteps() == 75)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 75)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 75)
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 2)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 75)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 75)

        # No eviction yet (but we are full).
        episode = self._get_episode(id_="C", episode_len=25)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 3)
        self.assertTrue(buffer.get_num_timesteps() == 100)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 100)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 100)
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 3)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 100)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 100)

        # Trigger eviction of first episode by adding a single timestep episode.
        episode = self._get_episode(id_="D", episode_len=1)
        buffer.add(episode)

        self.assertTrue(buffer.get_num_episodes() == 3)
        self.assertTrue(buffer.get_num_timesteps() == 51)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 51)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 101)
        self.assertTrue({eps.id_ for eps in buffer.episodes} == {"B", "C", "D"})
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 3)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 51)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 101)

        # Add another big episode and trigger another eviction.
        episode = self._get_episode(id_="E", episode_len=200)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 1)
        self.assertTrue(buffer.get_num_timesteps() == 200)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 200)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 301)
        self.assertTrue({eps.id_ for eps in buffer.episodes} == {"E"})
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 1)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 200)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 301)

        # Add another small episode and trigger another eviction.
        episode = self._get_episode(id_="F", episode_len=2)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 1)
        self.assertTrue(buffer.get_num_timesteps() == 2)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 2)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 303)
        self.assertTrue({eps.id_ for eps in buffer.episodes} == {"F"})
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 1)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 2)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 303)

        # Add N small episodes.
        for i in range(10):
            episode = self._get_episode(id_=str(i), episode_len=10)
            buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 10)
        self.assertTrue(buffer.get_num_timesteps() == 100)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 100)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 403)
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 10)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 100)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 403)

        # Add a 20-ts episode and expect to have evicted 3 episodes.
        episode = self._get_episode(id_="G", episode_len=21)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 8)
        self.assertTrue(buffer.get_num_timesteps() == 91)
        self.assertTrue(buffer.get_num_agent_timesteps() == 2 * 91)
        self.assertTrue(buffer.get_added_agent_timesteps() == 2 * 424)
        self.assertTrue(
            {eps.id_ for eps in buffer.episodes}
            == {"3", "4", "5", "6", "7", "8", "9", "G"}
        )
        for module_id in buffer.get_module_ids():
            self.assertTrue(buffer.get_num_episodes(module_id) == 8)
            self.assertTrue(buffer.get_num_timesteps(module_id) == 91)
            self.assertTrue(buffer.get_added_timesteps(module_id) == 424)

    def test_buffer_independent_sample_logic(self):
        """Samples independently from the multi-agent buffer."""
        buffer = MultiAgentPrioritizedEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for i in range(10):
            sample = buffer.sample(batch_size_B=16, n_step=1)
            check(buffer.get_sampled_timesteps(), 16 * (i + 1))
            module_ids = {eps.module_id for eps in sample}
            self.assertTrue("module_1" in module_ids)
            self.assertTrue("module_2" in module_ids)
            # For both modules, we should have 16 x (i + 1) timesteps sampled.
            # Note, this must be the same here as the number of timesteps sampled
            # altogether, b/c we sample both modules.
            check(buffer.get_sampled_timesteps("module_1"), 16 * (i + 1))
            check(buffer.get_sampled_timesteps("module_2"), 16 * (i + 1))
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

    def test_update_priorities(self):
        # Define replay buffer (alpha=1.0).
        buffer = MultiAgentPrioritizedEpisodeReplayBuffer(capacity=100)

        # Generate 200 episode of random length.
        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for _ in range(1000):
            # Now sample from the buffer and update priorities.
            sample = buffer.sample(batch_size_B=16, n_step=1)
            module_ids = {eps.module_id for eps in sample}
            import copy

            module_sample_indices = copy.deepcopy(
                buffer._module_to_last_sampled_indices
            )

            sum_segments = {
                mid: [
                    buffer._module_to_sum_segment[mid][i]
                    for i in module_sample_indices[mid]
                ]
                for mid in module_ids
            }

            # weights = sample["weights"]

            # # Make sure the initial weights are 1.0.
            # tolerance = 1e-5
            # check(np.all)
            # self.assertTrue(np.all(weights - 1 < tolerance))

            # Define some deltas.
            deltas = {mid: np.array(sum_segments[mid]) * 0.1 for mid in module_ids}
            # Get the last sampled indices (in the segment trees).
            # last_sampled_indices = buffer._last_sampled_indices
            # Update th epriorities of the last sampled transitions.
            for mid, delta in deltas.items():
                buffer.update_priorities(priorities=delta, module_id=mid)
            sum_segments_after = {
                mid: [
                    buffer._module_to_sum_segment[mid][i]
                    for i in module_sample_indices[mid]
                ]
                for mid in module_ids
            }
            for module_id in module_ids:
                for i in range(len(sum_segments[module_id])):
                    self.assertGreaterEqual(
                        sum_segments[module_id][i],
                        sum_segments_after[module_id][i],
                    )

    def test_get_state_set_state(self):

        # Define replay buffer (alpha=1.0).
        buffer = MultiAgentPrioritizedEpisodeReplayBuffer(capacity=100)

        # Generate 200 episode of random length.
        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        # Get the state Of the buffer.
        state = buffer.get_state()

        # Create a new buffer and set the state.
        buffer2 = MultiAgentPrioritizedEpisodeReplayBuffer(capacity=100)
        buffer2.set_state(state)

        # Check that the two buffers are the same.
        check(buffer.get_num_episodes(), buffer2.get_num_episodes())
        check(buffer.get_num_episodes_evicted(), buffer2.get_num_episodes_evicted())
        check(buffer.get_num_timesteps(), buffer2.get_num_timesteps())
        check(buffer.get_added_timesteps(), buffer2.get_added_timesteps())
        check(buffer.get_sampled_timesteps(), buffer2.get_sampled_timesteps())
        check(buffer.get_num_agent_timesteps(), buffer2.get_num_agent_timesteps())
        check(buffer.get_added_agent_timesteps(), buffer2.get_added_agent_timesteps())
        check(buffer.get_module_ids(), buffer2.get_module_ids())
        # TODO (simon): If `sample_idx_to_tree_idx` remains in the buffer, test it here,
        # too.
        # Test for each module, if data structures are identical.
        for module_id in buffer.get_module_ids():
            check(
                buffer.get_num_timesteps(module_id),
                buffer2.get_num_timesteps(module_id),
            )
            check(
                buffer.get_added_timesteps(module_id),
                buffer2.get_added_timesteps(module_id),
            )
            check(
                buffer.get_num_episodes(module_id), buffer2.get_num_episodes(module_id)
            )
            check(
                buffer.get_num_episodes_evicted(module_id),
                buffer2.get_num_episodes_evicted(module_id),
            )
            check(
                buffer._module_to_indices[module_id],
                buffer2._module_to_indices[module_id],
            )
            check(
                buffer._module_to_max_idx[module_id],
                buffer2._module_to_max_idx[module_id],
            )
            check(
                buffer._module_to_sum_segment[module_id].value,
                buffer2._module_to_sum_segment[module_id].value,
            )
            check(
                buffer._module_to_max_priority[module_id],
                buffer2._module_to_max_priority[module_id],
            )
            check(
                buffer._module_to_tree_idx_to_sample_idx[module_id],
                buffer2._module_to_tree_idx_to_sample_idx[module_id],
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
