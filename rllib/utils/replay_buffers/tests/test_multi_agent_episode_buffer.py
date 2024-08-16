import numpy as np
import unittest

from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.replay_buffers.multi_agent_episode_buffer import (
    MultiAgentEpisodeReplayBuffer,
)
from ray.rllib.utils.test_utils import check


class TestMultiAgentEpisodeReplayBuffer(unittest.TestCase):
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
        eps.is_truncated = False if eps.is_terminated else np.random.random() > 0.8
        return eps

    def test_add_and_eviction_logic(self):
        """Tests episodes getting properly added to buffer and cause proper
        eviction."""

        # Fill a buffer till capacity (100 ts).
        buffer = MultiAgentEpisodeReplayBuffer(capacity=100)

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
        self.assertTrue(buffer.get_num_episodes() == 2)
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
        buffer = MultiAgentEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for i in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=1)
            self.assertTrue(buffer.get_sampled_timesteps() == 16 * (i + 1))
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

    # def test_buffer_synchronized_sample_logic(self):
    #     """Samples synchronized from the multi-agent buffer."""
    #     buffer = MultiAgentEpisodeReplayBuffer(capacity=10000)

    #     for _ in range(200):
    #         episode = self._get_episode()
    #         buffer.add(episode)

    #     for i in range(1000):
    #         sample = buffer.sample(
    #             batch_size_B=16, n_step=1, replay_mode="synchronized"
    #         )
    #         self.assertTrue(buffer.get_sampled_timesteps() == 16 * (i + 1))
    #         self.assertTrue("module_1" in sample)
    #         self.assertTrue("module_2" in sample)
    #         for module_id in sample:
    #             self.assertTrue(buffer.get_sampled_timesteps(module_id) == 16 *
    # (i + 1))
    #             (
    #                 obs,
    #                 actions,
    #                 rewards,
    #                 next_obs,
    #                 is_terminated,
    #                 is_truncated,
    #                 weights,
    #                 n_steps,
    #             ) = (
    #                 sample[module_id]["obs"],
    #                 sample[module_id]["actions"],
    #                 sample[module_id]["rewards"],
    #                 sample[module_id]["new_obs"],
    #                 sample[module_id]["terminateds"],
    #                 sample[module_id]["truncateds"],
    #                 sample[module_id]["weights"],
    #                 sample[module_id]["n_step"],
    #             )

    #             # Make sure terminated and truncated are never both True.
    #             assert not np.any(np.logical_and(is_truncated, is_terminated))

    #             # All fields have same shape.
    #             assert (
    #                 obs.shape[:2]
    #                 == rewards.shape
    #                 == actions.shape
    #                 == next_obs.shape
    #                 == is_truncated.shape
    #                 == is_terminated.shape
    #             )

    #             # Note, floating point numbers cannot be compared directly.
    #             tolerance = 1e-8
    #             # Assert that actions correspond to the observations.
    #             self.assertTrue(np.all(actions - obs < tolerance))
    #             # Assert that next observations are correctly one step after
    #             # observations.
    #             self.assertTrue(np.all(next_obs - obs - 1 < tolerance))
    #             # Assert that the reward comes from the next observation.
    #             self.assertTrue(np.all(rewards * 10 - next_obs < tolerance))

    #             # Furthermore, assert that the importance sampling weights are
    #             # one for `beta=0.0`.
    #             self.assertTrue(np.all(weights - 1.0 < tolerance))

    #             # Assert that all n-steps are 1.0 as passed into `sample`.
    #             self.assertTrue(np.all(n_steps - 1.0 < tolerance))

    # def test_sample_with_modules_to_sample(self):
    #     """Samples synchronized from the multi-agent buffer."""
    #     buffer = MultiAgentEpisodeReplayBuffer(capacity=10000)

    #     for _ in range(200):
    #         episode = self._get_episode()
    #         buffer.add(episode)

    #     for i in range(1000):
    #         sample = buffer.sample(
    #             batch_size_B=16,
    #             n_step=1,
    #             replay_mode="synchronized",
    #             modules_to_sample=["module_1"],
    #         )
    #         self.assertTrue(buffer.get_sampled_timesteps() == 16 * (i + 1))
    #         self.assertTrue(buffer.get_sampled_timesteps("module_2") == 0)
    #         self.assertTrue("module_1" in sample)
    #         self.assertTrue("module_2" not in sample)
    #         for module_id in sample:
    #             self.assertTrue(buffer.get_sampled_timesteps(module_id) == 16 *
    # (i + 1))
    #             (
    #                 obs,
    #                 actions,
    #                 rewards,
    #                 next_obs,
    #                 is_terminated,
    #                 is_truncated,
    #                 weights,
    #                 n_steps,
    #             ) = (
    #                 sample[module_id]["obs"],
    #                 sample[module_id]["actions"],
    #                 sample[module_id]["rewards"],
    #                 sample[module_id]["new_obs"],
    #                 sample[module_id]["terminateds"],
    #                 sample[module_id]["truncateds"],
    #                 sample[module_id]["weights"],
    #                 sample[module_id]["n_step"],
    #             )

    #             # Make sure terminated and truncated are never both True.
    #             assert not np.any(np.logical_and(is_truncated, is_terminated))

    #             # All fields have same shape.
    #             assert (
    #                 obs.shape[:2]
    #                 == rewards.shape
    #                 == actions.shape
    #                 == next_obs.shape
    #                 == is_truncated.shape
    #                 == is_terminated.shape
    #             )

    #             # Note, floating point numbers cannot be compared directly.
    #             tolerance = 1e-8
    #             # Assert that actions correspond to the observations.
    #             self.assertTrue(np.all(actions - obs < tolerance))
    #             # Assert that next observations are correctly one step after
    #             # observations.
    #             self.assertTrue(np.all(next_obs - obs - 1 < tolerance))
    #             # Assert that the reward comes from the next observation.
    #             self.assertTrue(np.all(rewards * 10 - next_obs < tolerance))

    #             # Furthermore, assert that the importance sampling weights are
    #             # one for `beta=0.0`.
    #             self.assertTrue(np.all(weights - 1.0 < tolerance))

    #             # Assert that all n-steps are 1.0 as passed into `sample`.
    #             self.assertTrue(np.all(n_steps - 1.0 < tolerance))

    def test_get_state_and_set_state(self):
        """Tests getting and setting the state of the buffer.

        This test creates a buffer, fills it with episodes, gets the state of the
        buffer, creates a new buffer and sets the state of the new buffer to the
        state of the old buffer,and then checks that the two buffers are the same.
        Checks include the properties of the buffer and any internal data structures,.
        """
        # Create a buffer.
        buffer = MultiAgentEpisodeReplayBuffer(capacity=10000)
        # Fill it with episodes.
        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        # Now get the state of the buffer.
        state = buffer.get_state()

        # Create a new buffer and set the state.
        buffer2 = MultiAgentEpisodeReplayBuffer(capacity=10000)
        buffer2.set_state(state)

        # Ensure that the main properties are the same.
        check(buffer.get_num_episodes(), buffer2.get_num_episodes())
        check(buffer.get_num_episodes_evicted(), buffer2.get_num_episodes_evicted())
        check(buffer.get_num_timesteps(), buffer2.get_num_timesteps())
        check(buffer.get_added_timesteps(), buffer2.get_added_timesteps())
        check(buffer.get_sampled_timesteps(), buffer2.get_sampled_timesteps())
        check(buffer.get_num_agent_timesteps(), buffer2.get_num_agent_timesteps())
        check(buffer.get_added_agent_timesteps(), buffer2.get_added_agent_timesteps())
        check(buffer.get_module_ids(), buffer2.get_module_ids())
        # Test any data structures on equality.
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
