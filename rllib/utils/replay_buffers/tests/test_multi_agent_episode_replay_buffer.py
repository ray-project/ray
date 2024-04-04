import numpy as np
import unittest

from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.replay_buffers.multi_agent_episode_replay_buffer import (
    MultiAgentEpisodeReplayBuffer,
)


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
        """Tests episodes getting properly added to buffer and cause proper eviction."""

        # Fill a buffer till capacity (100 ts).
        buffer = MultiAgentEpisodeReplayBuffer(capacity=100)

        episode = self._get_episode(id_="A", episode_len=50)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 1)
        self.assertTrue(buffer.get_num_timesteps() == 50)

        episode = self._get_episode(id_="B", episode_len=25)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 2)
        self.assertTrue(buffer.get_num_timesteps() == 75)

        # No eviction yet (but we are full).
        episode = self._get_episode(id_="C", episode_len=25)
        buffer.add(episode)
        self.assertTrue(buffer.get_num_episodes() == 3)
        self.assertTrue(buffer.get_num_timesteps() == 100)

        # Trigger eviction of first episode by adding a single timestep episode.
        episode = self._get_episode(id_="D", episode_len=1)
        buffer.add(episode)

        self.assertTrue(buffer.get_num_episodes() == 3)
        self.assertTrue(buffer.get_num_timesteps() == 51)
        self.assertTrue({eps.id_ for eps in buffer.episodes} == {"B", "C", "D"})

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

    def test_buffer_independent_sample_logic(self):
        """Samples independently from the multi-agent buffer."""
        buffer = MultiAgentEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for _ in range(1000):
            sample = buffer.sample(batch_size_B=16, n_step=1)
            self.assertTrue("module_1" in sample)
            self.assertTrue("module_2" in sample)
            for module_id in sample:
                (
                    obs,
                    actions,
                    rewards,
                    next_obs,
                    is_terminated,
                    is_truncated,
                    weights,
                    n_steps,
                ) = (
                    sample[module_id]["obs"],
                    sample[module_id]["actions"],
                    sample[module_id]["rewards"],
                    sample[module_id]["new_obs"],
                    sample[module_id]["terminateds"],
                    sample[module_id]["truncateds"],
                    sample[module_id]["weights"],
                    sample[module_id]["n_steps"],
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

                # Furthermore, assert that the importance sampling weights are
                # one for `beta=0.0`.
                self.assertTrue(np.all(weights - 1.0 < tolerance))

                # Assert that all n-steps are 1.0 as passed into `sample`.
                self.assertTrue(np.all(n_steps - 1.0 < tolerance))

    def test_buffer_synchronized_sample_logic(self):
        """Samples synchronized from the multi-agent buffer."""
        buffer = MultiAgentEpisodeReplayBuffer(capacity=10000)

        for _ in range(200):
            episode = self._get_episode()
            buffer.add(episode)

        for _ in range(1000):
            sample = buffer.sample(
                batch_size_B=16, n_step=1, replay_mode="synchronized"
            )
            self.assertTrue("module_1" in sample)
            self.assertTrue("module_2" in sample)
            for module_id in sample:
                (
                    obs,
                    actions,
                    rewards,
                    next_obs,
                    is_terminated,
                    is_truncated,
                    weights,
                    n_steps,
                ) = (
                    sample[module_id]["obs"],
                    sample[module_id]["actions"],
                    sample[module_id]["rewards"],
                    sample[module_id]["new_obs"],
                    sample[module_id]["terminateds"],
                    sample[module_id]["truncateds"],
                    sample[module_id]["weights"],
                    sample[module_id]["n_steps"],
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

                # Furthermore, assert that the importance sampling weights are
                # one for `beta=0.0`.
                self.assertTrue(np.all(weights - 1.0 < tolerance))

                # Assert that all n-steps are 1.0 as passed into `sample`.
                self.assertTrue(np.all(n_steps - 1.0 < tolerance))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
