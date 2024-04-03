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
        """Tests batches getting properly added to buffer and cause proper eviction."""

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
