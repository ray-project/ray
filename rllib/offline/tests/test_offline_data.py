import functools
import gymnasium as gym
import ray
import shutil
import unittest

from pathlib import Path

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_data import OfflineData, OfflinePreLearner, SCHEMA


class TestOfflineData(unittest.TestCase):
    def setUp(self) -> None:
        data_path = "tests/data/cartpole/cartpole-v1_large"
        base_path = Path(__file__).parents[2]
        self.data_path = "local://" + base_path.joinpath(data_path).as_posix()
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_data_load(self):

        config = AlgorithmConfig().offline_data(input_=[self.data_path])

        offline_data = OfflineData(config)

        single_row = offline_data.data.take_batch(batch_size=1)
        self.assertTrue("obs" in single_row)

    def test_offline_convert_to_episodes(self):

        config = AlgorithmConfig().offline_data(
            input_=[self.data_path],
        )

        offline_data = OfflineData(config)

        batch = offline_data.data.take_batch(batch_size=10)
        episodes = OfflinePreLearner._map_to_episodes(False, batch)["episodes"]

        self.assertTrue(len(episodes) == 10)
        self.assertTrue(isinstance(episodes[0], SingleAgentEpisode))

    def test_sample(self):

        config = AlgorithmConfig().offline_data(input_=[self.data_path])

        offline_data = OfflineData(config)

        batch_iterator = offline_data.data.map_batches(
            functools.partial(
                OfflinePreLearner._map_to_episodes, offline_data.is_multi_agent
            )
        ).iter_batches(
            batch_size=10,
            prefetch_batches=1,
            local_shuffle_buffer_size=100,
        )

        batch = next(iter(batch_iterator))

        self.assertTrue("episodes" in batch)
        self.assertTrue(isinstance(batch["episodes"][0], SingleAgentEpisode))

    def test_offline_data_with_schema(self):

        # Create some data with a different schema.
        env = gym.make("CartPole-v1")
        obs, _ = env.reset()
        eps_id = 12345
        experiences = []
        for i in range(100):
            action = env.action_space.sample()
            next_obs, reward, terminated, truncated, _ = env.step(action)
            experience = {
                "o_t": obs,
                "a_t": action,
                "r_t": reward,
                "o_tp1": next_obs,
                "d_t": terminated or truncated,
                "episode_id": eps_id,
            }
            experiences.append(experience)
            if terminated or truncated:
                obs, info = env.reset()
                eps_id = eps_id + i
            obs = next_obs

        # Convert to `Dataset`.
        ds = ray.data.from_items(experiences)
        # Store unter the temporary directory.
        dir_path = "/tmp/ray/tests/data/test_offline_data_with_schema/test_data"
        ds.write_parquet(dir_path)

        # Define a config.
        config = AlgorithmConfig()
        config.input_ = [dir_path]
        # Explicitly request to use a different schema.
        config.input_read_schema = {
            Columns.OBS: "o_t",
            Columns.ACTIONS: "a_t",
            Columns.REWARDS: "r_t",
            Columns.NEXT_OBS: "o_tp1",
            Columns.EPS_ID: "episode_id",
            Columns.TERMINATEDS: "d_t",
        }
        # Create the `OfflineData` instance. Note, this tests reading
        # the files.
        offline_data = OfflineData(config)
        # Ensure that the data could be loaded.
        self.assertTrue(hasattr(offline_data, "data"))
        # Take a small batch.
        batch = offline_data.data.take_batch(10)
        self.assertTrue("o_t" in batch.keys())
        self.assertTrue("a_t" in batch.keys())
        self.assertTrue("r_t" in batch.keys())
        self.assertTrue("o_tp1" in batch.keys())
        self.assertTrue("d_t" in batch.keys())
        self.assertTrue("episode_id" in batch.keys())
        # Preprocess the batch to episodes. Note, here we test that the
        # user schema is used.
        episodes = OfflinePreLearner._map_to_episodes(
            is_multi_agent=False, batch=batch, schema=SCHEMA | config.input_read_schema
        )
        self.assertEqual(len(episodes["episodes"]), batch["o_t"].shape[0])
        # Finally, remove the files and folders.
        shutil.rmtree(dir_path)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
