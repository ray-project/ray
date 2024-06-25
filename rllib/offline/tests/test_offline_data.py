import functools
import ray
import unittest

from pathlib import Path

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_data import OfflineData


class TestOfflineData(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_data_load(self):

        data_path = "tests/data/cartpole/cartpole-v1.jsonl"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        config = AlgorithmConfig().offline_data(input_=[data_path])

        offline_data = OfflineData(config)

        single_row = offline_data.data.take_batch(batch_size=1)
        self.assertTrue("obs" in single_row)

    def test_offline_convert_to_episodes(self):

        data_path = "tests/data/cartpole/cartpole-v1.jsonl"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        config = AlgorithmConfig().offline_data(input_=[data_path])

        offline_data = OfflineData(config)

        batch = offline_data.data.take_batch(batch_size=10)
        episodes = offline_data._map_to_episodes(False, batch)["episodes"]

        self.assertTrue(len(episodes) == 10)
        self.assertTrue(isinstance(episodes[0], SingleAgentEpisode))

    def test_sample(self):

        data_path = "tests/data/cartpole/cartpole-v1.jsonl"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        config = AlgorithmConfig().offline_data(input_=[data_path])

        offline_data = OfflineData(config)

        batch_iterator = offline_data.data.map_batches(
            functools.partial(
                offline_data._map_to_episodes, offline_data.is_multi_agent
            )
        ).iter_batches(
            batch_size=10,
            prefetch_batches=1,
            local_shuffle_buffer_size=100,
        )

        batch = next(iter(batch_iterator))

        self.assertTrue("episodes" in batch)
        self.assertTrue(isinstance(batch["episodes"][0], SingleAgentEpisode))


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
