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

        data_path = "tests/data/pendulum/small.json"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        config = AlgorithmConfig().offline_data(input_=[data_path])

        offline_data = OfflineData(config)

        single_row = offline_data.data.take_batch(batch_size=1)
        self.assertTrue("obs" in single_row)

    def test_offline_convert_to_episodes(self):

        data_path = "tests/data/pendulum/small.json"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        config = AlgorithmConfig().offline_data(input_=[data_path])

        offline_data = OfflineData(config)

        batch = offline_data.data.take_batch(batch_size=10)
        episodes = offline_data._map_to_episodes(False, batch)["episodes"]

        self.assertTrue(len(episodes) == 10)
        self.assertTrue(isinstance(episodes[0], SingleAgentEpisode))


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
