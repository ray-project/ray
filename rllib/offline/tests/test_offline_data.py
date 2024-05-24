import ray
import unittest

from pathlib import Path

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

        offline_data = OfflineData(config={"input_": data_path})

        single_row = offline_data.data.take_batch(batch_size=1)
        self.assertTrue("obs" in single_row)

    def test_offline_convert_to_episodes(self):

        data_path = "tests/data/pendulum/small.json"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        offline_data = OfflineData(config={"input_": data_path})

        batch = offline_data.data.take_batch(batch_size=1)
        episodes = offline_data._convert_to_episodes(batch)

        self.assertTrue(len(episodes) == 10)
        self.assertTrue(isinstance(episodes[0], SingleAgentEpisode))


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
