import ray
import unittest

from pathlib import Path

from ray.rllib.offline.offline_data import OfflineData


class TestOfflineData(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_data_load(self):

        data_path = "tests/data/pendulum/large.json"
        base_path = Path(__file__).parents[2]
        data_path = "local://" + base_path.joinpath(data_path).as_posix()

        offline_data = OfflineData(config={"input_": data_path})

        single_row = offline_data.data.take_batch(batch_size=1)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
