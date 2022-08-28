import os
import unittest
from pathlib import Path

import ray
from ray.rllib.offline import IOContext
from ray.rllib.offline.json_reader import JsonReader


class TestJsonReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_itr_batches(self):
        """Test that the json reader iterates over batches of rows correctly."""
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "rllib/tests/data/pendulum/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        ioctx = IOContext(config={"train_batch_size": 1200}, worker_index=0)
        reader = JsonReader([data_file], ioctx)
        assert len(reader.next()) == 1200


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
