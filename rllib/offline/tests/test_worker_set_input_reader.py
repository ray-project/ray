import unittest
from pathlib import Path
import os

import ray
from ray.rllib.algorithms.crr import CRRConfig
from ray.rllib.offline import ShuffledInput, JsonReader
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestWorkerSetInputReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_module_input(self):
        """Test whether using a Python module as the input instantiates a correct ShuffledInput."""
        rllib_dir = Path(__file__).parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/pendulum/small.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        input_module = "ray.rllib.offline.json_reader.JsonReader"
        input_config = {"inputs": data_file}
        shuffle_buffer_size = 123

        config = (
            CRRConfig()
            .environment(env="Pendulum-v1")
            .framework("torch")
            .offline_data(
                input_=input_module,
                input_config=input_config,
                shuffle_buffer_size=shuffle_buffer_size,
            )
            .rollouts(num_rollout_workers=0)
        )
        algorithm = config.build()

        input_reader = algorithm.workers.local_worker().input_reader

        assert isinstance(input_reader, ShuffledInput)
        assert input_reader.n == shuffle_buffer_size
        assert isinstance(input_reader.child, JsonReader)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
