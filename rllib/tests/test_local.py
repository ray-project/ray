import unittest

import ray
from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.utils.test_utils import framework_iterator


class LocalModeTest(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(local_mode=True)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_local(self):
        cf = PGConfig().environment("CartPole-v1")
        cf.model["fcnet_hiddens"] = [10]
        cf.num_rollout_workers = 2

        for _ in framework_iterator(cf):
            algo = cf.build()
            print(algo.train())
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
