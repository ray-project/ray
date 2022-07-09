import unittest

import ray
from ray.rllib.algorithms.pg import PG, DEFAULT_CONFIG
from ray.rllib.utils.test_utils import framework_iterator


class LocalModeTest(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(local_mode=True)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_local(self):
        cf = DEFAULT_CONFIG.copy()
        cf["model"]["fcnet_hiddens"] = [10]
        cf["num_workers"] = 2

        for _ in framework_iterator(cf):
            agent = PG(cf, "CartPole-v0")
            print(agent.train())
            agent.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
