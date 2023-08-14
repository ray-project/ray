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
        config = (
            PGConfig()
            .environment("CartPole-v1")
            .rollouts(num_rollout_workers=2)
            .training(model={"fcnet_hiddens": [10]})
        )

        for _ in framework_iterator(config):
            algo = config.build()
            print(algo.train())
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
