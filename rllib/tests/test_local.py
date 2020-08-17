import unittest

import ray
from ray.rllib.agents.ppo import PPOTrainer, DEFAULT_CONFIG
from ray.rllib.utils.test_utils import framework_iterator


class LocalModeTest(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(local_mode=True)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_local(self):
        cf = DEFAULT_CONFIG.copy()
        for _ in framework_iterator(cf):
            agent = PPOTrainer(cf, "CartPole-v0")
            print(agent.train())
            agent.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
