import unittest

from ray.rllib.agents.ppo import PPOTrainer, DEFAULT_CONFIG
import ray


class LocalModeTest(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(local_mode=True)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_local(self):
        cf = DEFAULT_CONFIG.copy()
        agent = PPOTrainer(cf, "CartPole-v0")
        print(agent.train())


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
