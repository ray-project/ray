import unittest

import ray
from ray.rllib.agents.ppo import PPOTrainer, DEFAULT_CONFIG
from ray.rllib.utils.test_utils import NUM_GPUS, framework_iterator


class LocalModeTest(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(local_mode=True)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_local(self):
        cf = DEFAULT_CONFIG.copy()
        cf["num_gpus"] = NUM_GPUS
        #TODO remove "torch"
        for _ in framework_iterator(cf, frameworks="torch"):
            agent = PPOTrainer(cf, "CartPole-v0")
            print(agent.train())
            agent.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
