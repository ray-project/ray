import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import framework_iterator

tf = try_import_tf()


class TestAPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_appo_compilation(self):
        """Test whether an APPOTrainer can be built with both frameworks."""
        config = ppo.appo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        num_iterations = 2

        for _ in framework_iterator(
                config, frameworks=("torch", "tf", "eager")):
            trainer = ppo.APPOTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                trainer.train()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
