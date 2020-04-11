import unittest

import ray
import ray.rllib.agents.marwil as marwil
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import framework_iterator

tf = try_import_tf()


class TestMARWIL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_marwil_compilation(self):
        """Test whether a MARWILTrainer can be built with all frameworks."""
        config = marwil.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        num_iterations = 2

        # Test for all frameworks.
        for _ in framework_iterator(config):
            trainer = marwil.MARWILTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                trainer.train()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
