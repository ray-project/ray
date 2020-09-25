import unittest

import ray
import ray.rllib.agents.mbmpo as mbmpo
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestMBMPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_mbmpo_compilation(self):
        """Test whether an MBMPOTrainer can be built with all frameworks."""
        config = mbmpo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["horizon"] = 200
        num_iterations = 1

        # Test for torch framework (tf not implemented yet).
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = mbmpo.MBMPOTrainer(config=config, env="Pendulum-v0")
            for i in range(num_iterations):
                trainer.train()
            check_compute_single_action(
                trainer, include_prev_action_reward=True)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
