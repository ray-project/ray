import copy
import unittest

import ray
import ray.rllib.agents.mbmpo as mbmpo
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_action, framework_iterator

tf = try_import_tf()


class TestMBMPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_mbmpo_compilation(self):
        """Test whether a MBMPOTrainer can be built with both frameworks."""
        config = copy.deepcopy(mbmpo.DEFAULT_CONFIG)
        config["num_workers"] = 1
        num_iterations = 2

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = mbmpo.MBMPOTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                trainer.train()
            check_compute_action(trainer, include_prev_action_reward=True)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
