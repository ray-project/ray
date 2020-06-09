import copy
import unittest

import ray
import ray.rllib.agents.dyna as dyna
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_action, framework_iterator

tf = try_import_tf()


class TestDYNA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dyna_compilation(self):
        """Test whether a DYNATrainer can be built with both frameworks."""
        config = copy.deepcopy(dyna.DEFAULT_CONFIG)
        config["num_workers"] = 1
        num_iterations = 2

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = dyna.DYNATrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                trainer.train()
            check_compute_action(trainer)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
