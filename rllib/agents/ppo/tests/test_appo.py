import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


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
        config["num_workers"] = 1
        num_iterations = 2

        for _ in framework_iterator(config):
            print("w/o v-trace")
            _config = config.copy()
            _config["vtrace"] = False
            trainer = ppo.APPOTrainer(config=_config, env="CartPole-v0")
            for i in range(num_iterations):
                print(trainer.train())
            check_compute_single_action(trainer)
            trainer.stop()

            print("w/ v-trace")
            _config = config.copy()
            _config["vtrace"] = True
            trainer = ppo.APPOTrainer(config=_config, env="CartPole-v0")
            for i in range(num_iterations):
                print(trainer.train())
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
