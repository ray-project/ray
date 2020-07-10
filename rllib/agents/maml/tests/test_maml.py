import unittest

import ray
import ray.rllib.agents.maml as maml
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestMAML(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_maml_compilation(self):
        """Test whether a MAMLTrainer can be built with all frameworks."""
        config = maml.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["horizon"] = 200
        config["rollout_fragment_length"] = 200
        num_iterations = 1

        # Test for tf framework (torch not implemented yet).
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = maml.MAMLTrainer(
                config=config,
                env="ray.rllib.examples.env.pendulum_mass.PendulumMassEnv")
            for i in range(num_iterations):
                trainer.train()
            check_compute_single_action(
                trainer, include_prev_action_reward=True)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
