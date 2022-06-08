import unittest

import ray
import ray.rllib.algorithms.mbmpo as mbmpo
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestMBMPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_mbmpo_compilation(self):
        """Test whether MBMPO can be built with all frameworks."""
        config = (
            mbmpo.MBMPOConfig()
            .rollouts(num_rollout_workers=2, horizon=200)
            .training(dynamics_model={"ensemble_size": 2})
            .environment(env="ray.rllib.examples.env.mbmpo_env.CartPoleWrapper")
        )
        num_iterations = 1

        # Test for torch framework (tf not implemented yet).
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = config.build()

            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(trainer, include_prev_action_reward=False)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
