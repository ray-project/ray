import unittest

import ray
import ray.rllib.agents.a3c as a3c
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestA3C(unittest.TestCase):
    """Sanity tests for A2C exec impl."""

    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_a3c_compilation(self):
        """Test whether an A3CTrainer can be built with both frameworks."""
        config = a3c.DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["num_envs_per_worker"] = 2

        num_iterations = 1

        # Test against all frameworks.
        for _ in framework_iterator(config):
            for env in ["CartPole-v0", "Pendulum-v0", "PongDeterministic-v0"]:
                print("env={}".format(env))
                trainer = a3c.A3CTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)
                check_compute_single_action(trainer)
                trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
