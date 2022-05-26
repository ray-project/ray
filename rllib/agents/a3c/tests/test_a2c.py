import unittest

import ray
import ray.rllib.agents.a3c as a3c
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestA2C(unittest.TestCase):
    """Sanity tests for A2C exec impl."""

    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_a2c_compilation(self):
        """Test whether an A2CTrainer can be built with both frameworks."""
        config = a3c.A2CConfig().rollouts(num_rollout_workers=2, num_envs_per_worker=2)

        num_iterations = 1

        # Test against all frameworks.
        for _ in framework_iterator(config, with_eager_tracing=True):
            for env in ["CartPole-v0", "Pendulum-v1", "PongDeterministic-v0"]:
                trainer = config.build(env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(trainer)
                trainer.stop()

    def test_a2c_exec_impl(self):
        config = (
            a3c.A2CConfig()
            .environment(env="CartPole-v0")
            .reporting(min_time_s_per_reporting=0)
        )

        for _ in framework_iterator(config):
            trainer = config.build()
            results = trainer.train()
            check_train_results(results)
            print(results)
            check_compute_single_action(trainer)
            trainer.stop()

    def test_a2c_exec_impl_microbatch(self):
        config = (
            a3c.A2CConfig()
            .environment(env="CartPole-v0")
            .reporting(min_time_s_per_reporting=0)
            .training(microbatch_size=10)
        )

        for _ in framework_iterator(config):
            trainer = config.build()
            results = trainer.train()
            check_train_results(results)
            print(results)
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
