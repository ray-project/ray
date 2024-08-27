import unittest

from rllib_a2c.a2c import A2CConfig

import ray
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
        """Test whether an A2C can be built with both frameworks."""
        config = A2CConfig().rollouts(num_rollout_workers=2, num_envs_per_worker=2)

        num_iterations = 1

        # Test against all frameworks.
        for _ in framework_iterator(config, with_eager_tracing=True):
            for env in ["ALE/Pong-v5", "CartPole-v1", "Pendulum-v1"]:
                config.environment(env)
                algo = config.build()
                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(algo)
                algo.stop()

    def test_a2c_exec_impl(self):
        config = (
            A2CConfig()
            .environment(env="CartPole-v1")
            .reporting(min_time_s_per_iteration=0)
        )

        for _ in framework_iterator(config):
            algo = config.build()
            results = algo.train()
            check_train_results(results)
            print(results)
            check_compute_single_action(algo)
            algo.stop()

    def test_a2c_exec_impl_microbatch(self):
        config = (
            A2CConfig()
            .environment(env="CartPole-v1")
            .reporting(min_time_s_per_iteration=0)
            .training(microbatch_size=10)
        )

        for _ in framework_iterator(config):
            algo = config.build()
            results = algo.train()
            check_train_results(results)
            print(results)
            check_compute_single_action(algo)
            algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
