import os
import unittest

import ray
import ray.rllib.algorithms.a2c as a2c
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestA2C(unittest.TestCase):

    num_gpus = float(os.environ.get("RLLIB_NUM_GPUS", "0"))

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4 if not cls.num_gpus else None)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_a2c_compilation(self):
        """Test whether an A2C can be built with both frameworks."""
        config = (
            a2c.A2CConfig()
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            .resources(num_gpus=self.num_gpus).rollouts(
                num_rollout_workers=2, num_envs_per_worker=2
            )
        )

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
            a2c.A2CConfig()
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            .resources(num_gpus=self.num_gpus)
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
            a2c.A2CConfig()
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            .resources(num_gpus=self.num_gpus)
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
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
