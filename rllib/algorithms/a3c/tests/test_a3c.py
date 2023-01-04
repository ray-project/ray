import os
import unittest

import ray
import ray.rllib.algorithms.a3c as a3c
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestA3C(unittest.TestCase):
    """Sanity tests for A2C exec impl."""

    num_gpus = float(os.environ.get("RLLIB_NUM_GPUS", "0"))

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4 if not cls.num_gpus else None)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_a3c_compilation(self):
        """Test whether an A3C can be built with both frameworks."""
        config = a3c.A3CConfig().rollouts(num_rollout_workers=2, num_envs_per_worker=2)

        num_iterations = 2

        # Test against all frameworks.
        for _ in framework_iterator(config, with_eager_tracing=False):
            for env in ["ALE/Pong-v5", "CartPole-v1", "Pendulum-v1"]:
                print("env={}".format(env))
                config.model["use_lstm"] = env == "CartPole-v1"
                algo = config.build(env=env)
                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(
                    algo, include_state=config.model["use_lstm"]
                )
                algo.stop()

    def test_a3c_entropy_coeff_schedule(self):
        """Test A3C entropy coeff schedule support."""
        config = a3c.A3CConfig().rollouts(
            num_rollout_workers=1,
            num_envs_per_worker=1,
            batch_mode="truncate_episodes",
            rollout_fragment_length=10,
        )
        # Initial entropy coeff, doesn't really matter because of the schedule below.
        config.training(
            train_batch_size=20,
            entropy_coeff=0.01,
            entropy_coeff_schedule=[
                [0, 0.01],
                [120, 0.0001],
            ],
        )
        # 0 metrics reporting delay, this makes sure timestep,
        # which entropy coeff depends on, is updated after each worker rollout.
        config.reporting(
            min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=20
        )

        def _step_n_times(trainer, n: int):
            """Step trainer n times.

            Returns:
                learning rate at the end of the execution.
            """
            for _ in range(n):
                results = trainer.train()
            return results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY][
                "entropy_coeff"
            ]

        # Test against all frameworks.
        for _ in framework_iterator(config):
            algo = config.build(env="CartPole-v1")

            coeff = _step_n_times(algo, 1)  # 20 timesteps
            # Should be close to the starting coeff of 0.01
            self.assertGreaterEqual(coeff, 0.005)

            coeff = _step_n_times(algo, 10)  # 200 timesteps
            # Should have annealed to the final coeff of 0.0001.
            self.assertLessEqual(coeff, 0.00011)

            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
