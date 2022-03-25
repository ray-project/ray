import unittest

import ray
import ray.rllib.agents.a3c as a3c
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


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

        num_iterations = 2

        # Test against all frameworks.
        for _ in framework_iterator(config, with_eager_tracing=True):
            for env in ["CartPole-v1", "Pendulum-v1", "PongDeterministic-v0"]:
                print("env={}".format(env))
                config["model"]["use_lstm"] = env == "CartPole-v1"
                trainer = a3c.A3CTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(
                    trainer, include_state=config["model"]["use_lstm"]
                )
                trainer.stop()

    def test_a3c_entropy_coeff_schedule(self):
        """Test A3CTrainer entropy coeff schedule support."""
        config = a3c.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["num_envs_per_worker"] = 1
        config["train_batch_size"] = 20
        config["batch_mode"] = "truncate_episodes"
        config["rollout_fragment_length"] = 10
        config["timesteps_per_iteration"] = 20
        # 0 metrics reporting delay, this makes sure timestep,
        # which entropy coeff depends on, is updated after each worker rollout.
        config["min_time_s_per_reporting"] = 0
        # Initial lr, doesn't really matter because of the schedule below.
        config["entropy_coeff"] = 0.01
        schedule = [
            [0, 0.01],
            [120, 0.0001],
        ]
        config["entropy_coeff_schedule"] = schedule

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
            trainer = a3c.A3CTrainer(config=config, env="CartPole-v1")

            coeff = _step_n_times(trainer, 1)  # 20 timesteps
            # Should be close to the starting coeff of 0.01
            self.assertGreaterEqual(coeff, 0.005)

            coeff = _step_n_times(trainer, 10)  # 200 timesteps
            # Should have annealed to the final coeff of 0.0001.
            self.assertLessEqual(coeff, 0.00011)

            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
