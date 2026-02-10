import unittest

import ray
import ray.rllib.algorithms.appo as appo
from ray.rllib.algorithms.impala.impala import LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    LEARNER_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    check_train_results_new_api_stack,
)


class TestAPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_appo_compilation(self):
        """Test whether APPO can be built with both frameworks."""
        config = (
            appo.APPOConfig().environment("CartPole-v1").env_runners(num_env_runners=1)
        )
        algo = config.build()

        num_iterations = 2
        for i in range(num_iterations):
            results = algo.train()
            print(results)
        check_train_results_new_api_stack(results)

        algo.stop()

    def test_appo_compilation_use_kl_loss(self):
        """Test whether APPO can be built with kl_loss enabled."""
        config = (
            appo.APPOConfig().env_runners(num_env_runners=1).training(use_kl_loss=True)
        )
        num_iterations = 2

        algo = config.build(env="CartPole-v1")
        for i in range(num_iterations):
            results = algo.train()
            print(results)
        check_train_results_new_api_stack(results)
        algo.stop()

    def test_appo_two_optimizers_two_lrs(self):
        # Not explicitly setting this should cause a warning, but not fail.
        # config["_tf_policy_handles_more_than_one_loss"] = True
        config = (
            appo.APPOConfig()
            .api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )
            .env_runners(num_env_runners=1)
            .training(
                _separate_vf_optimizer=True,
                _lr_vf=0.002,
                # Make sure we have two completely separate models for policy and
                # value function.
                model={
                    "vf_share_layers": False,
                },
            )
        )

        num_iterations = 2

        # Only supported for tf so far.
        algo = config.build(env="CartPole-v1")
        for i in range(num_iterations):
            results = algo.train()
            check_train_results(results)
            print(results)
        check_compute_single_action(algo)
        algo.stop()

    def test_appo_entropy_coeff_schedule(self):
        config = (
            appo.APPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=1,
                rollout_fragment_length=10,
            )
            .training(
                train_batch_size_per_learner=20,
                entropy_coeff=[
                    [0, 0.1],
                    [50000, 0.01],
                ],
            )
            .reporting(
                min_train_timesteps_per_iteration=20,
                # 0 metrics reporting delay, this makes sure timestep,
                # which entropy coeff depends on, is updated after each worker rollout.
                min_time_s_per_iteration=0,
            )
        )

        def _step_n_times(algo, n: int):
            """Step Algorithm n times.

            Returns:
                learning rate at the end of the execution.
            """
            for _ in range(n):
                results = algo.train()
                print(results)
            return results[LEARNER_RESULTS][DEFAULT_POLICY_ID][
                LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY
            ]

        algo = config.build()

        coeff = _step_n_times(algo, 10)
        # Should be close to the starting coeff of 0.01.
        ts_sampled = algo.metrics.peek(
            (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
        )
        expected_coeff = 0.1 - ((0.1 - 0.01) / 50000 * ts_sampled)
        self.assertLessEqual(coeff, expected_coeff + 0.005)
        self.assertGreaterEqual(coeff, expected_coeff - 0.005)

        coeff = _step_n_times(algo, 20)
        ts_sampled = algo.metrics.peek(
            (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
        )
        expected_coeff = 0.1 - ((0.1 - 0.01) / 50000 * ts_sampled)
        self.assertLessEqual(coeff, expected_coeff + 0.005)
        self.assertGreaterEqual(coeff, expected_coeff - 0.005)

        algo.stop()

    def test_appo_learning_rate_schedule(self):
        config = (
            appo.APPOConfig()
            .env_runners(
                num_env_runners=1,
                batch_mode="truncate_episodes",
                rollout_fragment_length=10,
            )
            .training(
                train_batch_size_per_learner=20,
                entropy_coeff=0.01,
                # Setup lr schedule for testing.
                lr=[[0, 5e-2], [50000, 0.0]],
            )
            .reporting(
                min_train_timesteps_per_iteration=20,
                # 0 metrics reporting delay, this makes sure timestep,
                # which entropy coeff depends on, is updated after each worker rollout.
                min_time_s_per_iteration=0,
            )
        )

        def _step_n_times(algo, n: int):
            """Step Algorithm n times.

            Returns:
                learning rate at the end of the execution.
            """
            for _ in range(n):
                results = algo.train()
                print(results)
            return results[LEARNER_RESULTS][DEFAULT_POLICY_ID][
                "default_optimizer_learning_rate"
            ]

        algo = config.build(env="CartPole-v1")

        lr1 = _step_n_times(algo, 10)
        lr2 = _step_n_times(algo, 10)

        self.assertGreater(lr1, lr2)

        algo.stop()

    def test_appo_model_variables(self):
        config = (
            appo.APPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=1,
                rollout_fragment_length=10,
            )
            .training(
                train_batch_size_per_learner=20,
            )
            .rl_module(
                model_config=DefaultModelConfig(
                    fcnet_hiddens=[16],
                    vf_share_layers=True,
                ),
            )
        )

        algo = config.build()
        state = algo.get_module(DEFAULT_POLICY_ID).get_state()
        # Weights and biases for the encoder hidden layer (2) and the output layer
        # of the policy (2), plus the `log_std_clip` param (1), makes 5 altogether.
        # We should not get the tensors from the target model here or any value function
        # related parameters (inference-only).
        self.assertEqual(len(state), 5)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
