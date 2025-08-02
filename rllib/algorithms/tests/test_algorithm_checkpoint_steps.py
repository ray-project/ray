from __future__ import annotations

import tempfile
import unittest
from collections import defaultdict
from typing import TYPE_CHECKING

import tree
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)

import ray

if TYPE_CHECKING:
    from ray.rllib.algorithms import Algorithm
    from ray.rllib.utils.metrics.metrics_logger import MetricsLogger


ENV_STEPS_PER_ITERATION = 10
NUM_ENV_STEPS_IN_CALLBACK_LIFETIME = "num_env_steps_in_callback_lifetime"
A_MEAN_VALUE = "a_mean_value"
NUM_ENV_RUNNERS = 3

expected_results = {
    NUM_ENV_STEPS_IN_CALLBACK_LIFETIME: {
        "step_1": ENV_STEPS_PER_ITERATION,
        "step_2": ENV_STEPS_PER_ITERATION * 2,
        "step_3": ENV_STEPS_PER_ITERATION * 3,
        "step_10": ENV_STEPS_PER_ITERATION * 10,
    },
    NUM_ENV_STEPS_SAMPLED_LIFETIME: {
        "step_1": ENV_STEPS_PER_ITERATION,
        "step_2": ENV_STEPS_PER_ITERATION * 2,
        "step_3": ENV_STEPS_PER_ITERATION * 3,
        "step_10": ENV_STEPS_PER_ITERATION * 10,
    },
    A_MEAN_VALUE: defaultdict(lambda: ENV_STEPS_PER_ITERATION),
}


class TestAlgorithmCheckpointStepsAfterRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Remember to call algo.stop() to keep the needed cores at a minimum.
        ray.init(num_cpus=NUM_ENV_RUNNERS, include_dashboard=False)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setup_algos(self):
        """NOTE: This test needs a patch in ray earliest coming with 2.47.2+"""
        config = PPOConfig()
        config.training(
            train_batch_size_per_learner=ENV_STEPS_PER_ITERATION,
            num_epochs=1,
            minibatch_size=ENV_STEPS_PER_ITERATION,
        )
        config.debugging(
            seed=11,
            log_sys_usage=False,
        )
        config.reporting(
            metrics_num_episodes_for_smoothing=1, keep_per_episode_custom_metrics=True
        )  # no smoothing
        config.environment(env="CartPole-v1")

        def log_custom_metric(metrics_logger: MetricsLogger, **kwargs):
            # Track env steps in a second metric from a callback
            metrics_logger.log_value(
                NUM_ENV_STEPS_IN_CALLBACK_LIFETIME,
                metrics_logger.peek(NUM_ENV_STEPS_SAMPLED),
                reduce="sum",
                clear_on_reduce=False,
            )
            metrics_logger.log_value(
                A_MEAN_VALUE,
                ENV_STEPS_PER_ITERATION,
                reduce="mean",
            )

        config.callbacks(on_sample_end=log_custom_metric)
        algo_0_runner = config.env_runners(
            num_env_runners=0,
        ).build_algo()
        algo_1_runner = config.env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
        ).build_algo()
        return algo_0_runner, algo_1_runner

    def test_metrics_after_checkpoint(self):
        algo_0_runner, algo_1_runner = self.setup_algos()
        self._test_algo_checkpointing(
            algo_0_runner,
            algo_1_runner,
            metrics=[
                NUM_ENV_STEPS_SAMPLED_LIFETIME,
                NUM_ENV_STEPS_IN_CALLBACK_LIFETIME,
                A_MEAN_VALUE,
            ],
        )

    def _test_algo_checkpointing(
        self, algo_0_runner: Algorithm, algo_1_runner: Algorithm, metrics: list[str]
    ):
        assert (
            algo_0_runner.config
            and algo_1_runner.config
            and algo_0_runner.metrics
            and algo_1_runner.metrics
        )
        self.assertEqual(algo_0_runner.config.num_env_runners, 0)
        self.assertEqual(algo_1_runner.config.num_env_runners, NUM_ENV_RUNNERS)
        # --- Step 1 ---
        _result_algo_0_step1 = algo_0_runner.step()
        _result_algo_1_step1 = algo_1_runner.step()
        with (
            tempfile.TemporaryDirectory(prefix=".ckpt_a0_") as checkpoint_0_step1,
            tempfile.TemporaryDirectory(prefix=".ckpt_a1_") as checkpoint_1_step1,
            tempfile.TemporaryDirectory(prefix=".ckpt_b0_") as checkpoint_0_step2,
            tempfile.TemporaryDirectory(prefix=".ckpt_b1_") as checkpoint_1_step2,
            tempfile.TemporaryDirectory(
                prefix=".ckpt_c0_"
            ) as checkpoint_0_step2_restored,
            tempfile.TemporaryDirectory(
                prefix=".ckpt_c1_"
            ) as checkpoint_1_step2_restored,
        ):
            # Save Step 1
            algo_0_runner.save_checkpoint(checkpoint_0_step1)
            algo_1_runner.save_checkpoint(checkpoint_1_step1)
            # --- Step 2 ---
            result_algo_0_step2 = algo_0_runner.step()
            result_algo_1_step2 = algo_1_runner.step()
            for metric in metrics:
                with self.subTest(f"{metric} after step 2", metric=metric):
                    self.assertEqual(
                        result_algo_0_step2[ENV_RUNNER_RESULTS][metric],
                        result_algo_1_step2[ENV_RUNNER_RESULTS][metric],
                    )
                    self.assertEqual(
                        result_algo_0_step2[ENV_RUNNER_RESULTS][metric],
                        expected_results[metric]["step_2"],
                    )
            # Save Step 2
            algo_0_runner.save_checkpoint(checkpoint_0_step2)
            algo_1_runner.save_checkpoint(checkpoint_1_step2)

            # --- Step 3 ---
            result_algo_0_step3 = algo_0_runner.step()
            result_algo_1_step3 = algo_1_runner.step()
            algo_1_runner.stop()  # free resources
            algo_0_runner.stop()
            del algo_0_runner
            del algo_1_runner
            for metric in metrics:
                with self.subTest(f"{metric} after step 3", metric=metric):
                    self.assertEqual(
                        result_algo_0_step3[ENV_RUNNER_RESULTS][metric],
                        result_algo_1_step3[ENV_RUNNER_RESULTS][metric],
                    )
                    self.assertEqual(
                        result_algo_0_step3[ENV_RUNNER_RESULTS][metric],
                        expected_results[metric]["step_3"],
                    )
            # Load Step 1
            algo_0_runner_restored = PPO.from_checkpoint(checkpoint_0_step1)
            algo_1_runner_restored = PPO.from_checkpoint(checkpoint_1_step1)
            assert algo_0_runner_restored.metrics and algo_1_runner_restored.metrics
            # Check loaded metric
            for metric in metrics:
                with self.subTest(
                    f"(Checkpointed) {metric} after restored step 1",
                    metric=metric,
                ):
                    self.assertEqual(
                        algo_0_runner_restored.metrics.peek(
                            (ENV_RUNNER_RESULTS, metric)
                        ),
                        expected_results[metric]["step_1"],
                    )
                    self.assertEqual(
                        algo_1_runner_restored.metrics.peek(
                            (ENV_RUNNER_RESULTS, metric)
                        ),
                        expected_results[metric]["step_1"],
                    )
            tree.assert_same_structure(
                algo_0_runner_restored.metrics, algo_1_runner_restored.metrics
            )

            # --- Step 2 from restored ---
            result_algo0_step2_restored = algo_0_runner_restored.step()
            result_algo1_step2_restored = algo_1_runner_restored.step()
            # Check if metric was updated
            for metric in metrics:
                with self.subTest(
                    f"(Checkpointed) {metric} after restored step 2",
                    metric=metric,
                ):
                    self.assertEqual(
                        algo_0_runner_restored.metrics.peek(
                            (ENV_RUNNER_RESULTS, metric)
                        ),
                        expected_results[metric]["step_2"],
                        f"Expected {metric} to be {expected_results[metric]['step_2']}",
                    )
                    self.assertEqual(
                        algo_1_runner_restored.metrics.peek(
                            (ENV_RUNNER_RESULTS, metric)
                        ),
                        expected_results[metric]["step_2"],
                        f"Expected {metric} to be {expected_results[metric]['step_2']}",
                    )

            for metric in metrics:
                with self.subTest(
                    f"(Checkpointed) {metric} after step 2", metric=metric
                ):
                    with self.subTest("From checkpoint: env_runners=0 - Step 2"):
                        self.assertEqual(
                            result_algo_0_step2[ENV_RUNNER_RESULTS][metric],
                            result_algo0_step2_restored[ENV_RUNNER_RESULTS][metric],
                        )
                    with self.subTest("From checkpoint: env_runners=1 - Step 2"):
                        self.assertEqual(
                            result_algo_1_step2[ENV_RUNNER_RESULTS][metric],
                            result_algo1_step2_restored[ENV_RUNNER_RESULTS][metric],
                        )
            # Save a second time after step 2
            algo_0_runner_restored.save_checkpoint(checkpoint_0_step2_restored)
            algo_1_runner_restored.save_checkpoint(checkpoint_1_step2_restored)
            # --- Step 3 from restored ---
            result_algo0_step3_restored = algo_0_runner_restored.step()
            result_algo1_step3_restored = algo_1_runner_restored.step()
            algo_0_runner_restored.stop()
            algo_1_runner_restored.stop()  # free resources
            del algo_1_runner_restored
            del algo_0_runner_restored

            # Load and check restored when restored a second time
            algo_0_restored_x2 = PPO.from_checkpoint(checkpoint_0_step2_restored)
            algo_1_restored_x2 = PPO.from_checkpoint(checkpoint_1_step2_restored)
            assert algo_0_restored_x2.metrics and algo_1_restored_x2.metrics
            for metric in metrics:
                with self.subTest(
                    f"(Checkpointed x2) {metric} after step 2", metric=metric
                ):
                    self.assertEqual(
                        algo_0_restored_x2.metrics.peek((ENV_RUNNER_RESULTS, metric)),
                        expected_results[metric]["step_2"],
                    )
                    self.assertEqual(
                        algo_1_restored_x2.metrics.peek((ENV_RUNNER_RESULTS, metric)),
                        expected_results[metric]["step_2"],
                    )
            result_algo0_step3_restored_x2 = algo_0_restored_x2.step()
            result_algo1_step3_restored_x2 = algo_1_restored_x2.step()

            # Test that all results after step 3
            for metric in metrics:
                with self.subTest(
                    f"(Checkpointed) {metric} after step 3", metric=metric
                ):
                    with self.subTest("From checkpoint: env_runners=0 - Step 3"):
                        self.assertEqual(
                            result_algo_0_step3[ENV_RUNNER_RESULTS][metric],
                            result_algo0_step3_restored[ENV_RUNNER_RESULTS][metric],
                        )
                    with self.subTest(
                        "From checkpoint: env_runners=0 - Step 3 (restored x2)"
                    ):
                        self.assertEqual(
                            result_algo_0_step3[ENV_RUNNER_RESULTS][metric],
                            result_algo0_step3_restored_x2[ENV_RUNNER_RESULTS][metric],
                        )
                    with self.subTest("From checkpoint: env_runners=1 - Step 3"):
                        self.assertEqual(
                            result_algo_1_step3[ENV_RUNNER_RESULTS][metric],
                            result_algo1_step3_restored[ENV_RUNNER_RESULTS][metric],
                        )
                    with self.subTest(
                        "From checkpoint: env_runners=1 - Step 3 (restored x2)"
                    ):
                        self.assertEqual(
                            result_algo_1_step3[ENV_RUNNER_RESULTS][metric],
                            result_algo1_step3_restored_x2[ENV_RUNNER_RESULTS][metric],
                        )

    def test_steps_after_later_reload(self):
        """Assure that reload does not reset metrics."""
        algo_0_runner, algo_1_runner = self.setup_algos()
        for algo in (algo_0_runner, algo_1_runner):
            with self.subTest(num_env_runners=algo.config.num_env_runners):
                for _ in range(9):
                    algo.step()
                with tempfile.TemporaryDirectory(
                    prefix=".ckpt_1_"
                ) as checkpoint_1_step3:
                    algo.save_checkpoint(checkpoint_1_step3)
                    result_algo_1_step_10 = algo.step()  # Step 10
                    for metric in (
                        NUM_ENV_STEPS_SAMPLED_LIFETIME,
                        NUM_ENV_STEPS_IN_CALLBACK_LIFETIME,
                        A_MEAN_VALUE,
                    ):
                        self.assertEqual(
                            result_algo_1_step_10[ENV_RUNNER_RESULTS][metric],
                            expected_results[metric]["step_10"],
                        )
                    # Stop algo 1
                    algo.stop()
                    # Reload algo 1
                    algo_restored = PPO.from_checkpoint(checkpoint_1_step3)
                    assert algo_restored.metrics
                    # Step 4 from restored
                    result_algo_1_restored_step_10 = algo_restored.step()
                    for metric in (
                        NUM_ENV_STEPS_SAMPLED_LIFETIME,
                        NUM_ENV_STEPS_IN_CALLBACK_LIFETIME,
                        A_MEAN_VALUE,
                    ):
                        with self.subTest(f"{metric} after step 10", metric=metric):
                            self.assertEqual(
                                result_algo_1_restored_step_10[ENV_RUNNER_RESULTS][
                                    metric
                                ],
                                expected_results[metric]["step_10"],
                            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
