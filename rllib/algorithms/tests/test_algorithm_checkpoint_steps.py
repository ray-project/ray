from __future__ import annotations

import tempfile
import unittest

import tree

import ray
from ray.rllib.algorithms.ppo import PPOConfig, PPO
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.rllib.algorithms import Algorithm
    from ray.rllib.utils.metrics.metrics_logger import MetricsLogger


ENV_STEPS_PER_ITERATION = 10
NUM_ENV_STEPS_IN_CALLBACK_LIFETIME = "num_env_steps_in_callback"
NUM_ENV_RUNNERS = 5

expected_results = {
    NUM_ENV_STEPS_IN_CALLBACK_LIFETIME: {
        "step_1": ENV_STEPS_PER_ITERATION,
        "step_2": ENV_STEPS_PER_ITERATION * 2,
        "step_3": ENV_STEPS_PER_ITERATION * 3,
    },
    NUM_ENV_STEPS_SAMPLED_LIFETIME: {
        "step_1": ENV_STEPS_PER_ITERATION,
        "step_2": ENV_STEPS_PER_ITERATION * 2,
        "step_3": ENV_STEPS_PER_ITERATION * 3,
    },
}


class TestAlgorithmCheckpointStepsAfterRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_metrics_after_checkpoint(self):
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

        config.callbacks(on_sample_end=log_custom_metric)
        algo_0_runner = config.env_runners(
            num_env_runners=0,
        ).build_algo()
        algo_1_runner = config.env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
        ).build_algo()
        self._test_algo_checkpointing(
            algo_0_runner,
            algo_1_runner,
            metrics=[
                NUM_ENV_STEPS_SAMPLED_LIFETIME,
                NUM_ENV_STEPS_IN_CALLBACK_LIFETIME,
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

            # --- Step 2 from restored & checkpoint ---
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
            # Test after restoring a second time
            algo_0_runner_restored.save_checkpoint(checkpoint_0_step2_restored)
            algo_1_runner_restored.save_checkpoint(checkpoint_1_step2_restored)
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
            # Step 3 from restored and restored x2
            result_algo0_step3_restored = algo_0_runner_restored.step()
            result_algo1_step3_restored = algo_1_runner_restored.step()
            result_algo0_step3_restored_x2 = algo_0_restored_x2.step()
            result_algo1_step3_restored_x2 = algo_1_restored_x2.step()

            # Test that all results after step 3 are have 300 steps
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


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
