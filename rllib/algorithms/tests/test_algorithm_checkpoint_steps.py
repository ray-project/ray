import tempfile
import unittest

import tree

import ray
from ray.rllib.algorithms.ppo import PPOConfig, PPO
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME


class TestAlgorithmCheckpointStepsAfterRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_correct_steps_after_restore(self):
        ENV_STEPS_PER_ITERATION = 10
        config = PPOConfig()
        config.training(
            train_batch_size_per_learner=ENV_STEPS_PER_ITERATION,
            num_epochs=1,
            minibatch_size=ENV_STEPS_PER_ITERATION,
        )
        config.debugging(log_sys_usage=False)
        config.reporting(metrics_num_episodes_for_smoothing=1)
        config.environment(env="CartPole-v1")
        algo_0_runner = config.env_runners(
            num_env_runners=0,
        ).build_algo()
        algo_1_runner = config.env_runners(
            num_env_runners=1,
        ).build_algo()
        assert (
            algo_0_runner.config
            and algo_1_runner.config
            and algo_0_runner.metrics
            and algo_1_runner.metrics
        )
        self.assertEqual(algo_0_runner.config.num_env_runners, 0)
        self.assertEqual(algo_1_runner.config.num_env_runners, 1)
        # --- Step 1 ---
        _result_0 = algo_0_runner.step()
        _result_1 = algo_1_runner.step()
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
            self.assertEqual(
                result_algo_0_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                result_algo_1_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
            )
            self.assertEqual(
                result_algo_0_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                ENV_STEPS_PER_ITERATION * 2,
            )
            # Save Step 2
            algo_0_runner.save_checkpoint(checkpoint_0_step2)
            algo_1_runner.save_checkpoint(checkpoint_1_step2)
            self.assertEqual(
                algo_0_runner.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                ENV_STEPS_PER_ITERATION * 2,
            )
            self.assertEqual(
                algo_1_runner.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                ENV_STEPS_PER_ITERATION * 2,
            )

            # --- Step 3 ---
            result_algo_0_step3 = algo_0_runner.step()
            result_algo_1_step3 = algo_1_runner.step()
            self.assertEqual(
                result_algo_0_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                result_algo_1_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
            )
            self.assertEqual(
                result_algo_0_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                ENV_STEPS_PER_ITERATION * 3,
            )
            # Load Step 1
            algo_0_runner_restored = PPO.from_checkpoint(checkpoint_0_step1)
            algo_1_runner_restored = PPO.from_checkpoint(checkpoint_1_step1)
            assert algo_0_runner_restored.metrics and algo_1_runner_restored.metrics
            # Check loaded metric
            self.assertEqual(
                algo_0_runner_restored.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                ENV_STEPS_PER_ITERATION,
            )
            self.assertEqual(
                algo_0_runner_restored.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                algo_1_runner_restored.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
            )
            tree.assert_same_structure(
                algo_0_runner_restored.metrics, algo_1_runner_restored.metrics
            )

            # --- Step 2 from restored & checkpoint ---
            result_algo0_step2_restored = algo_0_runner_restored.step()
            result_algo1_step2_restored = algo_1_runner_restored.step()
            algo_0_runner_restored.save_checkpoint(checkpoint_0_step2_restored)
            algo_1_runner_restored.save_checkpoint(checkpoint_1_step2_restored)

            with self.subTest("From checkpoint: env_runners=0 - Step 2"):
                self.assertEqual(
                    result_algo_0_step2[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                    result_algo0_step2_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=1 - Step 2"):
                self.assertEqual(
                    result_algo_1_step2[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                    result_algo1_step2_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            # Test after restoring a second time
            # Load Restored from step 2
            algo_0_restored_x2 = PPO.from_checkpoint(checkpoint_0_step2_restored)
            algo_1_restored_x2 = PPO.from_checkpoint(checkpoint_1_step2_restored)
            assert algo_0_restored_x2.metrics and algo_1_restored_x2.metrics
            self.assertEqual(
                algo_0_restored_x2.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                ENV_STEPS_PER_ITERATION * 2,
                "metrics not loaded as expected",
            )
            self.assertEqual(
                algo_1_restored_x2.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                ENV_STEPS_PER_ITERATION * 2,
                "metrics not loaded as expected",
            )
            # Step 3 from restored
            result_algo0_step3_restored = algo_0_runner_restored.step()
            result_algo1_step3_restored = algo_1_runner_restored.step()
            result_algo0_step3_restored_x2 = algo_0_restored_x2.step()
            result_algo1_step3_restored_x2 = algo_1_restored_x2.step()

            # Test that all results after step 3 are have 300 steps
            with self.subTest("From checkpoint: env_runners=0 - Step 3"):
                self.assertEqual(
                    result_algo_0_step3[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                    result_algo0_step3_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=0 - Step 3 (restored x2)"):
                self.assertEqual(
                    result_algo_0_step3[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                    result_algo0_step3_restored_x2[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=1 - Step 3"):
                self.assertEqual(
                    result_algo_1_step3[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                    result_algo1_step3_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=1 - Step 3 (restored x2)"):
                self.assertEqual(
                    result_algo_1_step3[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                    result_algo1_step3_restored_x2[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
