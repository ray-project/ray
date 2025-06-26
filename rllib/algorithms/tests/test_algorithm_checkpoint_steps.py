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
        config = PPOConfig()
        config.training(
            train_batch_size_per_learner=100, num_epochs=2, minibatch_size=50
        )
        config.debugging(
            seed=11,
            log_sys_usage=False,
        )
        config.reporting(metrics_num_episodes_for_smoothing=1)
        config.environment(env="CartPole-v1")
        algo0 = config.env_runners(
            num_env_runners=0,
        ).build_algo()
        algo1 = config.env_runners(
            num_env_runners=1,
        ).build_algo()
        assert algo0.config and algo1.config
        self.assertEqual(algo0.config.num_env_runners, 0)
        self.assertEqual(algo1.config.num_env_runners, 1)
        _result0a = algo0.step()  # Step 1
        _result1a = algo1.step()  # Step 1
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
            algo0.save_checkpoint(checkpoint_0_step1)
            algo1.save_checkpoint(checkpoint_1_step1)
            # Step 2
            result0_step2 = algo0.step()
            result1_step2 = algo1.step()
            self.assertEqual(
                result0_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                result1_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
            )
            self.assertEqual(
                result0_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                200,
            )
            # Save Step 2
            algo0.save_checkpoint(checkpoint_0_step2)
            algo1.save_checkpoint(checkpoint_1_step2)
            # Step 3
            result0_step3 = algo0.step()
            result1_step3 = algo1.step()
            self.assertEqual(
                result0_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                result1_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
            )
            self.assertEqual(
                result0_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                300,
            )
            # Load Step 1
            algo0_restored = PPO.from_checkpoint(checkpoint_0_step1)
            algo1_restored = PPO.from_checkpoint(checkpoint_1_step1)
            assert algo0_restored.metrics and algo1_restored.metrics
            # Check loaded metric
            self.assertEqual(
                algo0_restored.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                100,
            )
            self.assertEqual(
                algo0_restored.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
                algo1_restored.metrics.peek(
                    (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                ),
            )
            tree.assert_same_structure(algo0_restored.metrics, algo1_restored.metrics)
            # Step 2 from restored & checkpoint
            result_algo0_step2_restored = algo0_restored.step()
            result_algo1_step2_restored = algo1_restored.step()
            algo0.save_checkpoint(checkpoint_0_step2_restored)
            algo1.save_checkpoint(checkpoint_1_step2_restored)

            with self.subTest("From checkpoint: env_runners=0 - Step 2"):
                self.assertEqual(
                    result0_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                    result_algo0_step2_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=1 - Step 2"):
                self.assertEqual(
                    result1_step2[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                    result_algo1_step2_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )

            # Load Restored from step 2
            algo0_restored_x2 = PPO.from_checkpoint(checkpoint_0_step2_restored)
            algo1_restored_x2 = PPO.from_checkpoint(checkpoint_1_step2_restored)
            # Step 3 from restored
            result_algo0_step3_restored = algo0_restored.step()
            result_algo1_step3_restored = algo1_restored.step()
            result_algo0_step3_restored_x2 = algo0_restored_x2.step()
            result_algo1_step3_restored_x2 = algo1_restored_x2.step()

            with self.subTest("From checkpoint: env_runners=0 - Step 3"):
                self.assertEqual(
                    result0_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                    result_algo0_step3_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=0 - Step 3 (restored x2)"):
                self.assertEqual(
                    result0_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                    result_algo0_step3_restored_x2[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=1 - Step 3"):
                self.assertEqual(
                    result1_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                    result_algo1_step3_restored[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )
            with self.subTest("From checkpoint: env_runners=1 - Step 3 (restored x2)"):
                self.assertEqual(
                    result1_step3[ENV_RUNNER_RESULTS][NUM_ENV_STEPS_SAMPLED_LIFETIME],
                    result_algo1_step3_restored_x2[ENV_RUNNER_RESULTS][
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
                    ],
                )


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
