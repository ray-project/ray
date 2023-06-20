import unittest

from rllib_crr.crr import CRRConfig

import ray
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check_compute_single_action, check_train_results

torch, _ = try_import_torch()


class TestCRR(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_crr_compilation(self):
        """Test whether a CRR algorithm can be built with all supported frameworks."""

        config = (
            CRRConfig()
            .environment(env="Pendulum-v1", clip_actions=True)
            .framework("torch")
            .offline_data(
                input_="dataset",
                input_config={
                    "format": "json",
                    "paths": [
                        "s3://anonymous@air-example-data/rllib/pendulum/large.json"
                    ],
                },
                actions_in_input_normalized=True,
            )
            .training(
                twin_q=True,
                train_batch_size=256,
                weight_type="bin",
                advantage_type="mean",
                n_action_sample=4,
                target_network_update_freq=10000,
                tau=1.0,
            )
            .evaluation(
                evaluation_interval=2,
                evaluation_num_workers=2,
                evaluation_duration=10,
                evaluation_duration_unit="episodes",
                evaluation_parallel_to_training=True,
                evaluation_config=CRRConfig.overrides(input_="sampler", explore=False),
            )
            .rollouts(num_rollout_workers=0)
        )

        num_iterations = 4

        for _ in ["torch"]:
            for loss_fn in ["mse", "huber"]:
                config.td_error_loss_fn = loss_fn
                algorithm = config.build()
                # check if 4 iterations raises any errors
                for i in range(num_iterations):
                    results = algorithm.train()
                    check_train_results(results)
                    print(results)
                    if (i + 1) % 2 == 0:
                        # evaluation happens every 2 iterations
                        eval_results = results["evaluation"]
                        print(
                            f"iter={algorithm.iteration} "
                            f"R={eval_results['episode_reward_mean']}"
                        )

                check_compute_single_action(algorithm)

                algorithm.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
