import os
import unittest

import ray
import ray.rllib.algorithms.impala as impala
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_off_policyness,
    check_train_results,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_compilation(self):
        """Test whether Impala can be built with both frameworks."""
        config = (
            impala.ImpalaConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .environment("CartPole-v1")
            .training(
                model={
                    "lstm_use_prev_action": True,
                    "lstm_use_prev_reward": True,
                },
            )
        )
        num_iterations = 2

        for _ in framework_iterator(config, with_eager_tracing=True):
            for lstm in [False, True]:
                config.num_aggregation_workers = 0 if not lstm else 1
                config.model["use_lstm"] = lstm
                print(
                    "lstm={} aggregation-workers={}".format(
                        lstm, config.num_aggregation_workers
                    )
                )
                # Test with and w/o aggregation workers (this has nothing
                # to do with LSTMs, though).
                algo = config.build()
                for i in range(num_iterations):
                    results = algo.train()
                    print(results)
                    check_train_results(results)
                    off_policy_ness = check_off_policyness(results, upper_limit=2.0)
                    print(f"off-policy'ness={off_policy_ness}")

                check_compute_single_action(
                    algo,
                    include_state=lstm,
                    include_prev_action_reward=lstm,
                )
                algo.stop()

    def test_impala_lr_schedule(self):
        # Test whether we correctly ignore the "lr" setting.
        # The first lr should be 0.05.
        config = (
            impala.ImpalaConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .training(
                lr=0.1,
                lr_schedule=[
                    [0, 0.05],
                    [100000, 0.000001],
                ],
                train_batch_size=100,
            )
            .rollouts(num_envs_per_worker=2)
            .environment(env="CartPole-v1")
        )

        def get_lr(result):
            return result["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY][
                "cur_lr"
            ]

        for fw in framework_iterator(config):
            algo = config.build()
            policy = algo.get_policy()

            try:
                if fw == "tf":
                    check(policy.get_session().run(policy.cur_lr), 0.05)
                else:
                    check(policy.cur_lr, 0.05)
                for _ in range(1):
                    r1 = algo.train()
                for _ in range(2):
                    r2 = algo.train()
                for _ in range(2):
                    r3 = algo.train()
                # Due to the asynch'ness of IMPALA, learner-stats metrics
                # could be delayed by one iteration. Do 3 train() calls here
                # and measure guaranteed decrease in lr between 1st and 3rd.
                lr1 = get_lr(r1)
                lr2 = get_lr(r2)
                lr3 = get_lr(r3)
                assert lr2 <= lr1, (lr1, lr2)
                assert lr3 <= lr2, (lr2, lr3)
                assert lr3 < lr1, (lr1, lr3)
            finally:
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
