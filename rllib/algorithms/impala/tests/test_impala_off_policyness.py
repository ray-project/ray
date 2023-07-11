import unittest

import ray
import ray.rllib.algorithms.impala as impala
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()


class TestIMPALAOffPolicyNess(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_off_policyness(self):
        config = (
            impala.ImpalaConfig()
            .environment("CartPole-v1")
            .resources(num_gpus=0)
            .rollouts(num_rollout_workers=4)
            .training(_enable_learner_api=True)
            .rl_module(_enable_rl_module_api=True)
        )
        num_iterations = 3
        num_aggregation_workers_options = [0, 1]

        for num_aggregation_workers in num_aggregation_workers_options:
            for _ in framework_iterator(config, frameworks=("tf2", "torch")):

                # We have to set exploration_config here manually because setting
                # it through config.exploration() only deepupdates it
                config.exploration_config = {}
                config.num_aggregation_workers = num_aggregation_workers
                print("aggregation-workers={}".format(config.num_aggregation_workers))
                algo = config.build()
                for i in range(num_iterations):
                    algo.train()
                    # TODO (Avnish): Add off-policiness check when the metrics are
                    #  added back to the IMPALA Learner.
                    # off_policy_ness = check_off_policyness(results, upper_limit=2.0)
                    # print(f"off-policy'ness={off_policy_ness}")

                check_compute_single_action(
                    algo,
                )
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
