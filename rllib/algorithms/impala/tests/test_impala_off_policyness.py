import unittest

import ray
import ray.rllib.algorithms.impala as impala
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_off_policyness,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()


class TestIMPALAOffPolicyNess(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_gpus=1)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_off_policyness(self):
        config = (
            impala.ImpalaConfig()
            .environment("CartPole-v1")
            .resources(num_gpus=1)
            .rollouts(num_rollout_workers=4)
        )
        num_iterations = 3

        for _ in framework_iterator(config, with_eager_tracing=True):
            for num_aggregation_workers in [0, 1]:
                config.num_aggregation_workers = num_aggregation_workers
                print("aggregation-workers={}".format(config.num_aggregation_workers))
                algo = config.build()
                for i in range(num_iterations):
                    results = algo.train()
                    off_policy_ness = check_off_policyness(results, upper_limit=2.0)
                    print(f"off-policy'ness={off_policy_ness}")

                check_compute_single_action(
                    algo,
                )
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
