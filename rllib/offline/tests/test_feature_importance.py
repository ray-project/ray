import unittest

import ray
from ray.rllib.algorithms.marwil import MARWILConfig
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.offline.feature_importance import FeatureImportance


class TestFeatureImportance(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_feat_importance_cartpole(self):
        config = (
            MARWILConfig()
            .api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )
            .environment("CartPole-v1")
            .framework("torch")
        )
        algo = config.build()
        policy = algo.env_runner.get_policy()
        sample_batch = synchronous_parallel_sample(worker_set=algo.env_runner_group)

        for repeat in [1, 10]:
            evaluator = FeatureImportance(policy=policy, repeat=repeat)

            estimate = evaluator.estimate(sample_batch)

            # Check if the estimate is positive.
            assert all(val > 0 for val in estimate.values())

    def test_feat_importance_estimate_on_dataset(self):
        # TODO (Kourosh): add a test for this
        pass


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
