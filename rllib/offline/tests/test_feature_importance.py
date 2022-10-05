import unittest
import ray

from ray.rllib.algorithms.crr import CRRConfig, CRR
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.offline.estimators.feature_importance import FeatureImportance


class TestFeatureImportance(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_feat_importance_cartpole(self):
        config = CRRConfig().framework("torch")
        runner = CRR(config, env="CartPole-v0")
        policy = runner.workers.local_worker().get_policy()
        sample_batch = synchronous_parallel_sample(worker_set=runner.workers)

        for repeat in [1, 10]:
            evaluator = FeatureImportance(policy=policy, repeat=repeat)

            estimate = evaluator.estimate(sample_batch)

            # check if the estimate is positive
            assert all([val > 0 for val in estimate.values()])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
