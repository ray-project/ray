import gym
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID


class TestWorkerSet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_foreach_worker(self):
        """Test to make sure basic sychronous calls to remote workers work."""
        ws = WorkerSet(
            env_creator=lambda _: gym.make("CartPole-v1"),
            default_policy_class=RandomPolicy,
            config=AlgorithmConfig().rollouts(num_rollout_workers=2),
            num_workers=2,
        )

        policies = ws.foreach_worker(
            lambda w: w.get_policy(DEFAULT_POLICY_ID),
            local_worker=True,
        )

        # 3 policies including the one from the local worker.
        self.assertEqual(len(policies), 3)
        for p in policies:
            self.assertIsInstance(p, RandomPolicy)

        policies = ws.foreach_worker(
            lambda w: w.get_policy(DEFAULT_POLICY_ID),
            local_worker=False,
        )

        # 3 policies from only the remote workers.
        self.assertEqual(len(policies), 2)

        ws.stop()

    def test_foreach_worker_async(self):
        """Test to make sure basic asychronous calls to remote workers work."""
        ws = WorkerSet(
            env_creator=lambda _: gym.make("CartPole-v1"),
            default_policy_class=RandomPolicy,
            config=AlgorithmConfig().rollouts(num_rollout_workers=2),
            num_workers=2,
        )

        # Fired async request against both remote workers.
        self.assertEqual(
            ws.foreach_worker_async(
                lambda w: w.get_policy(DEFAULT_POLICY_ID),
            ),
            2,
        )

        remote_results = ws.fetch_ready_async_reqs(timeout_seconds=None)
        self.assertEqual(len(remote_results), 2)
        for p in remote_results:
            self.assertIsInstance(p[1], RandomPolicy)

        ws.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
