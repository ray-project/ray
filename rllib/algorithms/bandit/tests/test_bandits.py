import unittest

import ray
from ray.rllib.algorithms.bandit import bandit
from ray.rllib.examples.env.bandit_envs_discrete import SimpleContextualBandit
from ray.rllib.utils.test_utils import check_train_results, framework_iterator


class TestBandits(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_bandit_lin_ts_compilation(self):
        """Test whether BanditLinTS can be built on all frameworks."""
        config = (
            bandit.BanditLinTSConfig()
            .environment(env=SimpleContextualBandit)
            .rollouts(num_rollout_workers=2, num_envs_per_worker=2)
        )
        num_iterations = 5

        for _ in framework_iterator(
            config, frameworks=("tf2", "torch"), with_eager_tracing=True
        ):
            for train_batch_size in [1, 10]:
                config.training(train_batch_size=train_batch_size)
                algo = config.build()
                results = None
                for _ in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                # Force good learning behavior (this is a very simple env).
                self.assertTrue(results["episode_reward_mean"] == 10.0)
                algo.stop()

    def test_bandit_lin_ucb_compilation(self):
        """Test whether BanditLinUCB can be built on all frameworks."""
        config = (
            bandit.BanditLinUCBConfig()
            .environment(env=SimpleContextualBandit)
            .rollouts(num_envs_per_worker=2)
        )

        num_iterations = 5

        for _ in framework_iterator(
            config, frameworks=("tf2", "torch"), with_eager_tracing=True
        ):
            for train_batch_size in [1, 10]:
                config.training(train_batch_size=train_batch_size)
                trainer = config.build()
                results = None
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)
                # Force good learning behavior (this is a very simple env).
                self.assertTrue(results["episode_reward_mean"] == 10.0)
                trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
