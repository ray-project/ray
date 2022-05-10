import unittest

import ray
import ray.rllib.agents.bandit.bandit as bandit
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
        """Test whether a BanditLinTSTrainer can be built on all frameworks."""
        config = {
            # Use a simple bandit-friendly env.
            "env": SimpleContextualBandit,
            "num_envs_per_worker": 2,  # Test batched inference.
            "num_workers": 2,  # Test distributed bandits.
        }

        num_iterations = 5

        for _ in framework_iterator(config, frameworks="torch"):
            for train_batch_size in [1, 10]:
                config["train_batch_size"] = train_batch_size
                trainer = bandit.BanditLinTSTrainer(config=config)
                results = None
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)
                # Force good learning behavior (this is a very simple env).
                self.assertTrue(results["episode_reward_mean"] == 10.0)
                trainer.stop()

    def test_bandit_lin_ucb_compilation(self):
        """Test whether a BanditLinUCBTrainer can be built on all frameworks."""
        config = {
            # Use a simple bandit-friendly env.
            "env": SimpleContextualBandit,
            "num_envs_per_worker": 2,  # Test batched inference.
        }

        num_iterations = 5

        for _ in framework_iterator(config, frameworks="torch"):
            for train_batch_size in [1, 10]:
                config["train_batch_size"] = train_batch_size
                trainer = bandit.BanditLinUCBTrainer(config=config)
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
