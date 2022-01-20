import unittest

import ray
import ray.rllib.agents.bandits.lin_ts as lin_ts
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
        """Test whether a BanditLinTSTrainer can be built on all frameworks.
        """
        config = lin_ts.DEFAULT_CONFIG.copy()
        # Use a simple bandit friendly env.
        config["env"] = SimpleContextualBandit
        # Run locally.
        config["num_workers"] = 0

        num_iterations = 5

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = lin_ts.BanditLinTSTrainer(config=config)
            results = None
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
            # Force good learning behavior (this is a very simple env).
            self.assertTrue(results["episode_reward_mean"] == 10.0)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
