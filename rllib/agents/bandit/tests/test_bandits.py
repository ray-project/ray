import unittest

import ray
from ray import tune
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
            # Use a simple bandit friendly env.
            "env": SimpleContextualBandit,
        }

        num_iterations = 5

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = bandit.BanditLinTSTrainer(config=config)
            results = None
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
            # Force good learning behavior (this is a very simple env).
            self.assertTrue(results["episode_reward_mean"] == 10.0)

    def test_bandit_lin_ucb_compilation(self):
        """Test whether a BanditLinUCBTrainer can be built on all frameworks."""
        config = {
            # Use a simple bandit friendly env.
            "env": SimpleContextualBandit,
        }

        num_iterations = 5

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = bandit.BanditLinUCBTrainer(config=config)
            results = None
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
            # Force good learning behavior (this is a very simple env).
            self.assertTrue(results["episode_reward_mean"] == 10.0)

    def test_deprecated_locations(self):
        """Tests, whether importing from old contrib dir fails gracefully.

        Also checks for proper handling of tune.run("contrib/Lin...").
        """

        def try_import_lints():
            from ray.rllib.contrib.bandits.agents.lin_ts import LinTS  # noqa

        self.assertRaisesRegex(
            DeprecationWarning, "has been deprecated. Use", try_import_lints
        )

        def try_import_linucb():
            from ray.rllib.contrib.bandits.agents.lin_ucb import LinUCB  # noqa

        self.assertRaisesRegex(
            DeprecationWarning, "has been deprecated. Use", try_import_linucb
        )

        def try_import_anything():
            from ray.rllib.contrib.bandits.some_crazy_module import (  # noqa: F401
                SomeCrazyClass,
            )

        self.assertRaisesRegex(
            DeprecationWarning, "has been deprecated. Use", try_import_anything
        )

        # Assert that tune also raises an error.
        self.assertRaises(ray.tune.error.TuneError, lambda: tune.run("contrib/LinTS"))

        self.assertRaises(ray.tune.error.TuneError, lambda: tune.run("contrib/LinUCB"))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
