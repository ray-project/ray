import unittest

import ray
import ray.rllib.agents.impala as impala
import ray.rllib.algorithms.pg as pg
from ray.rllib.utils.error import EnvError
from ray.rllib.utils.test_utils import framework_iterator


class TestErrors(unittest.TestCase):
    """Tests various failure-modes, making sure we produce meaningful errmsgs."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_no_gpus_error(self):
        """Tests errors related to no-GPU/too-few GPUs/etc.

        This test will only work ok on a CPU-only machine.
        """

        config = impala.DEFAULT_CONFIG.copy()
        env = "CartPole-v0"

        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                RuntimeError,
                # (?s): "dot matches all" (also newlines).
                "(?s)Found 0 GPUs on your machine.+To change the config",
                lambda: impala.ImpalaTrainer(config=config, env=env),
            )

    def test_bad_envs(self):
        """Tests different "bad env" errors."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0

        # Non existing/non-registered gym env string.
        env = "Alien-Attack-v42"
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{env}'\\) is",
                lambda: pg.PGTrainer(config=config, env=env),
            )

        # Malformed gym env string (must have v\d at end).
        env = "Alien-Attack-part-42"
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{env}'\\) is",
                lambda: pg.PGTrainer(config=config, env=env),
            )

        # Non-existing class in a full-class-path.
        env = "ray.rllib.examples.env.random_env.RandomEnvThatDoesntExist"
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{env}'\\) is",
                lambda: pg.PGTrainer(config=config, env=env),
            )

        # Non-existing module inside a full-class-path.
        env = "ray.rllib.examples.env.module_that_doesnt_exist.SomeEnv"
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{env}'\\) is",
                lambda: pg.PGTrainer(config=config, env=env),
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
