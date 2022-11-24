import unittest

import ray
import ray.rllib.algorithms.impala as impala
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

        config = impala.ImpalaConfig().environment("CartPole-v1")

        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                RuntimeError,
                # (?s): "dot matches all" (also newlines).
                "(?s)Found 0 GPUs on your machine.+To change the config",
                lambda: config.build(),
            )

    def test_bad_envs(self):
        """Tests different "bad env" errors."""
        config = (
            pg.PGConfig().rollouts(num_rollout_workers=0)
            # Non existing/non-registered gym env string.
            .environment("Alien-Attack-v42")
        )

        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{config.env}'\\) is",
                lambda: config.build(),
            )

        # Malformed gym env string (must have v\d at end).
        config.environment("Alien-Attack-part-42")
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{config.env}'\\) is",
                lambda: config.build(),
            )

        # Non-existing class in a full-class-path.
        config.environment("ray.rllib.examples.env.random_env.RandomEnvThatDoesntExist")
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{config.env}'\\) is",
                lambda: config.build(),
            )

        # Non-existing module inside a full-class-path.
        config.environment("ray.rllib.examples.env.module_that_doesnt_exist.SomeEnv")
        for _ in framework_iterator(config):
            self.assertRaisesRegex(
                EnvError,
                f"The env string you provided \\('{config.env}'\\) is",
                lambda: config.build(),
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
