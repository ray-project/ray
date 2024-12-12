import unittest

import ray
import ray.rllib.algorithms.impala as impala
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.utils.error import EnvError


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

        config = (
            impala.IMPALAConfig()
            .api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )
            .environment("CartPole-v1")
        )

        self.assertRaisesRegex(
            RuntimeError,
            # (?s): "dot matches all" (also newlines).
            "(?s)Found 0 GPUs on your machine.+To change the config",
            lambda: config.build(),
        )

    def test_bad_envs(self):
        """Tests different "bad env" errors."""
        config = (
            ppo.PPOConfig().env_runners(num_env_runners=0)
            # Non existing/non-registered gym env string.
            .environment("Alien-Attack-v42")
        )

        self.assertRaisesRegex(
            EnvError,
            f"The env string you provided \\('{config.env}'\\) is",
            lambda: config.build(),
        )

        # Malformed gym env string (must have v\d at end).
        config.environment("Alien-Attack-part-42")
        self.assertRaisesRegex(
            EnvError,
            f"The env string you provided \\('{config.env}'\\) is",
            lambda: config.build(),
        )

        # Non-existing class in a full-class-path.
        config.environment(
            "ray.rllib.examples.envs.classes.random_env.RandomEnvThatDoesntExist"
        )
        self.assertRaisesRegex(
            EnvError,
            f"The env string you provided \\('{config.env}'\\) is",
            lambda: config.build(),
        )

        # Non-existing module inside a full-class-path.
        config.environment("ray.rllib.examples.envs.module_that_doesnt_exist.SomeEnv")
        self.assertRaisesRegex(
            EnvError,
            f"The env string you provided \\('{config.env}'\\) is",
            lambda: config.build(),
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
