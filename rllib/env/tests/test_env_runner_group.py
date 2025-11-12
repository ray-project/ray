import time
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.env_runner_group import EnvRunnerGroup


class TestEnvRunnerGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_foreach_env_runner(self):
        """Test to make sure basic sychronous calls to remote workers work."""
        ws = EnvRunnerGroup(
            config=(
                PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=2)
            ),
        )

        modules = ws.foreach_env_runner(
            lambda w: w.module,
            local_env_runner=True,
        )

        # 3 policies including the one from the local worker.
        self.assertEqual(len(modules), 3)
        for m in modules:
            self.assertIsInstance(m, RLModule)

        modules = ws.foreach_env_runner(
            lambda w: w.module,
            local_env_runner=False,
        )

        # 2 policies from only the remote workers.
        self.assertEqual(len(modules), 2)

        ws.stop()

    def test_foreach_env_runner_return_obj_refss(self):
        """Test to make sure return_obj_refs parameter works."""
        ws = EnvRunnerGroup(
            config=(
                PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=2)
            ),
        )

        module_refs = ws.foreach_env_runner(
            lambda w: isinstance(w.module, RLModule),
            local_env_runner=False,
            return_obj_refs=True,
        )

        # 2 policy references from remote workers.
        self.assertEqual(len(module_refs), 2)
        self.assertTrue(isinstance(module_refs[0], ray.ObjectRef))
        self.assertTrue(isinstance(module_refs[1], ray.ObjectRef))

        ws.stop()

    def test_foreach_env_runner_async(self):
        """Test to make sure basic asychronous calls to remote workers work."""
        ws = EnvRunnerGroup(
            config=(
                PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=2)
            ),
        )

        # Fired async request against both remote workers.
        self.assertEqual(
            ws.foreach_env_runner_async(
                lambda w: isinstance(w.module, RLModule),
            ),
            2,
        )

        remote_results = ws.fetch_ready_async_reqs(timeout_seconds=None)
        self.assertEqual(len(remote_results), 2)
        for p in remote_results:
            # p is in the format of (worker_id, result).
            # First is the id of the remote worker.
            self.assertTrue(p[0] in [1, 2])
            # Next is the actual policy.
            self.assertTrue(p[1])

        ws.stop()

    def test_foreach_env_runner_async_fetch_ready(self):
        """Test to make sure that test_foreach_env_runner_async_fetch_ready works."""
        ws = EnvRunnerGroup(
            config=(
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_env_runners=2, rollout_fragment_length=1)
            ),
        )

        # Sample from both env runners.
        # First call to foreach_env_runner_async_fetch_ready should not return ready results.
        self.assertEqual(
            len(
                ws.foreach_env_runner_async_fetch_ready(
                    lambda w: w.sample(),
                    tag="sample",
                )
            ),
            0,
        )
        time.sleep(1)

        # Second call to foreach_env_runner_async_fetch_ready should return ready results.
        self.assertEqual(
            len(
                ws.foreach_env_runner_async_fetch_ready(
                    lambda w: w.sample(),
                    tag="sample",
                )
            ),
            2,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
