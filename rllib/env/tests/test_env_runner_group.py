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

    def test_num_env_runners_dropped_lifetime_no_drops(self):
        """No EnvRunner should be reported as dropped when calls complete in time."""
        ws = EnvRunnerGroup(
            config=(
                PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=2)
            ),
        )

        # Baseline: counter starts at zero.
        self.assertEqual(ws.num_env_runners_dropped_lifetime(), 0)

        # A fast, timeout-bounded call should not register any drops.
        results = ws.foreach_env_runner(
            lambda w: 1,
            local_env_runner=False,
            timeout_seconds=10.0,
        )
        self.assertEqual(len(results), 2)
        self.assertEqual(ws.num_env_runners_dropped_lifetime(), 0)

        # A non-timeout-bounded call must never increment the counter, even if
        # it returned fewer results than the number of remote actors.
        ws.foreach_env_runner(
            lambda w: 1,
            local_env_runner=False,
            timeout_seconds=None,
        )
        self.assertEqual(ws.num_env_runners_dropped_lifetime(), 0)

        ws.stop()

    def test_num_env_runners_dropped_lifetime_ignores_fire_and_forget(self):
        """Calls with ``timeout_seconds == 0`` must NOT inflate the counter.

        ``sync_weights`` defaults to ``timeout_seconds=0.0`` (fire-and-forget)
        and propagates that into ``foreach_env_runner``; under such calls
        ``ray.wait(timeout=0.0)`` returns immediately and typically with zero
        results. Treating that as a drop would make the metric meaningless
        in normal training.
        """
        ws = EnvRunnerGroup(
            config=(
                PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=2)
            ),
        )

        # Make the remote call slow relative to ``timeout_seconds=0.0`` so
        # ``ray.wait(timeout=0.0)`` returns with zero results. A short sleep
        # is enough; we don't need the workers to be busy for long, just
        # long enough for the fire-and-forget poll to come back empty.
        def _slow(w):
            time.sleep(0.5)
            return 1

        ws.foreach_env_runner(
            _slow,
            local_env_runner=False,
            timeout_seconds=0.0,
        )
        self.assertEqual(ws.num_env_runners_dropped_lifetime(), 0)

        ws.stop()

    def test_num_env_runners_dropped_lifetime_counts_timeouts(self):
        """Verify the lifetime counter increments when remote calls time out."""
        ws = EnvRunnerGroup(
            config=(
                PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=2)
            ),
        )

        self.assertEqual(ws.num_env_runners_dropped_lifetime(), 0)

        # Force both remote workers to exceed a short positive timeout by
        # sleeping in the remote call. The sleep just needs to exceed the
        # timeout; keeping it small keeps the test fast in CI and avoids
        # leaving long-running actor work alive past the test boundary.
        def _slow(w):
            time.sleep(0.5)
            return 1

        results = ws.foreach_env_runner(
            _slow,
            local_env_runner=False,
            timeout_seconds=0.05,
        )
        self.assertEqual(len(results), 0)
        self.assertEqual(ws.num_env_runners_dropped_lifetime(), 2)

        ws.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
