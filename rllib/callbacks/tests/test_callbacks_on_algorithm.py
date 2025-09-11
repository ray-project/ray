import tempfile
import time
import unittest

import ray
from ray import tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.examples.envs.classes.cartpole_crashing import CartPoleCrashing
from ray.rllib.utils.test_utils import check


def on_env_runners_recreated(
    algorithm,
    env_runner_group,
    env_runner_indices,
    is_evaluation,
    **kwargs,
):
    # Store in the algorithm object's counters the number of times, this worker
    # (ID'd by index and whether eval or not) has been recreated/restarted.
    for id_ in env_runner_indices:
        key = f"{'eval_' if is_evaluation else ''}worker_{id_}_recreated"
        # Increase the counter.
        algorithm.metrics.log_value(key, 1, reduce="sum")
        print(f"changed {key} to {algorithm._counters[key]}")

    # Execute some dummy code on each of the recreated workers.
    results = env_runner_group.foreach_env_runner(lambda w: w.ping())
    print(results)  # should print "pong" n times (one for each recreated worker).


class InitAndCheckpointRestoredCallbacks(RLlibCallback):
    def on_algorithm_init(self, *, algorithm, metrics_logger, **kwargs):
        self._on_init_was_called = True

    def on_checkpoint_loaded(self, *, algorithm, **kwargs):
        self._on_checkpoint_loaded_was_called = True


class TestCallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_on_env_runners_recreated_callback(self):
        tune.register_env("env", lambda cfg: CartPoleCrashing(cfg))

        config = (
            PPOConfig()
            .environment("env", env_config={"p_crash": 1.0})
            .callbacks(on_env_runners_recreated=on_env_runners_recreated)
            .env_runners(num_env_runners=3)
            .fault_tolerance(
                restart_failed_env_runners=True,
                delay_between_env_runner_restarts_s=0,
            )
        )

        algo = config.build()
        original_env_runner_ids = algo.env_runner_group.healthy_worker_ids()
        for id_ in original_env_runner_ids:
            check(algo.metrics.peek(f"worker_{id_}_recreated", default=0), 0)
        check(algo.metrics.peek("total_num_workers_recreated", default=0), 0)

        # After building the algorithm, we should have 2 healthy (remote) workers.
        self.assertTrue(len(original_env_runner_ids) == 3)

        # Train a bit (and have the envs/workers crash).
        for _ in range(3):
            print(algo.train())
            time.sleep(15.0)

        algo.restore_env_runners(algo.env_runner_group)
        # After training, the `on_workers_recreated` callback should have captured
        # the exact worker IDs recreated (the exact number of times) as the actor
        # manager itself. This confirms that the callback is triggered correctly,
        # always.
        new_worker_ids = algo.env_runner_group.healthy_worker_ids()
        self.assertEqual(len(new_worker_ids), 3)
        for id_ in new_worker_ids:
            # num_restored = algo.env_runner_group.restored_actors_history[id_]
            self.assertTrue(algo.metrics.peek(f"worker_{id_}_recreated") > 1)
        algo.stop()

    def test_on_init_and_checkpoint_loaded(self):
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .callbacks(InitAndCheckpointRestoredCallbacks)
        )
        algo = config.build()
        callbacks = algo.callbacks[0]
        self.assertTrue(callbacks._on_init_was_called)
        self.assertTrue(not hasattr(callbacks, "_on_checkpoint_loaded_was_called"))
        algo.train()
        # Save algo and restore.
        with tempfile.TemporaryDirectory() as tmpdir:
            algo.save(checkpoint_dir=tmpdir)
            self.assertTrue(not hasattr(callbacks, "_on_checkpoint_loaded_was_called"))
            algo.load_checkpoint(checkpoint_dir=tmpdir)
            self.assertTrue(callbacks._on_checkpoint_loaded_was_called)
        algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
