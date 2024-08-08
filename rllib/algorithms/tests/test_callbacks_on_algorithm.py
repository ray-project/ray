import tempfile
import time
import unittest

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.cartpole_crashing import CartPoleCrashing
from ray.rllib.utils.test_utils import framework_iterator
from ray import tune


class OnWorkersRecreatedCallbacks(DefaultCallbacks):
    def on_workers_recreated(
        self,
        *,
        algorithm,
        worker_set,
        worker_ids,
        is_evaluation,
        **kwargs,
    ):
        # Store in the algorithm object's counters the number of times, this worker
        # (ID'd by index and whether eval or not) has been recreated/restarted.
        for id_ in worker_ids:
            key = f"{'eval_' if is_evaluation else ''}worker_{id_}_recreated"
            # Increase the counter.
            algorithm._counters[key] += 1
            print(f"changed {key} to {algorithm._counters[key]}")

        # Execute some dummy code on each of the recreated workers.
        results = worker_set.foreach_worker(lambda w: w.ping())
        print(results)  # should print "pong" n times (one for each recreated worker).


class InitAndCheckpointRestoredCallbacks(DefaultCallbacks):
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

    def test_on_workers_recreated_callback(self):
        tune.register_env("env", lambda cfg: CartPoleCrashing(cfg))

        config = (
            APPOConfig()
            .environment("env")
            .callbacks(OnWorkersRecreatedCallbacks)
            .env_runners(num_env_runners=3)
            .fault_tolerance(
                recreate_failed_env_runners=True,
                delay_between_env_runner_restarts_s=0,
            )
        )

        algo = config.build()
        original_worker_ids = algo.env_runner_group.healthy_worker_ids()
        for id_ in original_worker_ids:
            self.assertTrue(algo._counters[f"worker_{id_}_recreated"] == 0)
        self.assertTrue(algo._counters["total_num_workers_recreated"] == 0)

        # After building the algorithm, we should have 2 healthy (remote) workers.
        self.assertTrue(len(original_worker_ids) == 3)

        # Train a bit (and have the envs/workers crash).
        for _ in range(3):
            print(algo.train())
        # Restore workers after the iteration (automatically, workers are only
        # restored before the next iteration).
        time.sleep(20.0)
        algo.restore_workers(algo.env_runner_group)
        # After training, the `on_workers_recreated` callback should have captured
        # the exact worker IDs recreated (the exact number of times) as the actor
        # manager itself. This confirms that the callback is triggered correctly,
        # always.
        new_worker_ids = algo.env_runner_group.healthy_worker_ids()
        self.assertEquals(len(new_worker_ids), 3)
        for id_ in new_worker_ids:
            # num_restored = algo.env_runner_group.restored_actors_history[id_]
            self.assertTrue(algo._counters[f"worker_{id_}_recreated"] > 1)
        algo.stop()

    def test_on_init_and_checkpoint_loaded(self):
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .callbacks(InitAndCheckpointRestoredCallbacks)
        )
        for _ in framework_iterator(config, frameworks=("torch", "tf2")):
            algo = config.build()
            self.assertTrue(algo.callbacks._on_init_was_called)
            self.assertTrue(
                not hasattr(algo.callbacks, "_on_checkpoint_loaded_was_called")
            )
            algo.train()
            # Save algo and restore.
            with tempfile.TemporaryDirectory() as tmpdir:
                algo.save(checkpoint_dir=tmpdir)
                self.assertTrue(
                    not hasattr(algo.callbacks, "_on_checkpoint_loaded_was_called")
                )
                algo.load_checkpoint(checkpoint_dir=tmpdir)
                self.assertTrue(algo.callbacks._on_checkpoint_loaded_was_called)
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
