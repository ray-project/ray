import unittest

import ray
from ray._private.test_utils import get_other_nodes
from ray.cluster_utils import Cluster
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)


num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 3

assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)


class TestNodeFailures(unittest.TestCase):
    def setUp(self):
        # Simulate a cluster on one machine.
        self.cluster = Cluster()

        for i in range(num_nodes):
            self.cluster.add_node(
                redis_port=6379 if i == 0 else None,
                num_redis_shards=num_redis_shards if i == 0 else None,
                num_cpus=2,
                num_gpus=0,
                object_store_memory=object_store_memory,
                redis_max_memory=redis_max_memory,
                dashboard_host="0.0.0.0",
            )
        self.cluster.wait_for_nodes()
        ray.init(address=self.cluster.address)

    def tearDown(self):
        ray.shutdown()
        self.cluster.shutdown()

    def test_node_failure_ignore(self):
        # We ignore EnvRunners once failed nodes have come back and continue training
        # with fewer EnvRunners.
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=6,
                validate_env_runners_after_construction=True,
            )
            .fault_tolerance(
                ignore_env_runner_failures=True,
                recreate_failed_env_runners=False,
            )
        )

        self._train(config=config, iters=10, min_reward=150.0, preempt_freq=4)

    def test_node_failure_recreate_env_runners(self):
        # We recreate failed EnvRunners and continue training.
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=6,
                validate_env_runners_after_construction=True,
            )
            .fault_tolerance(
                recreate_failed_env_runners=True,
                ignore_env_runner_failures=False,  # True also ok here we recreate.
            )
        )

        self._train(config=config, iters=30, min_reward=450.0, preempt_freq=5)

    def test_node_failure_expect_crash(self):
        # We do not ignore EnvRunner failures and expect to crash upon failure.
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=6,
                validate_env_runners_after_construction=True,
            )
            .fault_tolerance(
                ignore_env_runner_failures=False,
                recreate_failed_env_runners=False,
            )
        )

        self.assertRaisesRegex(
            ray.exceptions.RayError,
            "The actor died unexpectedly before",
            lambda: (
                self._train(config=config, iters=10, min_reward=1000.0, preempt_freq=2)
            ),
        )

    def _train(self, *, config, iters, min_reward, preempt_freq):
        algo = config.build()

        best_return = 0.0
        for i in range(iters):
            results = algo.train()
            print(f"ITER={i} of {iters} results={results}")

            best_return = max(
                best_return, results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
            )
            avg_batch = results[LEARNER_RESULTS][DEFAULT_MODULE_ID][
                "module_train_batch_size_mean"
            ]
            self.assertGreaterEqual(avg_batch, config.total_train_batch_size)
            self.assertLess(
                avg_batch,
                config.total_train_batch_size + config.get_rollout_fragment_length(),
            )

            self.assertEqual(algo.env_runner_group.num_remote_env_runners(), 6)
            healthy_env_runners = algo.env_runner_group.num_healthy_remote_workers()
            # After node has been removed and we recreate failed EnvRunners, we'd expect
            # 2 EnvRunners to be gone.
            # If we ignore EnvRunner failures, and both nodes have been shut down at
            # least once, we might even only see 2 EnvRunners left (the ones on the head
            # node, which are always safe from preemption).
            if (i - 1) % preempt_freq == 0:
                if config.recreate_failed_env_runners:
                    self.assertEqual(healthy_env_runners, 4)
                elif config.ignore_env_runner_failures:
                    self.assertIn(healthy_env_runners, [2, 4])
            # After the 0th iteration, in which we already killed one node, if
            # we don't recreate, the number of EnvRunners should be 2 (only head
            # EnvRunners left) or 4 (one node down).
            elif i > 0 and not config.recreate_failed_env_runners:
                self.assertIn(healthy_env_runners, [2, 4])
            # Otherwise, all EnvRunners should be there (but might still be in the
            # process of coming up).
            else:
                self.assertIn(healthy_env_runners, [4, 5, 6])

            # Shut down one node every n iterations.
            if i % preempt_freq == 0:
                to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
                print(f"Killing node {to_kill} ...")
                self.cluster.remove_node(to_kill)

            # Bring back a previously failed node.
            elif (i - 1) % preempt_freq == 0:
                print("Bringing back node ...")
                self.cluster.add_node(
                    redis_port=None,
                    num_redis_shards=None,
                    num_cpus=2,
                    num_gpus=0,
                    object_store_memory=object_store_memory,
                    redis_max_memory=redis_max_memory,
                    dashboard_host="0.0.0.0",
                )

        self.assertGreaterEqual(best_return, min_reward)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
