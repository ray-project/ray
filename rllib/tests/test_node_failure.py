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

    def test_continue_training_on_env_runner_node_failures(self):
        # We tolerate failing workers and pause training.
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
                recreate_failed_env_runners=True,
            )
        )
        algo = config.build()

        best_return = 0.0
        for i in range(40):
            results = algo.train()
            print(f"ITER={i} results={results}")

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

            self.assertEqual(algo.env_runner_group.num_remote_workers(), 6)
            healthy_env_runners = algo.env_runner_group.num_healthy_remote_workers()
            # After node has been removed, we expect 2 workers to be gone.
            if (i - 1) % 5 == 0:
                self.assertEqual(healthy_env_runners, 4)
            # Otherwise, all workers should be there (but might still be in the process
            # of coming up).
            else:
                self.assertIn(healthy_env_runners, [4, 5, 6])

            # print(f"healthy workers = {algo.env_runner_group.healthy_worker_ids()}")
            # Shut down one node every n iterations.
            if i % 5 == 0:
                to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
                print(f"Killing node {to_kill} ...")
                self.cluster.remove_node(to_kill)

            # Bring back a previously failed node.
            elif (i - 1) % 5 == 0:
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

        self.assertGreaterEqual(best_return, 450.0)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
