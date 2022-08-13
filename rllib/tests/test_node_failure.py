# This workload tests RLlib's ability to recover from failing workers nodes
import threading
import time
import unittest

import ray
from ray._private.test_utils import get_other_nodes
from ray.cluster_utils import Cluster
from ray.exceptions import RayActorError
from ray.rllib.algorithms.ppo import PPO, PPOConfig

num_redis_shards = 5
redis_max_memory = 10 ** 8
object_store_memory = 10 ** 8
num_nodes = 3

assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)


class NodeFailureTests(unittest.TestCase):
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

    def test_fail_on_node_failure(self):
        # We do not tolerate failing workers and stop training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                ignore_worker_failures=False,
                recreate_failed_workers=False,
            )
            .training()
        )
        ppo = PPO(config=config, env="CartPole-v0")

        # One step with all nodes up, enough to satisfy resource requirements
        ppo.step()

        self.assertEqual(len(ppo.workers._remote_workers), 6)

        # Remove the first non-head node.
        node_to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
        self.cluster.remove_node(node_to_kill)

        # Check faulty worker indices
        # All nodes have 2 CPUs and we require a driver thread + 5 rollout workers
        self.assertEqual(len(ppo.workers._worker_health_check()), 2)
        self.assertEqual(len(ppo.workers._remote_workers), 6)

        # Fail with a node down, resource requirements not satisfied anymore
        with self.assertRaises(RayActorError):
            ppo.step()

    def test_continue_training_on_failure(self):
        # We tolerate failing workers and don't pause training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                ignore_worker_failures=True,
                recreate_failed_workers=False,
            )
            .training()
        )
        ppo = PPO(config=config, env="CartPole-v0")

        # One step with all nodes up, enough to satisfy resource requirements
        ppo.step()

        self.assertEqual(len(ppo.workers._remote_workers), 6)

        # Remove the first non-head node.
        node_to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
        self.cluster.remove_node(node_to_kill)

        # Check faulty worker indices
        # All nodes have 2 CPUs and we require a driver thread + 5 rollout workers
        self.assertEqual(len(ppo.workers._worker_health_check()), 2)
        self.assertEqual(len(ppo.workers._remote_workers), 6)

        # One step with a node down, resource requirements not satisfied anymore
        ppo.step()

        # Training should have proceeded without errors, but two workers missing
        self.assertEqual(len(ppo.workers._worker_health_check()), 0)
        self.assertEqual(len(ppo.workers._remote_workers), 4)

    def test_recreate_workers_on_next_iter(self):
        # We tolerate failing workers and pause training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                recreate_failed_workers=True,
                validate_workers_after_construction=True,
            )
            .training()
        )
        ppo = PPO(config=config, env="CartPole-v0")

        # One step with all nodes up, enough to satisfy resource requirements
        ppo.step()

        self.assertEqual(len(ppo.workers._worker_health_check()), 0)
        self.assertEqual(len(ppo.workers._remote_workers), 6)

        # Remove the first non-head node.
        node_to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
        self.cluster.remove_node(node_to_kill)

        assert len(ppo.workers._worker_health_check()) == 2
        self.assertEqual(len(ppo.workers._remote_workers), 6)

        # node comes back immediately.
        self.cluster.add_node(
            redis_port=None,
            num_redis_shards=None,
            num_cpus=2,
            num_gpus=0,
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            dashboard_host="0.0.0.0",
        )

        # step() should restore the missing workers on the added node.
        ppo.step()

        # Workers should be back up, everything back to normal.
        self.assertEqual(len(ppo.workers._worker_health_check()), 0)
        self.assertEqual(len(ppo.workers._remote_workers), 6)

    def test_wait_for_nodes_on_failure(self):
        # We tolerate failing workers and pause training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                ignore_worker_failures=False,
                recreate_failed_workers=True,
            )
            .training()
        )
        ppo = PPO(config=config, env="CartPole-v0")

        # One step with all nodes up, enough to satisfy resource requirements
        ppo.step()

        # Remove the first non-head node.
        node_to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
        self.cluster.remove_node(node_to_kill)

        # Check faulty worker indices
        # All nodes have 2 CPUs and we require a driver thread + 5 rollout workers
        self.assertEqual(len(ppo.workers._worker_health_check()), 2)
        self.assertEqual(len(ppo.workers._remote_workers), 6)

        def _step_target():
            # Resource requirements satisfied after approx 30s
            ppo.step()

        time_before_step = time.time()

        # kill one node after n seconds
        t = threading.Thread(target=_step_target)
        t.start()

        # Wait 30 seconds until the missing node reappears
        time.sleep(30)
        self.cluster.add_node(
            redis_port=None,
            num_redis_shards=None,
            num_cpus=2,
            num_gpus=0,
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            dashboard_host="0.0.0.0",
        )

        t.join()

        td = time.time() - time_before_step

        # TODO: Find out what values make sense here
        self.assertGreaterEqual(td, 30, msg="Stepped before node was added.")
        self.assertLess(td, 60, msg="Took too long to step after node was added.")

        # Workers should be back up
        self.assertEqual(len(ppo.workers._worker_health_check()), 0)
        self.assertEqual(len(ppo.workers._remote_workers), 6)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
