# This workload tests RLlib's ability to recover from failing workers nodes
import time
import unittest

import ray
from ray._private.test_utils import get_other_nodes
from ray.cluster_utils import Cluster
from ray.experimental.state.api import list_actors
from ray.rllib.algorithms.ppo import PPO, PPOConfig


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

    def test_continue_training_on_failure(self):
        # We tolerate failing workers and pause training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                recreate_failed_workers=True,
                validate_workers_after_construction=True,
            )
            .training(
                train_batch_size=300,
            )
        )
        ppo = PPO(config=config, env="CartPole-v1")

        # One step with all nodes up, enough to satisfy resource requirements
        ppo.step()

        self.assertEqual(ppo.workers.num_healthy_remote_workers(), 6)
        self.assertEqual(ppo.workers.num_remote_workers(), 6)

        # Remove the first non-head node.
        node_to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
        self.cluster.remove_node(node_to_kill)

        # step() should continue with 4 rollout workers.
        ppo.step()

        self.assertEqual(ppo.workers.num_healthy_remote_workers(), 4)
        self.assertEqual(ppo.workers.num_remote_workers(), 6)

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

        # Now, let's wait for Ray to restart all the RolloutWorker actors.
        while True:
            states = [
                a["state"] == "ALIVE"
                for a in list_actors()
                if a["class_name"] == "RolloutWorker"
            ]
            if all(states):
                break
            # Otherwise, wait a bit.
            time.sleep(1)

        # This step should continue with 4 workers, but by the end
        # of weight syncing, the 2 recovered rollout workers should
        # be back.
        ppo.step()

        # Workers should be back up, everything back to normal.
        self.assertEqual(ppo.workers.num_healthy_remote_workers(), 6)
        self.assertEqual(ppo.workers.num_remote_workers(), 6)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
