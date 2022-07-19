# This workload tests RLlib's ability to recover from failing workers nodes
import time
import unittest
import threading

import ray
from ray.cluster_utils import Cluster
from ray._private.test_utils import get_other_nodes

from ray.rllib.algorithms.ppo import PPO, PPOConfig

num_redis_shards = 5
redis_max_memory = 10 ** 8
object_store_memory = 10 ** 8
num_nodes = 3

message = (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)
assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), message


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
        ray.init(address=self.cluster.address)

    def tearDown(self):
        ray.shutdown()

    def test_continue_training_on_failure(self):
        # We tolerate failing workers and don't stop training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                ignore_worker_failures=True,
                num_failing_workers_tolerance=2,
            )
            .training()
        )
        ppo = PPO(config=config, env="CartPole-v0")

        # One step with all nodes up, enough to satisfy resource requirements
        ppo.step()

        assert len(ppo.workers._remote_workers) == 6

        # Remove the first non-head node.
        node_to_kill = get_other_nodes(self.cluster, exclude_head=True)[0]
        self.cluster.remove_node(node_to_kill)

        # Check faulty worker indices
        # All nodes have 2 CPUs and we require a driver thread + 5 rollout workers
        assert len(ppo.workers._worker_health_check()) == 2
        assert len(ppo.workers._remote_workers) == 6

        # One step with a node down, resource requirements not satisfied anymore
        ppo.step()

        # Training should have proceeded without errors, but two workers missing
        assert len(ppo.workers._worker_health_check()) == 0
        assert len(ppo.workers._remote_workers) == 4

    def test_continue_training_on_failure_recreate_workers(self):
        # We tolerate failing workers and don't stop training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                ignore_worker_failures=True,
                num_failing_workers_tolerance=3,
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
        assert len(ppo.workers._worker_health_check()) == 2
        assert len(ppo.workers._remote_workers) == 6

        # One step with a node down, resource requirements not satisfied anymore
        ppo.step()

        # Training should have been continued with only 4 workers
        assert len(ppo.workers._worker_health_check()) == 0
        assert len(ppo.workers._remote_workers) == 4

        self.cluster.add_node(
            redis_port=None,
            num_redis_shards=None,
            num_cpus=1,
            num_gpus=0,
            object_store_memory=object_store_memory,
            redis_max_memory=redis_max_memory,
            dashboard_host="0.0.0.0",
        )

        # Resource requirements satisfied again
        ppo.step()

        # Workers should be back up
        assert len(ppo.workers._worker_health_check()) == 0
        assert len(ppo.workers._remote_workers) == 6

    def test_wait_for_nodes_on_failure(self):
        # We tolerate failing workers and don't stop training
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=6,
                ignore_worker_failures=True,
                num_failing_workers_tolerance=0,
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
        assert len(ppo.workers._worker_health_check()) == 2
        assert len(ppo.workers._remote_workers) == 6

        def _add_node_after_n_s():
            time.sleep(60)
            self.cluster.add_node()

        # kill one node after n seconds
        t = threading.Thread(target=_add_node_after_n_s)
        t.start()

        previous_time = time.time()

        # Resource requirements satisfied after approx 1 min
        ppo.step()
        td = time.time() - previous_time

        assert 60 < td < 90, (
            "Node came back up after 60 seconds, but algorithm did "
            "not wait that long and ended up finishing an iteration "
            "in {} seconds.".format(td)
        )  # TODO: Find out what values make sense here

        # Workers should be back up
        assert len(ppo.workers._worker_health_check()) == 0
        assert len(ppo.workers._remote_workers) == 6


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
