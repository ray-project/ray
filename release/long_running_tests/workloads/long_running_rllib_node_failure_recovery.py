# This workload tests RLlib's ability to recover from failing workers nodes
import json
import os
import pytest
import sys
import time

import ray
from ray.cluster_utils import Cluster
from ray._private.test_utils import get_other_nodes

from ray.rllib.algorithms.ppo import PPO, PPOConfig


def update_progress(result):
    result["last_update"] = time.time()
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/release_test_output.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


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

# Simulate a cluster on one machine.

cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=1,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        dashboard_host="0.0.0.0",
    )
ray.init(address=cluster.address)

# Run the workload.

iteration = 0
start_time = time.time()
previous_time = start_time

# We tolerate failing workers and don't stop training
config = (
    PPOConfig()
    .rollouts(
        num_rollout_workers=2,
        ignore_worker_failures=True,
        num_failing_workers_tolerance=3,
    )
    .training()
)
ppo = PPO(config=config, env="CartPole-v0")

# One step with all nodes up, enough to satisfy resource requirements
ppo.step()
iteration += 1
new_time = time.time()
update_progress(
    {
        "iteration": iteration,
        "iteration_time": new_time - previous_time,
        "elapsed_time": new_time - start_time,
    }
)

# Remove the first non-head node.
node_to_kill = get_other_nodes(cluster, exclude_head=True)[0]
cluster.remove_node(node_to_kill)

# One step with a node down, resource requirements not satisfied anymore
ppo.step()
iteration += 1
new_time = time.time()
update_progress(
    {
        "iteration": iteration,
        "iteration_time": new_time - previous_time,
        "elapsed_time": new_time - start_time,
    }
)

cluster.add_node(
    redis_port=6379 if i == 0 else None,
    num_redis_shards=num_redis_shards if i == 0 else None,
    num_cpus=1,
    num_gpus=0,
    resources={str(i): 2},
    object_store_memory=object_store_memory,
    redis_max_memory=redis_max_memory,
    dashboard_host="0.0.0.0",
)

# Resource requirements satisfied again
ppo.step()
iteration += 1
new_time = time.time()
update_progress(
    {
        "iteration": iteration,
        "iteration_time": new_time - previous_time,
        "elapsed_time": new_time - start_time,
    }
)

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
