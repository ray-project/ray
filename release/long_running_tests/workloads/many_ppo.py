# This workload tests running many instances of PPO (many actors)
# This covers https://github.com/ray-project/ray/pull/12148

import ray
from ray.cluster_utils import Cluster
from ray.tune import run_experiments

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**9
num_nodes = 3

message = ("Make sure there is enough memory on this machine to run this "
           "workload. We divide the system memory by 2 to provide a buffer.")
assert (num_nodes * object_store_memory + num_redis_shards * redis_max_memory <
        ray.utils.get_system_memory() / 2), message

# Simulate a cluster on one machine.

cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=20,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        dashboard_host="0.0.0.0")
ray.init(address=cluster.address)

# Run the workload.

run_experiments({
    "ppo": {
        "run": "PPO",
        "env": "CartPole-v0",
        "num_samples": 10000,
        "config": {
            "num_workers": 8,
            "num_gpus": 0,
            "num_sgd_iter": 1,
        },
        "stop": {
            "timesteps_total": 1,
        },
    }
})
