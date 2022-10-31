# This workload tests running IMPALA with remote envs

import ray
from ray.tune import run_experiments
import os

from ray.tune.utils.release_test_util import ProgressCallback

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 1

message = (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)
assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), message

# Simulate a cluster on one machine.

# cluster = Cluster()
# for i in range(num_nodes):
#     cluster.add_node(
#         redis_port=6379 if i == 0 else None,
#         num_redis_shards=num_redis_shards if i == 0 else None,
#         num_cpus=10,
#         num_gpus=0,
#         resources={str(i): 2},
#         object_store_memory=object_store_memory,
#         redis_max_memory=redis_max_memory,
#         dashboard_host="0.0.0.0")
# ray.init(address=cluster.address)

if "RAY_ADDRESS" in os.environ:
    del os.environ["RAY_ADDRESS"]

ray.init()
# Run the workload.

# Whitespace diff to test things.
run_experiments(
    {
        "impala": {
            "run": "IMPALA",
            "env": "CartPole-v1",
            "config": {
                "num_workers": 8,
                "num_gpus": 0,
                "num_envs_per_worker": 5,
                "remote_worker_envs": True,
                "remote_env_batch_wait_ms": 99999999,
                "rollout_fragment_length": 50,
                "train_batch_size": 100,
            },
        },
    },
    callbacks=[ProgressCallback()],
)
