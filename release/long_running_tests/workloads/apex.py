# This workload tests running APEX

import ray
from ray.tune import run_experiments
from ray.tune.utils.release_test_util import ProgressCallback

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**9
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

# cluster = Cluster()
# for i in range(num_nodes):
#     cluster.add_node(redis_port=6379 if i == 0 else None,
#                      num_redis_shards=num_redis_shards if i == 0 else None,
#                      num_cpus=20,
#                      num_gpus=0,
#                      resources={str(i): 2},
#                      object_store_memory=object_store_memory,
#                      redis_max_memory=redis_max_memory,
#                      dashboard_host="0.0.0.0")
# ray.init(address=cluster.address)
ray.init()

# Run the workload.

run_experiments(
    {
        "apex": {
            "run": "APEX",
            "env": "Pong-v0",
            "config": {
                "num_workers": 3,
                "num_gpus": 0,
                "replay_buffer_config": {
                    "capacity": 10000,
                },
                "num_steps_sampled_before_learning_starts": 0,
                "rollout_fragment_length": "auto",
                "train_batch_size": 1,
                "min_time_s_per_iteration": 10,
                "min_sample_timesteps_per_iteration": 10,
            },
        }
    },
    callbacks=[ProgressCallback()],
)
